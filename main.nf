#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

/* =============================================================================
   NF-SCHEMA HOOKS
   ========================================================================== */
include { validateParameters; paramsSummaryLog; samplesheetToList } from 'plugin/nf-schema'

/* =============================================================================
   INPUT RESOLUTION
   ========================================================================== */
def _raw = params.input ?: 'samplesheet.csv'
def _isUrl = (_raw ==~ /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//)
def _abs   = file(_raw).isAbsolute()
def repoPath = file("${projectDir}/${_raw}")
def RESOLVED_INPUT = _isUrl ? _raw
                    : (_abs && file(_raw).exists()) ? file(_raw).toString()
                    : (repoPath.exists() ? repoPath.toString() : repoPath.toString())

log.info "projectDir          = ${projectDir}"
log.info "params.input (raw)  = ${params.input ?: '(unset)'}"
log.info "resolved_input      = ${RESOLVED_INPUT}"
log.info "resolved exists?    = ${file(RESOLVED_INPUT).exists()}"

// Validate & summary
validateParameters()
log.info paramsSummaryLog(workflow)

// Build input channel of meta rows
ch_input = Channel.fromList( samplesheetToList(RESOLVED_INPUT, "assets/schema_input.json") )

/* =============================================================================
   SMALL HELPERS (closures — safer in older NF/Groovy)
   ========================================================================== */
eidOf = { meta ->
  if (meta instanceof Map) {
    return meta.entityId ?: meta.entityid ?: meta.entity_id ?: 'unknown_syn'
  }
  if (meta instanceof List) {
    def m = meta.find { it instanceof CharSequence && (it ==~ /syn\d+/) }
    return m ?: 'unknown_syn'
  }
  'unknown_syn'
}

getf = { meta, keys, defval = '' ->
  if (!(meta instanceof Map)) return defval
  for (k in keys) {
    if (meta.containsKey(k) && meta[k] != null) {
      def v = meta[k].toString().trim()
      if (v) return v
    }
  }
  return defval
}

/* =============================================================================
   PROCESS: synapse_get  (DUMMY GENERATOR)
   - Creates file named like meta.file_name (or <entity>.tmp), size = meta.file_size bytes (default 1 MiB)
   - Emits: tuple val(meta), path('*')
   ========================================================================== */
process synapse_get {

  container 'python:3.11-slim'
  tag "${ eidOf(meta) }"

  input:
  val(meta)

  output:
  tuple val(meta), path('*')

  script:
  def eid   = eidOf(meta)
  def fname = (getf(meta, ['file_name'], "${eid}.tmp") as String)
  def fsize = (getf(meta, ['file_size'], '') as String)
  """
  #!/usr/bin/env bash
  set -euo pipefail

  RAW_NAME="!{fname}"
  SAFE_NAME="\$(basename "\$RAW_NAME" | tr -c 'A-Za-z0-9._-' '_' )"

  RAW_SIZE="!{fsize}"
  RAW_SIZE="\${RAW_SIZE//,/}"
  RAW_SIZE="\${RAW_SIZE// /}"

  # default to 1 MiB if size missing/non-numeric
  if ! [[ "\$RAW_SIZE" =~ ^[0-9]+$ ]] || [ -z "\$RAW_SIZE" ]; then
    RAW_SIZE=1048576
  fi

  echo "Creating dummy file: \${SAFE_NAME} (\${RAW_SIZE} bytes)"
  truncate -s "\$RAW_SIZE" "\${SAFE_NAME}" || dd if=/dev/zero of="\${SAFE_NAME}" bs=1 count="\$RAW_SIZE" >/dev/null 2>&1 || true
  dd if=/dev/urandom of="\${SAFE_NAME}" bs=1024 count=1 conv=notrunc >/dev/null 2>&1 || true

  ls -lAh "\${SAFE_NAME}"
  """
}

/* =============================================================================
   PROCESS: make_metadata_tsv
   - TSV from samplesheet values only (no probing)
   - Picks data file (prefer file_name match; else first regular file)
   - Emits: tuple val(meta), path("DATAFILE_SELECTED"), path("<eid>_Metadata.tsv")
   ========================================================================== */
process make_metadata_tsv {

  tag "${ eidOf(meta) }"

  input:
  tuple val(meta), path(downloaded_files)

  output:
  tuple val(meta), path("DATAFILE_SELECTED"), path("*_Metadata.tsv")

  script:
  def eid               = eidOf(meta)
  def study_phs         = getf(meta, ['study.phs_accession','study_phs_accession'], 'phs002371')
  def participant_id    = getf(meta, ['participant.study_participant_id','study_participant_id'], '')
  def sample_id         = getf(meta, ['sample.sample_id','sample_id','HTAN_Assayed_Biospecimen_ID'], '')
  def file_name         = getf(meta, ['file_name'], '')
  def file_type         = getf(meta, ['file_type'], '')
  def file_description  = getf(meta, ['file_description'], '')
  def file_size         = getf(meta, ['file_size'], '')
  def md5sum            = getf(meta, ['md5sum','MD5','checksum_md5'], '')
  def strategy          = getf(meta, ['experimental_strategy_and_data_subtypes','experimental_strategy'], '')
  def submission_version= getf(meta, ['submission_version'], '')
  def checksum_value    = getf(meta, ['checksum_value'], '')
  def checksum_algorithm= getf(meta, ['checksum_algorithm'], 'md5')
  def file_mapping_level= getf(meta, ['file_mapping_level'], '')
  def release_datetime  = getf(meta, ['release_datetime'], '')
  def is_supplementary  = getf(meta, ['is_supplementary_file'], '')
  def desired           = file_name?.replace(' ', '_') ?: ''
  """
  set -euo pipefail

  # choose data file (exclude Nextflow internals)
  SELECTED=""
  if [ -n "${desired}" ] && [ -f "${desired}" ]; then
    SELECTED="${desired}"
  else
    mapfile -t FILES < <(find . -maxdepth 1 -type f ! -name ".command*" -printf "%f\\n" | sort)
    if [ "\${#FILES[@]}" -gt 0 ]; then
      SELECTED="\${FILES[0]}"
    fi
  fi
  if [ -z "\$SELECTED" ] || [ ! -f "\$SELECTED" ]; then
    echo "ERROR: No data file found for !{eid}" >&2
    exit 1
  fi

  cat > !{eid}_Metadata.tsv <<'TSV'
type	study.phs_accession	participant.study_participant_id	sample.sample_id	file_name	file_type	file_description	file_size	md5sum	experimental_strategy_and_data_subtypes	submission_version	checksum_value	checksum_algorithm	file_mapping_level	release_datetime	is_supplementary_file
file	${study_phs}	${participant_id}	${sample_id}	${file_name}	${file_type}	${file_description}	${file_size}	${md5sum}	${strategy}	${submission_version}	${checksum_value}	${checksum_algorithm}	${file_mapping_level}	${release_datetime}	${is_supplementary}
TSV

  ln -sf "\$SELECTED" DATAFILE_SELECTED
  ls -lh !{eid}_Metadata.tsv
  """
}

/* =============================================================================
   PROCESS: make_uploader_config
   - Emits cli-config-<eid>.yml (references Tower secrets at runtime)
   - Emits: tuple val(meta), path(data_file), path("cli-config-*.yml")
   ========================================================================== */
process make_uploader_config {

  tag "${ eidOf(meta) }"

  input:
  tuple val(meta), path(data_file), path(metadata_tsv)

  output:
  tuple val(meta), path(data_file), path("cli-config-*.yml")

  script:
  def eid         = eidOf(meta)
  def data_format = (getf(meta, ['file_type'], '') ?: '').toString()
  def overwrite   = (params.overwrite != null ? params.overwrite : true)
  def dry_run     = (params.dry_run  != null ? params.dry_run  : false)
  def dataFormatLine = data_format ? "    data_format: \\\"${data_format}\\\"\\n" : ""
  """
  #!/usr/bin/env bash
  set -euo pipefail

  cat > cli-config-!{eid}.yml <<EOF
version: 1
submission:
  id: \${CRDC_SUBMISSION_ID}
auth:
  token_env: CRDC_API_TOKEN

files:
  - data_file: "!{data_file}"
    metadata_file: "!{metadata_tsv}"
!{dataFormatLine}    overwrite: ${overwrite}
    dry_run: ${dry_run}
EOF

  echo "Wrote cli-config-!{eid}.yml"
  """
}

/* =============================================================================
   PROCESS: crdc_upload  (REAL UPLOADER)
   - Installs CRDC uploader CLI if missing and runs it.
   - Requires Seqera secrets: CRDC_API_TOKEN, CRDC_SUBMISSION_ID
   - Emits: tuple val(meta), path("upload.log")
   ========================================================================== */
process crdc_upload {

  container 'python:3.11-slim'
  tag "${ eidOf(meta) }"

  input:
  tuple val(meta), path(data_file), path(config_yml)

  secret 'CRDC_API_TOKEN'
  secret 'CRDC_SUBMISSION_ID'

  output:
  tuple val(meta), path("upload.log")

  script:
  def uploader = (task.ext.uploader_cmd ?: 'crdc-uploader').toString()
  """
  #!/usr/bin/env bash
  set -euo pipefail

  apt-get update -y >/dev/null 2>&1 || true
  apt-get install -y --no-install-recommends git ca-certificates >/dev/null 2>&1 || true
  python -m pip install --no-cache-dir --upgrade pip >/dev/null

  if ! command -v ${uploader} >/dev/null 2>&1; then
    echo "Installing CRDC uploader CLI from GitHub..."
    python -m pip install --no-cache-dir "git+https://github.com/CBIIT/crdc-datahub-cli-uploader.git" >/dev/null
  fi

  uploader_cmd=""
  for c in ${uploader} crdc-uploader crdc_datahub_uploader; do
    if command -v "\$c" >/dev/null 2>&1; then uploader_cmd="\$c"; break; fi
  done
  if [ -z "\$uploader_cmd" ]; then
    echo "ERROR: CRDC uploader CLI not found after installation." >&2
    exit 1
  fi
  echo "Using uploader: \${uploader_cmd}"
  \${uploader_cmd} --version || true

  export CRDC_API_TOKEN="\$CRDC_API_TOKEN"
  export CRDC_SUBMISSION_ID="\$CRDC_SUBMISSION_ID"

  set -x
  \${uploader_cmd} upload --config "!{config_yml}" 2>&1 | tee upload.log
  set +x

  # Consider this a success only if typical markers are present (non-fatal otherwise)
  grep -Ei "success|completed|submitted" upload.log >/dev/null || true
  """
}

/* =============================================================================
   WORKFLOW
   ========================================================================== */
workflow {
  ch_dl   = ch_input          | synapse_get
  ch_meta = ch_dl             | make_metadata_tsv
  ch_cfg  = ch_meta           | make_uploader_config
  ch_up   = ch_cfg            | crdc_upload

  // Optional debug
  // ch_meta.view { meta, datafile, tsv -> "METADATA:\t${eidOf(meta)}\t${tsv}" }
  // ch_cfg.view  { meta, datafile, yml  -> "CONFIG:\t${eidOf(meta)}\t${yml}" }
  // ch_up.view   { meta, log            -> "UPLOAD:\t${eidOf(meta)}\t${log}" }
}

