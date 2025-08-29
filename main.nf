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
   HELPERS (robust, header-insensitive)
   ========================================================================== */
// Normalize a key for lookups: lower-case, spaces/dots/hyphens -> underscore
normKey = { k -> k.toString().toLowerCase().replaceAll(/[\\s.\\-]/, '_') }

// Case/format-insensitive getter: tries several candidate keys
getcx = { meta, List candidates, defval = '' ->
  if (!(meta instanceof Map)) return defval
  def idx = [:]
  meta.keySet().each { k ->
    idx[k.toString()]                = k
    idx[k.toString().toLowerCase()]  = k
    idx[normKey(k)]                  = k
  }
  for (c in candidates) {
    def probes = [ c.toString(), c.toString().toLowerCase(), normKey(c) ]
    for (p in probes) {
      if (idx.containsKey(p)) {
        def v = meta[idx[p]]
        if (v != null && v.toString().trim()) return v.toString()
      }
    }
  }
  return defval
}

// Preferred Synapse id from common header variants or list row
eidOf = { meta ->
  if (meta instanceof Map) {
    return getcx(meta, ['entityid','entity_id','entityId'], 'unknown_syn')
  }
  if (meta instanceof List) {
    def m = meta.find { it instanceof CharSequence && (it ==~ /syn\\d+/) }
    return m ?: 'unknown_syn'
  }
  'unknown_syn'
}

/* =============================================================================
   PROCESS: synapse_get  (DUMMY GENERATOR)
   - Creates a file named exactly like samplesheet file_name (spaces -> underscores),
     and of size file_size (bytes; default 1 MiB).
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
  // Resolve fields in Groovy (no bash regex/subshells)
  def eid         = eidOf(meta)
  def rawName     = getcx(meta, ['file_name','filename','file.name','file name'], "${eid}.tmp")
  def safeName    = new File(rawName).getName().replace(' ', '_')   // preserve extension; only spaces->_
  def rawSizeStr  = getcx(meta, ['file_size','filesize','file.size','size','file size'], '').replaceAll(/[ ,]/,'')
  def size        = (rawSizeStr ==~ /^\\d+$/) ? rawSizeStr : '1048576'  // 1 MiB default
  def metaKeys    = (meta instanceof Map) ? meta.keySet().join(', ') : meta.toString()

  """
  #!/usr/bin/env bash
  set -euo pipefail

  echo "META_KEYS: ${metaKeys}"
  echo "RESOLVED entity: ${eid}"
  echo "RESOLVED file_name -> ${rawName}"
  echo "RESOLVED file_size -> ${size}"

  echo "Creating dummy file: ${safeName} (${size} bytes)"
  truncate -s ${size} "${safeName}" || dd if=/dev/zero of="${safeName}" bs=1 count=${size} >/dev/null 2>&1 || true
  dd if=/dev/urandom of="${safeName}" bs=1024 count=1 conv=notrunc >/dev/null 2>&1 || true

  ls -lAh "${safeName}"
  """
}

/* =============================================================================
   PROCESS: make_metadata_tsv
   - TSV from samplesheet values only (no probing)
   - Picks data file (prefer exact file_name rule from above; else first regular file)
   - Emits: tuple val(meta), path("DATAFILE_SELECTED"), path("<eid>_Metadata.tsv")
   ========================================================================== */
process make_metadata_tsv {

  tag "${ eidOf(meta) }"

  input:
  tuple val(meta), path(downloaded_files)

  output:
  tuple val(meta), path("DATAFILE_SELECTED"), path("*_Metadata.tsv")

  script:
  // Resolve all fields using getcx to be robust to header variants
  def eid               = eidOf(meta)
  def study_phs         = getcx(meta, ['study.phs_accession','study_phs_accession'], 'phs002371')
  def participant_id    = getcx(meta, ['participant.study_participant_id','study_participant_id'], '')
  def sample_id         = getcx(meta, ['sample.sample_id','sample_id','HTAN_Assayed_Biospecimen_ID'], '')
  def file_name         = getcx(meta, ['file_name','filename','file.name','file name'], '')
  def file_type         = getcx(meta, ['file_type'], '')
  def file_description  = getcx(meta, ['file_description'], '')
  def file_size         = getcx(meta, ['file_size','filesize','file.size','size','file size'], '')
  def md5sum            = getcx(meta, ['md5sum','MD5','checksum_md5'], '')
  def strategy          = getcx(meta, ['experimental_strategy_and_data_subtypes','experimental_strategy'], '')
  def submission_version= getcx(meta, ['submission_version'], '')
  def checksum_value    = getcx(meta, ['checksum_value'], '')
  def checksum_algorithm= getcx(meta, ['checksum_algorithm'], 'md5')
  def file_mapping_level= getcx(meta, ['file_mapping_level'], '')
  def release_datetime  = getcx(meta, ['release_datetime'], '')
  def is_supplementary  = getcx(meta, ['is_supplementary_file'], '')

  // Match the same naming rule used in synapse_get (spaces -> underscores)
  def desiredSafe = (file_name ?: '').replace(' ', '_')

  """
  #!/usr/bin/env bash
  set -euo pipefail

  SELECTED="${desiredSafe}"
  if [ -z "${desiredSafe}" ] || [ ! -f "${desiredSafe}" ]; then
    SELECTED=""
    for f in *; do
      if [ -f "$f" ]; then
        case "$f" in
          .command*) ;;        # skip NF internals
          *) SELECTED="$f"; break ;;
        esac
      fi
    done
  fi

  if [ -z "$SELECTED" ] || [ ! -f "$SELECTED" ]; then
    echo "ERROR: No data file found for ${eid}" >&2
    exit 1
  fi

  cat > ${eid}_Metadata.tsv <<EOF
type\tstudy.phs_accession\tparticipant.study_participant_id\tsample.sample_id\tfile_name\tfile_type\tfile_description\tfile_size\tmd5sum\texperimental_strategy_and_data_subtypes\tsubmission_version\tchecksum_value\tchecksum_algorithm\tfile_mapping_level\trelease_datetime\tis_supplementary_file
file\t${study_phs}\t${participant_id}\t${sample_id}\t${file_name}\t${file_type}\t${file_description}\t${file_size}\t${md5sum}\t${strategy}\t${submission_version}\t${checksum_value}\t${checksum_algorithm}\t${file_mapping_level}\t${release_datetime}\t${is_supplementary}
EOF

  ln -sf "$SELECTED" DATAFILE_SELECTED
  ls -lh ${eid}_Metadata.tsv
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
  def data_format = getcx(meta, ['file_type'], '')
  def overwrite   = (params.overwrite != null ? params.overwrite : true)
  def dry_run     = (params.dry_run  != null ? params.dry_run  : false)
  def dataFormatLine = data_format ? "    data_format: \\\"${data_format}\\\"\\n" : ""

  """
  #!/usr/bin/env bash
  set -euo pipefail

  cat > cli-config-${eid}.yml <<EOF
version: 1
submission:
  id: \${CRDC_SUBMISSION_ID}
auth:
  token_env: CRDC_API_TOKEN

files:
  - data_file: "${data_file}"
    metadata_file: "${metadata_tsv}"
${dataFormatLine}    overwrite: ${overwrite}
    dry_run: ${dry_run}
EOF

  echo "Wrote cli-config-${eid}.yml"
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
    if command -v "$c" >/dev/null 2>&1; then uploader_cmd="$c"; break; fi
  done
  if [ -z "$uploader_cmd" ]; then
    echo "ERROR: CRDC uploader CLI not found after installation." >&2
    exit 1
  fi
  echo "Using uploader: $uploader_cmd"
  $uploader_cmd --version || true

  export CRDC_API_TOKEN="$CRDC_API_TOKEN"
  export CRDC_SUBMISSION_ID="$CRDC_SUBMISSION_ID"

  set -x
  $uploader_cmd upload --config "${config_yml}" 2>&1 | tee upload.log
  set +x

  # Non-fatal success heuristic
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
}
