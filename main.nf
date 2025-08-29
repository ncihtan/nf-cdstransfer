#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PARAMETERS / INPUT RESOLUTION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { validateParameters; paramsSummaryLog; samplesheetToList } from 'plugin/nf-schema'

// Resolve samplesheet path (URL, absolute, or repo-relative "samplesheet.csv")
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

// Validate & print summary (nf-schema plugin)
validateParameters()
log.info paramsSummaryLog(workflow)

// Build channel of meta rows from the samplesheet according to schema
Channel
  .fromList( samplesheetToList(RESOLVED_INPUT, "assets/schema_input.json") )
  .set { ch_input }

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    HELPERS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Prefer these keys for Synapse ID; if a List row, grab the first "syn####"
def eidOf = { m ->
  if (m instanceof Map)  return m.entityId ?: m.entityid ?: m.entity_id
  if (m instanceof List) return (m.find { it instanceof CharSequence && (it ==~ /syn\d+/) } ?: 'unknown_syn')
  'unknown_syn'
}

// Get first existing non-empty field from a Map, else default
def getf = { m, List<String> keys, def defval = '' ->
  (m instanceof Map) ? (keys.findResult { k -> (m.containsKey(k) && m[k] != null && m[k].toString().trim()) ? m[k] : null } ?: defval) : defval
}

// Best-effort guesses if a row is a List (no headers available)
def guessFromList = { List row ->
  def firstMatch = { Closure<Boolean> pred -> row.find { it instanceof CharSequence && pred(it as CharSequence) } ?: '' }
  def isMd5      = { CharSequence s -> s ==~ /(?i)^[a-f0-9]{32}$/ }
  def isNumber   = { CharSequence s -> s ==~ /^\d+$/ }
  def isType     = { CharSequence s -> s in ['BAM','FASTQ','TSV','CSV','CRAM','VCF','TXT'] }
  def looksFile  = { CharSequence s -> s ==~ /.+\.(bam|bai|cram|crai|fastq(\.gz)?|fq(\.gz)?|vcf(\.gz)?|tsv|csv|txt)$/ }
  [
    study_phs_accession : firstMatch { it ==~ /^phs\d{6,}$/ },
    participant_id      : '',
    sample_id           : '',
    file_name           : firstMatch { looksFile(it) },
    file_type           : firstMatch { isType(it) },
    file_description    : '',
    file_size           : firstMatch { isNumber(it) },
    md5sum              : firstMatch { isMd5(it) },
    strategy            : '',
    submission_version  : '',
    checksum_value      : '',
    checksum_algorithm  : 'md5',
    file_mapping_level  : '',
    release_datetime    : '',
    is_supplementary    : ''
  ]
}

/*
================================================================================
 PROCESS: synapse_get  (DUMMY GENERATOR)
 - Does NOT contact Synapse.
 - Creates a dummy file named like meta.file_name (or <entity>.tmp) and sized
   to meta.file_size bytes (or 1 MiB default).
 - Emits: tuple val(meta), path('*')
================================================================================
*/
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

    RAW_NAME="${fname}"
    SAFE_NAME="\$(basename "\$RAW_NAME" | tr -c 'A-Za-z0-9._-' '_' )"

    RAW_SIZE="${fsize:-}"
    RAW_SIZE="\${RAW_SIZE//,/}"
    RAW_SIZE="\${RAW_SIZE// /}"

    # default to 1 MiB if size is missing/non-numeric
    if ! [[ "\$RAW_SIZE" =~ ^[0-9]+$ ]] || [ -z "\$RAW_SIZE" ]; then
      RAW_SIZE=1048576
    fi

    echo "Creating dummy file: \${SAFE_NAME} (\${RAW_SIZE} bytes)"
    # Create sparse file at requested size; then write 1 KiB random so it's not all zeros
    truncate -s "\$RAW_SIZE" "\${SAFE_NAME}"
    dd if=/dev/urandom of="\${SAFE_NAME}" bs=1024 count=1 conv=notrunc >/dev/null 2>&1 || true

    ls -lAh "\${SAFE_NAME}"
    """
}

/*
================================================================================
 PROCESS: make_metadata_tsv
 - Builds CRDC-style TSV strictly from samplesheet values (no file probing).
 - Selects a data file to pass downstream (prefer file_name match, else first file).
 - Emits: tuple val(meta), path("DATAFILE_SELECTED"), path("<eid>_Metadata.tsv")
================================================================================
*/
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

    if (!(meta instanceof Map) && (meta instanceof List)) {
      def g = guessFromList(meta as List)
      study_phs          = study_phs          ?: g.study_phs_accession
      file_name          = file_name          ?: g.file_name
      file_type          = file_type          ?: g.file_type
      file_size          = file_size          ?: g.file_size
      md5sum             = md5sum             ?: g.md5sum
      checksum_algorithm = checksum_algorithm ?: g.checksum_algorithm
    }

    def desired = file_name?.replace(' ', '_') ?: ''

    """
    set -euo pipefail

    # Choose a data file (exclude Nextflow log files)
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
      echo "ERROR: No data file found for ${eid}" >&2
      exit 1
    fi

    # Write the TSV from samplesheet values only
    cat > ${eid}_Metadata.tsv <<'TSV'
type	study.phs_accession	participant.study_participant_id	sample.sample_id	file_name	file_type	file_description	file_size	md5sum	experimental_strategy_and_data_subtypes	submission_version	checksum_value	checksum_algorithm	file_mapping_level	release_datetime	is_supplementary_file
file	${study_phs}	${participant_id}	${sample_id}	${file_name}	${file_type}	${file_description}	${file_size}	${md5sum}	${strategy}	${submission_version}	${checksum_value}	${checksum_algorithm}	${file_mapping_level}	${release_datetime}	${is_supplementary}
TSV

    # Expose the selected file under a stable name
    ln -sf "\$SELECTED" DATAFILE_SELECTED

    ls -lh ${eid}_Metadata.tsv
    """
}

/*
================================================================================
 PROCESS: make_uploader_config
 - Creates per-file uploader YAML referencing Tower secrets at runtime.
 - Passes the selected data file through.
 - Emits: tuple val(meta), path(data_file), path("cli-config-<eid>.yml")
================================================================================
*/
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

    """
    set -euo pipefail

    cat > cli-config-${eid}.yml <<'YAML'
version: 1
submission:
  id: ${'$'}{CRDC_SUBMISSION_ID}
auth:
  token_env: CRDC_API_TOKEN

files:
  - data_file: "${data_file}"
    metadata_file: "${metadata_tsv}"
    ${ data_format ? "data_format: \"${data_format}\"" : "" }
    overwrite: ${overwrite}
    dry_run: ${dry_run}
YAML

    echo "Wrote cli-config-${eid}.yml"
    """
}

/*
================================================================================
 PROCESS: crdc_upload
 - Uses Tower secrets CRDC_API_TOKEN / CRDC_SUBMISSION_ID.
 - Real uploader is commented; this stub just logs. Enable real upload later.
 - Emits: tuple val(meta), path("upload.log")
================================================================================
*/
process crdc_upload {

    container 'python:3.11-slim'
    tag "${ eidOf(meta) }"

    input:
    tuple val(meta), path(data_file), path(config_yml)

    secret 'CRDC_API_TOKEN'
    secret 'CRDC_SUBMISSION_ID'

    output:
    tuple val(meta), path("upload.log")

    /*
    // --- Real uploader example (uncomment to enable) ---
    script:
    def uploader = (task.ext.uploader_cmd ?: 'crdc-uploader').toString()
    """
    set -euo pipefail
    python -m pip install --no-cache-dir --upgrade pip >/dev/null
    python -m pip install --no-cache-dir "git+https://github.com/CBIIT/crdc-datahub-cli-uploader.git" >/dev/null
    export CRDC_API_TOKEN="\$CRDC_API_TOKEN"
    export CRDC_SUBMISSION_ID="\$CRDC_SUBMISSION_ID"
    set -x
    crdc-uploader upload --config "${config_yml}" 2>&1 | tee upload.log
    set +x
    """
    */

    // --- Safe stub (default) ---
    script:
    def eid = eidOf(meta)
    """
    set -euo pipefail
    echo "Stub upload for ${eid}" | tee upload.log
    """
}

/*
================================================================================
 WORKFLOW
================================================================================
*/
workflow {
  ch_dl   = ch_input          | synapse_get
  ch_meta = ch_dl             | make_metadata_tsv
  ch_cfg  = ch_meta           | make_uploader_config
  ch_up   = ch_cfg            | crdc_upload

  // Visibility in the main log
  ch_meta.view { meta, datafile, tsv -> "METADATA:\t${eidOf(meta)}\t${tsv}" }
  ch_cfg.view  { meta, datafile, yml  -> "CONFIG:\t${eidOf(meta)}\t${yml}" }
  ch_up.view   { meta, log            -> "UPLOAD:\t${eidOf(meta)}\t${log}" }
}
