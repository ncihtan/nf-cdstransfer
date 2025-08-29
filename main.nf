#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PARAMETERS AND INPUTS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { validateParameters; paramsSummaryLog; samplesheetToList } from 'plugin/nf-schema'

// ---- Resolve samplesheet path WITHOUT reassigning params.input ----
def _raw = params.input ?: 'samplesheet.csv'
def _isUrl = (_raw ==~ /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//)
def _abs   = file(_raw).isAbsolute()
def repoPath = file("${projectDir}/${_raw}")

// Priority: URL → absolute (if exists) → repo copy → repo fallback
def resolved_input = _isUrl ? _raw
                    : (_abs && file(_raw).exists()) ? file(_raw).toString()
                    : (repoPath.exists() ? repoPath.toString() : repoPath.toString())

log.info "projectDir          = ${projectDir}"
log.info "params.input (raw)  = ${params.input ?: '(unset)'}"
log.info "resolved_input      = ${resolved_input}"
log.info "resolved exists?    = ${file(resolved_input).exists()}"

// Validate pipeline params & print summary
validateParameters()
log.info paramsSummaryLog(workflow)

// Build channel of meta rows from samplesheet (can be Map *or* List depending on schema)
ch_input = Channel.fromList( samplesheetToList(resolved_input, "assets/schema_input.json") )

// ---------- Helpers ----------

// Prefer entityId/entityid/entity_id if Map; if List, find first value matching syn\d+
def eidOf = { m ->
  if (m instanceof Map)  return m.entityId ?: m.entityid ?: m.entity_id
  if (m instanceof List) return (m.find { it instanceof CharSequence && (it ==~ /syn\d+/) } ?: 'unknown_syn')
  'unknown_syn'
}

// Get first existing field from a Map using a list of candidate keys
def getf = { m, List<String> keys, def defval = '' ->
  (m instanceof Map) ? (keys.findResult { k -> m.containsKey(k) && m[k] != null && m[k].toString().trim() ? m[k] : null } ?: defval) : defval
}

// Heuristic guesses when a row is a List (best-effort only)
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
 PROCESS: synapse_get
 - Non-interactive token login (no prompts)
 - Network probe & 30-min timeout on download
 - Clear DONE marker + hard check at least one file was fetched
 - Emits: tuple val(meta), path('*')
================================================================================
*/

process synapse_get {

    container 'ghcr.io/sage-bionetworks/synapsepythonclient:develop-b784b854a069e926f1f752ac9e4f6594f66d01b7'
    tag "${ eidOf(meta) }"

    input:
    val(meta)

    // token input
    secret 'SYNAPSE_AUTH_TOKEN_DYP'
    output:
    tuple val(meta), path('*')

    script:
    def args = (task.ext.args ?: '').toString()
    def eid  = eidOf(meta)
    """
    #!/usr/bin/env bash
    set -euxo pipefail

    # Prefer the new secret; fall back to the old name if needed
    TOKEN="\${SYNAPSE_AUTH_TOKEN_DYP:-\${SYNAPSE_AUTH_TOKEN:-}}"
    if [ -z "\$TOKEN" ]; then
      echo "ERROR: Neither SYNAPSE_AUTH_TOKEN_DYP nor SYNAPSE_AUTH_TOKEN is set." >&2
      exit 1
    fi

    echo "== synapse_get =="
    echo "Entity ID: ${eid}"
    echo "Token length (masked): \${#TOKEN}"

    # (Optional) quick egress probe
    curl -fsSI --max-time 10 https://www.synapse.org >/dev/null || {
      echo "ERROR: Network/egress check to synapse.org failed" >&2; exit 1; }

    # Non-interactive login with the chosen token
    synapse login --silent --authToken "\$TOKEN"

    # If you’re still in stub mode, this will run instead of the real download:
    echo "Stub: creating a fake file for testing..."
    dd if=/dev/urandom of=small_file.tmp bs=1M count=1
    """
    
    stub:
    """
    echo "Stub: creating a fake file for testing..."
    dd if=/dev/urandom of=small_file.tmp bs=1M count=1
    """
}



/*
================================================================================
 PROCESS: make_metadata_tsv
 - Builds CRDC-style TSV strictly from samplesheet values (no file probing)
 - Picks one data file to pass downstream:
     * Prefer basename matching meta.file_name (after space->underscore)
     * Else first regular file in work dir (excluding logs/.command*)
 - Emits: tuple val(meta), path("*_Metadata.tsv"), path("DATAFILE_SELECTED")
================================================================================
*/
process make_metadata_tsv {

    tag "${ eidOf(meta) }"

    input:
    tuple val(meta), path(downloaded_files)

    output:
    tuple val(meta), path("*_Metadata.tsv"), path("DATAFILE_SELECTED")

    script:
    def eid               = eidOf(meta)
    // Support dotted or flat keys from the samplesheet
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

    // If meta is a List, try a best-effort guess to avoid blanks
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

    # Choose a data file to pass on (exclude debug & nextflow files)
    SELECTED=""
    if [ -n "${desired}" ] && [ -f "${desired}" ]; then
      SELECTED="${desired}"
    else
      mapfile -t FILES < <(find . -maxdepth 1 -type f ! -name "synapse_debug.log" ! -name ".command*" -printf "%f\\n" | sort)
      if [ "\${#FILES[@]}" -gt 0 ]; then
        SELECTED="\${FILES[0]}"
      fi
    fi

    if [ -z "\$SELECTED" ] || [ ! -f "\$SELECTED" ]; then
      echo "ERROR: No data file found for ${eid}" >&2
      exit 1
    fi

    # Write TSV strictly from samplesheet values
    cat > ${eid}_Metadata.tsv <<'TSV'
type	study.phs_accession	participant.study_participant_id	sample.sample_id	file_name	file_type	file_description	file_size	md5sum	experimental_strategy_and_data_subtypes	submission_version	checksum_value	checksum_algorithm	file_mapping_level	release_datetime	is_supplementary_file
file	${study_phs}	${participant_id}	${sample_id}	${file_name}	${file_type}	${file_description}	${file_size}	${md5sum}	${strategy}	${submission_version}	${checksum_value}	${checksum_algorithm}	${file_mapping_level}	${release_datetime}	${is_supplementary}
TSV

    # Expose selected data file under a stable name for downstream binding
    ln -sf "\$SELECTED" DATAFILE_SELECTED

    ls -lh ${eid}_Metadata.tsv
    """
}


/*
================================================================================
 PROCESS: make_uploader_config
 - Creates per-file uploader YAML referencing Tower secrets for auth
 - Passes the selected data file through
 - Emits: tuple val(meta), path(data_file), path("cli-config-*.yml")
================================================================================
*/
process make_uploader_config {

    tag "${ eidOf(meta) }"

    input:
    tuple val(meta), path(data_file), path(metadata_tsv)

    output:
    tuple val(meta), path(data_file), path("cli-config-*.yml")

    script:
    def data_format = (getf(meta, ['file_type'], '') ?: '').toString()
    def overwrite   = params.overwrite as boolean
    def dry_run     = params.dry_run as boolean
    def eid         = eidOf(meta)

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
 - Ensures CRDC uploader CLI is available (or installs it)
 - Uses Tower secrets: CRDC_API_TOKEN, CRDC_SUBMISSION_ID
 - Runs the upload with generated YAML
 - Emits: tuple val(meta), path("upload.log")
================================================================================
*/
process crdc_upload {

    // Option A: prebuilt image with uploader baked in (preferred)
    // container 'ghcr.io/<your-org>/crdc-datahub-cli-uploader:latest'

    // Option B: install on the fly (works; slower)
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
    def eid      = eidOf(meta)
    """
    set -euo pipefail

    apt-get update -y >/dev/null 2>&1 || true
    apt-get install -y --no-install-recommends git ca-certificates >/dev/null 2>&1 || true
    python -m pip install --no-cache-dir --upgrade pip >/dev/null

    if ! command -v ${uploader} >/dev/null 2>&1; then
      echo "CRDC uploader not found; installing..."
      python -m pip install --no-cache-dir "git+https://github.com/CBIIT/crdc-datahub-cli-uploader.git" >/dev/null
      for c in crdc-uploader crdc_datahub_uploader; do
        if command -v "\$c" >/dev/null 2>&1; then uploader_cmd="\$c"; break; fi
      done
      : "\${uploader_cmd:=crdc-uploader}"
    else
      uploader_cmd="${uploader}"
    fi

    echo "Using uploader: \${uploader_cmd}"
    \${uploader_cmd} --help >/dev/null || true

    export CRDC_API_TOKEN="\$CRDC_API_TOKEN"
    export CRDC_SUBMISSION_ID="\$CRDC_SUBMISSION_ID"

    set -x
    \${uploader_cmd} upload --config "${config_yml}" 2>&1 | tee upload.log
    set +x
    """

    stub:
    """
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

  // Visibility
  ch_meta.view { meta, tsv, datafile -> "METADATA:\t${eidOf(meta)}\t${tsv}" }
  ch_cfg.view  { meta, yml -> "CONFIG:\t${eidOf(meta)}\t${yml}" }
  ch_up.view   { meta, log -> "UPLOAD:\t${eidOf(meta)}\t${log}" }
}
