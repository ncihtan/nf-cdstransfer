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

//  Guesses when a row is a List (best-effort only)
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
 - Hard timeout on download
 - Groups all fetched files into ./out and emits that directory
 - Emits: tuple val(meta), path("out")  (directory)
================================================================================
*/

process synapse_get {

    container 'ghcr.io/sage-bionetworks/synapsepythonclient:develop-b784b854a069e926f1f752ac9e4f6594f66d01b7'
    tag "${ eidOf(meta) ?: 'no-id' }"

    input:
    val(meta)

    secret 'SYNAPSE_AUTH_TOKEN_DYP'

    output:
    tuple val(meta), path('out/*')

    script:
    def args = task.ext.args ?: ''
    def eid  = eidOf(meta)
    if (!eid) throw new IllegalArgumentException("synapse_get: could not resolve entity id from meta=${meta}")

    """
    #!/usr/bin/env bash
    set -euo pipefail

    export PYTHONUNBUFFERED=1
    export PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring
    export HOME="\$PWD"                 # keep synapse config/cache local
    export SYNAPSE_CACHE_DIR="\$PWD/.synapseCache"
    mkdir -p "\$SYNAPSE_CACHE_DIR" out

    echo "== synapse_get =="
    echo "Entity ID: ${eid}"

    synapse --version || true

    # Download
    synapse -p \$SYNAPSE_AUTH_TOKEN_DYP get ${args} ${eid}

    # Normalize: replace spaces in top-level files
    shopt -s nullglob
    for f in *\\ *; do mv "\${f}" "\${f// /_}"; done

    # Move only real payloads to ./out (skip NF internals & logs)
    shopt -s dotglob
    for f in *; do
      [[ "\$f" == "." || "\$f" == ".." ]] && continue
      [[ "\$f" == "out" || "\$f" == ".command"* || "\$f" == "synapse_debug.log" ]] && continue
      if [ -f "\$f" ]; then
        mv "\$f" out/
      fi
    done

    echo "Final listing in ./out:"
    ls -lAh out || true

    """

    stub:
    """
    echo "Making a fake file for testing..."
    ## Make a random file of 1MB and save to small_file.tmp
    dd if=/dev/urandom of=small_file.tmp bs=1M count=1
    """
}

/*
================================================================================
 PROCESS: make_metadata_tsv
 - Builds CRDC-style TSV strictly from samplesheet values (no file probing)
 - Picks one data file from ./out to pass downstream:
     * Prefer basename matching meta.file_name (after space->underscore)
     * Else first regular file under ./out
 - Emits: tuple val(meta), path("DATAFILE_SELECTED"), path("*_Metadata.tsv")
   (note: order matches next process input)
================================================================================
*/
process make_metadata_tsv {

    tag "${ eidOf(meta) }"

    input:
    tuple val(meta), path(downloaded_dir)

    output:
    tuple val(meta), path("DATAFILE_SELECTED"), path("*_Metadata.tsv")

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

    # Work under the staged download directory
    D="${downloaded_dir}"
    [ -d "$D" ] || { echo "Missing download dir: $D" >&2; exit 1; }
    
    # Show what we actually have (2 levels, first 50 files)
    echo "Downloaded tree (up to 2 levels):"
    find "$D" -maxdepth 2 -type f -printf "%p\t%k KB\n" | head -n 50 || true
    
    # Prefer an exact basename match to meta.file_name (spaces -> underscores)
    desired="$(printf "%s" "${file_name}" | tr ' ' '_')"
    desired_base="$(basename -- "$desired")"
    
    SELECTED=""
    
    if [ -n "$desired_base" ]; then
      # Find files anywhere under D whose BASENAME equals desired_base (case-sensitive)
      # Use -printf "%f\t%p" to compare basenames reliably
      mapfile -t MATCHES < <(find "$D" -type f -printf "%f\t%p\n" \
        | awk -F'\t' -v d="$desired_base" '$1==d {print $2}' \
        | sort)
      if [ "${#MATCHES[@]}" -gt 0 ]; then
        SELECTED="${MATCHES[0]}"
      fi
    fi
    
    # Fallback: first plausible data file anywhere under D
    if [ -z "$SELECTED" ]; then
      # Adjust this glob as needed; keep logs/aux out
      mapfile -t FILES < <(find "$D" -type f \
        ! -name "synapse_debug.log" \
        ! -name ".command*" \
        ! -name "cli-config-*.yml" \
        ! -name "*_Metadata.tsv" \
        \( -iname "*.bam" -o -iname "*.bai" -o -iname "*.cram" -o -iname "*.crai" \
           -o -iname "*.fastq" -o -iname "*.fq" -o -iname "*.fastq.gz" -o -iname "*.fq.gz" \
           -o -iname "*.vcf" -o -iname "*.vcf.gz" -o -iname "*.tsv" -o -iname "*.csv" -o -iname "*.txt" \) \
        -printf "%p\n" | sort)
      if [ "${#FILES[@]}" -gt 0 ]; then
        SELECTED="${FILES[0]}"
      fi
    fi
    
    if [ -z "$SELECTED" ] || [ ! -f "$SELECTED" ]; then
      echo "ERROR: No data file found under ${D} for ${eid}" >&2
      exit 1
    fi
    
    # Expose selected data file under a stable name for downstream binding
    ln -sf "$SELECTED" DATAFILE_SELECTED

    # Write TSV strictly from samplesheet values
    cat > ${eid}_Metadata.tsv <<'TSV'
type	study.phs_accession	participant.study_participant_id	sample.sample_id	file_name	file_type	file_description	file_size	md5sum	experimental_strategy_and_data_subtypes	submission_version	checksum_value	checksum_algorithm	file_mapping_level	release_datetime	is_supplementary_file
file	${study_phs}	${participant_id}	${sample_id}	${file_name}	${file_type}	${file_description}	${file_size}	${md5sum}	${strategy}	${submission_version}	${checksum_value}	${checksum_algorithm}	${file_mapping_level}	${release_datetime}	${is_supplementary}
TSV

    ls -lh ${eid}_Metadata.tsv DATAFILE_SELECTED
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
      python -m pip install --no-cache-dir "git+https://github.com/CBIIT/crdc-datahub-cli-uploader.git" >/devnull 2>&1 || \
      python -m pip install --no-cache-dir crdc-datahub-cli-uploader >/dev/null 2>&1 || true

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

  // Prefer function-style process invocation for clarity & correctness
  ch_dl   = synapse_get( ch_input )               // (meta, out/)
  ch_meta = make_metadata_tsv( ch_dl )            // (meta, DATAFILE_SELECTED, *_Metadata.tsv)
  ch_cfg  = make_uploader_config( ch_meta )       // (meta, DATAFILE_SELECTED, cli-config-*.yml)
  ch_up   = crdc_upload( ch_cfg )                 // (meta, upload.log)

  // Visibility
  ch_meta.view { meta, datafile, tsv -> "METADATA:\t${eidOf(meta)}\t${tsv}\t->\t${datafile}" }
  ch_cfg.view  { meta, datafile, yml -> "CONFIG:\t${eidOf(meta)}\t${yml}" }
  ch_up.view   { meta, log -> "UPLOAD:\t${eidOf(meta)}\t${log}" }
}
