#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PARAMETERS AND INPUTS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { validateParameters; paramsSummaryLog; samplesheetToList } from 'plugin/nf-schema'

// Validate input parameters
validateParameters()

// Print summary of supplied parameters
log.info paramsSummaryLog(workflow)

// Build channel of meta maps from samplesheet (aliases handled by schema "meta")
ch_input = Channel
    .fromList(samplesheetToList(params.input, "assets/schema_input.json"))

/*
================================================================================
 PROCESS: synapse_get
 - Downloads Synapse entity (meta.entityId) using SYNAPSE_AUTH_TOKEN (Tower secret)
 - Renames spaces in filenames to underscores
 - Emits: tuple val(meta), path('*')
================================================================================
*/
process synapse_get {

    // TODO: Update to the latest tag when available
    container 'ghcr.io/sage-bionetworks/synapsepythonclient:develop-b784b854a069e926f1f752ac9e4f6594f66d01b7'

    tag "${meta.entityId}"

    input:
    val(meta)

    secret 'SYNAPSE_AUTH_TOKEN'

    output:
    tuple val(meta), path('*')

    script:
    def args = task.ext.args ?: ''
    """
    set -euo pipefail
    echo "Fetching entity ${meta.entityId} from Synapse..."
    synapse -p \$SYNAPSE_AUTH_TOKEN get $args ${meta.entityId}

    shopt -s nullglob
    for f in *\\ *; do mv "\${f}" "\${f// /_}"; done  # Replace spaces with underscores
    """

    stub:
    """
    echo "Making a fake file for testing..."
    dd if=/dev/urandom of=small_file.tmp bs=1M count=1
    """
}

/*
================================================================================
 PROCESS: make_metadata_tsv
 - Builds CRDC-style TSV strictly from samplesheet values (no file probing)
 - Picks one data file to pass downstream:
     * Prefer basename matching meta.file_name (after space->underscore)
     * Else first regular file in work dir
 - Emits: tuple val(meta), path("${meta.entityId}_Metadata.tsv"), path("DATAFILE_SELECTED")
================================================================================
*/
process make_metadata_tsv {

    tag "${meta.entityId}"

    input:
    tuple val(meta), path(downloaded_files)

    output:
    tuple val(meta), path("${meta.entityId}_Metadata.tsv"), path("DATAFILE_SELECTED")

    script:
    // Resolve values from samplesheet (fallbacks/defaults)
    def study_phs             = meta.study_phs_accession ?: meta['study.phs_accession'] ?: 'phs002371'
    def participant_id        = meta.study_participant_id ?: meta['participant.study_participant_id'] ?: ''
    def sample_id             = meta.sample_id ?: meta['sample.sample_id'] ?: meta.HTAN_Assayed_Biospecimen_ID ?: ''
    def file_name             = meta.file_name ?: ''
    def file_type             = meta.file_type ?: ''
    def file_description      = meta.file_description ?: ''
    def file_size             = meta.file_size ?: ''
    def md5sum                = meta.md5sum ?: ''
    def strategy              = meta.experimental_strategy_and_data_subtypes ?: meta.experimental_strategy ?: ''
    def submission_version    = meta.submission_version ?: ''
    def checksum_value        = meta.checksum_value ?: ''
    def checksum_algorithm    = meta.checksum_algorithm ?: 'md5'
    def file_mapping_level    = meta.file_mapping_level ?: ''
    def release_datetime      = meta.release_datetime ?: ''
    def is_supplementary_file = meta.is_supplementary_file ?: ''

    def desired = file_name?.replace(' ', '_') ?: ''

    """
    set -euo pipefail

    # Choose a data file to pass on
    SELECTED=""
    if [ -n "${desired}" ] && [ -f "${desired}" ]; then
      SELECTED="${desired}"
    else
      mapfile -t FILES < <(find . -maxdepth 1 -type f -printf "%f\\n" | sort)
      if [ "\${#FILES[@]}" -gt 0 ]; then
        SELECTED="\${FILES[0]}"
      fi
    fi

    if [ -z "\$SELECTED" ] || [ ! -f "\$SELECTED" ]; then
      echo "ERROR: No data file found for ${meta.entityId}" >&2
      exit 1
    fi

    # Write TSV strictly from samplesheet values
    cat > ${meta.entityId}_Metadata.tsv <<'TSV'
type	study.phs_accession	participant.study_participant_id	sample.sample_id	file_name	file_type	file_description	file_size	md5sum	experimental_strategy_and_data_subtypes	submission_version	checksum_value	checksum_algorithm	file_mapping_level	release_datetime	is_supplementary_file
file	${study_phs}	${participant_id}	${sample_id}	${file_name}	${file_type}	${file_description}	${file_size}	${md5sum}	${strategy}	${submission_version}	${checksum_value}	${checksum_algorithm}	${file_mapping_level}	${release_datetime}	${is_supplementary_file}
TSV

    # Expose selected data file under a stable name for downstream binding
    ln -sf "\$SELECTED" DATAFILE_SELECTED

    ls -lh ${meta.entityId}_Metadata.tsv
    """
}

/*
================================================================================
 PROCESS: make_uploader_config
 - Creates per-file uploader YAML referencing Tower secrets for auth
 - Passes the selected data file through
 - Emits: tuple val(meta), path(data_file), path("cli-config-${meta.entityId}.yml")
================================================================================
*/
process make_uploader_config {

    tag "${meta.entityId}"

    input:
    tuple val(meta), path(data_file), path(metadata_tsv)

    output:
    tuple val(meta), path(data_file), path("cli-config-${meta.entityId}.yml")

    script:
    def data_format = (meta.file_type ?: '').toString()
    def overwrite   = params.overwrite as boolean
    def dry_run     = params.dry_run as boolean

    """
    set -euo pipefail

    cat > cli-config-${meta.entityId}.yml <<'YAML'
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

    echo "Wrote cli-config-${meta.entityId}.yml"
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

    tag "${meta.entityId}"

    input:
    tuple val(meta), path(data_file), path(config_yml)

    secret 'CRDC_API_TOKEN'
    secret 'CRDC_SUBMISSION_ID'

    output:
    tuple val(meta), path("upload.log")

    script:
    def uploader = (task.ext.uploader_cmd ?: 'crdc-uploader').toString()

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
    echo "Stub upload for ${meta.entityId}" | tee upload.log
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
  ch_meta.view { meta, tsv, datafile -> "METADATA:\t${meta.entityId}\t${tsv}" }
  ch_cfg.view  { meta, yml -> "CONFIG:\t${meta.entityId}\t${yml}" }
  ch_up.view   { meta, log -> "UPLOAD:\t${meta.entityId}\t${log}" }
}


