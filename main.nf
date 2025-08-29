#!/usr/bin/env nextflow
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

ch_input = Channel
    .fromList(samplesheetToList(params.input, "assets/schema_input.json"))
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    synapse_get
    This process downloads the entity from Synapse using the entityid.
    Spaces in filenames are replaced with underscores to ensure compatibility.
    The process takes a tuple of entityid and metadata and outputs a tuple containing 
    the metadata and downloaded files, which are saved in a directory specific to each entityid.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

process synapse_get {

    // TODO: Update the container to the latest tag when available
    container 'ghcr.io/sage-bionetworks/synapsepythonclient:develop-b784b854a069e926f1f752ac9e4f6594f66d01b7'

    tag "${meta.entityid}"

    input:
    val(meta)

    secret 'SYNAPSE_AUTH_TOKEN'

    output:
    tuple val(meta), path('*')

    script:
    def args = task.ext.args ?: ''
    """
    echo "Fetching entity ${meta.entityid} from Synapse..."
    synapse -p \$SYNAPSE_AUTH_TOKEN get $args ${meta.entityid}

    shopt -s nullglob
    for f in *\\ *; do mv "\${f}" "\${f// /_}"; done  # Rename files with spaces
    """

    stub:
    """
    echo "Making a fake file for testing..."
    ## Make a random file of 1MB and save to small_file.tmp
    dd if=/dev/urandom of=small_file.tmp bs=1M count=1
    """
}


/*
 * Build a CRDC-style metadata TSV strictly from samplesheet columns (meta).
 * No file probing (size, md5, type) is done here.
 * Provide these columns in your samplesheet (or they’ll default to empty):
 *   entityid (used for naming), study_phs_accession, study_participant_id,
 *   sample_id, file_name, file_type, file_description, file_size, md5sum,
 *   experimental_strategy_and_data_subtypes, submission_version,
 *   checksum_value, checksum_algorithm, file_mapping_level, release_datetime,
 *   is_supplementary_file
 */

process make_metadata_tsv {

    tag "${meta.entityid}"

    input:
    val(meta)

    output:
    tuple val(meta), path("${meta.entityid}_Metadata.tsv")

    script:
    // Resolve values from the samplesheet (fall back to blank/defaults)
    def study_phs             = meta.study_phs_accession ?: meta['study.phs_accession'] ?: 'phs002371'
    def participant_id        = meta.study_participant_id ?: meta['participant.study_participant_id'] ?: ''
    def sample_id             = meta.sample_id ?: meta['sample.sample_id'] ?: meta.HTAN_Assayed_Biospecimen_ID ?: ''
    def file_name             = meta.file_name ?: ''
    def file_type             = meta.file_type ?: ''
    def file_description      = meta.file_description ?: ''
    def file_size             = meta.file_size ?: ''        // keep blank if you don’t provide it
    def md5sum                = meta.md5sum ?: ''           // keep blank if you don’t provide it
    def strategy              = meta.experimental_strategy_and_data_subtypes ?: meta.experimental_strategy ?: ''
    def submission_version    = meta.submission_version ?: ''
    def checksum_value        = meta.checksum_value ?: ''   // for external checksums if you have them
    def checksum_algorithm    = meta.checksum_algorithm ?: 'md5'
    def file_mapping_level    = meta.file_mapping_level ?: ''
    def release_datetime      = meta.release_datetime ?: ''
    def is_supplementary_file = meta.is_supplementary_file ?: ''

    """
    set -euo pipefail

    cat > ${meta.entityid}_Metadata.tsv <<'TSV'
type	study.phs_accession	participant.study_participant_id	sample.sample_id	file_name	file_type	file_description	file_size	md5sum	experimental_strategy_and_data_subtypes	submission_version	checksum_value	checksum_algorithm	file_mapping_level	release_datetime	is_supplementary_file
file	${study_phs}	${participant_id}	${sample_id}	${file_name}	${file_type}	${file_description}	${file_size}	${md5sum}	${strategy}	${submission_version}	${checksum_value}	${checksum_algorithm}	${file_mapping_level}	${release_datetime}	${is_supplementary_file}
TSV

    ls -lh ${meta.entityid}_Metadata.tsv
    """
}

/*
 * Create a CRDC uploader config YAML strictly from samplesheet (meta)
 * and the produced metadata TSV, plus the staged data file path from synapse_get.
 *
 * Expects these in meta (canonical keys from nf-schema normalization):
 *   - entityId            (required)
 *   - file_name           (required)
 *   - file_type           (optional; used as data_format)
 *   - study_phs_accession (optional; defaults in TSV step)
 *   - sample_id, study_participant_id, ... (optional; for TSV only)
 *
 * Uses Tower secrets at runtime (do NOT hardcode):
 *   - CRDC_API_TOKEN      (set in Tower; used later by uploader step)
 *   - CRDC_SUBMISSION_ID  (set in Tower)
 */

process make_uploader_config {

    tag "${meta.entityId}"

    input:
    tuple val(meta), path(data_file), path(metadata_tsv)

    output:
    // pass the data file through + emit the YAML
    tuple val(meta), path(data_file), path("cli-config-${meta.entityId}.yml")

    script:
    def data_format = (meta.file_type ?: '').toString()
    def overwrite   = params.overwrite ?: true
    def dry_run     = params.dry_run ?: false

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

process crdc_upload {

    // Option A: use a prebuilt image that already includes the uploader
    // container 'ghcr.io/<your-org>/crdc-datahub-cli-uploader:latest'

    // Option B: install the uploader on the fly (works with vanilla Python image)
    container 'python:3.11-slim'

    tag "${meta.entityId}"

    input:
    tuple val(meta), path(data_file), path(config_yml)

    secret 'CRDC_API_TOKEN'
    secret 'CRDC_SUBMISSION_ID'

    output:
    tuple val(meta), path("upload.log")

    script:
    // Allow overriding the CLI name via `task.ext.uploader_cmd` (default 'crdc-uploader')
    def uploader = (task.ext.uploader_cmd ?: 'crdc-uploader').toString()

    """
    set -euo pipefail

    # Ensure basic tools
    apt-get update -y >/dev/null 2>&1 || true
    apt-get install -y --no-install-recommends git ca-certificates >/dev/null 2>&1 || true
    python -m pip install --no-cache-dir --upgrade pip >/dev/null

    # Try to use preinstalled CLI; if missing, install from GitHub
    if ! command -v ${uploader} >/dev/null 2>&1; then
      echo "CRDC uploader not found; installing..."
      python -m pip install --no-cache-dir "git+https://github.com/CBIIT/crdc-datahub-cli-uploader.git" >/dev/null
      # Common installed entrypoint names to try
      for c in crdc-uploader crdc_datahub_uploader; do
        if command -v "$c" >/dev/null 2>&1; then
          uploader_cmd="$c"
          break
        fi
      done
      : "\${uploader_cmd:=crdc-uploader}"
    else
      uploader_cmd="${uploader}"
    fi

    # Sanity
    echo "Using uploader: \${uploader_cmd}"
    \${uploader_cmd} --help >/dev/null || true

    # Run upload; YAML references the env vars, so just export them
    export CRDC_API_TOKEN="\$CRDC_API_TOKEN"
    export CRDC_SUBMISSION_ID="\$CRDC_SUBMISSION_ID"

    set -x
    \${uploader_cmd} upload --config "${config_yml}" 2>&1 | tee upload.log
    set +x

    # Basic success heuristic (adjust if your CLI prints a specific success token)
    grep -E "Completed|Success|submitted" -i upload.log >/dev/null || true
    """
    
    stub:
    """
    echo "Stub upload for ${meta.entityId}" | tee upload.log
    """
}


workflow {
  // ch_input yields val(meta)

  ch_dl   = ch_input          | synapse_get
  ch_meta = ch_dl             | make_metadata_tsv
  ch_cfg  = ch_meta           | make_uploader_config
  ch_up   = ch_cfg            | crdc_upload

  // For debugging/logging
  ch_meta.view { meta, tsv, datafile -> "METADATA:\t${meta.entityId}\t${tsv}" }
  ch_cfg.view  { meta, yml -> "CONFIG:\t${meta.entityId}\t${yml}" }
  ch_up.view   { meta, log -> "UPLOAD:\t${meta.entityId}\t${log}" }
}

