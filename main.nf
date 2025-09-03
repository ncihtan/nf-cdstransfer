#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

/*
================================================================================
    PARAMETERS AND INPUTS
================================================================================
*/

include { validateParameters; paramsSummaryLog; samplesheetToList } from 'plugin/nf-schema'

// ---- Resolve samplesheet path (local or GitHub/raw URL) ----
def _raw = params.input ?: 'samplesheet.tsv'
def _isUrl = (_raw ==~ /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//)
def _abs   = file(_raw).isAbsolute()
def repoPath = file("${projectDir}/${_raw}")

// Priority: URL (GitHub raw, HTTPS, etc.) → absolute path → repo copy
def resolved_input = _isUrl ? _raw
                    : (_abs && file(_raw).exists()) ? file(_raw).toString()
                    : (repoPath.exists() ? repoPath.toString() : repoPath.toString())

// Validate pipeline params
validateParameters()

// headers must match your TSV
def headers = [
  "type", "study.phs_accession", "participant.study_participant_id",
  "sample.sample_id", "file_name", "file_type", "file_description",
  "file_size", "md5sum", "experimental_strategy_and_data_subtypes",
  "submission_version", "checksum_value", "checksum_algorithm",
  "file_mapping_level", "release_datetime", "is_supplementary_file", "entityid"
]

ch_input = Channel.fromList(
    samplesheetToList(resolved_input, "assets/schema_input.json")
).map { row ->
    if (row instanceof List) {
        return headers.collectEntries { h -> [h, row[headers.indexOf(h)]] }
    } else {
        return row
    }
}


/*
================================================================================
    PROCESSES
================================================================================
*/

process synapse_get {

    container 'ghcr.io/sage-bionetworks/synapsepythonclient:develop-b784b854a069e926f1f752ac9e4f6594f66d01b7'

    tag "${meta.entityid}"

    input:
    val(meta)

    secret 'SYNAPSE_AUTH_TOKEN_DYP'

    output:
    tuple val(meta), path('*')

    script:
    def args = task.ext.args ?: ''
    def token = System.getenv('SYNAPSE_AUTH_TOKEN_DYP')
    """
    echo "Fetching entity ${meta.entityid} from Synapse into flat directory..."
    synapse -p \$SYNAPSE_AUTH_TOKEN_DYP get $args ${meta.entityid}
    """

}

process subset_row_without_entityid {

    container 'python:3.11-slim' 

    tag "${meta.sample_id ?: meta.file_name}"

    input:
    tuple val(meta), path(files)

    output:
    tuple val(clean_meta), path(files)

    script:
    def clean_meta = meta.findAll { k, v -> k != 'entityid' }
    """
    echo "Subset row for ${meta.file_name}, dropped entityid"
    """
}


process make_config_yml {

    tag "${meta.file_name}"

    input:
    tuple val(meta), path(files), path(global_tsv)

    output:
    tuple val(meta), path(files), path("cli-config-*_file.yml"), path(global_tsv)

    script:
    def dryrun_value = params.dry_run ? "true" : "false"

    """
    cat > cli-config-${meta.file_name}_file.yml <<'YML'
    Config:
      api-url: https://hub.datacommons.cancer.gov/api/graphql
      dryrun: ${dryrun_value}
      overwrite: ${params.overwrite}
      retries: 3
      submission: ${params.submission_uuid ?: System.getenv('CRDC_SUBMISSION_ID')}
      manifest: samplesheet_no_entityid.tsv
      token: \${CRDC_API_TOKEN}
      type: data file
    YML
    """
}

process write_clean_tsv {

    publishDir "results", mode: 'copy'

    input:
    val(all_meta)

    output:
    path("samplesheet_no_entityid.tsv")

    script:
    """
    python3 - <<'PYCODE'
    import pandas as pd
    rows = ${all_meta}
    df = pd.DataFrame(rows)
    df.to_csv("samplesheet_no_entityid.tsv", sep="\\t", index=False)
    PYCODE
    """
}

process crdc_upload {

    tag "${meta.file_name}"

    container 'python:3.11-slim'

    input:
    tuple val(meta), path(files), path(config), path(global_tsv)

    secret 'CRDC_API_TOKEN'

    output:
    tuple val(meta), path(files), path(config), path(global_tsv)

    script:
    def dryrun_flag = params.dry_run ? "--dry-run" : ""

    """
    set -euo pipefail

    echo "Installing CRDC uploader from GitHub..."
    pip install --quiet git+https://github.com/CBIIT/crdc-datahub-cli-uploader.git

    echo "Uploading ${meta.file_name} to CRDC..."
    python3 -m uploader --config ${config} $dryrun_flag
    """
}

/*
================================================================================
    WORKFLOW
================================================================================
*/

workflow {

    // Step 1: download and clean rows
    cleaned = ch_input \
        | synapse_get \
        | subset_row_without_entityid

    // Step 2: collect all cleaned meta into one TSV
    cleaned.map { meta, files -> meta }
        .collect()
        .set { all_meta }

    global_tsv = write_clean_tsv(all_meta)

    // Step 3: add YAML configs referencing the global TSV
    with_yaml = cleaned
        .combine(global_tsv)
        | make_config_yml

    // Step 4: upload each file using its YAML + global TSV
    crdc_upload(with_yaml)
}

