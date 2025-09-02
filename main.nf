#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PARAMETERS AND INPUTS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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

// Build channel of meta rows from TSV samplesheet
ch_input = Channel.fromList(
    samplesheetToList(resolved_input, "assets/schema_input.json")
)

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PROCESSES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

process synapse_get {

    // Synapse Python Client container
    container 'ghcr.io/sage-bionetworks/synapsepythonclient:develop-b784b854a069e926f1f752ac9e4f6594f66d01b7'

    tag "${meta.entityid}"

    input:
    val(meta)

    // Use your custom secret
    secret 'SYNAPSE_AUTH_TOKEN_DYP'

    output:
    tuple val(meta), path('*')

    script:
    def args = task.ext.args ?: ''
    """
    echo "Fetching entity \${meta.entityid} from Synapse into flat directory..."
    synapse -p \$SYNAPSE_AUTH_TOKEN_DYP get $args \${meta.entityid}
    """
}

process subset_row_without_entityid {

    tag "${meta.sample_id ?: meta.file_name}"

    input:
    tuple val(meta), path(files)

    output:
    tuple val(clean_meta), path(files)

    script:
    // Strip out 'entityid' key from this row (meta Map)
    def clean_meta = meta.findAll { k, v -> k != 'entityid' }
    """
    # Nothing to change in the files, just forwarding
    echo "Subset row for \${meta.file_name}, dropped entityid"
    """
}

process make_config_yml {

    tag "${meta.file_name}"

    input:
    tuple val(meta), path(files)

    output:
    tuple val(meta), path("cli-config-*_file.yml")

    script:
    // Derive manifest name dynamically
    def manifest = "CDS_Data_Loading_v8.0.3_Stanford_Submission_${meta.file_name}_Metadata.tsv"

    """
    cat > cli-config-${meta.file_name}_file.yml <<'YML'
    Config:
      api-url: https://hub.datacommons.cancer.gov/api/graphql
      dryrun: false
      overwrite: false
      retries: 3
      submission: ${params.submission_uuid}
      manifest: ${manifest}
      token: \${CRDC_API_TOKEN}
      type: data file
    YML
    """
}

process crdc_upload {

    tag "${meta.file_name}"

    // Use Python container as base
    container 'python:3.11-slim'

    input:
    tuple val(meta), path(files), path(config)

    secret 'CRDC_API_TOKEN'

    output:
    tuple val(meta), path(files), path(config)

    script:
    """
    set -euo pipefail

    echo "Installing CRDC uploader from GitHub..."
    pip install --quiet git+https://github.com/CBIIT/crdc-datahub-cli-uploader.git

    echo "Uploading ${meta.file_name} to CRDC..."
    crdc-uploader upload --config ${config} --file ${files}
    """
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow {
    ch_input \
        | synapse_get \
        | subset_row_without_entityid \
        | make_config_yml \
        | crdc_upload
}
