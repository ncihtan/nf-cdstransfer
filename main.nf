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

def resolved_input = _isUrl ? _raw
                    : (_abs && file(_raw).exists()) ? file(_raw).toString()
                    : (repoPath.exists() ? repoPath.toString() : repoPath.toString())

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
    """
    echo "Fetching entity ${meta.entityid} from Synapse into flat directory..."
    synapse -p \$SYNAPSE_AUTH_TOKEN_DYP get $args ${meta.entityid}
    """
}

process make_config_yml {

    container 'python:3.11'

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

    container 'python:3.11'
    publishDir "results", mode: 'copy'

    input:
    val(all_meta)

    output:
    path("samplesheet_no_entityid.tsv")

    script:
    def json = groovy.json.JsonOutput.toJson(all_meta)
    """
    pip install --quiet pandas
    python3 - <<'PYCODE'
    import pandas as pd, json
    rows = json.loads('''${json}''')
    df = pd.DataFrame(rows)
    df.to_csv("samplesheet_no_entityid.tsv", sep="\\t", index=False)
    PYCODE
    """
}


process crdc_upload {

    container 'python:3.11'

    tag "${meta.file_name}"

    input:
    tuple val(meta), path(files), path(config), path(global_tsv)

    secret 'CRDC_API_TOKEN'

    output:
    tuple val(meta), path(files), path(config), path(global_tsv)

    script:
    def dryrun_flag = params.dry_run ? "--dry-run" : ""
    """
    set -euo pipefail

    echo "Fetching CRDC uploader source from GitHub..."
    git clone https://github.com/CBIIT/crdc-datahub-cli-uploader.git
    git clone https://github.com/CBIIT/bento.git

    echo "Installing dependencies..."
    pip install --quiet -r bento/requirements.txt
    pip install ./bento

    echo "Running CRDC uploader..."
    cd crdc-datahub-cli-uploader
    python3 src/uploader.py --config ../${config} $dryrun_flag
    """
}




/*
================================================================================
    WORKFLOW
================================================================================
*/

workflow {

    // Step 1: download files from Synapse
    fetched = ch_input | synapse_get

    // Step 2: inline map to drop entityid
    cleaned = fetched.map { meta, files ->
        def clean_meta = meta.findAll { k, v -> k != 'entityid' }
        tuple(clean_meta, files)
    }

    // Step 3: collect all cleaned meta into one TSV
    cleaned.map { meta, files -> meta }
        .collect()
        .set { all_meta }

    global_tsv = write_clean_tsv(all_meta)

    // Step 4: make YAML configs referencing the global TSV
    with_yaml = cleaned
        .combine(global_tsv)
        | make_config_yml

    // Step 5: upload each file
    crdc_upload(with_yaml)
}
