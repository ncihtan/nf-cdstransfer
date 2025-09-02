#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

/*
  Minimal pipeline: download one Synapse file, 
  make TSV + config, upload with CRDC uploader
*/

params.input = params.input ?: 'syn12345678'

/*
  PROCESS: synapse_get
  Downloads a single Synapse entity to ./out
*/
process synapse_get {
    container 'ghcr.io/sage-bionetworks/synapsepythonclient:develop-b784b854a069e926f1f752ac9e4f6594f66d01b7'
    secret 'SYNAPSE_AUTH_TOKEN_DYP'

    input:
    val(eid)

    output:
    tuple val(eid), path('out/*')

    script:
    """
    export SYNAPSE_CACHE_DIR="\$PWD/.synapseCache"
    mkdir -p \$SYNAPSE_CACHE_DIR out
    synapse -p \$SYNAPSE_AUTH_TOKEN_DYP get \$eid
    mv * out/ || true
    """
}

/*
  PROCESS: make_metadata_tsv
  Writes a tiny TSV and symlinks the data file
*/
process make_metadata_tsv {
    input:
    tuple val(eid), path(downloads)

    output:
    tuple val(eid), path("DATAFILE_SELECTED"), path("*_Metadata.tsv")

    script:
    """
    FILE=\$(find \$downloads -type f | head -n1)
    ln -sf "\$FILE" DATAFILE_SELECTED

    cat > \${eid}_Metadata.tsv <<EOF
type\tfile_name
file\t\$(basename \$FILE)
EOF
    """
}

/*
  PROCESS: make_uploader_config
  Creates YAML for uploader
*/
process make_uploader_config {
    input:
    tuple val(eid), path(data_file), path(tsv)

    output:
    tuple val(eid), path(data_file), path("cli-config-*.yml")

    script:
    """
    cat > cli-config-\${eid}.yml <<EOF
version: 1
auth:
  token_env: CRDC_API_TOKEN
submission:
  id: \${CRDC_SUBMISSION_ID}
files:
  - data_file: "\${data_file}"
    metadata_file: "\${tsv}"
    overwrite: true
    dry_run: false
EOF
    """
}

/*
  PROCESS: crdc_upload
  Runs the uploader
*/
process crdc_upload {
    container 'python:3.11-slim'
    secret 'CRDC_API_TOKEN'
    secret 'CRDC_SUBMISSION_ID'

    input:
    tuple val(eid), path(file), path(cfg)

    output:
    path("upload.log")

    script:
    """
    pip install --quiet --no-cache-dir crdc-datahub-cli-uploader
    crdc-uploader upload --config "\$cfg" | tee upload.log
    """
}

/*
  WORKFLOW
*/
workflow {
    ch_dl   = synapse_get( Channel.of(params.input) )
    ch_meta = make_metadata_tsv( ch_dl )
    ch_cfg  = make_uploader_config( ch_meta )
    crdc_upload( ch_cfg )
}
