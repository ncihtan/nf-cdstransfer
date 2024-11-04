#!/usr/bin/env nextflow
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ncihtan/mf-cdstransfer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Github : https://github.com/nncihtan/nf-cdstransfer
----------------------------------------------------------------------------------------
*/

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PARAMATERS AND INPUTS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

params.input = params.input ?: 'samplesheet.csv'
params.dryrun = params.dryrun ?: false

ch_input = Channel.fromPath(params.input).splitCsv(sep: ',', skip: 1)


/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    SAMPLESHEET SPLIT
    This workflow takes the samplesheet and splits it into individual entities.
    It takes a samplesheet with two columns: entityid and aws_uri.
    It passes a tuple of entityid and aws_uri to the next process.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow SAMPLESHEET_SPLIT {
    take:
    samplesheet

    main:
    ch_input
        .map { row -> 
            [
                entityid: row[0]?.trim(),
                aws_uri: row[1]?.trim()
            ]
        }
        .set { entities }
        
    emit: 
    entities
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    synapse_get
    This process downloads the entity from Synapse using the entityid.
    Spaces in filenames are replaced with underscores.
    It takes a tuple of entityid and aws_uri.
    It passes a tuple of the metamap and the file to the next process.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

process synapse_get {

    container "quay.io/sagebionetworks/synapsepythonclient:v2.5.1"

    tag "${meta.entityid}"

    input:
    val meta

    secret 'SYNAPSE_AUTH_TOKEN'

    output:
    tuple val(meta), path('*')  // Adjust the pattern if necessary

    script:
    def args = task.ext.args ?: ''
    """
    synapse \\
        -p \$SYNAPSE_AUTH_TOKEN \\
        get \\
        $args \\
        $meta.entityid

    shopt -s nullglob
    for f in *\\ *; do mv "\${f}" "\${f// /_}"; done
    """
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    cds_upload
    This process uploads the file to the CDS using the aws_uri.
    It takes a tuple of the metamap and the file.
    It passes a tuple of the metamap and the file to the next process.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

process cds_upload {
    container "quay.io/brunograndephd/aws-cli:latest"

    tag "${meta.entityid}"

    input:
    tuple val(meta), path(entity)
    secret 'CDS_AWS_ACCESS_KEY_ID'
    secret 'CDS_AWS_SECRET_ACCESS_KEY'

    output:
    tuple val(meta), path(entity)

    script:
    """
    AWS_ACCESS_KEY_ID=\$CDS_AWS_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY=\$CDS_AWS_SECRET_ACCESS_KEY \
    aws s3 cp $entity $meta.aws_uri ${params.dryrun ? '--dryrun' : ''}
    """
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    MAIN WORKFLOW

    This workflow takes the samplesheet and splits it into individual entities.
    It downloads the entity from Synapse using the entityid.
    It uploads the file to the CDS using the aws_uri.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow {
    samplesheet = file(params.input)

    SAMPLESHEET_SPLIT(samplesheet) \
        | synapse_get \
        | cds_upload
}