#!/usr/bin/env nextflow
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ncihtan/mf-cdstransfer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Github : https://github.com/ncihtan/nf-cdstransfer
----------------------------------------------------------------------------------------
*/

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
    // Unpack the tuple
    .map { it -> it[0] }

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    synapse_get
    This process downloads the entity from Synapse using the entityid.
    Spaces in filenames are replaced with underscores to ensure compatibility.
    The process takes a tuple of entityid and aws_uri and outputs a tuple containing 
    the metadata and downloaded files, which are saved in a directory specific to each entityid.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

process synapse_get {

    container "quay.io/sagebionetworks/synapsepythonclient:v2.5.1"

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
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    cds_upload
    This process uploads the downloaded file to the CDS using the provided aws_uri.
    It takes a tuple of the metadata and downloaded file and outputs a tuple indicating
    the successful upload.

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
    tuple val(meta), val(true)  // Indicate successful upload

    script:
    """
    echo "Uploading ${entity} to ${meta.aws_uri}..."
    AWS_ACCESS_KEY_ID=\$CDS_AWS_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY=\$CDS_AWS_SECRET_ACCESS_KEY \
    aws s3 cp $entity $meta.aws_uri ${params.dryrun ? '--dryrun' : ''}
    """
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    generate_report
    This process generates a CSV report based on the original samplesheet with an added
    status column that shows whether each entry was processed successfully.
    It takes a tuple of metadata and success status, as well as the original samplesheet.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    MAIN WORKFLOW
    This workflow processes the samplesheet by splitting it, downloading entities 
    from Synapse, uploading to CDS, and generating a report of the results.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow {
    ch_input \
        | take ( params.take_n ?: -1 ) \// Optionally limit the number of entities to process
        | synapse_get \
        | cds_upload
}
