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
    // If change_bucket is provided then regex replace the bucket name in aws_uri. eg if change_buckert = bucket2 then s3://bucket1/key1 -> s3://bucket2/key1
    .map { it ->
        // Check if `change_bucket` is provided and update `aws_uri` if needed
        if (params.change_bucket) {
            it.aws_uri = it.aws_uri.replaceAll("(s3://)[^/]+(/.*)", "\$1${params.change_bucket}\$2")
        }
        return it
    }

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
    secret "${params.aws_secret_prefix}_AWS_ACCESS_KEY_ID"
    secret "${params.aws_secret_prefix}_AWS_SECRET_ACCESS_KEY"

    output:
    tuple val(meta), path(entity)
    tuple val(meta), val(true)  // Indicate successful upload

    script:
    """
    echo "Uploading ${entity} to ${meta.aws_uri}..."
    AWS_ACCESS_KEY_ID=\$${params.aws_secret_prefix}_AWS_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY=\$${params.aws_secret_prefix}_AWS_SECRET_ACCESS_KEY \
    aws s3 cp $entity $meta.aws_uri ${params.dryrun ? '--dryrun' : ''}
    """

    stub:
    """
    echo "Making a fake upload..."
    """
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    MAIN WORKFLOW
    This workflow processes the samplesheet by splitting it, downloading entities 
    from Synapse and uploading to CDS.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow {
    ch_input \
        | take ( params.take_n ?: -1 ) \
        | synapse_get \
        | cds_upload
}
