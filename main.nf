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

params.input = params.input ?: 'samplesheet.csv'
params.dryrun = params.dryrun ?: false
params.output_report = params.output_report ?: 'output_report.csv'

ch_input = Channel.fromPath(params.input).splitCsv(sep: ',', skip: 1)


/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    SAMPLESHEET SPLIT
    This workflow takes the samplesheet and splits it into individual entities.
    It expects a samplesheet with two columns: entityid and aws_uri.
    Each row is passed as a tuple of entityid and aws_uri to the next process.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow SAMPLESHEET_SPLIT {
    take:
    samplesheet

    main:
    ch_input
        .filter { row -> 
            // Validate each row to ensure it has entityid and aws_uri
            def isValid = row[0]?.trim() && row[1]?.trim()
            if (!isValid) println "Warning: Skipping invalid row with missing entityid or aws_uri -> ${row}"
            return isValid
        }
        .map { row -> 
            // Map each row to a tuple of entityid and aws_uri
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
    Spaces in filenames are replaced with underscores to ensure compatibility.
    The process takes a tuple of entityid and aws_uri and outputs a tuple containing 
    the metadata and downloaded files, which are saved in a directory specific to each entityid.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

process synapse_get {

    container "quay.io/sagebionetworks/synapsepythonclient:v2.5.1"

    tag "${meta.entityid}"

    input:
    val meta

    secret 'SYNAPSE_AUTH_TOKEN'

    output:
    tuple val(meta), path("*")

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

process generate_report {
    input:
    tuple val(meta), val(success)
    path samplesheet

    output:
    path(params.output_report)

    script:
    """
    echo "Generating report..."
    import csv

    with open('${samplesheet}', 'r') as infile, open('${params.output_report}', 'w') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        // Add a header for the output file
        header = next(reader) + ['Status']
        writer.writerow(header)

        // Write each row with the status
        for row in reader:
            entity_id = row[0].strip()
            status = 'Completed' if entity_id == meta['entityid'] and success else 'Failed'
            writer.writerow(row + [status])
    """
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    MAIN WORKFLOW
    This workflow processes the samplesheet by splitting it, downloading entities 
    from Synapse, uploading to CDS, and generating a report of the results.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow {
    samplesheet = file(params.input)

    SAMPLESHEET_SPLIT(samplesheet) \
        | synapse_get \
        | cds_upload \
        | generate_report
}
