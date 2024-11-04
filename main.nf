params.input = params.input ?: 'samplesheet.csv'
params.dryrun = params.dryrun ?: false
params.output_report = params.output_report ?: 'output_report.csv'

ch_input = Channel.fromPath(params.input).splitCsv(sep: ',', skip: 1)
ch_input.view()

workflow SAMPLESHEET_SPLIT {
    take:
    samplesheet

    main:
    ch_input
        .filter { row -> 
            def isValid = row[0]?.trim() && row[1]?.trim()
            if (!isValid) println "Warning: Skipping invalid row with missing entityid or aws_uri -> ${row}"
            return isValid
        }
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

process synapse_get {

    container "quay.io/sagebionetworks/synapsepythonclient:v2.5.1"

    tag "${meta.entityid}"

    input:
    val meta

    secret 'SYNAPSE_AUTH_TOKEN'

    output:
    tuple val(meta), path("${meta.entityid}_output/*")

    script:
    def args = task.ext.args ?: ''
    """
    echo "Fetching entity ${meta.entityid} from Synapse..."
    synapse -p \$SYNAPSE_AUTH_TOKEN get $args ${meta.entityid}

    if [[ $? -ne 0 ]]; then
        echo "Error: Failed to download entity ${meta.entityid}."
        exit 1
    fi

    shopt -s nullglob
    for f in *\\ *; do mv "\${f}" "\${f// /_}"; done
    """
}

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

    if [[ $? -ne 0 ]]; then
        echo "Error: Failed to upload ${entity} to ${meta.aws_uri}."
        exit 1
    fi
    """
}

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

        # Add a header for the output file
        header = next(reader) + ['Status']
        writer.writerow(header)

        # Write each row with the status
        for row in reader:
            entity_id = row[0].strip()
            status = 'Completed' if entity_id == meta['entityid'] and success else 'Failed'
            writer.writerow(row + [status])

    if [[ $? -ne 0 ]]; then
        echo "Error: Failed to generate the report."
        exit 1
    fi
    """
}

workflow {
    samplesheet = file(params.input)

    SAMPLESHEET_SPLIT(samplesheet) \
        | synapse_get \
        | cds_upload \
        | generate_report
}


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

workflow {
    samplesheet = file(params.input)

    SAMPLESHEET_SPLIT(samplesheet) \
        | synapse_get \
        | cds_upload
}
