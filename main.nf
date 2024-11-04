params.input = params.input ?: 'samplesheet.csv'
params.dryrun = params.dryrun ?: false

ch_input = Channel.fromPath(params.input).splitCsv(sep: ',', skip: 1)
ch_input.view()

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