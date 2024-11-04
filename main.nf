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

    container "ghcr.io/ncihtan/nf-imagecleaner"

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
    container "ghcr.io/ncihtan/nf-imagecleaner"

    tag "${meta.entityid}"

    input:
    tuple val(meta), path(entity)
    secret 'AWS_ACCESS_KEY_ID'
    secret 'AWS_SECRET_ACCESS_KEY'

    output:
    tuple val(meta), path(entity)

    script:
    """
    set -e
    unset AWS_PROFILE  # Ensure no profile is interfering

    # Run the AWS S3 copy command with environment variables
    aws s3 cp $entity $meta.aws_uri ${params.dryrun ? '--dryrun' : ''}
    """
}

workflow {
    samplesheet = file(params.input)

    SAMPLESHEET_SPLIT(samplesheet) \
        | synapse_get \
        | cds_upload
}