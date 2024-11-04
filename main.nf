params.input = 'samplesheet.csv'
params.dryrun = false

// include { validateParameters; paramsSummaryLog; samplesheetToList } from 'plugin/nf-schema'

// Validate input parameters
// validateParameters()

// Print summary of supplied parameters
// log.info paramsSummaryLog(workflow)

// Create a new channel of metadata from a sample sheet passed to the pipeline through the --input parameter
// ch_input = Channel.fromList(samplesheetToList(params.input, "assets/schema_input.json"))

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
    //secret 'AWS_SESSION_TOKEN'

    output:
    tuple val(meta), path(entity)

    script:
    """
    set -e
    unset AWS_PROFILE  # Ensure no profile is interfering

    # Check AWS CLI installation and version for debugging
    aws --version
    echo "Attempting to list S3 buckets to check credentials"
    aws s3 ls

    # Run the AWS S3 copy command with environment variables
    aws s3 cp $entity $meta.aws_uri ${params.dryrun ? '--dryrun' : ''}
    """
}

workflow SYNAPSE_GET {
    take:
    entities
    main:
    files = synapse_get(entities)
    emit:
    files
}

workflow CDS_UPLOAD {
    take:
    files
    main:
    cds_upload(files)
    emit:
    files
}

workflow {
    samplesheet = file(params.input)
    entities = SAMPLESHEET_SPLIT(samplesheet)
    entities.view()
    files = SYNAPSE_GET(entities)
    CDS_UPLOAD(files)
}