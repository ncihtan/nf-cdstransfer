params.input = 'samplesheet.csv'

workflow SAMPLESHEET_SPLIT {
    take:
    samplesheet

    main:
    Channel
        .fromPath(samplesheet)
        .splitCsv(header: true, sep: ',', quote: '"')
        .filter { row -> 
            row.File_Size?.isNumber() && row.File_Size.toLong() < 50 * 1024 * 1024
        }
        .randomSample(5)
        .map { row -> 
            [
                entityid: row['entityId']?.trim(),
                aws_uri: row['file_url_in_cds']?.trim()
            ]
        }
        .set { entities }
        
    emit: 
    entities
}

process synapse_get {

    conda "bioconda::synapseclient=2.6.0"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/synapseclient:2.6.0--pyh5e36f6f_0' :
        'quay.io/biocontainers/synapseclient:2.6.0--pyh5e36f6f_0' }"

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
    conda "bioconda::awscli=1.18.69"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/awscli:1.18.69--pyh9f0ad1d_0' :
        'quay.io/biocontainers/awscli:1.18.69--pyh9f0ad1d_0' }"

    input:
    tuple val(meta), path(entity)
    secret 'AWS_ACCESS_KEY_ID'
    secret 'AWS_SECRET_ACCESS_KEY'    
    secret 'AWS_SESSION_TOKEN'

    output:
    tuple val(meta), path(entity)

    script:
    """
    aws s3 cp \\
        $entity $meta.aws_uri
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