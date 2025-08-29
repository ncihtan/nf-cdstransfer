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
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    synapse_get
    This process downloads the entity from Synapse using the entityid.
    Spaces in filenames are replaced with underscores to ensure compatibility.
    The process takes a tuple of entityid and metadata and outputs a tuple containing 
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
 * Build a CRDC-style metadata TSV strictly from samplesheet columns (meta).
 * No file probing (size, md5, type) is done here.
 * Provide these columns in your samplesheet (or they’ll default to empty):
 *   entityid (used for naming), study_phs_accession, study_participant_id,
 *   sample_id, file_name, file_type, file_description, file_size, md5sum,
 *   experimental_strategy_and_data_subtypes, submission_version,
 *   checksum_value, checksum_algorithm, file_mapping_level, release_datetime,
 *   is_supplementary_file
 */

process make_metadata_tsv {

    tag "${meta.entityid}"

    input:
    val(meta)

    output:
    tuple val(meta), path("${meta.entityid}_Metadata.tsv")

    script:
    // Resolve values from the samplesheet (fall back to blank/defaults)
    def study_phs             = meta.study_phs_accession ?: meta['study.phs_accession'] ?: 'phs002371'
    def participant_id        = meta.study_participant_id ?: meta['participant.study_participant_id'] ?: ''
    def sample_id             = meta.sample_id ?: meta['sample.sample_id'] ?: meta.HTAN_Assayed_Biospecimen_ID ?: ''
    def file_name             = meta.file_name ?: ''
    def file_type             = meta.file_type ?: ''
    def file_description      = meta.file_description ?: ''
    def file_size             = meta.file_size ?: ''        // keep blank if you don’t provide it
    def md5sum                = meta.md5sum ?: ''           // keep blank if you don’t provide it
    def strategy              = meta.experimental_strategy_and_data_subtypes ?: meta.experimental_strategy ?: ''
    def submission_version    = meta.submission_version ?: ''
    def checksum_value        = meta.checksum_value ?: ''   // for external checksums if you have them
    def checksum_algorithm    = meta.checksum_algorithm ?: 'md5'
    def file_mapping_level    = meta.file_mapping_level ?: ''
    def release_datetime      = meta.release_datetime ?: ''
    def is_supplementary_file = meta.is_supplementary_file ?: ''

    """
    set -euo pipefail

    cat > ${meta.entityid}_Metadata.tsv <<'TSV'
type	study.phs_accession	participant.study_participant_id	sample.sample_id	file_name	file_type	file_description	file_size	md5sum	experimental_strategy_and_data_subtypes	submission_version	checksum_value	checksum_algorithm	file_mapping_level	release_datetime	is_supplementary_file
file	${study_phs}	${participant_id}	${sample_id}	${file_name}	${file_type}	${file_description}	${file_size}	${md5sum}	${strategy}	${submission_version}	${checksum_value}	${checksum_algorithm}	${file_mapping_level}	${release_datetime}	${is_supplementary_file}
TSV

    ls -lh ${meta.entityid}_Metadata.tsv
    """
}


workflow {
  // ch_input yields val(meta)
  ch_input \
    | synapse_get \
    | map { meta, _ -> meta } \
    | make_metadata_tsv \
    | view { meta, tsv -> "METADATA:\t${meta.entityid}\t${tsv}" }
}

