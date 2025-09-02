#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

include { validateParameters; paramsSummaryLog; samplesheetToList } from 'plugin/nf-schema'

// ---- Resolve samplesheet path (local or GitHub/raw URL) ----
def _raw = params.input ?: 'samplesheet.tsv'
def _isUrl = (_raw ==~ /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//)
def _abs   = file(_raw).isAbsolute()
def repoPath = file("${projectDir}/${_raw}")

// Priority: URL (GitHub raw, HTTPS, etc.) → absolute path → repo copy
def resolved_input = _isUrl ? _raw
                    : (_abs && file(_raw).exists()) ? file(_raw).toString()
                    : (repoPath.exists() ? repoPath.toString() : repoPath.toString())

// Validate pipeline params
validateParameters()

// Build channel of meta rows from TSV samplesheet
ch_input = Channel.fromList(
    samplesheetToList(resolved_input, "assets/schema_input.json")
)

