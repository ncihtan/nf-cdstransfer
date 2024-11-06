# nf-cdstransfer

## Overview

This Nextflow workflow is designed to process a sample sheet (`samplesheet.csv`), retrieve files from Synapse based on `entityId`, and upload them to an AWS S3 bucket. The workflow consists of three main steps:

1. **SAMPLESHEET_SPLIT**: Filters and samples rows from the input CSV based on file size and retrieves relevant metadata.
2. **SYNAPSE_GET**: Downloads the files from Synapse using the `entityId` from the sample sheet.
3. **CDS_UPLOAD**: Uploads the downloaded files to a specified AWS S3 bucket.

## Input

### Parameters
- **params.input**: The path to the sample sheet CSV file (`samplesheet.csv`). This file should include columns like `entityId`, `file_url_in_cds`, and `File_Size`.

## Workflow Details

### Step 1: SAMPLESHEET_SPLIT

The `SAMPLESHEET_SPLIT` workflow reads the input CSV file, filters the rows based on the file size, randomly samples five rows, and maps the necessary metadata.

#### Input
- `samplesheet.csv`: A CSV file with columns including `entityId`, `file_url_in_cds`, and `File_Size`.

#### Processing
- Filters rows where `File_Size` is less than 50 MB.
- Randomly samples 5 rows from the filtered data.
- Maps `entityId` and `aws_uri` (file URL in S3) from the CSV.

#### Output
- A set of tuples containing `entityid` and `aws_uri`.

### Step 2: SYNAPSE_GET

The `SYNAPSE_GET` process downloads files from Synapse based on the `entityId` retrieved from the sample sheet.

#### Input
- `entityid`: The Synapse `entityId` used to identify and download files.

#### Output
- A tuple containing the `meta` information and the downloaded file path.

#### Dependencies
- Requires a Synapse authentication token (`SYNAPSE_AUTH_TOKEN`).

### Step 3: CDS_UPLOAD

The `CDS_UPLOAD` process uploads the files downloaded from Synapse to an AWS S3 bucket using the AWS CLI.

#### Input
- `meta`: The metadata associated with the file, including the destination S3 URI.
- `entity`: The path to the file that will be uploaded.

#### Output
- A tuple containing the `meta` information and the file path after upload.

#### Dependencies
- Requires AWS credentials set as `CDS_AWS_ACCESS_KEY_ID`, `CDS_AWS_SECRET_ACCESS_KEY` (note `CDS_` prefix)

  Set these as `nextflow secrets set CDS_AWS_ACCESS_KEY_ID <your_access_key_id>`

## Running the Workflow

### Prerequisites
- Ensure Nextflow is installed.
- Ensure you have access to the necessary containers (`synapseclient`, `awscli`).
- Ensure you have the appropriate credentials for Synapse and AWS.

### Example usage

Run the workflow with the following command:

```bash
nextflow run ncihtan/nf-cdstransfer --input path/to/samplesheet.csv
```

Using the test profile will use a built in samplesheet. Note that this requires your provided AWS credentials have acccess to the Sage test bucket `s3://htan-cds-transfer-test-bucket`

```bash
nextflow run ncihtan/nf-cdstransfer -profile test
```

To avoid having to reset secrets when moving between destination accounts you can set your secrets
using a prefix

```bash
nextflow secrets set MYCREDS_AWS_ACCESS_KEY_ID
nextflow secrets set MYCREDS_AWS_SECRET_ACCESS_KEY
nextflow run ncihtan/nf-cdstransfer --aws_secret_prefix MYCREDS
```

or use a configured profile in which params.aws_secret_prefix is set

```bash
nextflow run ncihtan/nf-cdstransfer -profile CDS --input samplesheet.csv
```

### Outputs
- The final output will be the files successfully uploaded to the specified AWS S3 bucket.

## Secrets

The following environment variables should be set with your credentials:

- `SYNAPSE_AUTH_TOKEN`: Synapse authentication token.
- `<params.aws_secret_prefix>_AWS_ACCESS_KEY_ID`: AWS access key ID. eg `CDS_AWS_ACCESS_KEY_ID`
- `<params.aws_secret_prefix>`: AWS secret access key. eg `CDS_AWS_SECRET_ACCESS_KEY`
