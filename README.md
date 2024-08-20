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
- Requires AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN`).

## Running the Workflow

### Prerequisites
- Ensure Nextflow is installed.
- Ensure you have access to the necessary containers (`synapseclient`, `awscli`).
- Ensure you have the appropriate credentials for Synapse and AWS.

### Command
Run the workflow with the following command:

```bash
nextflow run main.nf --input path/to/samplesheet.csv
```

### Example

```bash
nextflow run main.nf --input samplesheet.csv
```

### Outputs
- The final output will be the files successfully uploaded to the specified AWS S3 bucket.

## Secrets

The following environment variables should be set with your credentials:

- `SYNAPSE_AUTH_TOKEN`: Synapse authentication token.
- `AWS_ACCESS_KEY_ID`: AWS access key ID.
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key.
- `AWS_SESSION_TOKEN` (optional): AWS session token for temporary credentials.

## License

This workflow is provided as-is without any warranties. Modify and use it at your own risk.

---

This documentation should provide you with a clear understanding of how the workflow operates, the inputs it requires, and how to run it effectively.