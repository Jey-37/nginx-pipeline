# Nginx Logs Pipeline

The Apache Beam program that reads nginx access logs from Google Cloud Pub/Sub, parses them, and saves into BigQuery.

Both local Beam pipeline running and Google Dataflow are supported.

## Environment Variables

To run this project, you need `GOOGLE_APPLICATION_CREDENTIALS` environment variable – the path to the service account key in JSON format.

## Command line arguments

### Mandatory arguments:

`--project` – GCP project name

`--inputSubscription` – short name of the Cloud Pub/Sub subscription to read from

`--dataset` – name of the BigQuery dataset

`--table` – name of the table in the dataset

### To use Dataflow Runner the below arguments are also required:

`--runner=DataflowRunner`

`--region` – Dataflow regional endpoint

`--gcpTempLocation` – GCP location for Dataflow to download temporary files

