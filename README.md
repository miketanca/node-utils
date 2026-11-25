# DynamoDB stream archive

The `test` table stream sends INSERT, MODIFY and REMOVE events through SQS to a
second Lambda, which archives unmarshalled keys and old/new images in S3.

## Deploy

Use AWS credentials for the intended account, then run on Windows:

```powershell
npm run typecheck
npm test
$env:AWS_REGION = "us-east-2"
npm run deploy
```

The deployment requires the existing `test` table. It enables
`NEW_AND_OLD_IMAGES` streams if disabled, but refuses to replace a stream using
another view type. Credentials must permit DynamoDB stream configuration,
IAM role/policy management and `iam:PassRole`, Lambda deployment/mappings,
SQS provisioning, S3 provisioning, and STS identity lookup.

Deployment creates missing resources and updates existing pipeline configuration:

- `test-table-stream-handler` and `test-table-sqs-s3-handler` Lambdas and roles.
- `test-table-stream-events` queue, encrypted with SQS-managed encryption.
- `test-table-stream-events-dlq`, with redrive after five receives.
- Private, AES256-encrypted bucket
  `test-table-stream-events-<account-id>-<region>`.
- DynamoDB and SQS event-source mappings with partial batch failure reporting.

The writer starts before the stream producer is updated. New stream mappings
start at `TRIM_HORIZON` to include available stream history; existing mappings
keep their checkpoints. Re-running deployment does not create duplicate mappings.
The stream Lambda uses `QUEUE_URL`; the writer uses `BUCKET_NAME`.

## Hourly files and delivery

There is one JSONL object per **UTC event creation hour**, not processing hour:

```text
events/2026-11-25/14.jsonl
events/2026-11-25/15.jsonl
```

Each line contains `eventID`, `eventName`, `eventSourceARN`,
`approximateCreationDateTime` (Unix seconds), `keys`, `newImage`, and `oldImage`
(images absent from the source record are omitted).

S3 does not support append for these objects. The writer reads the current hour
file, deduplicates by DynamoDB `eventID`, and conditionally replaces it using
ETags (`IfMatch`) or creates it with `IfNoneMatch`. Concurrent changes trigger
bounded retries; exhausted retries or other errors return SQS partial failures.
Late events update their original hour. Lines are not guaranteed to be globally
ordered. Do not write to the archive objects outside this pipeline.

This read/merge/write approach targets modest volumes: each update reads and
rewrites the entire hour, so memory usage and S3 traffic grow with the hourly
file size. The writer has 512 MB memory, a 300-second timeout and maximum SQS
concurrency of two. For high-volume streams, use immutable chunks plus a
compaction job instead. No automatic archival retention/deletion is configured.

The queue visibility timeout is 1,800 seconds; queue and DLQ retention are
14 days. Monitor both Lambda error logs and DLQ depth; redrive failed messages
after resolving errors. DynamoDB streams retain records for only 24 hours, so
producer outages beyond that window can lose events. Resources incur AWS
charges and remain deployed until explicitly removed.
