import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import type { AttributeValue } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import type { DynamoDBRecord, SQSBatchResponse, SQSEvent } from "aws-lambda";

export interface PipeArchiveEvent {
  eventID: string;
  eventName: string;
  eventSourceARN?: string;
  approximateCreationDateTime: number;
  sequenceNumber?: string;
  keys?: Record<string, unknown>;
  newImage?: Record<string, unknown>;
  oldImage?: Record<string, unknown>;
}

export type ObjectWriter = (key: string, body: string) => Promise<void>;

const image = (value: Record<string, unknown> | undefined) =>
  value ? unmarshall(value as Record<string, AttributeValue>) : undefined;

// EventBridge Pipes forwards each raw DynamoDB stream record as the SQS message body.
export function parsePipeMessage(body: string): PipeArchiveEvent[] {
  const value: unknown = JSON.parse(body);
  const records = (Array.isArray(value) ? value : [value]) as DynamoDBRecord[];
  return records.map((record) => {
    const time = record?.dynamodb?.ApproximateCreationDateTime;
    if (
      typeof record !== "object" || record === null ||
      typeof record.eventID !== "string" || !record.eventID ||
      typeof record.eventName !== "string" ||
      typeof time !== "number" || !Number.isFinite(time)
    ) {
      throw new Error("Invalid pipe message: expected a DynamoDB stream record");
    }
    return {
      eventID: record.eventID,
      eventName: record.eventName,
      eventSourceARN: record.eventSourceARN,
      approximateCreationDateTime: time,
      sequenceNumber: record.dynamodb?.SequenceNumber,
      keys: image(record.dynamodb?.Keys),
      newImage: image(record.dynamodb?.NewImage),
      oldImage: image(record.dynamodb?.OldImage),
    };
  });
}

export function objectKey(event: PipeArchiveEvent): string {
  const iso = new Date(event.approximateCreationDateTime * 1000).toISOString();
  const [date, time] = iso.split("T") as [string, string];
  return `events/${date.replaceAll("-", "/")}/${time.slice(0, 2)}/${encodeURIComponent(event.eventID)}.json`;
}

export function createPipeSqsHandler(write: ObjectWriter) {
  return async (event: SQSEvent): Promise<SQSBatchResponse> => {
    const failures: { itemIdentifier: string }[] = [];
    await Promise.all(event.Records.map(async (record) => {
      try {
        for (const archived of parsePipeMessage(record.body)) {
          await write(objectKey(archived), JSON.stringify(archived));
        }
      } catch (error) {
        console.error(`Failed to archive SQS message ${record.messageId}`, error);
        failures.push({ itemIdentifier: record.messageId });
      }
    }));
    console.log(`Archived ${event.Records.length - failures.length}/${event.Records.length} message(s)`);
    return { batchItemFailures: failures };
  };
}

const s3 = new S3Client({});

export const handler = createPipeSqsHandler(async (key, body) => {
  const Bucket = process.env.BUCKET_NAME;
  if (!Bucket) throw new Error("BUCKET_NAME is required");
  await s3.send(new PutObjectCommand({
    Bucket, Key: key, Body: body, ContentType: "application/json",
  }));
});
