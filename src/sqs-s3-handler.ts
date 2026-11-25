import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
  S3ServiceException,
} from "@aws-sdk/client-s3";
import type { SQSEvent, SQSBatchResponse, SQSRecord } from "aws-lambda";
import { hourlyKey, parseStreamEvent, type StreamEvent } from "./stream-event.js";

export interface HourlyStore {
  read(key: string): Promise<{ body: string; etag: string } | undefined>;
  write(key: string, body: string, etag?: string): Promise<void>;
}

const s3 = new S3Client({});
const bucket = () => {
  if (!process.env.BUCKET_NAME) throw new Error("BUCKET_NAME is required");
  return process.env.BUCKET_NAME;
};
const store: HourlyStore = {
  async read(key) {
    try {
      const result = await s3.send(
        new GetObjectCommand({ Bucket: bucket(), Key: key }),
      );
      if (!result.Body || !result.ETag) throw new Error(`Incomplete S3 object: ${key}`);
      return { body: await result.Body.transformToString(), etag: result.ETag };
    } catch (error) {
      if (error instanceof S3ServiceException && error.name === "NoSuchKey") return undefined;
      throw error;
    }
  },
  async write(key, body, etag) {
    await s3.send(new PutObjectCommand({
      Bucket: bucket(),
      Key: key,
      Body: body,
      ContentType: "application/x-ndjson",
      ...(etag ? { IfMatch: etag } : { IfNoneMatch: "*" }),
    }));
  },
};

async function appendHour(
  target: HourlyStore,
  key: string,
  events: StreamEvent[],
): Promise<void> {
  for (let attempt = 0; attempt < 8; attempt++) {
    const existing = await target.read(key);
    const lines = existing?.body.split("\n").filter(Boolean) ?? [];
    const ids = new Set(lines.map((line) => parseStreamEvent(line).eventID));
    const additions: string[] = [];
    for (const event of events) {
      if (!ids.has(event.eventID)) {
        additions.push(JSON.stringify(event));
        ids.add(event.eventID);
      }
    }
    if (additions.length === 0) return;
    try {
      await target.write(key, [...lines, ...additions].join("\n") + "\n", existing?.etag);
      return;
    } catch (error) {
      const conflict = error instanceof S3ServiceException &&
        [409, 412].includes(error.$metadata.httpStatusCode ?? 0);
      if (!conflict || attempt === 7) throw error;
      await new Promise((resolve) =>
        setTimeout(resolve, Math.random() * Math.min(1000, 25 * 2 ** attempt)),
      );
    }
  }
}

export function createSqsHandler(target: HourlyStore) {
  return async (event: SQSEvent): Promise<SQSBatchResponse> => {
    const failures: { itemIdentifier: string }[] = [];
    const groups = new Map<string, { record: SQSRecord; event: StreamEvent }[]>();
    for (const record of event.Records) {
      try {
        const parsed = parseStreamEvent(record.body);
        const key = hourlyKey(parsed);
        const group = groups.get(key) ?? [];
        group.push({ record, event: parsed });
        groups.set(key, group);
      } catch (error) {
        console.error(`Invalid SQS message ${record.messageId}`, error);
        failures.push({ itemIdentifier: record.messageId });
      }
    }
    for (const [key, group] of groups) {
      try {
        await appendHour(target, key, group.map((item) => item.event));
        console.log(`Archived ${group.length} message(s) to ${key}`);
      } catch (error) {
        console.error(`Failed to archive ${key}`, error);
        failures.push(...group.map(({ record }) => ({ itemIdentifier: record.messageId })));
      }
    }
    return { batchItemFailures: failures };
  };
}

export const handler = createSqsHandler(store);
