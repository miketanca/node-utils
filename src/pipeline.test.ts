import assert from "node:assert/strict";
import { test } from "node:test";
import { S3ServiceException } from "@aws-sdk/client-s3";
import type { DynamoDBRecord, SQSRecord } from "aws-lambda";
import { createStreamHandler } from "./dynamo-stream-handler.js";
import { createSqsHandler, type HourlyStore } from "./sqs-s3-handler.js";
import { hourlyKey, parseStreamEvent, type StreamEvent } from "./stream-event.js";

const timestamp = Date.parse("2026-11-25T14:59:59Z") / 1000;
const event = (id: string, time = timestamp): StreamEvent => ({
  eventID: id, eventName: "INSERT", approximateCreationDateTime: time,
  keys: { id }, newImage: { id, value: 42 },
});
const message = (id: string, body = JSON.stringify(event(id))): SQSRecord => ({
  messageId: id, body, receiptHandle: "receipt", attributes: {
    ApproximateReceiveCount: "1", SentTimestamp: "0",
    SenderId: "sender", ApproximateFirstReceiveTimestamp: "0",
  },
  messageAttributes: {}, md5OfBody: "", eventSource: "aws:sqs",
  eventSourceARN: "arn:aws:sqs:us-east-2:123456789012:events", awsRegion: "us-east-2",
});
const streamRecord = (id: string): DynamoDBRecord => ({
  eventID: id, eventName: "MODIFY",
  eventSourceARN: "arn:aws:dynamodb:us-east-2:123456789012:table/test/stream/test",
  dynamodb: {
    SequenceNumber: id, ApproximateCreationDateTime: timestamp,
    Keys: { id: { S: id } },
    NewImage: { value: { N: "42" } }, OldImage: { value: { N: "41" } },
  },
});

class MemoryStore implements HourlyStore {
  objects = new Map<string, { body: string; etag: string }>();
  writes = 0;
  async read(key: string) { return this.objects.get(key); }
  async write(key: string, body: string, etag?: string) {
    if (this.objects.get(key)?.etag !== etag) {
      throw new S3ServiceException({
        name: "PreconditionFailed", $fault: "client",
        $metadata: { httpStatusCode: 412 },
      });
    }
    this.objects.set(key, { body, etag: String(++this.writes) });
  }
}

test("stream forwards unmarshalled images and stable metadata", async () => {
  const sent: string[] = [];
  const handler = createStreamHandler(async (body) => { sent.push(body); });
  assert.deepEqual(await handler({ Records: [streamRecord("1")] }), { batchItemFailures: [] });
  assert.deepEqual(parseStreamEvent(sent[0]!), {
    eventID: "1", eventName: "MODIFY",
    eventSourceARN: streamRecord("1").eventSourceARN,
    approximateCreationDateTime: timestamp,
    keys: { id: "1" }, newImage: { value: 42 }, oldImage: { value: 41 },
  });
});

test("stream stops at failed send and checkpoints its sequence number", async () => {
  const sent: string[] = [];
  const handler = createStreamHandler(async (body) => {
    const parsed = parseStreamEvent(body);
    if (parsed.eventID === "2") throw new Error("SQS unavailable");
    sent.push(parsed.eventID);
  });
  assert.deepEqual(await handler({ Records: ["1", "2", "3"].map(streamRecord) }), {
    batchItemFailures: [{ itemIdentifier: "2" }],
  });
  assert.deepEqual(sent, ["1"]);
});

test("writes one file per UTC event hour and deduplicates redeliveries", async () => {
  const store = new MemoryStore();
  const handler = createSqsHandler(store);
  const records = [
    message("a"), message("b"), message("a-duplicate", JSON.stringify(event("a"))),
    message("c", JSON.stringify(event("c", timestamp + 1))),
  ];
  assert.deepEqual(await handler({ Records: records }), { batchItemFailures: [] });
  await handler({ Records: records });
  assert.equal(store.objects.size, 2);
  assert.equal(store.writes, 2);
  const first = store.objects.get("events/2026-11-25/14.jsonl")!;
  assert.ok(first.body.endsWith("\n"));
  assert.deepEqual(first.body.trim().split("\n").map((line) => parseStreamEvent(line).eventID), ["a", "b"]);
  assert.equal(store.objects.get("events/2026-11-25/15.jsonl")!.body, JSON.stringify(event("c", timestamp + 1)) + "\n");
});

test("concurrent writers retry conditional conflicts without losing events", async () => {
  const store = new MemoryStore();
  const handler = createSqsHandler(store);
  const responses = await Promise.all([
    handler({ Records: [message("a")] }),
    handler({ Records: [message("b")] }),
  ]);
  assert.deepEqual(responses, [{ batchItemFailures: [] }, { batchItemFailures: [] }]);
  const lines = store.objects.get(hourlyKey(event("a")))!.body.trim().split("\n");
  assert.deepEqual(new Set(lines.map((line) => parseStreamEvent(line).eventID)), new Set(["a", "b"]));
});

test("bad messages and failed hour writes return only affected message IDs", async () => {
  const memory = new MemoryStore();
  const store: HourlyStore = {
    read: (key) => memory.read(key),
    write: async (key, body, etag) => {
      if (key.endsWith("/14.jsonl")) throw new Error("S3 unavailable");
      await memory.write(key, body, etag);
    },
  };
  const response = await createSqsHandler(store)({ Records: [
    message("invalid", "{}"), message("a"), message("b"),
    message("c", JSON.stringify(event("c", timestamp + 1))),
  ] });
  assert.deepEqual(response, {
    batchItemFailures: [
      { itemIdentifier: "invalid" }, { itemIdentifier: "a" }, { itemIdentifier: "b" },
    ],
  });
  assert.equal(memory.objects.size, 1);
});

test("exhausted conflicts and corrupt existing JSONL never acknowledge messages", async () => {
  const conflict = new S3ServiceException({
    name: "ConditionalRequestConflict", $fault: "client",
    $metadata: { httpStatusCode: 409 },
  });
  const response = await createSqsHandler({
    read: async () => undefined,
    write: async () => { throw conflict; },
  })({ Records: [message("a")] });
  assert.deepEqual(response, { batchItemFailures: [{ itemIdentifier: "a" }] });
  const corrupt = await createSqsHandler({
    read: async () => ({ body: "not JSON\n", etag: "1" }),
    write: async () => { assert.fail("must not overwrite corrupt object"); },
  })({ Records: [message("b")] });
  assert.deepEqual(corrupt, { batchItemFailures: [{ itemIdentifier: "b" }] });
});
