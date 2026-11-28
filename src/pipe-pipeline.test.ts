import assert from "node:assert/strict";
import { test } from "node:test";
import type { DynamoDBRecord, SQSRecord } from "aws-lambda";
import { createPipeSqsHandler, objectKey, parsePipeMessage } from "./pipe-sqs-s3-handler.js";

const timestamp = Date.parse("2026-11-25T14:59:59Z") / 1000;
const record = (id: string): DynamoDBRecord => ({
  eventID: id, eventName: "MODIFY",
  eventSourceARN: "arn:aws:dynamodb:us-east-2:123456789012:table/pipe-demo-table/stream/x",
  dynamodb: {
    SequenceNumber: `seq-${id}`, ApproximateCreationDateTime: timestamp,
    Keys: { id: { S: id } },
    NewImage: { id: { S: id }, value: { N: "42" } }, OldImage: { id: { S: id }, value: { N: "41" } },
  },
});
const message = (id: string, body = JSON.stringify(record(id))): SQSRecord => ({
  messageId: id, body, receiptHandle: "receipt", attributes: {
    ApproximateReceiveCount: "1", SentTimestamp: "0",
    SenderId: "sender", ApproximateFirstReceiveTimestamp: "0",
  },
  messageAttributes: {}, md5OfBody: "", eventSource: "aws:sqs",
  eventSourceARN: "arn:aws:sqs:us-east-2:123456789012:pipe-demo-events", awsRegion: "us-east-2",
});

test("pipe messages are unmarshalled and keyed by event hour and eventID", () => {
  const [event] = parsePipeMessage(JSON.stringify(record("a/1")));
  assert.deepEqual(event, {
    eventID: "a/1", eventName: "MODIFY", eventSourceARN: record("a/1").eventSourceARN,
    approximateCreationDateTime: timestamp, sequenceNumber: "seq-a/1",
    keys: { id: "a/1" }, newImage: { id: "a/1", value: 42 }, oldImage: { id: "a/1", value: 41 },
  });
  assert.equal(objectKey(event!), "events/2026/11/25/14/a%2F1.json");
});

test("handler writes each event once per message and reports only failed messages", async () => {
  const written = new Map<string, string>();
  const handler = createPipeSqsHandler(async (key, body) => {
    if (key.includes("fail")) throw new Error("S3 unavailable");
    written.set(key, body);
  });
  const response = await handler({ Records: [
    message("a"), message("invalid", "{}"), message("fail"),
    message("batch", JSON.stringify([record("b"), record("c")])),
  ] });
  assert.deepEqual(
    new Set(response.batchItemFailures.map((failure) => failure.itemIdentifier)),
    new Set(["invalid", "fail"]),
  );
  assert.deepEqual([...written.keys()].sort(), [
    "events/2026/11/25/14/a.json", "events/2026/11/25/14/b.json", "events/2026/11/25/14/c.json",
  ]);
});
