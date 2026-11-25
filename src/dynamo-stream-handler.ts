import { unmarshall } from "@aws-sdk/util-dynamodb";
import type { AttributeValue } from "@aws-sdk/client-dynamodb";
import type { DynamoDBStreamEvent, DynamoDBBatchResponse } from "aws-lambda";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { parseStreamEvent } from "./stream-event.js";

const sqs = new SQSClient({});

export function createStreamHandler(
  send: (body: string) => Promise<void>,
) {
  return async (event: DynamoDBStreamEvent): Promise<DynamoDBBatchResponse> => {
    console.log(`Received ${event.Records.length} stream record(s)`);
    for (const record of event.Records) {
      try {
        const { eventName, dynamodb } = record;
        const image = (value: Record<string, unknown> | undefined) =>
          value ? unmarshall(value as Record<string, AttributeValue>) : undefined;
        const body = JSON.stringify({
          eventID: record.eventID,
          eventName,
          eventSourceARN: record.eventSourceARN,
          approximateCreationDateTime: dynamodb?.ApproximateCreationDateTime,
          keys: image(dynamodb?.Keys),
          newImage: image(dynamodb?.NewImage),
          oldImage: image(dynamodb?.OldImage),
        });
        parseStreamEvent(body);
        await send(body);
      } catch (error) {
        console.error(`Failed to enqueue stream record ${record.eventID}`, error);
        if (!record.dynamodb?.SequenceNumber) throw error;
        return {
          batchItemFailures: [{ itemIdentifier: record.dynamodb.SequenceNumber }],
        };
      }
    }
    return { batchItemFailures: [] };
  };
}

export const handler = createStreamHandler(async (body) => {
  const QueueUrl = process.env.QUEUE_URL;
  if (!QueueUrl) throw new Error("QUEUE_URL is required");
  await sqs.send(new SendMessageCommand({ QueueUrl, MessageBody: body }));
});
