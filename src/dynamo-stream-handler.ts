import { unmarshall } from "@aws-sdk/util-dynamodb";
import type { AttributeValue } from "@aws-sdk/client-dynamodb";
import type { DynamoDBStreamHandler } from "aws-lambda";

/**
 * Lambda handler invoked by the DynamoDB stream of the "test" table.
 * Logs every INSERT / MODIFY / REMOVE event with unmarshalled images.
 */
export const handler: DynamoDBStreamHandler = async (event) => {
  console.log(`Received ${event.Records.length} stream record(s)`);

  for (const record of event.Records) {
    const { eventName, dynamodb } = record;

    const keys = dynamodb?.Keys
      ? unmarshall(dynamodb.Keys as Record<string, AttributeValue>)
      : undefined;
    const newImage = dynamodb?.NewImage
      ? unmarshall(dynamodb.NewImage as Record<string, AttributeValue>)
      : undefined;
    const oldImage = dynamodb?.OldImage
      ? unmarshall(dynamodb.OldImage as Record<string, AttributeValue>)
      : undefined;

    console.log(
      JSON.stringify({ eventName, keys, newImage, oldImage }, null, 2),
    );
  }
};
