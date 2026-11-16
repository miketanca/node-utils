import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  GetCommand,
  ScanCommand,
} from "@aws-sdk/lib-dynamodb";

export const TABLE_NAME = "test";

// Credentials are resolved automatically from the environment
// (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, ~/.aws/credentials, SSO, etc.)
const client = new DynamoDBClient({
  region: process.env.AWS_REGION ?? "us-east-2",
});

// Document client marshals/unmarshals plain JS objects for you
const docClient = DynamoDBDocumentClient.from(client);

/** Read every item in the table (paginated scan). */
export async function scanAllItems() {
  const items: Record<string, unknown>[] = [];
  let lastEvaluatedKey: Record<string, unknown> | undefined;

  do {
    const result = await docClient.send(
      new ScanCommand({
        TableName: TABLE_NAME,
        ExclusiveStartKey: lastEvaluatedKey,
      }),
    );
    items.push(...(result.Items ?? []));
    lastEvaluatedKey = result.LastEvaluatedKey;
  } while (lastEvaluatedKey);

  return items;
}

/** Read a single item by its primary key. Adjust the key name to match your table schema. */
export async function getItemById(id: string) {
  const result = await docClient.send(
    new GetCommand({
      TableName: TABLE_NAME,
      Key: { id },
    }),
  );
  return result.Item;
}

/** Close the underlying DynamoDB client connection. */
export function destroyClient() {
  client.destroy();
}

export async function testDynamoRead() {
  try {
    const items = await scanAllItems();
    console.log(`Found ${items.length} item(s) in table "${TABLE_NAME}":`);
    console.log(JSON.stringify(items, null, 2));

    // Example of reading a single item by key:
    // const item = await getItemById("some-id");
    // console.log("Single item:", item);
  } catch (error) {
    console.error("DynamoDB read error:", error);
    process.exitCode = 1;
  } finally {
    destroyClient();
  }
}
