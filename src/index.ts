import { runRemoteCommand } from "./ssh-remote.js";
import { scanAllItems, destroyClient, TABLE_NAME } from "./dynamo-read.js";

await runRemoteCommand();

async function testDynamoRead() {
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

await testDynamoRead();
