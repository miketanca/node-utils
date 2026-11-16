import { testLogRead } from "./cloudwatch-logs.js";

// await testLogWrite();
await testLogRead();

// import { runRemoteCommand } from "./ssh-remote.js";
// const result = await runRemoteCommand("aws dynamodb scan --table-name test");
// const obj = JSON.parse(result);
// console.log(obj);

// await testDynamoRead();
