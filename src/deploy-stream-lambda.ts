import {
  DynamoDBClient,
  DescribeTableCommand,
  UpdateTableCommand,
} from "@aws-sdk/client-dynamodb";
import {
  IAMClient,
  GetRoleCommand,
  CreateRoleCommand,
  AttachRolePolicyCommand,
  NoSuchEntityException,
} from "@aws-sdk/client-iam";
import {
  LambdaClient,
  GetFunctionCommand,
  CreateFunctionCommand,
  UpdateFunctionCodeCommand,
  ListEventSourceMappingsCommand,
  CreateEventSourceMappingCommand,
  ResourceNotFoundException,
  InvalidParameterValueException,
  waitUntilFunctionUpdatedV2,
} from "@aws-sdk/client-lambda";
import { execSync } from "node:child_process";
import { readFile } from "node:fs/promises";

const REGION = process.env.AWS_REGION ?? "us-east-2";
const TABLE_NAME = "test";
const FUNCTION_NAME = "test-table-stream-handler";
const ROLE_NAME = "test-table-stream-lambda-role";
const STREAM_POLICY_ARN =
  "arn:aws:iam::aws:policy/service-role/AWSLambdaDynamoDBExecutionRole";

const dynamo = new DynamoDBClient({ region: REGION });
const iam = new IAMClient({ region: REGION });
const lambda = new LambdaClient({ region: REGION });

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Enable the stream on the table (if needed) and return its ARN. */
async function ensureStreamEnabled(): Promise<string> {
  const { Table } = await dynamo.send(
    new DescribeTableCommand({ TableName: TABLE_NAME }),
  );

  if (!Table?.StreamSpecification?.StreamEnabled) {
    console.log(`Enabling stream on table "${TABLE_NAME}"...`);
    await dynamo.send(
      new UpdateTableCommand({
        TableName: TABLE_NAME,
        StreamSpecification: {
          StreamEnabled: true,
          StreamViewType: "NEW_AND_OLD_IMAGES",
        },
      }),
    );
  }

  // Wait until the table is ACTIVE and the stream ARN is available
  for (let i = 0; i < 30; i++) {
    const { Table: t } = await dynamo.send(
      new DescribeTableCommand({ TableName: TABLE_NAME }),
    );
    if (t?.TableStatus === "ACTIVE" && t.LatestStreamArn) {
      console.log(`Stream ARN: ${t.LatestStreamArn}`);
      return t.LatestStreamArn;
    }
    await sleep(2000);
  }
  throw new Error("Timed out waiting for the table stream to become active");
}

/** Create the Lambda execution role (if needed) and return its ARN. */
async function ensureRole(): Promise<string> {
  try {
    const { Role } = await iam.send(
      new GetRoleCommand({ RoleName: ROLE_NAME }),
    );
    console.log(`Using existing role: ${Role!.Arn}`);
    return Role!.Arn!;
  } catch (error) {
    if (!(error instanceof NoSuchEntityException)) throw error;
  }

  console.log(`Creating role "${ROLE_NAME}"...`);
  const { Role } = await iam.send(
    new CreateRoleCommand({
      RoleName: ROLE_NAME,
      AssumeRolePolicyDocument: JSON.stringify({
        Version: "2012-10-17",
        Statement: [
          {
            Effect: "Allow",
            Principal: { Service: "lambda.amazonaws.com" },
            Action: "sts:AssumeRole",
          },
        ],
      }),
    }),
  );
  // Grants DynamoDB stream read + CloudWatch Logs write
  await iam.send(
    new AttachRolePolicyCommand({
      RoleName: ROLE_NAME,
      PolicyArn: STREAM_POLICY_ARN,
    }),
  );
  console.log(`Created role: ${Role!.Arn}`);
  return Role!.Arn!;
}

/** Bundle the handler with esbuild and zip it; returns the zip contents. */
async function buildZip(): Promise<Uint8Array> {
  console.log("Bundling handler with esbuild...");
  execSync(
    "npx esbuild src/dynamo-stream-handler.ts --bundle --platform=node " +
      "--target=node22 --format=esm --outfile=dist/lambda/index.mjs",
    { stdio: "inherit" },
  );
  execSync(
    'powershell -NoProfile -Command "Compress-Archive -Path dist/lambda/index.mjs -DestinationPath dist/lambda/function.zip -Force"',
    { stdio: "inherit" },
  );
  return readFile("dist/lambda/function.zip");
}

/** Create the Lambda function or update its code; returns the function ARN. */
async function ensureFunction(
  roleArn: string,
  zip: Uint8Array,
): Promise<string> {
  try {
    const { Configuration } = await lambda.send(
      new GetFunctionCommand({ FunctionName: FUNCTION_NAME }),
    );
    console.log(`Updating code of existing function "${FUNCTION_NAME}"...`);
    await lambda.send(
      new UpdateFunctionCodeCommand({
        FunctionName: FUNCTION_NAME,
        ZipFile: zip,
      }),
    );
    await waitUntilFunctionUpdatedV2(
      { client: lambda, maxWaitTime: 120 },
      { FunctionName: FUNCTION_NAME },
    );
    return Configuration!.FunctionArn!;
  } catch (error) {
    if (!(error instanceof ResourceNotFoundException)) throw error;
  }

  console.log(`Creating function "${FUNCTION_NAME}"...`);
  // Retry: a freshly created IAM role takes a few seconds to propagate
  for (let i = 0; ; i++) {
    try {
      const result = await lambda.send(
        new CreateFunctionCommand({
          FunctionName: FUNCTION_NAME,
          Runtime: "nodejs22.x",
          Role: roleArn,
          Handler: "index.handler",
          Code: { ZipFile: zip },
          Description: `Logs stream events from DynamoDB table "${TABLE_NAME}"`,
          Timeout: 30,
          MemorySize: 128,
        }),
      );
      console.log(`Created function: ${result.FunctionArn}`);
      return result.FunctionArn!;
    } catch (error) {
      if (error instanceof InvalidParameterValueException && i < 10) {
        console.log("Waiting for IAM role propagation...");
        await sleep(3000);
        continue;
      }
      throw error;
    }
  }
}

/** Wire the table stream to the Lambda (if not already wired). */
async function ensureEventSourceMapping(streamArn: string) {
  const { EventSourceMappings } = await lambda.send(
    new ListEventSourceMappingsCommand({ FunctionName: FUNCTION_NAME }),
  );
  const existing = EventSourceMappings?.find(
    (m) => m.EventSourceArn === streamArn,
  );
  if (existing) {
    console.log(
      `Event source mapping already exists (${existing.UUID}, state: ${existing.State})`,
    );
    return;
  }

  console.log("Creating event source mapping...");
  const mapping = await lambda.send(
    new CreateEventSourceMappingCommand({
      FunctionName: FUNCTION_NAME,
      EventSourceArn: streamArn,
      StartingPosition: "LATEST",
      BatchSize: 100,
      MaximumBatchingWindowInSeconds: 0,
    }),
  );
  console.log(`Created event source mapping: ${mapping.UUID}`);
}

export async function deployStreamLambda() {
  try {
    const streamArn = await ensureStreamEnabled();
    const roleArn = await ensureRole();
    const zip = await buildZip();
    await ensureFunction(roleArn, zip);
    await ensureEventSourceMapping(streamArn);
    console.log(
      `\nDone. Writes to table "${TABLE_NAME}" now invoke "${FUNCTION_NAME}" ` +
        `(logs: /aws/lambda/${FUNCTION_NAME}).`,
    );
  } catch (error) {
    console.error("Deployment error:", error);
    process.exitCode = 1;
  } finally {
    dynamo.destroy();
    iam.destroy();
    lambda.destroy();
  }
}
