import {
  DynamoDBClient, DescribeTableCommand, CreateTableCommand, UpdateTableCommand,
  ResourceNotFoundException as TableNotFound, waitUntilTableExists,
} from "@aws-sdk/client-dynamodb";
import {
  IAMClient, GetRoleCommand, CreateRoleCommand, AttachRolePolicyCommand,
  PutRolePolicyCommand, NoSuchEntityException,
} from "@aws-sdk/client-iam";
import {
  LambdaClient, GetFunctionCommand, CreateFunctionCommand,
  UpdateFunctionCodeCommand, UpdateFunctionConfigurationCommand,
  ListEventSourceMappingsCommand, CreateEventSourceMappingCommand,
  UpdateEventSourceMappingCommand, GetEventSourceMappingCommand,
  ResourceNotFoundException, InvalidParameterValueException,
  waitUntilFunctionUpdatedV2, waitUntilFunctionActiveV2,
} from "@aws-sdk/client-lambda";
import {
  PipesClient, DescribePipeCommand, CreatePipeCommand, UpdatePipeCommand,
  NotFoundException, ValidationException,
} from "@aws-sdk/client-pipes";
import {
  SQSClient, CreateQueueCommand, GetQueueUrlCommand,
  GetQueueAttributesCommand, SetQueueAttributesCommand, QueueDoesNotExist,
} from "@aws-sdk/client-sqs";
import {
  S3Client, HeadBucketCommand, CreateBucketCommand,
  PutPublicAccessBlockCommand, PutBucketEncryptionCommand, S3ServiceException,
  type BucketLocationConstraint,
} from "@aws-sdk/client-s3";
import { STSClient, GetCallerIdentityCommand } from "@aws-sdk/client-sts";
import { execFileSync } from "node:child_process";
import { mkdir, readFile } from "node:fs/promises";
import { build } from "esbuild";
import { resolve, join } from "node:path";

const REGION = process.env.AWS_REGION ?? "us-east-2";
const PREFIX = "pipe-demo";
const TABLE_NAME = `${PREFIX}-table`;
const PIPE_NAME = `${PREFIX}-table-to-sqs`;
const PIPE_ROLE = `${PREFIX}-pipe-role`;
const QUEUE_NAME = `${PREFIX}-events`;
const QUEUE_DLQ_NAME = `${QUEUE_NAME}-dlq`;
const PIPE_DLQ_NAME = `${PREFIX}-pipe-dlq`;
const WRITER_NAME = `${PREFIX}-sqs-s3-writer`;
const WRITER_ROLE = `${PREFIX}-sqs-s3-writer-role`;
const ENTRY = "pipe-sqs-s3-handler";

const dynamo = new DynamoDBClient({ region: REGION });
const iam = new IAMClient({ region: REGION });
const lambda = new LambdaClient({ region: REGION });
const pipes = new PipesClient({ region: REGION });
const sqs = new SQSClient({ region: REGION });
const s3 = new S3Client({ region: REGION });
const sts = new STSClient({ region: REGION });
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function ensureTable(): Promise<string> {
  let table;
  try {
    table = (await dynamo.send(new DescribeTableCommand({ TableName: TABLE_NAME }))).Table;
  } catch (error) {
    if (!(error instanceof TableNotFound)) throw error;
  }
  if (!table) {
    await dynamo.send(new CreateTableCommand({
      TableName: TABLE_NAME,
      BillingMode: "PAY_PER_REQUEST",
      AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
      KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
      StreamSpecification: { StreamEnabled: true, StreamViewType: "NEW_AND_OLD_IMAGES" },
    }));
    await waitUntilTableExists({ client: dynamo, maxWaitTime: 300 }, { TableName: TABLE_NAME });
  } else if (!table.StreamSpecification?.StreamEnabled) {
    await dynamo.send(new UpdateTableCommand({
      TableName: TABLE_NAME,
      StreamSpecification: { StreamEnabled: true, StreamViewType: "NEW_AND_OLD_IMAGES" },
    }));
  }
  for (let i = 0; i < 90; i++) {
    const { Table } = await dynamo.send(new DescribeTableCommand({ TableName: TABLE_NAME }));
    if (Table?.TableStatus === "ACTIVE" && Table.LatestStreamArn) {
      console.log(`Table ready: ${TABLE_NAME} (stream ${Table.LatestStreamArn})`);
      return Table.LatestStreamArn;
    }
    await sleep(2000);
  }
  throw new Error(`Timed out waiting for ${TABLE_NAME} stream`);
}

async function ensureQueue(name: string, attributes: Record<string, string>) {
  let url: string | undefined;
  try {
    url = (await sqs.send(new GetQueueUrlCommand({ QueueName: name }))).QueueUrl;
  } catch (error) {
    if (!(error instanceof QueueDoesNotExist)) throw error;
  }
  if (!url) {
    url = (await sqs.send(new CreateQueueCommand({ QueueName: name, Attributes: attributes }))).QueueUrl;
  } else {
    await sqs.send(new SetQueueAttributesCommand({ QueueUrl: url, Attributes: attributes }));
  }
  if (!url) throw new Error(`Missing queue URL for ${name}`);
  const { Attributes } = await sqs.send(new GetQueueAttributesCommand({
    QueueUrl: url, AttributeNames: ["QueueArn"],
  }));
  if (!Attributes?.QueueArn) throw new Error(`Missing queue ARN for ${name}`);
  console.log(`Queue ready: ${url}`);
  return { url, arn: Attributes.QueueArn };
}

async function ensureBucket(name: string) {
  try {
    await s3.send(new HeadBucketCommand({ Bucket: name }));
  } catch (error) {
    if (!(error instanceof S3ServiceException) || error.$metadata.httpStatusCode !== 404) throw error;
    await s3.send(new CreateBucketCommand({
      Bucket: name,
      ...(REGION === "us-east-1" ? {} : {
        CreateBucketConfiguration: { LocationConstraint: REGION as BucketLocationConstraint },
      }),
    }));
  }
  await s3.send(new PutPublicAccessBlockCommand({
    Bucket: name,
    PublicAccessBlockConfiguration: {
      BlockPublicAcls: true, IgnorePublicAcls: true,
      BlockPublicPolicy: true, RestrictPublicBuckets: true,
    },
  }));
  await s3.send(new PutBucketEncryptionCommand({
    Bucket: name,
    ServerSideEncryptionConfiguration: {
      Rules: [{ ApplyServerSideEncryptionByDefault: { SSEAlgorithm: "AES256" } }],
    },
  }));
  console.log(`Bucket ready: ${name}`);
}

async function ensureRole(
  name: string, trust: object, policy: object, managedPolicy?: string,
): Promise<{ arn: string; created: boolean }> {
  let arn: string | undefined;
  try {
    arn = (await iam.send(new GetRoleCommand({ RoleName: name }))).Role?.Arn;
  } catch (error) {
    if (!(error instanceof NoSuchEntityException)) throw error;
  }
  const created = !arn;
  if (!arn) {
    arn = (await iam.send(new CreateRoleCommand({
      RoleName: name, AssumeRolePolicyDocument: JSON.stringify(trust),
    }))).Role?.Arn;
  }
  if (!arn) throw new Error(`Missing role ARN for ${name}`);
  if (managedPolicy) {
    await iam.send(new AttachRolePolicyCommand({ RoleName: name, PolicyArn: managedPolicy }));
  }
  await iam.send(new PutRolePolicyCommand({
    RoleName: name, PolicyName: `${PREFIX}-access`, PolicyDocument: JSON.stringify(policy),
  }));
  console.log(`Role ready: ${name}`);
  return { arn, created };
}

async function buildZip(): Promise<Uint8Array> {
  const directory = resolve("dist", ENTRY);
  await mkdir(directory, { recursive: true });
  await build({
    entryPoints: [resolve("src", `${ENTRY}.ts`)],
    bundle: true, platform: "node", target: "node22",
    format: "cjs", outfile: join(directory, "index.cjs"),
  });
  const zip = join(directory, "function.zip");
  execFileSync("powershell", [
    "-NoProfile", "-Command",
    `Compress-Archive -LiteralPath '${join(directory, "index.cjs").replaceAll("'", "''")}' -DestinationPath '${zip.replaceAll("'", "''")}' -Force`,
  ], { stdio: "inherit" });
  return readFile(zip);
}

async function ensureFunction(role: string, bucket: string) {
  const zip = await buildZip();
  let exists = false;
  try {
    await lambda.send(new GetFunctionCommand({ FunctionName: WRITER_NAME }));
    exists = true;
  } catch (error) {
    if (!(error instanceof ResourceNotFoundException)) throw error;
  }
  const settings = {
    FunctionName: WRITER_NAME, Runtime: "nodejs22.x" as const, Role: role,
    Handler: "index.handler", Timeout: 60, MemorySize: 256,
    Environment: { Variables: { BUCKET_NAME: bucket } },
  };
  const wait = () => waitUntilFunctionUpdatedV2({ client: lambda, maxWaitTime: 180 }, { FunctionName: WRITER_NAME });
  if (exists) {
    await wait();
    await lambda.send(new UpdateFunctionConfigurationCommand(settings));
    await wait();
    await lambda.send(new UpdateFunctionCodeCommand({ FunctionName: WRITER_NAME, ZipFile: zip }));
    await wait();
  } else {
    for (let attempt = 0; ; attempt++) {
      try {
        await lambda.send(new CreateFunctionCommand({ ...settings, Code: { ZipFile: zip } }));
        break;
      } catch (error) {
        if (!(error instanceof InvalidParameterValueException) ||
          !error.message.includes("cannot be assumed by Lambda") || attempt >= 10) throw error;
        console.log("Waiting for IAM role propagation for the writer Lambda");
        await sleep(3000);
      }
    }
    await waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 180 }, { FunctionName: WRITER_NAME });
  }
  console.log(`Function ready: ${WRITER_NAME}`);
}

async function ensureMapping(queueArn: string) {
  const existing = await lambda.send(new ListEventSourceMappingsCommand({
    FunctionName: WRITER_NAME, EventSourceArn: queueArn,
  }));
  let uuid = existing.EventSourceMappings?.[0]?.UUID;
  const settings = {
    FunctionName: WRITER_NAME, Enabled: true, BatchSize: 10,
    MaximumBatchingWindowInSeconds: 0,
    FunctionResponseTypes: ["ReportBatchItemFailures" as const],
  };
  if (uuid) {
    await lambda.send(new UpdateEventSourceMappingCommand({ UUID: uuid, ...settings }));
  } else {
    for (let attempt = 0; ; attempt++) {
      try {
        uuid = (await lambda.send(new CreateEventSourceMappingCommand({
          ...settings, EventSourceArn: queueArn,
        }))).UUID;
        break;
      } catch (error) {
        if (!(error instanceof InvalidParameterValueException) ||
          !error.message.includes("execution role") || attempt >= 10) throw error;
        console.log("Waiting for SQS permissions on the writer role");
        await sleep(3000);
      }
    }
  }
  if (!uuid) throw new Error("Missing SQS event source mapping");
  for (let i = 0; i < 90; i++) {
    const mapping = await lambda.send(new GetEventSourceMappingCommand({ UUID: uuid }));
    if (mapping.State === "Enabled") {
      console.log(`Mapping enabled: ${QUEUE_NAME} -> ${WRITER_NAME} (${uuid})`);
      return;
    }
    if (mapping.State === "Disabled") {
      throw new Error(`Mapping ${uuid} disabled: ${mapping.StateTransitionReason}`);
    }
    await sleep(2000);
  }
  throw new Error("Timed out enabling SQS mapping");
}

async function ensurePipe(role: string, streamArn: string, queueArn: string, dlqArn: string) {
  const streamParameters = {
    BatchSize: 10,
    MaximumBatchingWindowInSeconds: 0,
    MaximumRetryAttempts: 10,
    DeadLetterConfig: { Arn: dlqArn },
  };
  let exists = false;
  try {
    await pipes.send(new DescribePipeCommand({ Name: PIPE_NAME }));
    exists = true;
  } catch (error) {
    if (!(error instanceof NotFoundException)) throw error;
  }
  for (let attempt = 0; ; attempt++) {
    try {
      if (exists) {
        await pipes.send(new UpdatePipeCommand({
          Name: PIPE_NAME, RoleArn: role, DesiredState: "RUNNING", Target: queueArn,
          SourceParameters: { DynamoDBStreamParameters: streamParameters },
        }));
      } else {
        await pipes.send(new CreatePipeCommand({
          Name: PIPE_NAME, RoleArn: role, DesiredState: "RUNNING",
          Source: streamArn, Target: queueArn,
          Description: `${TABLE_NAME} stream -> ${QUEUE_NAME}`,
          SourceParameters: {
            DynamoDBStreamParameters: { ...streamParameters, StartingPosition: "TRIM_HORIZON" },
          },
        }));
      }
      break;
    } catch (error) {
      if (!(error instanceof ValidationException) || attempt >= 10) throw error;
      console.log(`Waiting for pipe role propagation: ${error.message}`);
      await sleep(5000);
    }
  }
  for (let i = 0; i < 120; i++) {
    const pipe = await pipes.send(new DescribePipeCommand({ Name: PIPE_NAME }));
    if (pipe.CurrentState === "RUNNING") {
      console.log(`Pipe running: ${PIPE_NAME}`);
      return;
    }
    if (pipe.CurrentState?.endsWith("FAILED")) {
      throw new Error(`Pipe ${PIPE_NAME} ${pipe.CurrentState}: ${pipe.StateReason}`);
    }
    await sleep(5000);
  }
  throw new Error(`Timed out waiting for pipe ${PIPE_NAME}`);
}

export async function deployPipePipeline() {
  try {
    const identity = await sts.send(new GetCallerIdentityCommand({}));
    if (!identity.Account || !identity.Arn) throw new Error("Unable to determine AWS account");
    const partition = identity.Arn.split(":")[1];
    const bucket = `${PREFIX}-events-${identity.Account}-${REGION}`;
    console.log(`Deploying ${PREFIX} pipeline to account ${identity.Account}, region ${REGION}`);

    const streamArn = await ensureTable();
    const queueDlq = await ensureQueue(QUEUE_DLQ_NAME, {
      MessageRetentionPeriod: "1209600", SqsManagedSseEnabled: "true",
    });
    const pipeDlq = await ensureQueue(PIPE_DLQ_NAME, {
      MessageRetentionPeriod: "1209600", SqsManagedSseEnabled: "true",
    });
    const queue = await ensureQueue(QUEUE_NAME, {
      VisibilityTimeout: "360", MessageRetentionPeriod: "1209600",
      SqsManagedSseEnabled: "true",
      RedrivePolicy: JSON.stringify({ deadLetterTargetArn: queueDlq.arn, maxReceiveCount: 5 }),
    });
    await ensureBucket(bucket);

    const writerRole = await ensureRole(WRITER_ROLE, {
      Version: "2012-10-17",
      Statement: [{
        Effect: "Allow", Principal: { Service: "lambda.amazonaws.com" }, Action: "sts:AssumeRole",
      }],
    }, {
      Version: "2012-10-17",
      Statement: [
        {
          Effect: "Allow",
          Action: ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
          Resource: queue.arn,
        },
        {
          Effect: "Allow", Action: ["s3:PutObject"],
          Resource: `arn:${partition}:s3:::${bucket}/events/*`,
        },
      ],
    }, `arn:${partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole`);
    const pipeRole = await ensureRole(PIPE_ROLE, {
      Version: "2012-10-17",
      Statement: [{
        Effect: "Allow", Principal: { Service: "pipes.amazonaws.com" }, Action: "sts:AssumeRole",
        Condition: { StringEquals: { "aws:SourceAccount": identity.Account } },
      }],
    }, {
      Version: "2012-10-17",
      Statement: [
        {
          Effect: "Allow",
          Action: [
            "dynamodb:DescribeStream", "dynamodb:GetRecords",
            "dynamodb:GetShardIterator", "dynamodb:ListStreams",
          ],
          Resource: streamArn,
        },
        { Effect: "Allow", Action: ["sqs:SendMessage"], Resource: [queue.arn, pipeDlq.arn] },
      ],
    });

    // Start the consumer before the pipe so no message waits on a missing reader.
    await ensureFunction(writerRole.arn, bucket);
    await ensureMapping(queue.arn);
    if (pipeRole.created) await sleep(10000);
    await ensurePipe(pipeRole.arn, streamArn, queue.arn, pipeDlq.arn);

    console.log(`Deployment complete: ${TABLE_NAME} -> pipe ${PIPE_NAME} -> ${QUEUE_NAME} -> ${WRITER_NAME} -> s3://${bucket}/events/`);
  } catch (error) {
    console.error("Deployment error:", error);
    process.exitCode = 1;
  } finally {
    for (const client of [dynamo, iam, lambda, pipes, sqs, s3, sts]) client.destroy();
  }
}
