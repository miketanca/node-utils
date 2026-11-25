import {
  DynamoDBClient, DescribeTableCommand, UpdateTableCommand,
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
const TABLE_NAME = "test";
const FUNCTION_NAME = "test-table-stream-handler";
const WRITER_NAME = "test-table-sqs-s3-handler";
const ROLE_NAME = "test-table-stream-lambda-role";
const WRITER_ROLE = "test-table-sqs-s3-lambda-role";
const QUEUE_NAME = "test-table-stream-events";
const DLQ_NAME = `${QUEUE_NAME}-dlq`;
const dynamo = new DynamoDBClient({ region: REGION });
const iam = new IAMClient({ region: REGION });
const lambda = new LambdaClient({ region: REGION });
const sqs = new SQSClient({ region: REGION });
const s3 = new S3Client({ region: REGION });
const sts = new STSClient({ region: REGION });
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function ensureStreamEnabled(): Promise<string> {
  const { Table } = await dynamo.send(new DescribeTableCommand({ TableName: TABLE_NAME }));
  if (!Table) throw new Error(`Table ${TABLE_NAME} was not found`);
  if (!Table.StreamSpecification?.StreamEnabled) {
    await dynamo.send(new UpdateTableCommand({
      TableName: TABLE_NAME,
      StreamSpecification: { StreamEnabled: true, StreamViewType: "NEW_AND_OLD_IMAGES" },
    }));
  } else if (Table.StreamSpecification.StreamViewType !== "NEW_AND_OLD_IMAGES") {
    throw new Error("Existing stream must use NEW_AND_OLD_IMAGES; refusing to replace it and lose pending events");
  }
  for (let i = 0; i < 60; i++) {
    const { Table: current } = await dynamo.send(new DescribeTableCommand({ TableName: TABLE_NAME }));
    if (current?.TableStatus === "ACTIVE" && current.LatestStreamArn) return current.LatestStreamArn;
    await sleep(2000);
  }
  throw new Error("Timed out waiting for the table stream");
}

async function ensureQueue(name: string, attributes: Record<string, string>) {
  let url: string | undefined;
  try {
    url = (await sqs.send(new GetQueueUrlCommand({ QueueName: name }))).QueueUrl;
  } catch (error) {
    if (!(error instanceof QueueDoesNotExist)) throw error;
  }
  if (!url) {
    url = (await sqs.send(new CreateQueueCommand({
      QueueName: name, Attributes: attributes,
    }))).QueueUrl;
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

async function ensureRole(name: string, policy: object, stream = false): Promise<string> {
  let arn: string | undefined;
  try {
    arn = (await iam.send(new GetRoleCommand({ RoleName: name }))).Role?.Arn;
  } catch (error) {
    if (!(error instanceof NoSuchEntityException)) throw error;
  }
  if (!arn) {
    arn = (await iam.send(new CreateRoleCommand({
      RoleName: name,
      AssumeRolePolicyDocument: JSON.stringify({
        Version: "2012-10-17",
        Statement: [{
          Effect: "Allow", Principal: { Service: "lambda.amazonaws.com" },
          Action: "sts:AssumeRole",
        }],
      }),
    }))).Role?.Arn;
  }
  if (!arn) throw new Error(`Missing role ARN for ${name}`);
  await iam.send(new AttachRolePolicyCommand({
    RoleName: name,
    PolicyArn: stream
      ? "arn:aws:iam::aws:policy/service-role/AWSLambdaDynamoDBExecutionRole"
      : "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
  }));
  await iam.send(new PutRolePolicyCommand({
    RoleName: name, PolicyName: "stream-archive-access",
    PolicyDocument: JSON.stringify(policy),
  }));
  return arn;
}

async function buildZip(entry: string): Promise<Uint8Array> {
  const directory = resolve("dist", entry);
  await mkdir(directory, { recursive: true });
  await build({
    entryPoints: [resolve("src", `${entry}.ts`)],
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

async function ensureFunction(
  name: string, role: string, entry: string,
  environment: Record<string, string>, timeout: number, memory: number,
): Promise<string> {
  const zip = await buildZip(entry);
  let configuration;
  try {
    configuration = (await lambda.send(new GetFunctionCommand({ FunctionName: name }))).Configuration;
  } catch (error) {
    if (!(error instanceof ResourceNotFoundException)) throw error;
  }
  const settings = {
    FunctionName: name, Runtime: "nodejs22.x" as const, Role: role,
    Handler: "index.handler", Timeout: timeout, MemorySize: memory,
    Environment: { Variables: { ...configuration?.Environment?.Variables, ...environment } },
  };
  if (configuration) {
    await waitUntilFunctionUpdatedV2({ client: lambda, maxWaitTime: 180 }, { FunctionName: name });
    await lambda.send(new UpdateFunctionConfigurationCommand(settings));
    await waitUntilFunctionUpdatedV2({ client: lambda, maxWaitTime: 180 }, { FunctionName: name });
    await lambda.send(new UpdateFunctionCodeCommand({ FunctionName: name, ZipFile: zip }));
    await waitUntilFunctionUpdatedV2({ client: lambda, maxWaitTime: 180 }, { FunctionName: name });
  } else {
    for (let attempt = 0; ; attempt++) {
      try {
        await lambda.send(new CreateFunctionCommand({ ...settings, Code: { ZipFile: zip } }));
        break;
      } catch (error) {
        if (!(error instanceof InvalidParameterValueException) ||
          !error.message.includes("cannot be assumed by Lambda") || attempt >= 10) throw error;
        console.log(`Waiting for IAM role propagation for ${name}`);
        await sleep(3000);
      }
    }
    await waitUntilFunctionActiveV2({ client: lambda, maxWaitTime: 180 }, { FunctionName: name });
  }
  const result = await lambda.send(new GetFunctionCommand({ FunctionName: name }));
  if (!result.Configuration?.FunctionArn) throw new Error(`Missing function ARN for ${name}`);
  console.log(`Function ready: ${name}`);
  return result.Configuration.FunctionArn;
}

async function ensureMapping(name: string, source: string, stream: boolean) {
  let marker: string | undefined;
  let uuid: string | undefined;
  do {
    const page = await lambda.send(new ListEventSourceMappingsCommand({
      FunctionName: name, EventSourceArn: source, Marker: marker,
    }));
    uuid = page.EventSourceMappings?.[0]?.UUID;
    marker = page.NextMarker;
  } while (!uuid && marker);
  const settings = {
    FunctionName: name, Enabled: true,
    BatchSize: stream ? 100 : 10, MaximumBatchingWindowInSeconds: 0,
    FunctionResponseTypes: ["ReportBatchItemFailures" as const],
    ...(stream ? {} : { ScalingConfig: { MaximumConcurrency: 2 } }),
  };
  if (uuid) {
    await lambda.send(new UpdateEventSourceMappingCommand({ UUID: uuid, ...settings }));
  } else {
    for (let attempt = 0; ; attempt++) {
      try {
        uuid = (await lambda.send(new CreateEventSourceMappingCommand({
          ...settings, EventSourceArn: source,
          ...(stream ? { StartingPosition: "TRIM_HORIZON" as const } : {}),
        }))).UUID;
        break;
      } catch (error) {
        if (!(error instanceof InvalidParameterValueException) ||
          !error.message.includes("execution role") || attempt >= 10) throw error;
        console.log(`Waiting for event source permissions for ${name}`);
        await sleep(3000);
      }
    }
  }
  if (!uuid) throw new Error(`Missing event source mapping for ${name}`);
  for (let i = 0; i < 90; i++) {
    const mapping = await lambda.send(new GetEventSourceMappingCommand({ UUID: uuid }));
    if (mapping.State === "Enabled") {
      console.log(`Mapping enabled: ${name} (${uuid})`);
      return;
    }
    if (mapping.State === "Failed" || mapping.State === "Disabled") {
      throw new Error(`Mapping ${uuid}: ${mapping.State}: ${mapping.StateTransitionReason}`);
    }
    await sleep(2000);
  }
  throw new Error(`Timed out enabling mapping for ${name}`);
}

export async function deployStreamLambda() {
  try {
    const identity = await sts.send(new GetCallerIdentityCommand({}));
    if (!identity.Account || !identity.Arn) throw new Error("Unable to determine AWS account");
    const partition = identity.Arn.split(":")[1];
    const bucket = `test-table-stream-events-${identity.Account}-${REGION}`;
    console.log(`Deploying to account ${identity.Account}, region ${REGION}`);
    const streamArn = await ensureStreamEnabled();
    const dlq = await ensureQueue(DLQ_NAME, {
      MessageRetentionPeriod: "1209600", SqsManagedSseEnabled: "true",
    });
    const queue = await ensureQueue(QUEUE_NAME, {
      VisibilityTimeout: "1800", MessageRetentionPeriod: "1209600",
      SqsManagedSseEnabled: "true",
      RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlq.arn, maxReceiveCount: 5 }),
    });
    await ensureBucket(bucket);
    const streamRole = await ensureRole(ROLE_NAME, {
      Version: "2012-10-17",
      Statement: [{ Effect: "Allow", Action: ["sqs:SendMessage"], Resource: queue.arn }],
    }, true);
    const writerRole = await ensureRole(WRITER_ROLE, {
      Version: "2012-10-17",
      Statement: [
        {
          Effect: "Allow",
          Action: ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
          Resource: queue.arn,
        },
        {
          Effect: "Allow", Action: ["s3:GetObject", "s3:PutObject"],
          Resource: `arn:${partition}:s3:::${bucket}/events/*`,
        },
        {
          Effect: "Allow", Action: ["s3:ListBucket"],
          Resource: `arn:${partition}:s3:::${bucket}`,
        },
      ],
    });
    // Bring the downstream consumer online before changing the existing stream handler.
    await ensureFunction(WRITER_NAME, writerRole, "sqs-s3-handler", { BUCKET_NAME: bucket }, 300, 512);
    await ensureMapping(WRITER_NAME, queue.arn, false);
    // Enable partial failures before deploying a producer that can return them.
    let producerExists = false;
    try {
      await lambda.send(new GetFunctionCommand({ FunctionName: FUNCTION_NAME }));
      producerExists = true;
    } catch (error) {
      if (!(error instanceof ResourceNotFoundException)) throw error;
    }
    if (producerExists) await ensureMapping(FUNCTION_NAME, streamArn, true);
    await ensureFunction(FUNCTION_NAME, streamRole, "dynamo-stream-handler", { QUEUE_URL: queue.url }, 60, 256);
    if (!producerExists) await ensureMapping(FUNCTION_NAME, streamArn, true);
    console.log(`Deployment complete: ${TABLE_NAME} -> ${queue.url} -> s3://${bucket}/events/`);
  } catch (error) {
    console.error("Deployment error:", error);
    process.exitCode = 1;
  } finally {
    for (const client of [dynamo, iam, lambda, sqs, s3, sts]) client.destroy();
  }
}
