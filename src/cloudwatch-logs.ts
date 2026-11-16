import {
  CloudWatchLogsClient,
  FilterLogEventsCommand,
  type FilteredLogEvent,
} from "@aws-sdk/client-cloudwatch-logs";
import { writeFile } from "node:fs/promises";

export const LOG_GROUP_NAME = "test";

// Credentials are resolved automatically from the environment
// (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, ~/.aws/credentials, SSO, etc.)
const client = new CloudWatchLogsClient({
  region: process.env.AWS_REGION ?? "us-east-2",
});

/** Read every event in the log group (paginated), optionally bounded by a time range. */
export async function readAllLogEvents(options?: {
  startTime?: number;
  endTime?: number;
}) {
  const events: FilteredLogEvent[] = [];
  let nextToken: string | undefined;

  do {
    const result = await client.send(
      new FilterLogEventsCommand({
        logGroupName: LOG_GROUP_NAME,
        startTime: options?.startTime,
        endTime: options?.endTime,
        nextToken,
      }),
    );
    events.push(...(result.events ?? []));
    nextToken = result.nextToken;
  } while (nextToken);

  return events;
}

/** Read the whole log group and write its events to a file. Returns the event count. */
export async function writeLogGroupToFile(filePath: string) {
  const events = await readAllLogEvents();

  const lines = events.map((event) => {
    const timestamp = event.timestamp
      ? new Date(event.timestamp).toISOString()
      : "unknown-time";
    return `[${timestamp}] [${event.logStreamName ?? "unknown-stream"}] ${event.message?.trimEnd() ?? ""}`;
  });

  await writeFile(
    filePath,
    lines.join("\n") + (lines.length ? "\n" : ""),
    "utf8",
  );
  return events.length;
}

/** Close the underlying CloudWatch Logs client connection. */
export function destroyLogsClient() {
  client.destroy();
}

export async function testLogRead(filePath = "log-group-test.log") {
  try {
    const count = await writeLogGroupToFile(filePath);
    console.log(
      `Wrote ${count} event(s) from log group "${LOG_GROUP_NAME}" to "${filePath}"`,
    );
  } catch (error) {
    console.error("CloudWatch Logs read error:", error);
    process.exitCode = 1;
  } finally {
    destroyLogsClient();
  }
}
