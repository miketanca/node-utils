export interface StreamEvent {
  eventID: string;
  eventName: string;
  eventSourceARN?: string;
  approximateCreationDateTime: number;
  keys?: Record<string, unknown>;
  newImage?: Record<string, unknown>;
  oldImage?: Record<string, unknown>;
}

export function parseStreamEvent(body: string): StreamEvent {
  const value: unknown = JSON.parse(body);
  if (
    typeof value !== "object" ||
    value === null ||
    !("eventID" in value) ||
    typeof value.eventID !== "string" ||
    !value.eventID ||
    !("eventName" in value) ||
    typeof value.eventName !== "string" ||
    !("approximateCreationDateTime" in value) ||
    typeof value.approximateCreationDateTime !== "number" ||
    !Number.isFinite(value.approximateCreationDateTime) ||
    Number.isNaN(new Date(value.approximateCreationDateTime * 1000).getTime())
  ) {
    throw new Error("Invalid stream event: expected eventID, eventName and creation timestamp");
  }
  return value as StreamEvent;
}

export function hourlyKey(event: StreamEvent): string {
  const hour = new Date(event.approximateCreationDateTime * 1000)
    .toISOString()
    .slice(0, 13);
  return `events/${hour.slice(0, 10)}/${hour.slice(11)}.jsonl`;
}
