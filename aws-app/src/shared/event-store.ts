// ─────────────────────────────────────────────────────────────
// Shared runtime for model: model
// Target: AWS-native (CDK + Lambda, TypeScript)
// The COMMON part — emitted once from the whole model. Contains the
// stored-event envelope, the merged EventTypes map, the createEvent
// factory, the DynamoDB/Kinesis/Redis runtime, and the shared CDK infra.
// Each slice imports from './shared/event-store' instead of re-emitting it.
// Source of truth is the model .md — regenerate, don't hand-edit.
// ─────────────────────────────────────────────────────────────

// ── Domain events — immutable facts in the DynamoDB event store ──────
// The stored envelope. Events are keyed by a unique eventId (PK) and a
// monotonic global `seq` (a ULID). `tags` holds the DCB tag-axis values
// this event carries (e.g. { roomId, email }); each tag is projected into
// a per-axis GSI so a consistency boundary can be queried by tag value.
export interface DomainEvent {
    eventId: string;      // unique id (partition key)
    seq: string;          // global monotonic sequence (ULID) — total order
    eventType: string;    // language-independent stored name
    timestamp: string;    // ISO-8601
    tags: Record<string, string>;   // DCB tag-axis values carried by this event
    payload: Record<string, unknown>;
}

// Every stored event name in the model — the migration contract. Merged
// across all slices so there is a single source of truth for event names.
export const EventTypes = {
    REGISTERED: "Registered",
    ROOM_ADDED: "RoomAdded",
    AVAILABILITY_ROLLED: "AvailabilityRolled",
    BOOKED: "Booked",
    READY: "Ready",
    CHECKED_IN: "CheckedIn",
    GUEST_LEFT: "GuestLeft",
    CHECKED_OUT: "CheckedOut",
    PAYMENT_REQUESTED: "PaymentRequested",
    PAYMENT_SUBMITTED: "PaymentSubmitted",
    PAYMENT_SUCCEEDED: "PaymentSucceeded",
    OCCUPANCY_FORECASTED: "OccupancyForecasted",
    WEEK_ELAPSED: "WeekElapsed",
    POSITION_UPDATED: "PositionUpdated",
    GATEWAY_CONFIRMED: "GatewayConfirmed",
    FORECAST_TICK: "ForecastTick",
} as const;

// ── AWS clients + config (shared by every handler) ──────────────────
import { APIGatewayProxyResult } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
    DynamoDBDocumentClient,
    QueryCommand,
    GetCommand,
    TransactWriteCommand,
} from '@aws-sdk/lib-dynamodb';
import { KinesisClient, PutRecordCommand } from '@aws-sdk/client-kinesis';
import {
    SageMakerRuntimeClient,
    InvokeEndpointCommand,
} from '@aws-sdk/client-sagemaker-runtime';
import { ulid } from 'ulid';
import Redis from 'ioredis';

const dynamodb = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  // Drop undefined values so optional/unmapped payload fields don't break marshalling.
  marshallOptions: { removeUndefinedValues: true },
});
const kinesis = new KinesisClient({});
const sagemakerRuntime = new SageMakerRuntimeClient({});
const TABLE_NAME = process.env.EVENT_TABLE_NAME!;
const STREAM_NAME = process.env.KINESIS_STREAM_NAME!;
// Endpoint invoked by automation slices that call a model for inference.
// Set on the command Lambda by the CDK stack; empty until an endpoint exists.
const SAGEMAKER_ENDPOINT_NAME = process.env.SAGEMAKER_ENDPOINT_NAME || '';

// ── Dynamic Consistency Boundary (DCB) primitives ───────────────────
//
// A DCB is defined per command by its `reads [types] by [axes]` criteria: a
// set of event types scoped by tag values. There is no fixed aggregate. We
// enforce the boundary on DynamoDB as follows:
//
//   • Each event stores its tag values in `tags` and is written with one
//     attribute per axis (tag_<axis>) so a per-axis GSI (gsi_<axis>,
//     partition key tag_<axis>, sort key seq) indexes exactly the events
//     carrying that tag. A boundary branch is a Query on that GSI.
//   • GSIs are eventually consistent, so they only SEED the state fold.
//     Enforcement rides on a strongly-consistent guard item per tag value
//     (TAGPOS#<axis>#<value>, holding the last seq appended to that tag),
//     asserted inside a TransactWriteItems. If a concurrent command moved
//     any guard since we read it, the transaction fails and we retry.

// A single branch of a command's boundary: match these event types among
// events carrying tag <axis> = <value>.
export interface BoundaryBranch {
    axis: string;
    value: string;
    types: readonly string[];
}

// The result of reading a boundary: the matching events (for the caller to
// fold) and the guard positions observed, which the append asserts unchanged.
export interface BoundaryRead {
    events: DomainEvent[];
    guards: { key: string; lastSeq: string | null }[];
}

const tagPosKey = (axis: string, value: string) => `TAGPOS#${axis}#${value}`;

// Factory for a stored event. `tags` are the DCB tag-axis values (e.g.
// { roomId, email }); a fresh ULID gives a globally monotonic seq.
export function createEvent(
    eventType: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): DomainEvent {
    return {
        eventId: ulid(),
        seq: ulid(),
        eventType,
        timestamp: new Date().toISOString(),
        tags,
        payload,
    };
}

// Query one boundary branch: all events carrying tag <axis> = <value>,
// restricted to the branch's event types. Reads the per-axis GSI. Because
// GSIs are eventually consistent, the result only seeds the state fold —
// enforcement is the guard asserted in appendWithinBoundary.
export async function queryBoundaryBranch(branch: BoundaryBranch): Promise<DomainEvent[]> {
    const result = await dynamodb.send(
        new QueryCommand({
            TableName: TABLE_NAME,
            IndexName: `gsi_${branch.axis}`,
            KeyConditionExpression: `tag_${branch.axis} = :v`,
            ExpressionAttributeValues: { ':v': branch.value },
            ScanIndexForward: true, // oldest first, by seq
        })
    );
    const items = (result.Items || []) as DomainEvent[];
    return items.filter((e) => branch.types.includes(e.eventType));
}

// Read the whole boundary: fold-events from every branch (deduped by
// eventId, ordered by seq) plus the strongly-consistent guard position for
// each distinct tag value. The guards are what the append asserts unchanged.
export async function readBoundary(criteria: BoundaryBranch[]): Promise<BoundaryRead> {
    // Fold events, seeded from the (eventually consistent) GSIs.
    const byId = new Map<string, DomainEvent>();
    for (const branch of criteria) {
        for (const e of await queryBoundaryBranch(branch)) byId.set(e.eventId, e);
    }
    const events = [...byId.values()].sort((a, b) => (a.seq < b.seq ? -1 : a.seq > b.seq ? 1 : 0));

    // Strongly-consistent guard read: one TAGPOS item per distinct (axis,value).
    const seenKeys = new Set<string>();
    const guards: { key: string; lastSeq: string | null }[] = [];
    for (const branch of criteria) {
        const key = tagPosKey(branch.axis, branch.value);
        if (seenKeys.has(key)) continue;
        seenKeys.add(key);
        const res = await dynamodb.send(
            new GetCommand({ TableName: TABLE_NAME, Key: { eventId: key, seq: 'POS' }, ConsistentRead: true })
        );
        guards.push({ key, lastSeq: (res.Item?.lastSeq as string | undefined) ?? null });
    }
    return { events, guards };
}

// Thrown when the boundary moved between read and append; the caller retries.
export class ConcurrencyError extends Error {
    constructor() { super('DCB boundary changed; retry'); this.name = 'ConcurrencyError'; }
}

// Append an event within its consistency boundary, atomically. In one
// TransactWriteItems we: (1) assert each observed TAGPOS guard is unchanged,
// (2) Put the event (with tag_<axis> attributes so the GSIs index it), and
// (3) advance each TAGPOS guard to this event's seq. If any guard moved, the
// transaction is cancelled and we raise ConcurrencyError.
export async function appendWithinBoundary(
    domainEvent: DomainEvent,
    guards: { key: string; lastSeq: string | null }[]
): Promise<void> {
    // The item carries a tag_<axis> attribute per tag so each gsi_<axis> indexes it.
    // Skip empty/undefined tag values: DynamoDB rejects an empty string as a
    // GSI key, and an unset axis simply should not be indexed on this event.
    const item: Record<string, unknown> = { ...domainEvent };
    for (const [axis, value] of Object.entries(domainEvent.tags)) {
        if (value !== undefined && value !== '') item[`tag_${axis}`] = value;
    }

    const txItems: unknown[] = [
        { Put: { TableName: TABLE_NAME, Item: item, ConditionExpression: 'attribute_not_exists(eventId)' } },
    ];
    for (const g of guards) {
        // One operation per item: a single Update that both asserts the guard is
        // unchanged (ConditionExpression) and advances it to this event's seq.
        // (TransactWriteItems forbids two operations on the same item key, so we
        // cannot use a separate ConditionCheck + Update on the same TAGPOS item.)
        if (g.lastSeq === null) {
            txItems.push({ Update: {
                TableName: TABLE_NAME, Key: { eventId: g.key, seq: 'POS' },
                UpdateExpression: 'SET lastSeq = :seq',
                ConditionExpression: 'attribute_not_exists(lastSeq)',
                ExpressionAttributeValues: { ':seq': domainEvent.seq },
            } });
        } else {
            txItems.push({ Update: {
                TableName: TABLE_NAME, Key: { eventId: g.key, seq: 'POS' },
                UpdateExpression: 'SET lastSeq = :seq',
                ConditionExpression: 'lastSeq = :prev',
                ExpressionAttributeValues: { ':seq': domainEvent.seq, ':prev': g.lastSeq },
            } });
        }
    }

    try {
        await dynamodb.send(new TransactWriteCommand({ TransactItems: txItems as never }));
    } catch (err: unknown) {
        const name = (err as { name?: string }).name;
        if (name === 'TransactionCanceledException' || name === 'ConditionalCheckFailedException') {
            throw new ConcurrencyError();
        }
        throw err;
    }
}

// Publish an event to Kinesis for downstream consumers.
export async function publishToKinesis(domainEvent: DomainEvent): Promise<void> {
    await kinesis.send(
        new PutRecordCommand({
            StreamName: STREAM_NAME,
            PartitionKey: domainEvent.eventId,
            Data: Buffer.from(JSON.stringify(domainEvent)),
        })
    );
}

// Invoke a SageMaker real-time endpoint for inference. Automation slices
// call this to turn a feature vector into a prediction, then record the
// result as a domain event. `features` is JSON-serialised as the request
// body; the parsed JSON response is returned to the caller. Throws if no
// endpoint is configured or the response body is empty.
export async function invokeSageMaker<T = Record<string, unknown>>(
    features: Record<string, unknown>,
    endpointName: string = SAGEMAKER_ENDPOINT_NAME
): Promise<T> {
    if (!endpointName) {
        throw new Error('SAGEMAKER_ENDPOINT_NAME is not set — no endpoint to invoke');
    }
    const result = await sagemakerRuntime.send(
        new InvokeEndpointCommand({
            EndpointName: endpointName,
            ContentType: 'application/json',
            Accept: 'application/json',
            Body: Buffer.from(JSON.stringify(features)),
        })
    );
    if (!result.Body) throw new Error('SageMaker returned an empty response body');
    const text = Buffer.from(result.Body as Uint8Array).toString('utf-8');
    return JSON.parse(text) as T;
}

// Lazily-initialised Redis (ElastiCache) client for the read side.
let redis: Redis;
export function getRedis(): Redis {
    if (!redis) {
        redis = new Redis({
            host: process.env.REDIS_HOST!,
            port: parseInt(process.env.REDIS_PORT || '6379'),
            tls: process.env.REDIS_TLS === 'true' ? {} : undefined,
            connectTimeout: 5000,
            maxRetriesPerRequest: 3,
        });
    }
    return redis;
}

// Shared API Gateway JSON response helper.
export function response(statusCode: number, body: unknown): APIGatewayProxyResult {
    return {
        statusCode,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify(body),
    };
}

// The shared CDK infrastructure (the DynamoDB event table, Kinesis stream,
// VPC, ElastiCache Redis, API Gateway) is emitted as its own compilable
// file — infra/stacks/regional-stack.ts — via the 'infra' generation part,
// not inlined here. This keeps the runtime module free of stack code.
