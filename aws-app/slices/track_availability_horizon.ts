// ─────────────────────────────────────────────────────────────
// Generated from slice: track_availability_horizon
// Target: AWS-native (CDK + Lambda, TypeScript)
// CQRS/Event Sourcing: API Gateway + Lambda + DynamoDB event store
//                      + Kinesis + DynamoDB Streams + ElastiCache (Redis).
// Source of truth is the .md slice spec — regenerate, don't hand-edit.
// ─────────────────────────────────────────────────────────────

import { DomainEvent, EventTypes, createEvent, response, getRedis } from "../shared/event-store";

// Domain events
/** Room Added */
export interface RoomAdded {
    roomNumber: number;
    floor: number;
    roomType: string;
    capacity: number;
}

/** Availability Rolled */
export interface AvailabilityRolled {
    roomNumber: number;
    roomType: string;
    capacity: number;
    fromNight: string;
    throughNight: string;
    rolledAt: string;
}

// External events
/** Week Elapsed */
export interface WeekElapsed {
    occurredAt: string;
}

// Read models
/** Availability Horizon */
export interface HorizonReadModel {
    roomNumber: number;
    roomType: string;
    capacity: number;
    seededThrough: string;
    requiredThrough: string;
}

// ── Projector Lambda (read side) — DynamoDB Streams → Redis ─────────
// Consumes the event store's stream and folds each source event into the
// ElastiCache/Redis read model. The read model is disposable: it can be
// rebuilt at any time by replaying the events. The Redis client and the
// response helper come from the shared runtime.
import { APIGatewayProxyEvent, APIGatewayProxyResult, DynamoDBStreamEvent, DynamoDBRecord } from 'aws-lambda';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { AttributeValue } from '@aws-sdk/client-dynamodb';
import Redis from 'ioredis';

export async function handler(event: DynamoDBStreamEvent): Promise<void> {
    const client = getRedis();
    for (const record of event.Records) {
        if (record.eventName !== 'INSERT') continue;
        await processRecord(client, record);
    }
}

async function processRecord(client: Redis, record: DynamoDBRecord): Promise<void> {
    if (!record.dynamodb?.NewImage) return;
    const item = unmarshall(record.dynamodb.NewImage as Record<string, AttributeValue>) as DomainEvent;
    const { eventId, eventType, timestamp, tags, payload } = item;
    // The record key: the read model's identity from the event tags, else the eventId.
    const recordKey = tags["roomNumber"] ?? eventId;
    switch (eventType) {
        case EventTypes.ROOM_ADDED:
            await onRoomAdded(client, recordKey, timestamp, tags, payload);
            break;
        case EventTypes.WEEK_ELAPSED:
            await onWeekElapsed(client, recordKey, timestamp, tags, payload);
            break;
        case EventTypes.AVAILABILITY_ROLLED:
            await onAvailabilityRolled(client, recordKey, timestamp, tags, payload);
            break;
        default:
            console.warn(`Unknown event type: ${eventType}`);
    }
}

async function onRoomAdded(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Room Added" into the HorizonReadModel record.
    const existing = await client.get(`horizon:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomNumber: recordKey };
    view.roomNumber = tags.roomNumber;
    view.floor = payload.floor;
    view.roomType = payload.roomType;
    view.capacity = payload.capacity;
    // TODO: set view.status to the status this event transitions to.
    const pipeline = client.pipeline();
    pipeline.set(`horizon:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('horizon:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onWeekElapsed(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Week Elapsed" into the HorizonReadModel record.
    const existing = await client.get(`horizon:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomNumber: recordKey };
    view.occurredAt = payload.occurredAt;
    // TODO: set view.status to the status this event transitions to.
    const pipeline = client.pipeline();
    pipeline.set(`horizon:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('horizon:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onAvailabilityRolled(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Availability Rolled" into the HorizonReadModel record.
    const existing = await client.get(`horizon:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomNumber: recordKey };
    view.roomNumber = tags.roomNumber;
    view.roomType = payload.roomType;
    view.capacity = payload.capacity;
    view.fromNight = payload.fromNight;
    view.throughNight = payload.throughNight;
    view.rolledAt = payload.rolledAt;
    // TODO: set view.status to the status this event transitions to.
    const pipeline = client.pipeline();
    pipeline.set(`horizon:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('horizon:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

// ── Query Lambda (read side) — serves GET from the Redis read model ──
// Reads the projection only; never touches the event store. This is the
// query half of CQRS (e.g. GET /api/horizon/{id}).
export async function queryHandler(
    event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> {
    const client = getRedis();
    const id = event.pathParameters?.id;
    if (id) {
        const data = await client.get(`horizon:${id}`);
        if (!data) return response(404, { error: 'Not found' });
        return response(200, JSON.parse(data));
    }
    const ids = await client.zrevrange('horizon:all', 0, 49);
    if (ids.length === 0) return response(200, []);
    const pipeline = client.pipeline();
    for (const key of ids) pipeline.get(`horizon:${key}`);
    const results = await pipeline.exec();
    const items = (results || [])
        .map(([err, data]) => (err ? null : data ? JSON.parse(data as string) : null))
        .filter(Boolean);
    return response(200, items);
}

// ── CDK wiring ──────────────────────────────────────────────────────
// This handler is wired into infrastructure by the model-level 'infra'
// target — generate infra/stacks/regional-stack.ts (the "Generate AWS
// infra (CDK)" action on the model view). That stack declares the Lambda,
// its env + grants, the API Gateway route, and (for read models) the
// DynamoDB Streams event source — the single source of truth for
// deployment. Nothing to paste here.
