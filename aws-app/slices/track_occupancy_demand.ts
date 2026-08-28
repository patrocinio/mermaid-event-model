// ─────────────────────────────────────────────────────────────
// Generated from slice: track_occupancy_demand
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

/** Room Booked */
export interface Booked {
    bookingId: string;
    roomNumber: number;
    email: string;
    checkIn: string;
    checkOut: string;
    bookedAt: string;
}

/** Checked In */
export interface CheckedIn {
    bookingId: string;
    email: string;
    roomNumber: number;
    checkedInAt: string;
}

/** Checked Out */
export interface CheckedOut {
    bookingId: string;
    roomNumber: number;
    email: string;
    checkedOutAt: string;
}

// Read models
/** Occupancy Demand */
export interface OccupancyDemandReadModel {
    roomType: string;
    night: string;
    roomsAvailable: number;
    roomsBooked: number;
    roomsOccupied: number;
    bookingVelocity: number;
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
    switch (eventType) {
        case EventTypes.ROOM_ADDED:
            await onRoomAddedIntoOccupancyDemand(client, tags["roomType"] ?? eventId, timestamp, tags, payload);
            break;
        case EventTypes.BOOKED:
            await onBookedIntoOccupancyDemand(client, tags["roomType"] ?? eventId, timestamp, tags, payload);
            break;
        case EventTypes.CHECKED_IN:
            await onCheckedInIntoOccupancyDemand(client, tags["roomType"] ?? eventId, timestamp, tags, payload);
            break;
        case EventTypes.CHECKED_OUT:
            await onCheckedOutIntoOccupancyDemand(client, tags["roomType"] ?? eventId, timestamp, tags, payload);
            break;
        default:
            // Event not consumed by any read model in this model — ignore.
            break;
    }
}

async function onRoomAddedIntoOccupancyDemand(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Room Added" into the OccupancyDemandReadModel record.
    const existing = await client.get(`occupancyDemand:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomType: recordKey };
    if (tags.roomNumber !== undefined) view.roomNumber = tags.roomNumber;
    if (payload.floor !== undefined) view.floor = payload.floor;
    if (payload.roomType !== undefined) view.roomType = payload.roomType;
    if (payload.capacity !== undefined) view.capacity = payload.capacity;
    const pipeline = client.pipeline();
    pipeline.set(`occupancyDemand:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('occupancyDemand:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onBookedIntoOccupancyDemand(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Room Booked" into the OccupancyDemandReadModel record.
    const existing = await client.get(`occupancyDemand:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomType: recordKey };
    if (tags.bookingId !== undefined) view.bookingId = tags.bookingId;
    if (tags.roomNumber !== undefined) view.roomNumber = tags.roomNumber;
    if (payload.email !== undefined) view.email = payload.email;
    if (payload.checkIn !== undefined) view.checkIn = payload.checkIn;
    if (payload.checkOut !== undefined) view.checkOut = payload.checkOut;
    if (payload.bookedAt !== undefined) view.bookedAt = payload.bookedAt;
    const pipeline = client.pipeline();
    pipeline.set(`occupancyDemand:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('occupancyDemand:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onCheckedInIntoOccupancyDemand(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Checked In" into the OccupancyDemandReadModel record.
    const existing = await client.get(`occupancyDemand:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomType: recordKey };
    if (tags.bookingId !== undefined) view.bookingId = tags.bookingId;
    if (tags.email !== undefined) view.email = tags.email;
    if (payload.roomNumber !== undefined) view.roomNumber = payload.roomNumber;
    if (payload.checkedInAt !== undefined) view.checkedInAt = payload.checkedInAt;
    const pipeline = client.pipeline();
    pipeline.set(`occupancyDemand:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('occupancyDemand:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onCheckedOutIntoOccupancyDemand(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Checked Out" into the OccupancyDemandReadModel record.
    const existing = await client.get(`occupancyDemand:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomType: recordKey };
    if (tags.bookingId !== undefined) view.bookingId = tags.bookingId;
    if (tags.roomNumber !== undefined) view.roomNumber = tags.roomNumber;
    if (tags.email !== undefined) view.email = tags.email;
    if (payload.checkedOutAt !== undefined) view.checkedOutAt = payload.checkedOutAt;
    const pipeline = client.pipeline();
    pipeline.set(`occupancyDemand:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('occupancyDemand:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

// ── Query Lambda (read side) — serves GET from the Redis read models ─
// Reads the projection only; never touches the event store. Selects the
// read model via the `view` query-string param (defaults to the first);
// `GET /api/records?view=demandForecast&id=standard` reads one record,
// omitting `id` lists the most recent. Unknown views return 400.
const READ_MODELS: Record<string, string> = {
    "occupancyDemand": "occupancyDemand",
};
const DEFAULT_VIEW = "occupancyDemand";

export async function queryHandler(
    event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> {
    const client = getRedis();
    const view = event.queryStringParameters?.view ?? DEFAULT_VIEW;
    const prefix = READ_MODELS[view];
    if (!prefix) {
        return response(400, { error: `Unknown view: '${view}'`, views: Object.keys(READ_MODELS) });
    }
    const id = event.pathParameters?.id ?? event.queryStringParameters?.id;
    if (id) {
        const data = await client.get(`${prefix}:${id}`);
        if (!data) return response(404, { error: 'Not found' });
        return response(200, JSON.parse(data));
    }
    const ids = await client.zrevrange(`${prefix}:all`, 0, 49);
    if (ids.length === 0) return response(200, []);
    const pipeline = client.pipeline();
    for (const key of ids) pipeline.get(`${prefix}:${key}`);
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
