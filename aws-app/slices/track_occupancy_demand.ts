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
    // The record key: the read model's identity from the event tags, else the eventId.
    const recordKey = tags["roomType"] ?? eventId;
    switch (eventType) {
        case EventTypes.ROOM_ADDED:
            await onRoomAdded(client, recordKey, timestamp, tags, payload);
            break;
        case EventTypes.BOOKED:
            await onBooked(client, recordKey, timestamp, tags, payload);
            break;
        case EventTypes.CHECKED_IN:
            await onCheckedIn(client, recordKey, timestamp, tags, payload);
            break;
        case EventTypes.CHECKED_OUT:
            await onCheckedOut(client, recordKey, timestamp, tags, payload);
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
    // Merge "Room Added" into the OccupancyDemandReadModel record.
    const existing = await client.get(`occupancyDemand:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomType: recordKey };
    view.roomNumber = tags.roomNumber;
    view.floor = payload.floor;
    view.roomType = payload.roomType;
    view.capacity = payload.capacity;
    // TODO: set view.status to the status this event transitions to.
    const pipeline = client.pipeline();
    pipeline.set(`occupancyDemand:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('occupancyDemand:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onBooked(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Room Booked" into the OccupancyDemandReadModel record.
    const existing = await client.get(`occupancyDemand:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomType: recordKey };
    view.bookingId = tags.bookingId;
    view.roomNumber = tags.roomNumber;
    view.email = payload.email;
    view.checkIn = payload.checkIn;
    view.checkOut = payload.checkOut;
    view.bookedAt = payload.bookedAt;
    // TODO: set view.status to the status this event transitions to.
    const pipeline = client.pipeline();
    pipeline.set(`occupancyDemand:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('occupancyDemand:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onCheckedIn(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Checked In" into the OccupancyDemandReadModel record.
    const existing = await client.get(`occupancyDemand:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomType: recordKey };
    view.bookingId = tags.bookingId;
    view.email = tags.email;
    view.roomNumber = payload.roomNumber;
    view.checkedInAt = payload.checkedInAt;
    // TODO: set view.status to the status this event transitions to.
    const pipeline = client.pipeline();
    pipeline.set(`occupancyDemand:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('occupancyDemand:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onCheckedOut(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Checked Out" into the OccupancyDemandReadModel record.
    const existing = await client.get(`occupancyDemand:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomType: recordKey };
    view.bookingId = tags.bookingId;
    view.roomNumber = tags.roomNumber;
    view.email = tags.email;
    view.checkedOutAt = payload.checkedOutAt;
    // TODO: set view.status to the status this event transitions to.
    const pipeline = client.pipeline();
    pipeline.set(`occupancyDemand:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('occupancyDemand:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

// ── Query Lambda (read side) — serves GET from the Redis read model ──
// Reads the projection only; never touches the event store. This is the
// query half of CQRS (e.g. GET /api/occupancyDemand/{id}).
export async function queryHandler(
    event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> {
    const client = getRedis();
    const id = event.pathParameters?.id;
    if (id) {
        const data = await client.get(`occupancyDemand:${id}`);
        if (!data) return response(404, { error: 'Not found' });
        return response(200, JSON.parse(data));
    }
    const ids = await client.zrevrange('occupancyDemand:all', 0, 49);
    if (ids.length === 0) return response(200, []);
    const pipeline = client.pipeline();
    for (const key of ids) pipeline.get(`occupancyDemand:${key}`);
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
