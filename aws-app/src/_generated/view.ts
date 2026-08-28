// ─────────────────────────────────────────────────────────────
// Generated from slice: views
// Target: AWS-native (CDK + Lambda, TypeScript)
// CQRS/Event Sourcing: API Gateway + Lambda + DynamoDB event store
//                      + Kinesis + DynamoDB Streams + ElastiCache (Redis).
// Source of truth is the .md slice spec — regenerate, don't hand-edit.
// ─────────────────────────────────────────────────────────────

import { DomainEvent, EventTypes, createEvent, response, getRedis } from "../shared/event-store";

// Domain events
/** Registered */
export interface Registered {
    email: string;
    name: string;
    registeredAt: string;
}

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

/** Room Booked */
export interface Booked {
    bookingId: string;
    roomNumber: number;
    email: string;
    checkIn: string;
    checkOut: string;
    bookedAt: string;
}

/** Room Readied */
export interface Ready {
    roomNumber: number;
    readiedAt: string;
}

/** Checked In */
export interface CheckedIn {
    bookingId: string;
    email: string;
    roomNumber: number;
    checkedInAt: string;
}

/** Guest Left Hotel */
export interface GuestLeft {
    email: string;
    departedAt: string;
}

/** Checked Out */
export interface CheckedOut {
    bookingId: string;
    roomNumber: number;
    email: string;
    checkedOutAt: string;
}

/** Payment Requested */
export interface PaymentRequested {
    paymentId: string;
    bookingId: string;
    amount: number;
    currency: string;
    paymentMethod: string;
    requestedAt: string;
}

/** Payment Submitted */
export interface PaymentSubmitted {
    paymentId: string;
    bookingId: string;
    amount: number;
    submittedAt: string;
}

/** Payment Succeeded */
export interface PaymentSucceeded {
    paymentId: string;
    bookingId: string;
    amount: number;
    transactionRef: string;
    succeededAt: string;
}

/** Occupancy Forecasted */
export interface OccupancyForecasted {
    forecastId: string;
    roomType: string;
    forecastFrom: string;
    forecastThrough: string;
    predictedOccupancyRate: number;
    predictedDemand: number;
    modelVersion: string;
    endpointName: string;
    forecastedAt: string;
}

// External events
/** Week Elapsed */
export interface WeekElapsed {
    occurredAt: string;
}

/** Position Updated */
export interface PositionUpdated {
    email: string;
    latitude: number;
    longitude: number;
    timestamp: string;
}

/** Gateway Confirmed */
export interface GatewayConfirmed {
    paymentId: string;
    transactionRef: string;
    confirmedAt: string;
}

/** Forecast Tick */
export interface ForecastTick {
    occurredAt: string;
}

// Read models
/** Room Availability */
export interface AvailReadModel {
    roomNumber: number;
    night: string;
    roomType: string;
    capacity: number;
    isAvailable: boolean;
}

/** Availability Horizon */
export interface HorizonReadModel {
    roomNumber: number;
    roomType: string;
    capacity: number;
    seededThrough: string;
    requiredThrough: string;
}

/** Cleaning Schedule */
export interface CleaningScheduleReadModel {
    roomNumber: number;
    guestCheckOut: string;
    cleaningStatus: string;
}

/** Guest Roster */
export interface GuestRosterReadModel {
    email: string;
    guestName: string;
    roomNumber: number;
    checkedInAt: string;
    isPresent: boolean;
}

/** Payments to Process */
export interface PaymentsToProcessReadModel {
    paymentId: string;
    bookingId: string;
    amount: number;
    currency: string;
    paymentMethod: string;
    status: string;
}

/** Sales Report */
export interface SalesReportReadModel {
    totalRevenue: number;
    transactionCount: number;
    averageBookingValue: number;
    revenueByRoomType: string;
}

/** Occupancy Demand */
export interface OccupancyDemandReadModel {
    roomType: string;
    night: string;
    roomsAvailable: number;
    roomsBooked: number;
    roomsOccupied: number;
    bookingVelocity: number;
}

/** Demand Forecast */
export interface DemandForecastReadModel {
    roomType: string;
    forecastFrom: string;
    forecastThrough: string;
    predictedOccupancyRate: number;
    predictedDemand: number;
    modelVersion: string;
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
        case EventTypes.AVAILABILITY_ROLLED:
            await onAvailabilityRolledIntoAvail(client, tags["roomNumber"] ?? eventId, timestamp, tags, payload);
            await onAvailabilityRolledIntoHorizon(client, tags["roomNumber"] ?? eventId, timestamp, tags, payload);
            break;
        case EventTypes.BOOKED:
            await onBookedIntoAvail(client, tags["roomNumber"] ?? eventId, timestamp, tags, payload);
            await onBookedIntoCleaningSchedule(client, tags["roomNumber"] ?? eventId, timestamp, tags, payload);
            await onBookedIntoOccupancyDemand(client, tags["roomType"] ?? eventId, timestamp, tags, payload);
            break;
        case EventTypes.ROOM_ADDED:
            await onRoomAddedIntoHorizon(client, tags["roomNumber"] ?? eventId, timestamp, tags, payload);
            await onRoomAddedIntoOccupancyDemand(client, tags["roomType"] ?? eventId, timestamp, tags, payload);
            break;
        case EventTypes.WEEK_ELAPSED:
            await onWeekElapsedIntoHorizon(client, tags["roomNumber"] ?? eventId, timestamp, tags, payload);
            break;
        case EventTypes.CHECKED_IN:
            await onCheckedInIntoGuestRoster(client, tags["email"] ?? eventId, timestamp, tags, payload);
            await onCheckedInIntoOccupancyDemand(client, tags["roomType"] ?? eventId, timestamp, tags, payload);
            break;
        case EventTypes.GUEST_LEFT:
            await onGuestLeftIntoGuestRoster(client, tags["email"] ?? eventId, timestamp, tags, payload);
            break;
        case EventTypes.PAYMENT_REQUESTED:
            await onPaymentRequestedIntoPaymentsToProcess(client, tags["paymentId"] ?? eventId, timestamp, tags, payload);
            break;
        case EventTypes.PAYMENT_SUBMITTED:
            await onPaymentSubmittedIntoPaymentsToProcess(client, tags["paymentId"] ?? eventId, timestamp, tags, payload);
            break;
        case EventTypes.PAYMENT_SUCCEEDED:
            await onPaymentSucceededIntoPaymentsToProcess(client, tags["paymentId"] ?? eventId, timestamp, tags, payload);
            await onPaymentSucceededIntoSalesReport(client, tags["totalRevenue"] ?? eventId, timestamp, tags, payload);
            break;
        case EventTypes.CHECKED_OUT:
            await onCheckedOutIntoOccupancyDemand(client, tags["roomType"] ?? eventId, timestamp, tags, payload);
            break;
        case EventTypes.OCCUPANCY_FORECASTED:
            await onOccupancyForecastedIntoDemandForecast(client, tags["roomType"] ?? eventId, timestamp, tags, payload);
            break;
        default:
            // Event not consumed by any read model in this model — ignore.
            break;
    }
}

async function onAvailabilityRolledIntoAvail(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Availability Rolled" into the AvailReadModel record.
    const existing = await client.get(`avail:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomNumber: recordKey };
    if (tags.roomNumber !== undefined) view.roomNumber = tags.roomNumber;
    if (payload.roomType !== undefined) view.roomType = payload.roomType;
    if (payload.capacity !== undefined) view.capacity = payload.capacity;
    if (payload.fromNight !== undefined) view.fromNight = payload.fromNight;
    if (payload.throughNight !== undefined) view.throughNight = payload.throughNight;
    if (payload.rolledAt !== undefined) view.rolledAt = payload.rolledAt;
    const pipeline = client.pipeline();
    pipeline.set(`avail:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('avail:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onBookedIntoAvail(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Room Booked" into the AvailReadModel record.
    const existing = await client.get(`avail:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomNumber: recordKey };
    if (tags.bookingId !== undefined) view.bookingId = tags.bookingId;
    if (tags.roomNumber !== undefined) view.roomNumber = tags.roomNumber;
    if (payload.email !== undefined) view.email = payload.email;
    if (payload.checkIn !== undefined) view.checkIn = payload.checkIn;
    if (payload.checkOut !== undefined) view.checkOut = payload.checkOut;
    if (payload.bookedAt !== undefined) view.bookedAt = payload.bookedAt;
    const pipeline = client.pipeline();
    pipeline.set(`avail:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('avail:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onRoomAddedIntoHorizon(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Room Added" into the HorizonReadModel record.
    const existing = await client.get(`horizon:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomNumber: recordKey };
    if (tags.roomNumber !== undefined) view.roomNumber = tags.roomNumber;
    if (payload.floor !== undefined) view.floor = payload.floor;
    if (payload.roomType !== undefined) view.roomType = payload.roomType;
    if (payload.capacity !== undefined) view.capacity = payload.capacity;
    const pipeline = client.pipeline();
    pipeline.set(`horizon:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('horizon:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onWeekElapsedIntoHorizon(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Week Elapsed" into the HorizonReadModel record.
    const existing = await client.get(`horizon:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomNumber: recordKey };
    if (payload.occurredAt !== undefined) view.occurredAt = payload.occurredAt;
    const pipeline = client.pipeline();
    pipeline.set(`horizon:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('horizon:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onAvailabilityRolledIntoHorizon(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Availability Rolled" into the HorizonReadModel record.
    const existing = await client.get(`horizon:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomNumber: recordKey };
    if (tags.roomNumber !== undefined) view.roomNumber = tags.roomNumber;
    if (payload.roomType !== undefined) view.roomType = payload.roomType;
    if (payload.capacity !== undefined) view.capacity = payload.capacity;
    if (payload.fromNight !== undefined) view.fromNight = payload.fromNight;
    if (payload.throughNight !== undefined) view.throughNight = payload.throughNight;
    if (payload.rolledAt !== undefined) view.rolledAt = payload.rolledAt;
    const pipeline = client.pipeline();
    pipeline.set(`horizon:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('horizon:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onBookedIntoCleaningSchedule(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Room Booked" into the CleaningScheduleReadModel record.
    const existing = await client.get(`cleaningSchedule:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomNumber: recordKey };
    if (tags.bookingId !== undefined) view.bookingId = tags.bookingId;
    if (tags.roomNumber !== undefined) view.roomNumber = tags.roomNumber;
    if (payload.email !== undefined) view.email = payload.email;
    if (payload.checkIn !== undefined) view.checkIn = payload.checkIn;
    if (payload.checkOut !== undefined) view.checkOut = payload.checkOut;
    if (payload.bookedAt !== undefined) view.bookedAt = payload.bookedAt;
    const pipeline = client.pipeline();
    pipeline.set(`cleaningSchedule:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('cleaningSchedule:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onCheckedInIntoGuestRoster(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Checked In" into the GuestRosterReadModel record.
    const existing = await client.get(`guestRoster:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { email: recordKey };
    if (tags.bookingId !== undefined) view.bookingId = tags.bookingId;
    if (tags.email !== undefined) view.email = tags.email;
    if (payload.roomNumber !== undefined) view.roomNumber = payload.roomNumber;
    if (payload.checkedInAt !== undefined) view.checkedInAt = payload.checkedInAt;
    const pipeline = client.pipeline();
    pipeline.set(`guestRoster:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('guestRoster:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onGuestLeftIntoGuestRoster(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Guest Left Hotel" into the GuestRosterReadModel record.
    const existing = await client.get(`guestRoster:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { email: recordKey };
    if (payload.email !== undefined) view.email = payload.email;
    if (payload.departedAt !== undefined) view.departedAt = payload.departedAt;
    const pipeline = client.pipeline();
    pipeline.set(`guestRoster:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('guestRoster:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onPaymentRequestedIntoPaymentsToProcess(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Payment Requested" into the PaymentsToProcessReadModel record.
    const existing = await client.get(`paymentsToProcess:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { paymentId: recordKey };
    if (tags.paymentId !== undefined) view.paymentId = tags.paymentId;
    if (tags.bookingId !== undefined) view.bookingId = tags.bookingId;
    if (payload.amount !== undefined) view.amount = payload.amount;
    if (payload.currency !== undefined) view.currency = payload.currency;
    if (payload.paymentMethod !== undefined) view.paymentMethod = payload.paymentMethod;
    if (payload.requestedAt !== undefined) view.requestedAt = payload.requestedAt;
    const pipeline = client.pipeline();
    pipeline.set(`paymentsToProcess:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('paymentsToProcess:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onPaymentSubmittedIntoPaymentsToProcess(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Payment Submitted" into the PaymentsToProcessReadModel record.
    const existing = await client.get(`paymentsToProcess:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { paymentId: recordKey };
    if (tags.paymentId !== undefined) view.paymentId = tags.paymentId;
    if (payload.bookingId !== undefined) view.bookingId = payload.bookingId;
    if (payload.amount !== undefined) view.amount = payload.amount;
    if (payload.submittedAt !== undefined) view.submittedAt = payload.submittedAt;
    const pipeline = client.pipeline();
    pipeline.set(`paymentsToProcess:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('paymentsToProcess:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onPaymentSucceededIntoPaymentsToProcess(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Payment Succeeded" into the PaymentsToProcessReadModel record.
    const existing = await client.get(`paymentsToProcess:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { paymentId: recordKey };
    if (tags.paymentId !== undefined) view.paymentId = tags.paymentId;
    if (tags.bookingId !== undefined) view.bookingId = tags.bookingId;
    if (payload.amount !== undefined) view.amount = payload.amount;
    if (payload.transactionRef !== undefined) view.transactionRef = payload.transactionRef;
    if (payload.succeededAt !== undefined) view.succeededAt = payload.succeededAt;
    const pipeline = client.pipeline();
    pipeline.set(`paymentsToProcess:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('paymentsToProcess:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onPaymentSucceededIntoSalesReport(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Payment Succeeded" into the SalesReportReadModel record.
    const existing = await client.get(`salesReport:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { totalRevenue: recordKey };
    if (tags.paymentId !== undefined) view.paymentId = tags.paymentId;
    if (tags.bookingId !== undefined) view.bookingId = tags.bookingId;
    if (payload.amount !== undefined) view.amount = payload.amount;
    if (payload.transactionRef !== undefined) view.transactionRef = payload.transactionRef;
    if (payload.succeededAt !== undefined) view.succeededAt = payload.succeededAt;
    const pipeline = client.pipeline();
    pipeline.set(`salesReport:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('salesReport:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
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

async function onOccupancyForecastedIntoDemandForecast(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Occupancy Forecasted" into the DemandForecastReadModel record.
    const existing = await client.get(`demandForecast:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { roomType: recordKey };
    if (tags.forecastId !== undefined) view.forecastId = tags.forecastId;
    if (tags.roomType !== undefined) view.roomType = tags.roomType;
    if (payload.forecastFrom !== undefined) view.forecastFrom = payload.forecastFrom;
    if (payload.forecastThrough !== undefined) view.forecastThrough = payload.forecastThrough;
    if (payload.predictedOccupancyRate !== undefined) view.predictedOccupancyRate = payload.predictedOccupancyRate;
    if (payload.predictedDemand !== undefined) view.predictedDemand = payload.predictedDemand;
    if (payload.modelVersion !== undefined) view.modelVersion = payload.modelVersion;
    if (payload.endpointName !== undefined) view.endpointName = payload.endpointName;
    if (payload.forecastedAt !== undefined) view.forecastedAt = payload.forecastedAt;
    const pipeline = client.pipeline();
    pipeline.set(`demandForecast:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('demandForecast:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

// ── Query Lambda (read side) — serves GET from the Redis read models ─
// Reads the projection only; never touches the event store. Selects the
// read model via the `view` query-string param (defaults to the first);
// `GET /api/records?view=demandForecast&id=standard` reads one record,
// omitting `id` lists the most recent. Unknown views return 400.
const READ_MODELS: Record<string, string> = {
    "avail": "avail",
    "horizon": "horizon",
    "cleaningSchedule": "cleaningSchedule",
    "guestRoster": "guestRoster",
    "paymentsToProcess": "paymentsToProcess",
    "salesReport": "salesReport",
    "occupancyDemand": "occupancyDemand",
    "demandForecast": "demandForecast",
};
const DEFAULT_VIEW = "avail";

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
