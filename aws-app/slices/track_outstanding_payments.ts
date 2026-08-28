// ─────────────────────────────────────────────────────────────
// Generated from slice: track_outstanding_payments
// Target: AWS-native (CDK + Lambda, TypeScript)
// CQRS/Event Sourcing: API Gateway + Lambda + DynamoDB event store
//                      + Kinesis + DynamoDB Streams + ElastiCache (Redis).
// Source of truth is the .md slice spec — regenerate, don't hand-edit.
// ─────────────────────────────────────────────────────────────

import { DomainEvent, EventTypes, createEvent, response, getRedis } from "../shared/event-store";

// Domain events
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

// Read models
/** Payments to Process */
export interface PaymentsToProcessReadModel {
    paymentId: string;
    bookingId: string;
    amount: number;
    currency: string;
    paymentMethod: string;
    status: string;
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
    const recordKey = tags["paymentId"] ?? eventId;
    switch (eventType) {
        case EventTypes.PAYMENT_REQUESTED:
            await onPaymentRequested(client, recordKey, timestamp, tags, payload);
            break;
        case EventTypes.PAYMENT_SUBMITTED:
            await onPaymentSubmitted(client, recordKey, timestamp, tags, payload);
            break;
        case EventTypes.PAYMENT_SUCCEEDED:
            await onPaymentSucceeded(client, recordKey, timestamp, tags, payload);
            break;
        default:
            console.warn(`Unknown event type: ${eventType}`);
    }
}

async function onPaymentRequested(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Payment Requested" into the PaymentsToProcessReadModel record.
    const existing = await client.get(`paymentsToProcess:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { paymentId: recordKey };
    view.paymentId = tags.paymentId;
    view.bookingId = tags.bookingId;
    view.amount = payload.amount;
    view.currency = payload.currency;
    view.paymentMethod = payload.paymentMethod;
    view.requestedAt = payload.requestedAt;
    // TODO: set view.status to the status this event transitions to.
    const pipeline = client.pipeline();
    pipeline.set(`paymentsToProcess:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('paymentsToProcess:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onPaymentSubmitted(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Payment Submitted" into the PaymentsToProcessReadModel record.
    const existing = await client.get(`paymentsToProcess:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { paymentId: recordKey };
    view.paymentId = tags.paymentId;
    view.bookingId = payload.bookingId;
    view.amount = payload.amount;
    view.submittedAt = payload.submittedAt;
    // TODO: set view.status to the status this event transitions to.
    const pipeline = client.pipeline();
    pipeline.set(`paymentsToProcess:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('paymentsToProcess:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

async function onPaymentSucceeded(
    client: Redis,
    recordKey: string,
    timestamp: string,
    tags: Record<string, string>,
    payload: Record<string, unknown>
): Promise<void> {
    // Merge "Payment Succeeded" into the PaymentsToProcessReadModel record.
    const existing = await client.get(`paymentsToProcess:${recordKey}`);
    const view: Record<string, unknown> = existing ? JSON.parse(existing) : { paymentId: recordKey };
    view.paymentId = tags.paymentId;
    view.bookingId = tags.bookingId;
    view.amount = payload.amount;
    view.transactionRef = payload.transactionRef;
    view.succeededAt = payload.succeededAt;
    // TODO: set view.status to the status this event transitions to.
    const pipeline = client.pipeline();
    pipeline.set(`paymentsToProcess:${recordKey}`, JSON.stringify(view));
    pipeline.zadd('paymentsToProcess:all', Date.parse(timestamp).toString(), recordKey);
    await pipeline.exec();
}

// ── Query Lambda (read side) — serves GET from the Redis read model ──
// Reads the projection only; never touches the event store. This is the
// query half of CQRS (e.g. GET /api/paymentsToProcess/{id}).
export async function queryHandler(
    event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> {
    const client = getRedis();
    const id = event.pathParameters?.id;
    if (id) {
        const data = await client.get(`paymentsToProcess:${id}`);
        if (!data) return response(404, { error: 'Not found' });
        return response(200, JSON.parse(data));
    }
    const ids = await client.zrevrange('paymentsToProcess:all', 0, 49);
    if (ids.length === 0) return response(200, []);
    const pipeline = client.pipeline();
    for (const key of ids) pipeline.get(`paymentsToProcess:${key}`);
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
