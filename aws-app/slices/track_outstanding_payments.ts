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
    switch (eventType) {
        case EventTypes.PAYMENT_REQUESTED:
            await onPaymentRequestedIntoPaymentsToProcess(client, tags["paymentId"] ?? eventId, timestamp, tags, payload);
            break;
        case EventTypes.PAYMENT_SUBMITTED:
            await onPaymentSubmittedIntoPaymentsToProcess(client, tags["paymentId"] ?? eventId, timestamp, tags, payload);
            break;
        case EventTypes.PAYMENT_SUCCEEDED:
            await onPaymentSucceededIntoPaymentsToProcess(client, tags["paymentId"] ?? eventId, timestamp, tags, payload);
            break;
        default:
            // Event not consumed by any read model in this model — ignore.
            break;
    }
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

// ── Query Lambda (read side) — serves GET from the Redis read models ─
// Reads the projection only; never touches the event store. Selects the
// read model via the `view` query-string param (defaults to the first);
// `GET /api/records?view=demandForecast&id=standard` reads one record,
// omitting `id` lists the most recent. Unknown views return 400.
const READ_MODELS: Record<string, string> = {
    "paymentsToProcess": "paymentsToProcess",
};
const DEFAULT_VIEW = "paymentsToProcess";

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
