// ─────────────────────────────────────────────────────────────
// Generated from slice: gateway_confirmation
// Target: AWS-native (CDK + Lambda, TypeScript)
// CQRS/Event Sourcing: API Gateway + Lambda + DynamoDB event store
//                      + Kinesis + DynamoDB Streams + ElastiCache (Redis).
// Source of truth is the .md slice spec — regenerate, don't hand-edit.
// ─────────────────────────────────────────────────────────────

// ── Model-layer findings (raised, never resolved in code) ──────────
// Unmapped fields — the model leaves these unplaced:
//   - ProcessPayment.gatewayRef: no emitted event carries this field [OPEN]
//   - PaymentSucceeded.amount: no source stated; not carried by the command [OPEN]
//   - PaymentSucceeded.transactionRef: no source stated; not carried by the command [OPEN]
//   - PaymentSucceeded.succeededAt: no source stated; not carried by the command [OPEN]

import { DomainEvent, EventTypes, createEvent, response, BoundaryBranch, readBoundary, appendWithinBoundary, ConcurrencyError, publishToKinesis } from "../shared/event-store";

// Commands
/** Process Payment */
export interface ProcessPayment {
    paymentId: string;
    gatewayRef: string;
}

// Domain events
/** Payment Succeeded */
export interface PaymentSucceeded {
    paymentId: string;
    bookingId: string;
    amount: number;
    transactionRef: string;
    succeededAt: string;
}

// External events
/** Gateway Confirmed */
export interface GatewayConfirmed {
    paymentId: string;
    transactionRef: string;
    confirmedAt: string;
}

// ── Decision state — never stored; folded from the boundary events ──
// rehydrate() folds the events inside the command's consistency boundary
// (the events readBoundary() returned); validateCommand() enforces the
// slice's business rules. This is the pure core of the write side. There is
// no aggregate id or version — the boundary is a tag-scoped set of events,
// and `eventCount` records how many were folded (for reference/debugging).
export interface DecisionState {
    status: string | null;
    paymentId?: string;
    bookingId?: string;
    amount?: number;
    transactionRef?: string;
    succeededAt?: string;
    eventCount: number;
}

export function rehydrate(events: DomainEvent[]): DecisionState {
    let state: DecisionState = { status: null, eventCount: 0 };
    for (const event of events) state = applyEvent(state, event);
    return state;
}

function applyEvent(state: DecisionState, event: DomainEvent): DecisionState {
    switch (event.eventType) {
        case EventTypes.PAYMENT_SUCCEEDED:
            return {
                ...state,
                // TODO: set the status this event transitions to (e.g. 'PAYMENT_SUCCEEDED').
                status: state.status,
                paymentId: event.tags.paymentId as string,
                bookingId: event.tags.bookingId as string,
                amount: event.payload.amount as number,
                transactionRef: event.payload.transactionRef as string,
                succeededAt: event.payload.succeededAt as string,
                eventCount: state.eventCount + 1,
            };
        default:
            return state;
    }
}

// Business-rule validation. Returns null when valid, else the error
// message — copied verbatim from the slice's `then error[...]` items.
export function validateCommand(
    state: DecisionState,
    command: string
): string | null {
    switch (command) {
        case "ProcessPayment":
            // TODO: enforce this command's invariants against state.
            return null;
        default:
            return `Unknown command: ${command}`;
    }
}

// ── Command Lambda (write side, DCB-enforced) ───────────────────────
// API Gateway → this handler. Each command's `reads [types] by [axes]`
// becomes a consistency boundary: readBoundary() queries the per-axis
// GSIs and folds the matching events into decision state; the new event
// is appended with appendWithinBoundary(), which atomically asserts the
// boundary has not moved (TransactWriteItems over per-tag guard items) and
// retries on ConcurrencyError. State, event store, and helpers come from
// the shared runtime.
import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';

const MAX_RETRIES = 5;

export async function handler(
    event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> {
    try {
        if (event.httpMethod !== 'POST') return response(405, { error: 'Method not allowed' });
        const body = event.body ? JSON.parse(event.body) : {};
        // Only one command in this slice.
        return handleProcessPayment(event, body);
    } catch (err) {
        console.error('Command handler error:', err);
        return response(500, { error: 'Internal server error' });
    }
}

async function handleProcessPayment(
    event: APIGatewayProxyEvent,
    body: Record<string, unknown>
): Promise<APIGatewayProxyResult> {
    const paymentId = String(event.pathParameters?.id ?? body.paymentId ?? '');
    const bookingId = String(event.pathParameters?.id ?? body.bookingId ?? '');
    const { gatewayRef } = body as {
        gatewayRef?: string;
    };
    if (!paymentId) return response(400, { error: "paymentId is required" });

    // Consistency boundary (DCB): one branch per DSL `reads [...] by axis`.
    const criteria: BoundaryBranch[] = [
        { axis: "paymentId", value: paymentId, types: [EventTypes.PAYMENT_SUBMITTED, EventTypes.PAYMENT_SUCCEEDED] },
    ];

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        const { events, guards } = await readBoundary(criteria);
        const state = rehydrate(events);

        // Enforce business rules against the boundary state.
        const validationError = validateCommand(state, "ProcessPayment");
        if (validationError) return response(409, { error: validationError });

        const tags: Record<string, string> = {
            paymentId: paymentId,
            bookingId: bookingId,
        };
        const payload: Record<string, unknown> = {
            amount: body.amount,
            transactionRef: body.transactionRef,
            succeededAt: body.succeededAt,
        };
        const domainEvent = createEvent(EventTypes.PAYMENT_SUCCEEDED, tags, payload);

        try {
            // Atomic: assert the boundary is unchanged, then append.
            await appendWithinBoundary(domainEvent, guards);
            await publishToKinesis(domainEvent);
            return response(200, { eventId: domainEvent.eventId, paymentId });
        } catch (err) {
            // A concurrent command moved the boundary — reload and retry.
            if (err instanceof ConcurrencyError) continue;
            throw err;
        }
    }
    return response(409, { error: 'Conflict: boundary contended, retries exhausted' });
}

// ── CDK wiring ──────────────────────────────────────────────────────
// This handler is wired into infrastructure by the model-level 'infra'
// target — generate infra/stacks/regional-stack.ts (the "Generate AWS
// infra (CDK)" action on the model view). That stack declares the Lambda,
// its env + grants, the API Gateway route, and (for read models) the
// DynamoDB Streams event source — the single source of truth for
// deployment. Nothing to paste here.
