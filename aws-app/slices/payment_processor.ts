// ─────────────────────────────────────────────────────────────
// Generated from slice: payment_processor
// Target: AWS-native (CDK + Lambda, TypeScript)
// CQRS/Event Sourcing: API Gateway + Lambda + DynamoDB event store
//                      + Kinesis + DynamoDB Streams + ElastiCache (Redis).
// Source of truth is the .md slice spec — regenerate, don't hand-edit.
// ─────────────────────────────────────────────────────────────

// ── Model-layer findings (raised, never resolved in code) ──────────
// Unmapped fields — the model leaves these unplaced:
//   - SubmitPayment.currency: no emitted event carries this field [OPEN]
//   - SubmitPayment.paymentMethod: no emitted event carries this field [OPEN]
//   - PaymentSubmitted.bookingId: no source stated; not carried by the command [OPEN]
//   - PaymentSubmitted.submittedAt: no source stated; not carried by the command [OPEN]

import { DomainEvent, EventTypes, createEvent, response, BoundaryBranch, readBoundary, appendWithinBoundary, ConcurrencyError, publishToKinesis, invokeSageMaker } from "../shared/event-store";

// Commands
/** Submit Payment */
export interface SubmitPayment {
    paymentId: string;
    amount: number;
    currency: string;
    paymentMethod: string;
}

// Domain events
/** Payment Submitted */
export interface PaymentSubmitted {
    paymentId: string;
    bookingId: string;
    amount: number;
    submittedAt: string;
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
    submittedAt?: string;
    eventCount: number;
}

export function rehydrate(events: DomainEvent[]): DecisionState {
    let state: DecisionState = { status: null, eventCount: 0 };
    for (const event of events) state = applyEvent(state, event);
    return state;
}

function applyEvent(state: DecisionState, event: DomainEvent): DecisionState {
    switch (event.eventType) {
        case EventTypes.PAYMENT_SUBMITTED:
            return {
                ...state,
                // TODO: set the status this event transitions to (e.g. 'PAYMENT_SUBMITTED').
                status: state.status,
                paymentId: event.tags.paymentId as string,
                bookingId: event.payload.bookingId as string,
                amount: event.payload.amount as number,
                submittedAt: event.payload.submittedAt as string,
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
        case "SubmitPayment":
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
        // Route: handle "Submit Payment"
        return handleSubmitPayment(event, body);
    } catch (err) {
        console.error('Command handler error:', err);
        return response(500, { error: 'Internal server error' });
    }
}

async function handleSubmitPayment(
    event: APIGatewayProxyEvent,
    body: Record<string, unknown>
): Promise<APIGatewayProxyResult> {
    const paymentId = String(event.pathParameters?.id ?? body.paymentId ?? '');
    const { amount, currency, paymentMethod } = body as {
        amount?: number;
        currency?: string;
        paymentMethod?: string;
    };
    if (!paymentId) return response(400, { error: "paymentId is required" });

    // Consistency boundary (DCB): one branch per DSL `reads [...] by axis`.
    const criteria: BoundaryBranch[] = [
        { axis: "paymentId", value: paymentId, types: [EventTypes.PAYMENT_REQUESTED, EventTypes.PAYMENT_SUBMITTED] },
    ];

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        const { events, guards } = await readBoundary(criteria);
        const state = rehydrate(events);

        // Enforce business rules against the boundary state.
        const validationError = validateCommand(state, "SubmitPayment");
        if (validationError) return response(409, { error: validationError });

        const tags: Record<string, string> = {
            paymentId: paymentId,
        };

        // ── Inference: call the SageMaker endpoint for this slice ──────────
        // Feature vector for the model. The command fields and boundary state
        // are the inputs; adjust the shape to match your endpoint's contract.
        const features: Record<string, unknown> = {
            paymentId: paymentId,
            amount: amount,
            currency: currency,
            paymentMethod: paymentMethod,
        };
        // Prediction returned by the endpoint (the event's inferred fields).
        const prediction = await invokeSageMaker<{
            bookingId?: string;
            submittedAt?: string;
        }>(features);

        const payload: Record<string, unknown> = {
            bookingId: prediction.bookingId,
            amount: amount,
            submittedAt: prediction.submittedAt,
        };
        const domainEvent = createEvent(EventTypes.PAYMENT_SUBMITTED, tags, payload);

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
