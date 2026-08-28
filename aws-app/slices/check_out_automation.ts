// ─────────────────────────────────────────────────────────────
// Generated from slice: check_out_automation
// Target: AWS-native (CDK + Lambda, TypeScript)
// CQRS/Event Sourcing: API Gateway + Lambda + DynamoDB event store
//                      + Kinesis + DynamoDB Streams + ElastiCache (Redis).
// Source of truth is the .md slice spec — regenerate, don't hand-edit.
// ─────────────────────────────────────────────────────────────

// ── Model-layer findings (raised, never resolved in code) ──────────
// Unmapped fields — the model leaves these unplaced:
//   - CheckedOut.checkedOutAt: no source stated; not carried by the command [OPEN]

import { DomainEvent, EventTypes, createEvent, response, BoundaryBranch, readBoundary, appendWithinBoundary, ConcurrencyError, publishToKinesis, invokeSageMaker } from "../shared/event-store";

// Commands
/** Checked Out */
export interface CheckOut {
    bookingId: string;
}

// Domain events
/** Checked Out */
export interface CheckedOut {
    bookingId: string;
    roomNumber: number;
    email: string;
    checkedOutAt: string;
}

// Read models
/** Guest Roster */
export interface GuestRosterReadModel {
    email: string;
    guestName: string;
    roomNumber: number;
    checkedInAt: string;
    isPresent: boolean;
}

// ── Decision state — never stored; folded from the boundary events ──
// rehydrate() folds the events inside the command's consistency boundary
// (the events readBoundary() returned); validateCommand() enforces the
// slice's business rules. This is the pure core of the write side. There is
// no aggregate id or version — the boundary is a tag-scoped set of events,
// and `eventCount` records how many were folded (for reference/debugging).
export interface DecisionState {
    status: string | null;
    bookingId?: string;
    roomNumber?: number;
    email?: string;
    checkedOutAt?: string;
    eventCount: number;
}

export function rehydrate(events: DomainEvent[]): DecisionState {
    let state: DecisionState = { status: null, eventCount: 0 };
    for (const event of events) state = applyEvent(state, event);
    return state;
}

function applyEvent(state: DecisionState, event: DomainEvent): DecisionState {
    switch (event.eventType) {
        case EventTypes.CHECKED_OUT:
            return {
                ...state,
                // TODO: set the status this event transitions to (e.g. 'CHECKED_OUT').
                status: state.status,
                bookingId: event.tags.bookingId as string,
                roomNumber: event.tags.roomNumber as unknown as number,
                email: event.tags.email as string,
                checkedOutAt: event.payload.checkedOutAt as string,
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
        case "CheckOut":
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
        // Route: handle "Checked Out"
        return handleCheckOut(event, body);
    } catch (err) {
        console.error('Command handler error:', err);
        return response(500, { error: 'Internal server error' });
    }
}

async function handleCheckOut(
    event: APIGatewayProxyEvent,
    body: Record<string, unknown>
): Promise<APIGatewayProxyResult> {
    const bookingId = String(event.pathParameters?.id ?? body.bookingId ?? '');
    const roomNumber = String(event.pathParameters?.id ?? body.roomNumber ?? '');
    const email = String(event.pathParameters?.id ?? body.email ?? '');
    if (!bookingId) return response(400, { error: "bookingId is required" });

    // Consistency boundary (DCB): one branch per DSL `reads [...] by axis`.
    const criteria: BoundaryBranch[] = [
        { axis: "bookingId", value: bookingId, types: [EventTypes.CHECKED_IN, EventTypes.CHECKED_OUT] },
    ];

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        const { events, guards } = await readBoundary(criteria);
        const state = rehydrate(events);

        // Enforce business rules against the boundary state.
        const validationError = validateCommand(state, "CheckOut");
        if (validationError) return response(409, { error: validationError });

        const tags: Record<string, string> = {
            bookingId: bookingId,
            roomNumber: roomNumber,
            email: email,
        };

        // ── Inference: call the SageMaker endpoint for this slice ──────────
        // Feature vector for the model. The command fields and boundary state
        // are the inputs; adjust the shape to match your endpoint's contract.
        const features: Record<string, unknown> = {
            bookingId: bookingId,
        };
        // Prediction returned by the endpoint (the event's inferred fields).
        const prediction = await invokeSageMaker<{
            checkedOutAt?: string;
        }>(features);

        const payload: Record<string, unknown> = {
            checkedOutAt: prediction.checkedOutAt,
        };
        const domainEvent = createEvent(EventTypes.CHECKED_OUT, tags, payload);

        try {
            // Atomic: assert the boundary is unchanged, then append.
            await appendWithinBoundary(domainEvent, guards);
            await publishToKinesis(domainEvent);
            return response(200, { eventId: domainEvent.eventId, bookingId });
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
