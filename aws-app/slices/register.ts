// ─────────────────────────────────────────────────────────────
// Generated from slice: register
// Target: AWS-native (CDK + Lambda, TypeScript)
// CQRS/Event Sourcing: API Gateway + Lambda + DynamoDB event store
//                      + Kinesis + DynamoDB Streams + ElastiCache (Redis).
// Source of truth is the .md slice spec — regenerate, don't hand-edit.
// ─────────────────────────────────────────────────────────────

// ── Model-layer findings (raised, never resolved in code) ──────────
// Unmapped fields — the model leaves these unplaced:
//   - Register.password: no emitted event carries this field [OPEN]
//   - RegUi.password: no emitted event carries this field [OPEN]
//   - Registered.registeredAt: no source stated; not carried by the command [OPEN]

import { DomainEvent, EventTypes, createEvent, response, BoundaryBranch, readBoundary, appendWithinBoundary, ConcurrencyError, publishToKinesis } from "../shared/event-store";

// Commands
/** Register */
export interface Register {
    name: string;
    email: string;
    password: string;
}

// Domain events
/** Registered */
export interface Registered {
    email: string;
    name: string;
    registeredAt: string;
}

// ── Decision state — never stored; folded from the boundary events ──
// rehydrate() folds the events inside the command's consistency boundary
// (the events readBoundary() returned); validateCommand() enforces the
// slice's business rules. This is the pure core of the write side. There is
// no aggregate id or version — the boundary is a tag-scoped set of events,
// and `eventCount` records how many were folded (for reference/debugging).
export interface DecisionState {
    status: string | null;
    email?: string;
    name?: string;
    registeredAt?: string;
    eventCount: number;
}

export function rehydrate(events: DomainEvent[]): DecisionState {
    let state: DecisionState = { status: null, eventCount: 0 };
    for (const event of events) state = applyEvent(state, event);
    return state;
}

function applyEvent(state: DecisionState, event: DomainEvent): DecisionState {
    switch (event.eventType) {
        case EventTypes.REGISTERED:
            return {
                ...state,
                // TODO: set the status this event transitions to (e.g. 'REGISTERED').
                status: state.status,
                email: event.tags.email as string,
                name: event.payload.name as string,
                registeredAt: event.payload.registeredAt as string,
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
        case "Register":
            // TODO: gate on state.status; reject with the rule below when invalid.
            // if (/* invalid */ false) return "A guest cannot be registered more than once";
            return null;
        default:
            return `Unknown command: ${command}`;
    }
}

// Business rules enforced by this slice (verbatim from the spec tests):
//   - A guest cannot be registered more than once

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
        return handleRegister(event, body);
    } catch (err) {
        console.error('Command handler error:', err);
        return response(500, { error: 'Internal server error' });
    }
}

async function handleRegister(
    event: APIGatewayProxyEvent,
    body: Record<string, unknown>
): Promise<APIGatewayProxyResult> {
    const email = String(event.pathParameters?.id ?? body.email ?? '');
    const { name, password } = body as {
        name?: string;
        password?: string;
    };
    if (!email) return response(400, { error: "email is required" });

    // Consistency boundary (DCB): one branch per DSL `reads [...] by axis`.
    const criteria: BoundaryBranch[] = [
        { axis: "email", value: email, types: [EventTypes.REGISTERED] },
    ];

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        const { events, guards } = await readBoundary(criteria);
        const state = rehydrate(events);

        // Enforce business rules against the boundary state.
        const validationError = validateCommand(state, "Register");
        if (validationError) return response(409, { error: validationError });

        const tags: Record<string, string> = {
            email: email,
        };
        const payload: Record<string, unknown> = {
            name: name,
            registeredAt: body.registeredAt,
        };
        const domainEvent = createEvent(EventTypes.REGISTERED, tags, payload);

        try {
            // Atomic: assert the boundary is unchanged, then append.
            await appendWithinBoundary(domainEvent, guards);
            await publishToKinesis(domainEvent);
            return response(200, { eventId: domainEvent.eventId, email });
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
