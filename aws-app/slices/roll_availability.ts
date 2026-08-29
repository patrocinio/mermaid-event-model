// ─────────────────────────────────────────────────────────────
// Generated from slice: roll_availability
// Target: AWS-native (CDK + Lambda, TypeScript)
// CQRS/Event Sourcing: API Gateway + Lambda + DynamoDB event store
//                      + Kinesis + DynamoDB Streams + ElastiCache (Redis).
// Source of truth is the .md slice spec — regenerate, don't hand-edit.
// ─────────────────────────────────────────────────────────────

// ── Model-layer findings (raised, never resolved in code) ──────────
// Unmapped fields — the model leaves these unplaced:
//   - AvailabilityRolled.rolledAt: no source stated; not carried by the command [OPEN]

import { DomainEvent, EventTypes, createEvent, response, BoundaryBranch, readBoundary, appendWithinBoundary, ConcurrencyError, publishToKinesis, invokeSageMaker } from "../shared/event-store";

// Commands
/** Roll Availability */
export interface RollAvailability {
    roomNumber: number;
    roomType: string;
    capacity: number;
    fromNight: string;
    throughNight: string;
}

// Domain events
/** Availability Rolled */
export interface AvailabilityRolled {
    roomNumber: number;
    roomType: string;
    capacity: number;
    fromNight: string;
    throughNight: string;
    rolledAt: string;
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

// ── Decision state — never stored; folded from the boundary events ──
// rehydrate() folds the events inside the command's consistency boundary
// (the events readBoundary() returned); validateCommand() enforces the
// slice's business rules. This is the pure core of the write side. There is
// no aggregate id or version — the boundary is a tag-scoped set of events,
// and `eventCount` records how many were folded (for reference/debugging).
export interface DecisionState {
    status: string | null;
    roomNumber?: number;
    roomType?: string;
    capacity?: number;
    fromNight?: string;
    throughNight?: string;
    rolledAt?: string;
    eventCount: number;
}

export function rehydrate(events: DomainEvent[]): DecisionState {
    let state: DecisionState = { status: null, eventCount: 0 };
    for (const event of events) state = applyEvent(state, event);
    return state;
}

function applyEvent(state: DecisionState, event: DomainEvent): DecisionState {
    switch (event.eventType) {
        case EventTypes.AVAILABILITY_ROLLED:
            return {
                ...state,
                // TODO: set the status this event transitions to (e.g. 'AVAILABILITY_ROLLED').
                status: state.status,
                roomNumber: event.tags.roomNumber as unknown as number,
                roomType: event.payload.roomType as string,
                capacity: event.payload.capacity as number,
                fromNight: event.payload.fromNight as string,
                throughNight: event.payload.throughNight as string,
                rolledAt: event.payload.rolledAt as string,
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
        case "RollAvailability":
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
        return handleRollAvailability(event, body);
    } catch (err) {
        console.error('Command handler error:', err);
        return response(500, { error: 'Internal server error' });
    }
}

async function handleRollAvailability(
    event: APIGatewayProxyEvent,
    body: Record<string, unknown>
): Promise<APIGatewayProxyResult> {
    const roomNumber = String(event.pathParameters?.id ?? body.roomNumber ?? '');
    const { roomType, capacity, fromNight, throughNight } = body as {
        roomType?: string;
        capacity?: number;
        fromNight?: string;
        throughNight?: string;
    };
    if (!roomNumber) return response(400, { error: "roomNumber is required" });

    // Consistency boundary (DCB): one branch per DSL `reads [...] by axis`.
    const criteria: BoundaryBranch[] = [
        { axis: "roomNumber", value: roomNumber, types: [EventTypes.AVAILABILITY_ROLLED] },
    ];

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        const { events, guards } = await readBoundary(criteria);
        const state = rehydrate(events);

        // Enforce business rules against the boundary state.
        const validationError = validateCommand(state, "RollAvailability");
        if (validationError) return response(409, { error: validationError });

        const tagsRaw: Record<string, string> = {
            roomNumber: roomNumber,
        };
        const tags: Record<string, string> = Object.fromEntries(
            Object.entries(tagsRaw).filter(([, v]) => v !== undefined && v !== '')
        );

        // ── Inference: call the SageMaker endpoint for this slice ──────────
        // Feature vector for the model, in precedence order (later overrides
        // earlier): the request body (the demand snapshot the caller/scheduler
        // supplies), the rehydrated boundary state, then the typed command
        // fields. Adjust the shape to match your endpoint's contract.
        const features: Record<string, unknown> = {
            ...body,
            ...(state as unknown as Record<string, unknown>),
            roomNumber: roomNumber,
            roomType: roomType,
            capacity: capacity,
            fromNight: fromNight,
            throughNight: throughNight,
        };
        // Prediction returned by the endpoint (the event's inferred fields).
        const prediction = await invokeSageMaker<{
            rolledAt?: string;
        }>(features);

        const payload: Record<string, unknown> = {
            roomType: roomType,
            capacity: capacity,
            fromNight: fromNight,
            throughNight: throughNight,
            rolledAt: prediction.rolledAt,
        };
        const domainEvent = createEvent(EventTypes.AVAILABILITY_ROLLED, tags, payload);

        try {
            // Atomic: assert the boundary is unchanged, then append.
            await appendWithinBoundary(domainEvent, guards);
            await publishToKinesis(domainEvent);
            return response(200, { eventId: domainEvent.eventId, roomNumber });
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
