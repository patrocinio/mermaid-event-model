// ─────────────────────────────────────────────────────────────
// Generated from slice: add_room
// Target: AWS-native (CDK + Lambda, TypeScript)
// CQRS/Event Sourcing: API Gateway + Lambda + DynamoDB event store
//                      + Kinesis + DynamoDB Streams + ElastiCache (Redis).
// Source of truth is the .md slice spec — regenerate, don't hand-edit.
// ─────────────────────────────────────────────────────────────

import { DomainEvent, EventTypes, createEvent, response, BoundaryBranch, readBoundary, appendWithinBoundary, ConcurrencyError, publishToKinesis } from "../shared/event-store";

// Commands
/** Add Room */
export interface AddRoom {
    roomNumber: number;
    floor: number;
    roomType: string;
    capacity: number;
}

// Domain events
/** Room Added */
export interface RoomAdded {
    roomNumber: number;
    floor: number;
    roomType: string;
    capacity: number;
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
    floor?: number;
    roomType?: string;
    capacity?: number;
    eventCount: number;
}

export function rehydrate(events: DomainEvent[]): DecisionState {
    let state: DecisionState = { status: null, eventCount: 0 };
    for (const event of events) state = applyEvent(state, event);
    return state;
}

function applyEvent(state: DecisionState, event: DomainEvent): DecisionState {
    switch (event.eventType) {
        case EventTypes.ROOM_ADDED:
            return {
                ...state,
                // TODO: set the status this event transitions to (e.g. 'ROOM_ADDED').
                status: state.status,
                roomNumber: event.tags.roomNumber as unknown as number,
                floor: event.payload.floor as number,
                roomType: event.payload.roomType as string,
                capacity: event.payload.capacity as number,
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
        case "AddRoom":
            // TODO: gate on state.status; reject with the rule below when invalid.
            // if (/* invalid */ false) return "A room with that number already exists";
            return null;
        default:
            return `Unknown command: ${command}`;
    }
}

// Business rules enforced by this slice (verbatim from the spec tests):
//   - A room with that number already exists

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
        // Route: handle "Add Room"
        return handleAddRoom(event, body);
    } catch (err) {
        console.error('Command handler error:', err);
        return response(500, { error: 'Internal server error' });
    }
}

async function handleAddRoom(
    event: APIGatewayProxyEvent,
    body: Record<string, unknown>
): Promise<APIGatewayProxyResult> {
    const roomNumber = String(event.pathParameters?.id ?? body.roomNumber ?? '');
    const { floor, roomType, capacity } = body as {
        floor?: number;
        roomType?: string;
        capacity?: number;
    };
    if (!roomNumber) return response(400, { error: "roomNumber is required" });

    // Consistency boundary (DCB): one branch per DSL `reads [...] by axis`.
    const criteria: BoundaryBranch[] = [
        { axis: "roomNumber", value: roomNumber, types: [EventTypes.ROOM_ADDED] },
    ];

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        const { events, guards } = await readBoundary(criteria);
        const state = rehydrate(events);

        // Enforce business rules against the boundary state.
        const validationError = validateCommand(state, "AddRoom");
        if (validationError) return response(409, { error: validationError });

        const tags: Record<string, string> = {
            roomNumber: roomNumber,
        };
        const payload: Record<string, unknown> = {
            floor: floor,
            roomType: roomType,
            capacity: capacity,
        };
        const domainEvent = createEvent(EventTypes.ROOM_ADDED, tags, payload);

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
