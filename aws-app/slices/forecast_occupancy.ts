// ─────────────────────────────────────────────────────────────
// Generated from slice: forecast_occupancy
// Target: AWS-native (CDK + Lambda, TypeScript)
// CQRS/Event Sourcing: API Gateway + Lambda + DynamoDB event store
//                      + Kinesis + DynamoDB Streams + ElastiCache (Redis).
// Source of truth is the .md slice spec — regenerate, don't hand-edit.
// ─────────────────────────────────────────────────────────────

// ── Model-layer findings (raised, never resolved in code) ──────────
// Unmapped fields — the model leaves these unplaced:
//   - ForecastOccupancy.horizonNights: no emitted event carries this field [decided exclusion]
//   - OccupancyForecasted.forecastFrom: no source stated; not carried by the command [decided exclusion]
//   - OccupancyForecasted.forecastThrough: no source stated; not carried by the command [decided exclusion]
//   - OccupancyForecasted.predictedOccupancyRate: no source stated; not carried by the command [decided exclusion]
//   - OccupancyForecasted.predictedDemand: no source stated; not carried by the command [decided exclusion]
//   - OccupancyForecasted.modelVersion: no source stated; not carried by the command [decided exclusion]
//   - OccupancyForecasted.endpointName: no source stated; not carried by the command [decided exclusion]
//   - OccupancyForecasted.forecastedAt: no source stated; not carried by the command [decided exclusion]

import { DomainEvent, EventTypes, createEvent, response, BoundaryBranch, readBoundary, appendWithinBoundary, ConcurrencyError, publishToKinesis, invokeSageMaker } from "../shared/event-store";

// Commands
/** Forecast Occupancy */
export interface ForecastOccupancy {
    roomType: string;
    horizonNights: number;
}

// Domain events
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
/** Forecast Tick */
export interface ForecastTick {
    occurredAt: string;
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

// ── Decision state — never stored; folded from the boundary events ──
// rehydrate() folds the events inside the command's consistency boundary
// (the events readBoundary() returned); validateCommand() enforces the
// slice's business rules. This is the pure core of the write side. There is
// no aggregate id or version — the boundary is a tag-scoped set of events,
// and `eventCount` records how many were folded (for reference/debugging).
export interface DecisionState {
    status: string | null;
    forecastId?: string;
    roomType?: string;
    forecastFrom?: string;
    forecastThrough?: string;
    predictedOccupancyRate?: number;
    predictedDemand?: number;
    modelVersion?: string;
    endpointName?: string;
    forecastedAt?: string;
    eventCount: number;
}

export function rehydrate(events: DomainEvent[]): DecisionState {
    let state: DecisionState = { status: null, eventCount: 0 };
    for (const event of events) state = applyEvent(state, event);
    return state;
}

function applyEvent(state: DecisionState, event: DomainEvent): DecisionState {
    switch (event.eventType) {
        case EventTypes.OCCUPANCY_FORECASTED:
            return {
                ...state,
                // TODO: set the status this event transitions to (e.g. 'OCCUPANCY_FORECASTED').
                status: state.status,
                forecastId: event.tags.forecastId as string,
                roomType: event.tags.roomType as string,
                forecastFrom: event.payload.forecastFrom as string,
                forecastThrough: event.payload.forecastThrough as string,
                predictedOccupancyRate: event.payload.predictedOccupancyRate as number,
                predictedDemand: event.payload.predictedDemand as number,
                modelVersion: event.payload.modelVersion as string,
                endpointName: event.payload.endpointName as string,
                forecastedAt: event.payload.forecastedAt as string,
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
        case "ForecastOccupancy":
            // TODO: gate on state.status; reject with the rule below when invalid.
            // if (/* invalid */ false) return "Occupancy already forecasted for roomType and window";
            return null;
        default:
            return `Unknown command: ${command}`;
    }
}

// Business rules enforced by this slice (verbatim from the spec tests):
//   - Occupancy already forecasted for roomType and window

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
        return handleForecastOccupancy(event, body);
    } catch (err) {
        console.error('Command handler error:', err);
        return response(500, { error: 'Internal server error' });
    }
}

async function handleForecastOccupancy(
    event: APIGatewayProxyEvent,
    body: Record<string, unknown>
): Promise<APIGatewayProxyResult> {
    const roomType = String(event.pathParameters?.id ?? body.roomType ?? '');
    const forecastId = String(event.pathParameters?.id ?? body.forecastId ?? '');
    const { horizonNights } = body as {
        horizonNights?: number;
    };
    if (!roomType) return response(400, { error: "roomType is required" });

    // Consistency boundary (DCB): one branch per DSL `reads [...] by axis`.
    const criteria: BoundaryBranch[] = [
        { axis: "roomType", value: roomType, types: [EventTypes.OCCUPANCY_FORECASTED] },
    ];

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        const { events, guards } = await readBoundary(criteria);
        const state = rehydrate(events);

        // Enforce business rules against the boundary state.
        const validationError = validateCommand(state, "ForecastOccupancy");
        if (validationError) return response(409, { error: validationError });

        const tagsRaw: Record<string, string> = {
            forecastId: forecastId || (state.forecastId == null ? '' : String(state.forecastId)),
            roomType: roomType,
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
            roomType: roomType,
            horizonNights: horizonNights,
        };
        // Prediction returned by the endpoint (the event's inferred fields).
        const prediction = await invokeSageMaker<{
            forecastFrom?: string;
            forecastThrough?: string;
            predictedOccupancyRate?: number;
            predictedDemand?: number;
            modelVersion?: string;
            endpointName?: string;
            forecastedAt?: string;
        }>(features);

        const payload: Record<string, unknown> = {
            forecastFrom: prediction.forecastFrom,
            forecastThrough: prediction.forecastThrough,
            predictedOccupancyRate: prediction.predictedOccupancyRate,
            predictedDemand: prediction.predictedDemand,
            modelVersion: prediction.modelVersion,
            endpointName: prediction.endpointName,
            forecastedAt: prediction.forecastedAt,
        };
        const domainEvent = createEvent(EventTypes.OCCUPANCY_FORECASTED, tags, payload);

        try {
            // Atomic: assert the boundary is unchanged, then append.
            await appendWithinBoundary(domainEvent, guards);
            await publishToKinesis(domainEvent);
            return response(200, { eventId: domainEvent.eventId, roomType });
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
