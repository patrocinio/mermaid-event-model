// ─────────────────────────────────────────────────────────────
// Generated from slice: commands
// Target: AWS-native (CDK + Lambda, TypeScript)
// CQRS/Event Sourcing: API Gateway + Lambda + DynamoDB event store
//                      + Kinesis + DynamoDB Streams + ElastiCache (Redis).
// Source of truth is the .md slice spec — regenerate, don't hand-edit.
// ─────────────────────────────────────────────────────────────

// ── Model-layer findings (raised, never resolved in code) ──────────
// Unmapped fields — the model leaves these unplaced:
//   - Register.password: no emitted event carries this field [OPEN]
//   - ReadyRoom.cleanedBy: no emitted event carries this field [OPEN]
//   - ProcessPayment.gatewayRef: no emitted event carries this field [OPEN]
//   - ForecastOccupancy.horizonNights: no emitted event carries this field [OPEN]
//   - RegUi.password: no emitted event carries this field [OPEN]
//   - MaintenanceUi.cleaningStatus: no emitted event carries this field [OPEN]
//   - CheckinUi.guestName: no emitted event carries this field [OPEN]
//   - SalesUi.totalRevenue: no emitted event carries this field [OPEN]
//   - SalesUi.transactionCount: no emitted event carries this field [OPEN]
//   - SalesUi.averageBookingValue: no emitted event carries this field [OPEN]
//   - SalesUi.revenueByRoomType: no emitted event carries this field [OPEN]
//   - Registered.registeredAt: no source stated; not carried by the command [OPEN]
//   - AvailabilityRolled.rolledAt: no source stated; not carried by the command [OPEN]
//   - Booked.bookedAt: no source stated; not carried by the command [OPEN]
//   - Ready.readiedAt: no source stated; not carried by the command [OPEN]
//   - CheckedIn.checkedInAt: no source stated; not carried by the command [OPEN]
//   - GuestLeft.departedAt: no source stated; not carried by the command [OPEN]
//   - CheckedOut.checkedOutAt: no source stated; not carried by the command [OPEN]
//   - PaymentRequested.requestedAt: no source stated; not carried by the command [OPEN]
//   - PaymentSubmitted.submittedAt: no source stated; not carried by the command [OPEN]
//   - PaymentSucceeded.transactionRef: no source stated; not carried by the command [OPEN]
//   - PaymentSucceeded.succeededAt: no source stated; not carried by the command [OPEN]
//   - OccupancyForecasted.forecastFrom: no source stated; not carried by the command [OPEN]
//   - OccupancyForecasted.forecastThrough: no source stated; not carried by the command [OPEN]
//   - OccupancyForecasted.predictedOccupancyRate: no source stated; not carried by the command [OPEN]
//   - OccupancyForecasted.predictedDemand: no source stated; not carried by the command [OPEN]
//   - OccupancyForecasted.modelVersion: no source stated; not carried by the command [OPEN]
//   - OccupancyForecasted.endpointName: no source stated; not carried by the command [OPEN]
//   - OccupancyForecasted.forecastedAt: no source stated; not carried by the command [OPEN]

import { DomainEvent, EventTypes, createEvent, response, BoundaryBranch, readBoundary, appendWithinBoundary, ConcurrencyError, publishToKinesis, invokeSageMaker } from "../shared/event-store";

// Commands
/** Register */
export interface Register {
    name: string;
    email: string;
    password: string;
}

/** Add Room */
export interface AddRoom {
    roomNumber: number;
    floor: number;
    roomType: string;
    capacity: number;
}

/** Roll Availability */
export interface RollAvailability {
    roomNumber: number;
    roomType: string;
    capacity: number;
    fromNight: string;
    throughNight: string;
}

/** Book Room */
export interface BookRoom {
    email: string;
    roomNumber: number;
    checkIn: string;
    checkOut: string;
}

/** Ready Room */
export interface ReadyRoom {
    roomNumber: number;
    cleanedBy: string;
}

/** Check-in */
export interface Checkin {
    bookingId: string;
}

/** Hotel Proximity Translator */
export interface HotelProximityTranslator {
    email: string;
}

/** Checked Out */
export interface CheckOut {
    bookingId: string;
}

/** Pay */
export interface Pay {
    bookingId: string;
    amount: number;
    currency: string;
    paymentMethod: string;
}

/** Submit Payment */
export interface SubmitPayment {
    paymentId: string;
    amount: number;
    currency: string;
    paymentMethod: string;
}

/** Process Payment */
export interface ProcessPayment {
    paymentId: string;
    gatewayRef: string;
}

/** Forecast Occupancy */
export interface ForecastOccupancy {
    roomType: string;
    horizonNights: number;
}

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
    roomNumber?: number;
    floor?: number;
    roomType?: string;
    capacity?: number;
    fromNight?: string;
    throughNight?: string;
    rolledAt?: string;
    bookingId?: string;
    checkIn?: string;
    checkOut?: string;
    bookedAt?: string;
    checkedOutAt?: string;
    readiedAt?: string;
    checkedInAt?: string;
    paymentId?: string;
    amount?: number;
    currency?: string;
    paymentMethod?: string;
    requestedAt?: string;
    transactionRef?: string;
    succeededAt?: string;
    submittedAt?: string;
    forecastId?: string;
    forecastFrom?: string;
    forecastThrough?: string;
    predictedOccupancyRate?: number;
    predictedDemand?: number;
    modelVersion?: string;
    endpointName?: string;
    forecastedAt?: string;
    departedAt?: string;
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
        case EventTypes.BOOKED:
            return {
                ...state,
                // TODO: set the status this event transitions to (e.g. 'BOOKED').
                status: state.status,
                bookingId: event.tags.bookingId as string,
                roomNumber: event.tags.roomNumber as unknown as number,
                email: event.payload.email as string,
                checkIn: event.payload.checkIn as string,
                checkOut: event.payload.checkOut as string,
                bookedAt: event.payload.bookedAt as string,
                eventCount: state.eventCount + 1,
            };
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
        case EventTypes.READY:
            return {
                ...state,
                // TODO: set the status this event transitions to (e.g. 'READY').
                status: state.status,
                roomNumber: event.tags.roomNumber as unknown as number,
                readiedAt: event.payload.readiedAt as string,
                eventCount: state.eventCount + 1,
            };
        case EventTypes.CHECKED_IN:
            return {
                ...state,
                // TODO: set the status this event transitions to (e.g. 'CHECKED_IN').
                status: state.status,
                bookingId: event.tags.bookingId as string,
                email: event.tags.email as string,
                roomNumber: event.payload.roomNumber as number,
                checkedInAt: event.payload.checkedInAt as string,
                eventCount: state.eventCount + 1,
            };
        case EventTypes.PAYMENT_REQUESTED:
            return {
                ...state,
                // TODO: set the status this event transitions to (e.g. 'PAYMENT_REQUESTED').
                status: state.status,
                paymentId: event.tags.paymentId as string,
                bookingId: event.tags.bookingId as string,
                amount: event.payload.amount as number,
                currency: event.payload.currency as string,
                paymentMethod: event.payload.paymentMethod as string,
                requestedAt: event.payload.requestedAt as string,
                eventCount: state.eventCount + 1,
            };
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
        case "Register":
            // TODO: enforce this command's invariants against state.
            return null;
        case "AddRoom":
            // TODO: enforce this command's invariants against state.
            return null;
        case "RollAvailability":
            // TODO: enforce this command's invariants against state.
            return null;
        case "BookRoom":
            // TODO: enforce this command's invariants against state.
            return null;
        case "ReadyRoom":
            // TODO: enforce this command's invariants against state.
            return null;
        case "Checkin":
            // TODO: enforce this command's invariants against state.
            return null;
        case "HotelProximityTranslator":
            // TODO: enforce this command's invariants against state.
            return null;
        case "CheckOut":
            // TODO: enforce this command's invariants against state.
            return null;
        case "Pay":
            // TODO: enforce this command's invariants against state.
            return null;
        case "SubmitPayment":
            // TODO: enforce this command's invariants against state.
            return null;
        case "ProcessPayment":
            // TODO: enforce this command's invariants against state.
            return null;
        case "ForecastOccupancy":
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
        // Route: handle "Register"
        return handleRegister(event, body);
        // Route: handle "Add Room"
        return handleAddRoom(event, body);
        // Route: handle "Roll Availability"
        return handleRollAvailability(event, body);
        // Route: handle "Book Room"
        return handleBookRoom(event, body);
        // Route: handle "Ready Room"
        return handleReadyRoom(event, body);
        // Route: handle "Check-in"
        return handleCheckin(event, body);
        // Route: handle "Hotel Proximity Translator"
        return handleHotelProximityTranslator(event, body);
        // Route: handle "Checked Out"
        return handleCheckOut(event, body);
        // Route: handle "Pay"
        return handlePay(event, body);
        // Route: handle "Submit Payment"
        return handleSubmitPayment(event, body);
        // Route: handle "Process Payment"
        return handleProcessPayment(event, body);
        // Route: handle "Forecast Occupancy"
        return handleForecastOccupancy(event, body);
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

        // ── Inference: call the SageMaker endpoint for this slice ──────────
        // Feature vector for the model. The command fields and boundary state
        // are the inputs; adjust the shape to match your endpoint's contract.
        const features: Record<string, unknown> = {
            name: name,
            email: email,
            password: password,
        };
        // Prediction returned by the endpoint (the event's inferred fields).
        const prediction = await invokeSageMaker<{
            registeredAt?: string;
        }>(features);

        const payload: Record<string, unknown> = {
            name: name,
            registeredAt: prediction.registeredAt,
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

        // ── Inference: call the SageMaker endpoint for this slice ──────────
        // Feature vector for the model. The command fields and boundary state
        // are the inputs; adjust the shape to match your endpoint's contract.
        const features: Record<string, unknown> = {
            roomNumber: roomNumber,
            floor: floor,
            roomType: roomType,
            capacity: capacity,
        };
        const prediction = await invokeSageMaker(features);

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

        const tags: Record<string, string> = {
            roomNumber: roomNumber,
        };

        // ── Inference: call the SageMaker endpoint for this slice ──────────
        // Feature vector for the model. The command fields and boundary state
        // are the inputs; adjust the shape to match your endpoint's contract.
        const features: Record<string, unknown> = {
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

async function handleBookRoom(
    event: APIGatewayProxyEvent,
    body: Record<string, unknown>
): Promise<APIGatewayProxyResult> {
    const roomNumber = String(event.pathParameters?.id ?? body.roomNumber ?? '');
    const email = String(event.pathParameters?.id ?? body.email ?? '');
    const bookingId = String(event.pathParameters?.id ?? body.bookingId ?? '');
    const { checkIn, checkOut } = body as {
        checkIn?: string;
        checkOut?: string;
    };
    if (!roomNumber) return response(400, { error: "roomNumber is required" });
    if (!email) return response(400, { error: "email is required" });

    // Consistency boundary (DCB): one branch per DSL `reads [...] by axis`.
    const criteria: BoundaryBranch[] = [
        { axis: "roomNumber", value: roomNumber, types: [EventTypes.ROOM_ADDED, EventTypes.BOOKED, EventTypes.CHECKED_OUT] },
        { axis: "email", value: email, types: [EventTypes.REGISTERED] },
    ];

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        const { events, guards } = await readBoundary(criteria);
        const state = rehydrate(events);

        // Enforce business rules against the boundary state.
        const validationError = validateCommand(state, "BookRoom");
        if (validationError) return response(409, { error: validationError });

        const tags: Record<string, string> = {
            bookingId: bookingId,
            roomNumber: roomNumber,
        };

        // ── Inference: call the SageMaker endpoint for this slice ──────────
        // Feature vector for the model. The command fields and boundary state
        // are the inputs; adjust the shape to match your endpoint's contract.
        const features: Record<string, unknown> = {
            email: email,
            roomNumber: roomNumber,
            checkIn: checkIn,
            checkOut: checkOut,
        };
        // Prediction returned by the endpoint (the event's inferred fields).
        const prediction = await invokeSageMaker<{
            bookedAt?: string;
        }>(features);

        const payload: Record<string, unknown> = {
            email: email,
            checkIn: checkIn,
            checkOut: checkOut,
            bookedAt: prediction.bookedAt,
        };
        const domainEvent = createEvent(EventTypes.BOOKED, tags, payload);

        try {
            // Atomic: assert the boundary is unchanged, then append.
            await appendWithinBoundary(domainEvent, guards);
            await publishToKinesis(domainEvent);
            return response(200, { eventId: domainEvent.eventId, roomNumber, email });
        } catch (err) {
            // A concurrent command moved the boundary — reload and retry.
            if (err instanceof ConcurrencyError) continue;
            throw err;
        }
    }
    return response(409, { error: 'Conflict: boundary contended, retries exhausted' });
}

async function handleReadyRoom(
    event: APIGatewayProxyEvent,
    body: Record<string, unknown>
): Promise<APIGatewayProxyResult> {
    const roomNumber = String(event.pathParameters?.id ?? body.roomNumber ?? '');
    const { cleanedBy } = body as {
        cleanedBy?: string;
    };
    if (!roomNumber) return response(400, { error: "roomNumber is required" });

    // Consistency boundary (DCB): one branch per DSL `reads [...] by axis`.
    const criteria: BoundaryBranch[] = [
        { axis: "roomNumber", value: roomNumber, types: [EventTypes.ROOM_ADDED, EventTypes.CHECKED_OUT, EventTypes.READY] },
    ];

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        const { events, guards } = await readBoundary(criteria);
        const state = rehydrate(events);

        // Enforce business rules against the boundary state.
        const validationError = validateCommand(state, "ReadyRoom");
        if (validationError) return response(409, { error: validationError });

        const tags: Record<string, string> = {
            roomNumber: roomNumber,
        };

        // ── Inference: call the SageMaker endpoint for this slice ──────────
        // Feature vector for the model. The command fields and boundary state
        // are the inputs; adjust the shape to match your endpoint's contract.
        const features: Record<string, unknown> = {
            roomNumber: roomNumber,
            cleanedBy: cleanedBy,
        };
        // Prediction returned by the endpoint (the event's inferred fields).
        const prediction = await invokeSageMaker<{
            readiedAt?: string;
        }>(features);

        const payload: Record<string, unknown> = {
            readiedAt: prediction.readiedAt,
        };
        const domainEvent = createEvent(EventTypes.READY, tags, payload);

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

async function handleCheckin(
    event: APIGatewayProxyEvent,
    body: Record<string, unknown>
): Promise<APIGatewayProxyResult> {
    const bookingId = String(event.pathParameters?.id ?? body.bookingId ?? '');
    const email = String(event.pathParameters?.id ?? body.email ?? '');
    if (!bookingId) return response(400, { error: "bookingId is required" });

    // Consistency boundary (DCB): one branch per DSL `reads [...] by axis`.
    const criteria: BoundaryBranch[] = [
        { axis: "bookingId", value: bookingId, types: [EventTypes.BOOKED, EventTypes.CHECKED_IN] },
    ];

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        const { events, guards } = await readBoundary(criteria);
        const state = rehydrate(events);

        // Enforce business rules against the boundary state.
        const validationError = validateCommand(state, "Checkin");
        if (validationError) return response(409, { error: validationError });

        const tags: Record<string, string> = {
            bookingId: bookingId,
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
            roomNumber?: number;
            checkedInAt?: string;
        }>(features);

        const payload: Record<string, unknown> = {
            roomNumber: prediction.roomNumber,
            checkedInAt: prediction.checkedInAt,
        };
        const domainEvent = createEvent(EventTypes.CHECKED_IN, tags, payload);

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

async function handleHotelProximityTranslator(
    event: APIGatewayProxyEvent,
    body: Record<string, unknown>
): Promise<APIGatewayProxyResult> {
    const email = String(event.pathParameters?.id ?? body.email ?? '');
    if (!email) return response(400, { error: "email is required" });

    // Consistency boundary (DCB): one branch per DSL `reads [...] by axis`.
    const criteria: BoundaryBranch[] = [
        { axis: "email", value: email, types: [EventTypes.CHECKED_IN, EventTypes.CHECKED_OUT] },
    ];

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        const { events, guards } = await readBoundary(criteria);
        const state = rehydrate(events);

        // Enforce business rules against the boundary state.
        const validationError = validateCommand(state, "HotelProximityTranslator");
        if (validationError) return response(409, { error: validationError });

        const tags: Record<string, string> = {};

        // ── Inference: call the SageMaker endpoint for this slice ──────────
        // Feature vector for the model. The command fields and boundary state
        // are the inputs; adjust the shape to match your endpoint's contract.
        const features: Record<string, unknown> = {
            email: email,
        };
        // Prediction returned by the endpoint (the event's inferred fields).
        const prediction = await invokeSageMaker<{
            departedAt?: string;
        }>(features);

        const payload: Record<string, unknown> = {
            email: email,
            departedAt: prediction.departedAt,
        };
        const domainEvent = createEvent(EventTypes.GUEST_LEFT, tags, payload);

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

async function handlePay(
    event: APIGatewayProxyEvent,
    body: Record<string, unknown>
): Promise<APIGatewayProxyResult> {
    const bookingId = String(event.pathParameters?.id ?? body.bookingId ?? '');
    const paymentId = String(event.pathParameters?.id ?? body.paymentId ?? '');
    const { amount, currency, paymentMethod } = body as {
        amount?: number;
        currency?: string;
        paymentMethod?: string;
    };
    if (!bookingId) return response(400, { error: "bookingId is required" });

    // Consistency boundary (DCB): one branch per DSL `reads [...] by axis`.
    const criteria: BoundaryBranch[] = [
        { axis: "bookingId", value: bookingId, types: [EventTypes.BOOKED, EventTypes.PAYMENT_REQUESTED, EventTypes.PAYMENT_SUCCEEDED] },
    ];

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        const { events, guards } = await readBoundary(criteria);
        const state = rehydrate(events);

        // Enforce business rules against the boundary state.
        const validationError = validateCommand(state, "Pay");
        if (validationError) return response(409, { error: validationError });

        const tags: Record<string, string> = {
            paymentId: paymentId,
            bookingId: bookingId,
        };

        // ── Inference: call the SageMaker endpoint for this slice ──────────
        // Feature vector for the model. The command fields and boundary state
        // are the inputs; adjust the shape to match your endpoint's contract.
        const features: Record<string, unknown> = {
            bookingId: bookingId,
            amount: amount,
            currency: currency,
            paymentMethod: paymentMethod,
        };
        // Prediction returned by the endpoint (the event's inferred fields).
        const prediction = await invokeSageMaker<{
            requestedAt?: string;
        }>(features);

        const payload: Record<string, unknown> = {
            amount: amount,
            currency: currency,
            paymentMethod: paymentMethod,
            requestedAt: prediction.requestedAt,
        };
        const domainEvent = createEvent(EventTypes.PAYMENT_REQUESTED, tags, payload);

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

        // ── Inference: call the SageMaker endpoint for this slice ──────────
        // Feature vector for the model. The command fields and boundary state
        // are the inputs; adjust the shape to match your endpoint's contract.
        const features: Record<string, unknown> = {
            paymentId: paymentId,
            gatewayRef: gatewayRef,
        };
        // Prediction returned by the endpoint (the event's inferred fields).
        const prediction = await invokeSageMaker<{
            amount?: number;
            transactionRef?: string;
            succeededAt?: string;
        }>(features);

        const payload: Record<string, unknown> = {
            amount: prediction.amount,
            transactionRef: prediction.transactionRef,
            succeededAt: prediction.succeededAt,
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

        const tags: Record<string, string> = {
            forecastId: forecastId,
            roomType: roomType,
        };

        // ── Inference: call the SageMaker endpoint for this slice ──────────
        // Feature vector for the model. The command fields and boundary state
        // are the inputs; adjust the shape to match your endpoint's contract.
        const features: Record<string, unknown> = {
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
