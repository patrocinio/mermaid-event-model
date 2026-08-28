# Forecast Occupancy

<!-- slice id: forecast_occupancy -->

## Model

<!-- Derived from the parent eventModel and refreshed on every spec-slices run. Do not hand-edit. -->

**Pattern:** Automation

```mermaid
eventModel
	actor System
	readModel occupancyDemand["Occupancy Demand"] {
		*roomType: string
		*night: date
		roomsAvailable: int
		roomsBooked: int
		roomsOccupied: int
		bookingVelocity: int
	}
	externalEvent forecastTick["Forecast Tick"] {
		occurredAt: timestamp
	}
	automation:System demandForecaster["Demand Forecaster"]
	command forecastOccupancy["Forecast Occupancy"] {
		roomType: string
		horizonNights: int
	}
		reads [occupancyForecasted] by roomType
	domainEvent occupancyForecasted["Occupancy Forecasted"] {
		*forecastId: UUID
		*roomType: string
		forecastFrom: date
		forecastThrough: date
		predictedOccupancyRate: decimal
		predictedDemand: int
		modelVersion: string
		endpointName: string
		forecastedAt: timestamp
	}
	slice forecast_occupancy["Forecast Occupancy"]
		occupancyDemand-->demandForecaster
		forecastTick-->demandForecaster
		demandForecaster-->forecastOccupancy
		forecastOccupancy-->occupancyForecasted
```

## Description

The ML integration slice. On each `Forecast Tick` (a scheduled timer — e.g.
EventBridge firing daily), the `Demand Forecaster` automation reads the current
`Occupancy Demand` signal for a room type, assembles the recent demand history
into a feature vector, and calls an **Amazon SageMaker** real-time endpoint
(`InvokeEndpoint`) to predict occupancy over the next `horizonNights`. The
response is recorded by the `Forecast Occupancy` command as an
`Occupancy Forecasted` domain event.

Recording the prediction **as an event** is the whole point of the design:

- **Auditable** — every forecast captures the `modelVersion` and `endpointName`
  that produced it, so "why did we predict 92% occupancy on this date?" is
  answerable by replaying the event, not by guessing at a model that has since
  been retrained.
- **Replayable** — because `occupancyDemand` is a deterministic projection of
  the event log, the exact feature vector for any past tick can be
  reconstructed and re-scored.
- **Composable** — downstream slices (dynamic pricing, staffing, overbooking
  policy) subscribe to `occupancyForecasted` like any other domain event; they
  neither know nor care that SageMaker produced it.

The SageMaker call happens **off the write path** — it is triggered by the
`forecastTick` timer and the async demand projection, not inside a
guest-facing command — so endpoint latency never blocks a booking. The command
reads prior `occupancyForecasted` events `by roomType` so a tick that fires
twice for the same room type and window does not emit a duplicate forecast
(idempotency on the consistency boundary).

**Failure handling:** if the endpoint call fails or times out, no event is
emitted and the tick is retried on the next schedule; the system simply carries
the last known forecast until a fresh one succeeds. A persistent endpoint
outage degrades gracefully to stale forecasts rather than blocking operations.

## Tests

```mermaid
sliceTests
	test["Produces a forecast event from the demand signal on a tick"]
		given
			readModel occupancyDemand["Occupancy Demand"] {
				roomType: string = "deluxe"
				night: date = 2026-08-20
				roomsAvailable: int = 10
				roomsBooked: int = 7
				bookingVelocity: int = 3
			}
		when
			command forecastOccupancy["Forecast Occupancy"] {
				roomType: string = "deluxe"
				horizonNights: int = 14
			}
		then
			domainEvent occupancyForecasted["Occupancy Forecasted"] {
				roomType: string = "deluxe"
				predictedOccupancyRate: decimal = 0.92
				predictedDemand: int = 130
				modelVersion: string = "occupancy-v3"
				endpointName: string = "hotel-occupancy-forecast"
			}
	test["Does not emit a duplicate forecast for the same room type and window"]
		given
			domainEvent occupancyForecasted["Occupancy Forecasted"] {
				forecastId: UUID = "f-1"
				roomType: string = "deluxe"
				forecastFrom: date = 2026-08-20
				forecastThrough: date = 2026-09-03
			}
		when
			command forecastOccupancy["Forecast Occupancy"] {
				roomType: string = "deluxe"
				horizonNights: int = 14
			}
		then
			error["Occupancy already forecasted for roomType and window"]
```

## Decided Exclusions

The forecast's prediction fields are produced by the external Amazon SageMaker
endpoint at inference time, not derived from any prior event in the log. They
are deliberately carried by no upstream event; the `Demand Forecaster`
automation populates them from the `InvokeEndpoint` response before emitting
`Occupancy Forecasted`. Recorded here so the completeness checker treats them as
decided, not missing.

- `ForecastOccupancy.horizonNights` — input parameter of the forecast request; supplied by the tick/schedule configuration, not sourced from an event.
- `OccupancyForecasted.forecastFrom` — start of the predicted window; computed by the automation from the tick date.
- `OccupancyForecasted.forecastThrough` — end of the predicted window; computed as forecastFrom + horizonNights.
- `OccupancyForecasted.predictedOccupancyRate` — returned by the SageMaker endpoint response.
- `OccupancyForecasted.predictedDemand` — returned by the SageMaker endpoint response.
- `OccupancyForecasted.modelVersion` — SageMaker model/variant that produced the prediction, captured for audit.
- `OccupancyForecasted.endpointName` — SageMaker endpoint invoked, captured for audit.
- `OccupancyForecasted.forecastedAt` — wall-clock time the forecast was produced by the automation.

