# View Demand Forecast

<!-- slice id: view_demand_forecast -->

## Model

<!-- Derived from the parent eventModel and refreshed on every spec-slices run. Do not hand-edit. -->

**Pattern:** View

```mermaid
eventModel
	actor Manager
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
	readModel demandForecast["Demand Forecast"] {
		*roomType: string
		forecastFrom: date
		forecastThrough: date
		predictedOccupancyRate: decimal
		predictedDemand: int
		modelVersion: string
	}
	ui:Manager forecast_ui["Demand Forecast UI"] {
		roomType: string
		forecastFrom: date
		forecastThrough: date
		predictedOccupancyRate: decimal
		predictedDemand: int
	}
	slice view_demand_forecast["View Demand Forecast"]
		occupancyForecasted-->demandForecast
		demandForecast-->forecast_ui
```

## Description

Projects the `Occupancy Forecasted` events into a per-`roomType` read model the
manager can view — the latest forecast for each room type, with the window it
covers and the model version that produced it. This is the human-facing end of
the ML loop: the manager sees predicted occupancy and demand to drive pricing,
staffing, and overbooking decisions.

Because the projection keys on `roomType` and forecasts are ordered in the
event log, the read model naturally reflects the most recent forecast per room
type. Surfacing `modelVersion` in the UI keeps the prediction accountable — an
operator can tell at a glance whether a decision was made against the current
model or a stale one.

## Tests

```mermaid
sliceTests
	test["Latest forecast per room type is projected for the manager"]
		given
			domainEvent occupancyForecasted["Occupancy Forecasted"] {
				forecastId: UUID = "f-1"
				roomType: string = "deluxe"
				forecastFrom: date = 2026-08-20
				forecastThrough: date = 2026-09-03
				predictedOccupancyRate: decimal = 0.92
				predictedDemand: int = 130
				modelVersion: string = "occupancy-v3"
			}
		then
			readModel demandForecast["Demand Forecast"] {
				roomType: string = "deluxe"
				predictedOccupancyRate: decimal = 0.92
				predictedDemand: int = 130
				modelVersion: string = "occupancy-v3"
			}
```
