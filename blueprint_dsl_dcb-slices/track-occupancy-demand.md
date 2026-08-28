# Track Occupancy Demand

<!-- slice id: track_occupancy_demand -->

## Model

<!-- Derived from the parent eventModel and refreshed on every spec-slices run. Do not hand-edit. -->

**Pattern:** View

```mermaid
eventModel
	domainEvent roomAdded["Room Added"] {
		*roomNumber: int
		floor: int
		roomType: string
		capacity: int
	}
	domainEvent booked["Room Booked"] {
		*bookingId: UUID
		*roomNumber: int
		email: string
		checkIn: date
		checkOut: date
		bookedAt: timestamp
	}
	domainEvent checkedIn["Checked In"] {
		*bookingId: UUID
		*email: string
		roomNumber: int
		checkedInAt: timestamp
	}
	domainEvent checkedOut["Checked Out"] {
		*bookingId: UUID
		*roomNumber: int
		*email: string
		checkedOutAt: timestamp
	}
	readModel occupancyDemand["Occupancy Demand"] {
		*roomType: string
		*night: date
		roomsAvailable: int
		roomsBooked: int
		roomsOccupied: int
		bookingVelocity: int
	}
	slice track_occupancy_demand["Track Occupancy Demand"]
		roomAdded-->occupancyDemand
		booked-->occupancyDemand
		checkedIn-->occupancyDemand
		checkedOut-->occupancyDemand
```

## Description

Folds the room and stay lifecycle into a per-`roomType`, per-`night` demand
signal — the feature vector the demand forecast is built from. `roomAdded`
establishes how much inventory exists; `booked` increments booked room-nights
across the requested stay range; `checkedIn` and `checkedOut` move a room-night
between the booked and occupied counts as guests actually arrive and leave.
`bookingVelocity` captures how fast a room-night is filling (bookings observed
per unit time), which is the strongest short-horizon demand indicator.

This read model is the boundary between the transactional event log and the ML
integration: it is a plain projection (no ML here), so it stays fast, testable,
and independent of any model version. The forecaster reads it as its input, so
keeping the projection deterministic is what makes forecasts reproducible from
the event history.

## Tests

```mermaid
sliceTests
	test["Booking a room increments booked room-nights for each night in the stay"]
		given
			domainEvent roomAdded["Room Added"] {
				roomNumber: int = 101
				roomType: string = "deluxe"
				capacity: int = 2
			}
		when
			domainEvent booked["Room Booked"] {
				bookingId: UUID = "b-1"
				roomNumber: int = 101
				checkIn: date = 2026-08-20
				checkOut: date = 2026-08-22
			}
		then
			readModel occupancyDemand["Occupancy Demand"] {
				roomType: string = "deluxe"
				night: date = 2026-08-20
				roomsAvailable: int = 1
				roomsBooked: int = 1
			}
	test["Check-in moves a room-night from booked to occupied"]
		given
			domainEvent booked["Room Booked"] {
				bookingId: UUID = "b-1"
				roomNumber: int = 101
				checkIn: date = 2026-08-20
				checkOut: date = 2026-08-22
			}
		when
			domainEvent checkedIn["Checked In"] {
				bookingId: UUID = "b-1"
				roomNumber: int = 101
			}
		then
			readModel occupancyDemand["Occupancy Demand"] {
				roomType: string = "deluxe"
				night: date = 2026-08-20
				roomsOccupied: int = 1
			}
```
