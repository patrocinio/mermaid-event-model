"""
Minimal occupancy/demand inference handler for a SageMaker SKLearn
(script-mode) endpoint.

There is no trained model here — the point is to close the event-sourcing ->
SageMaker -> event loop with a real, invokable endpoint that returns the exact
JSON shape the generated command handler expects. The "prediction" is a simple,
deterministic function of the feature vector so the demo is explainable:

  predictedOccupancyRate = clamp((roomsBooked + bookingVelocity*horizon) / roomsAvailable)
  predictedDemand        = round(predictedOccupancyRate * roomsAvailable * horizon)

The command handler posts JSON like:
  { "roomType": "deluxe", "horizonNights": 14,
    "roomsAvailable": 10, "roomsBooked": 7, "bookingVelocity": 3, ... }
and expects back JSON with predictedOccupancyRate / predictedDemand /
forecastFrom / forecastThrough / modelVersion / endpointName / forecastedAt.
"""
import json
import os
from datetime import date, datetime, timedelta, timezone

MODEL_VERSION = "occupancy-demo-v1"


# ── SageMaker script-mode inference contract ────────────────────────────────

def model_fn(model_dir):
    # No real model to load; return a marker so predict_fn has something.
    return {"version": MODEL_VERSION}


def input_fn(request_body, content_type="application/json"):
    if content_type and "json" in content_type:
        return json.loads(request_body) if request_body else {}
    # Fall back to treating the raw body as JSON.
    try:
        return json.loads(request_body)
    except Exception:
        return {}


def _to_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def predict_fn(features, model):
    horizon = _to_float(features.get("horizonNights"), 14) or 14
    available = _to_float(features.get("roomsAvailable"), 0)
    booked = _to_float(features.get("roomsBooked"), 0)
    velocity = _to_float(features.get("bookingVelocity"), 0)

    if available <= 0:
        rate = 0.0
    else:
        projected = booked + velocity * horizon
        rate = max(0.0, min(1.0, projected / available))

    rate = round(rate, 4)
    demand = int(round(rate * available * horizon))

    today = date.today()
    forecast_from = today.isoformat()
    forecast_through = (today + timedelta(days=int(horizon))).isoformat()

    return {
        "predictedOccupancyRate": rate,
        "predictedDemand": demand,
        "forecastFrom": forecast_from,
        "forecastThrough": forecast_through,
        "modelVersion": model.get("version", MODEL_VERSION),
        "endpointName": os.environ.get("SM_ENDPOINT_NAME", ""),
        "forecastedAt": datetime.now(timezone.utc).isoformat(),
    }


def output_fn(prediction, accept="application/json"):
    return json.dumps(prediction), "application/json"
