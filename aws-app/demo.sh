#!/usr/bin/env bash
#
# Demo: the hotel occupancy-forecast loop, end to end, against the LIVE stack.
#
#   Client -> API Gateway -> Command Lambda
#     -> DCB boundary read -> Amazon SageMaker InvokeEndpoint (inference)
#     -> OccupancyForecasted event appended to the DynamoDB event store
#     -> DynamoDB Streams -> Projector Lambda -> Redis (Demand Forecast read model)
#     -> Query Lambda -> GET the forecast back
#
# It is READ/WRITE against real AWS resources but creates nothing new: it only
# invokes the deployed API, the SageMaker endpoint, and reads CloudWatch/DynamoDB.
#
# Usage:
#   ./demo.sh                # run the full demo
#   AWS_REGION=us-east-1 ./demo.sh
#   API=https://.../prod ./demo.sh   # override the API base URL
#
# Requirements: awscli v2, curl, and (optional) jq for pretty output.

set -euo pipefail

# ── Configuration ───────────────────────────────────────────────────────────
REGION="${AWS_REGION:-us-east-1}"
STACK="${STACK:-HotelRegionalPrimary}"
ENDPOINT_NAME="${ENDPOINT_NAME:-hotel-occupancy-forecast}"
TABLE="${TABLE:-HotelEvents}"
PROJECTOR_WAIT="${PROJECTOR_WAIT:-10}"   # seconds to let the stream -> projector settle

# ── Pretty helpers ────────────────────────────────────────────────────────────
BOLD=$'\033[1m'; DIM=$'\033[2m'; GRN=$'\033[32m'; CYN=$'\033[36m'; YEL=$'\033[33m'; RST=$'\033[0m'
have_jq() { command -v jq >/dev/null 2>&1; }
pp() { if have_jq; then jq .; else cat; fi; }
step() { echo; echo "${BOLD}${CYN}=== $* ===${RST}"; }
note() { echo "${DIM}$*${RST}"; }
pause() { if [ "${NONINTERACTIVE:-0}" = "1" ]; then sleep 1; else read -r -p "${DIM}(enter to continue)${RST} " _; fi; }

# ── Resolve the API endpoint from the stack output (unless provided) ──────────
step "Resolving deployed API endpoint"
if [ -z "${API:-}" ]; then
  API=$(aws cloudformation describe-stacks --stack-name "$STACK" --region "$REGION" \
    --query "Stacks[0].Outputs[?contains(OutputKey,'ApiEndpoint')].OutputValue | [0]" \
    --output text)
fi
API="${API%/}"   # strip trailing slash
echo "API:      ${GRN}$API${RST}"
echo "Region:   $REGION"
echo "Endpoint: $ENDPOINT_NAME"

# ── 0. Show the model behind the deployment ───────────────────────────────────
step "0. What we're demoing"
note "The 'forecast_occupancy' Automation slice: a Forecast Occupancy command"
note "reads the demand signal, calls a SageMaker endpoint, and records the"
note "prediction as an OccupancyForecasted domain event — a first-class,"
note "auditable, replayable fact. Then the read side projects it into the"
note "Demand Forecast read model, served over the query API."
pause

# ── 1. Confirm the SageMaker endpoint is live ─────────────────────────────────
step "1. SageMaker endpoint status"
aws sagemaker describe-endpoint --endpoint-name "$ENDPOINT_NAME" --region "$REGION" \
  --query "{name:EndpointName,status:EndpointStatus}" --output table
pause

# ── 2. Fire forecast commands (write side -> inference -> event) ──────────────
# roomType, roomsAvailable, roomsBooked, bookingVelocity, horizonNights.
# The demo model computes: rate = clamp((booked + velocity*horizon)/available).
declare -a SCENARIOS=(
  "standard 20 3 1 7"     # low demand   -> ~0.50
  "deluxe 10 6 1 14"      # high demand   -> ~1.00 (capped)
  "suite 8 2 0 30"        # steady demand -> ~0.25
)

step "2. POST Forecast Occupancy commands (SageMaker inference in the loop)"
for s in "${SCENARIOS[@]}"; do
  read -r roomType avail booked vel horizon <<<"$s"
  body=$(printf '{"command":"forecastOccupancy","roomType":"%s","roomsAvailable":%s,"roomsBooked":%s,"bookingVelocity":%s,"horizonNights":%s}' \
    "$roomType" "$avail" "$booked" "$vel" "$horizon")
  echo "${YEL}-> $roomType${RST}  (available=$avail booked=$booked velocity=$vel horizon=${horizon}d)"
  echo "   request : $body"
  resp=$(curl -s -X POST "$API/api/records" -H 'content-type: application/json' -d "$body")
  echo "   response: $resp"
done
pause

# ── 3. Let the DynamoDB stream -> projector -> Redis settle ───────────────────
step "3. Waiting ${PROJECTOR_WAIT}s for the projection to catch up"
note "DynamoDB Streams -> Projector Lambda -> Redis (Demand Forecast read model)"
sleep "$PROJECTOR_WAIT"

# ── 4. Read the forecasts back through the query API (read side) ──────────────
step "4. Read the Demand Forecast read model via the query API"
for s in "${SCENARIOS[@]}"; do
  read -r roomType _ _ _ _ <<<"$s"
  echo "${YEL}GET /api/records?view=demandForecast&id=$roomType${RST}"
  curl -s "$API/api/records?view=demandForecast&id=$roomType" | pp
  echo
done
pause

# ── 5. Show the prediction is a first-class event in the store (audit) ────────
step "5. The OccupancyForecasted events in the DynamoDB event store"
note "Each forecast is an immutable event carrying modelVersion + endpointName —"
note "so 'why did we predict this?' is answerable by replay, not guesswork."
aws dynamodb query --table-name "$TABLE" --region "$REGION" --index-name gsi_roomType \
  --key-condition-expression "tag_roomType = :rt" \
  --expression-attribute-values '{":rt":{"S":"deluxe"}}' \
  --query "Items[?eventType.S=='OccupancyForecasted'].payload.M.{rate:predictedOccupancyRate.N,demand:predictedDemand.N,model:modelVersion.S,endpoint:endpointName.S,at:forecastedAt.S}" \
  --output table
pause

# ── 6. Prove SageMaker was actually invoked (not mocked) ──────────────────────
step "6. SageMaker invocation count (CloudWatch, last 15 min)"
START=$(date -u -v-15M +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d '15 minutes ago' +%Y-%m-%dT%H:%M:%S)
END=$(date -u +%Y-%m-%dT%H:%M:%S)
INVOCATIONS=$(aws cloudwatch get-metric-statistics --namespace AWS/SageMaker \
  --metric-name Invocations \
  --dimensions Name=EndpointName,Value="$ENDPOINT_NAME" Name=VariantName,Value=AllTraffic \
  --start-time "$START" --end-time "$END" --period 900 --statistics Sum \
  --region "$REGION" --query "Datapoints[].Sum | [0]" --output text 2>/dev/null || echo "n/a")
echo "SageMaker InvokeEndpoint calls: ${GRN}${INVOCATIONS}${RST}"

echo
echo "${BOLD}${GRN}Demo complete.${RST} The forecast loop ran end to end:"
echo "  command -> SageMaker inference -> event -> projection -> query."
echo
note "Reminder: the stack (NAT gateway + ElastiCache Redis) bills 24/7."
note "Tear down when done:  npx cdk destroy $STACK HotelEventStore"
note "                      (also delete the SageMaker endpoint/model/config)"
