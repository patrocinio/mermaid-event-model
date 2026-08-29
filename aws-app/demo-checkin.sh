#!/usr/bin/env bash
#
# Demo: the AWS-native check-in slice, end to end, against the LIVE stack.
#
#   POST bookRoom  -> Booked event (carries email on the DCB boundary)
#   POST checkin   -> CheckedIn event; its *email tag is sourced from the
#                     rehydrated boundary state (the Booked event), not the
#                     command — the fix that made this slice work.
#
# Shows: command → DCB boundary read → append to DynamoDB → the CheckedIn
# event indexed under both gsi_bookingId and gsi_email.
#
# Read/write against real AWS resources; creates nothing new.
#
# Usage:
#   ./demo-checkin.sh
#   AWS_REGION=us-east-1 ./demo-checkin.sh
#   API=https://.../prod ./demo-checkin.sh

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
STACK="${STACK:-HotelRegionalPrimary}"
TABLE="${TABLE:-HotelEvents}"

BOLD=$'\033[1m'; DIM=$'\033[2m'; GRN=$'\033[32m'; CYN=$'\033[36m'; YEL=$'\033[33m'; RST=$'\033[0m'
have_jq() { command -v jq >/dev/null 2>&1; }
pp() { if have_jq; then jq .; else cat; fi; }
step() { echo; echo "${BOLD}${CYN}=== $* ===${RST}"; }

# ── Resolve the API endpoint from the stack output ────────────────────────────
if [ -z "${API:-}" ]; then
  API=$(aws cloudformation describe-stacks --stack-name "$STACK" --region "$REGION" \
    --query "Stacks[0].Outputs[?contains(OutputKey,'ApiEndpoint')].OutputValue | [0]" --output text)
fi
API="${API%/}"
BID="checkin-demo-$(date +%s)"
EMAIL="ada@example.com"

echo "${BOLD}AWS-native check-in slice demo${RST}"
echo "API:     ${GRN}$API${RST}"
echo "Booking: $BID   Guest: $EMAIL"

# ── 1. Seed the booking (satisfies checkin's boundary: reads [booked,...] by bookingId) ──
step "1. POST bookRoom"
BODY=$(printf '{"command":"bookRoom","bookingId":"%s","roomNumber":101,"email":"%s","checkIn":"2026-09-01","checkOut":"2026-09-03"}' "$BID" "$EMAIL")
echo "${DIM}$BODY${RST}"
curl -s -X POST "$API/api/records" -H 'content-type: application/json' -d "$BODY" | pp

# ── 2. Check in — email is folded from the boundary state, not the command ──
step "2. POST checkin (command carries only bookingId)"
BODY=$(printf '{"command":"checkin","bookingId":"%s"}' "$BID")
echo "${DIM}$BODY${RST}"
curl -s -X POST "$API/api/records" -H 'content-type: application/json' -d "$BODY" | pp

# ── 3. The CheckedIn event in the store — tag_email sourced from state ──
step "3. The events for this booking (gsi_bookingId)"
aws dynamodb query --table-name "$TABLE" --region "$REGION" --index-name gsi_bookingId \
  --key-condition-expression "tag_bookingId = :b" \
  --expression-attribute-values "{\":b\":{\"S\":\"$BID\"}}" \
  --query "Items[].{type:eventType.S,tag_bookingId:tag_bookingId.S,tag_email:tag_email.S}" \
  --output table

step "4. CheckedIn is also indexed under gsi_email (tag non-empty)"
aws dynamodb query --table-name "$TABLE" --region "$REGION" --index-name gsi_email \
  --key-condition-expression "tag_email = :e" \
  --expression-attribute-values "{\":e\":{\"S\":\"$EMAIL\"}}" \
  --query "Items[?tag_bookingId.S=='$BID'].{type:eventType.S,tag_email:tag_email.S}" \
  --output table

echo
echo "${BOLD}${GRN}Done.${RST} bookRoom -> Booked, checkin -> CheckedIn; the *email tag on"
echo "CheckedIn was folded from the Booked event's boundary state, not the command."
echo "${DIM}Reminder: the stack (NAT + Redis) bills 24/7 — tear down with teardown.sh when finished.${RST}"
