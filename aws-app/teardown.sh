#!/usr/bin/env bash
#
# Teardown: remove EVERYTHING the occupancy-forecast demo provisioned in AWS.
#
# This is DESTRUCTIVE and IRREVERSIBLE. It deletes the event store (all events),
# the read models, the API, the networking, and the SageMaker endpoint/model.
# It asks for confirmation before doing anything (skip with FORCE=1).
#
# Order matters:
#   1. SageMaker endpoint -> endpoint-config -> model   (CLI-created, not CDK)
#   2. CDK stacks: HotelRegionalPrimary, then HotelEventStore (dependent first)
#   3. IAM role HotelOccupancySageMakerRole (detach/delete policies first)
#   4. S3 model artifact (best-effort)
#
# Each step is best-effort and idempotent: already-deleted resources are
# skipped, so re-running after a partial failure is safe.
#
# Usage:
#   ./teardown.sh            # prompts for confirmation
#   FORCE=1 ./teardown.sh    # no prompt (for CI / scripted use)
#   DRY_RUN=1 ./teardown.sh  # print what would be deleted, delete nothing

set -uo pipefail   # NOTE: no -e; teardown is best-effort, we don't abort on a single failure

# ── Configuration (override via env) ─────────────────────────────────────────
REGION="${AWS_REGION:-us-east-1}"
REGIONAL_STACK="${REGIONAL_STACK:-HotelRegionalPrimary}"
EVENTSTORE_STACK="${EVENTSTORE_STACK:-HotelEventStore}"
ENDPOINT_NAME="${ENDPOINT_NAME:-hotel-occupancy-forecast}"
ENDPOINT_CONFIG="${ENDPOINT_CONFIG:-hotel-occupancy-forecast-cfg}"
MODEL_NAME="${MODEL_NAME:-hotel-occupancy-forecast}"
SM_ROLE="${SM_ROLE:-HotelOccupancySageMakerRole}"
ASSET_BUCKET="${ASSET_BUCKET:-cdk-hnb659fds-assets-220133863472-us-east-1}"
MODEL_KEY="${MODEL_KEY:-sagemaker/occupancy/model.tar.gz}"

DRY_RUN="${DRY_RUN:-0}"

BOLD=$'\033[1m'; DIM=$'\033[2m'; RED=$'\033[31m'; GRN=$'\033[32m'; YEL=$'\033[33m'; RST=$'\033[0m'
step() { echo; echo "${BOLD}${YEL}=== $* ===${RST}"; }
ok()   { echo "${GRN}  ✓ $*${RST}"; }
skip() { echo "${DIM}  - $* (not present / already gone)${RST}"; }

# run <description> <command...> : execute unless DRY_RUN, report status
run() {
  local desc="$1"; shift
  if [ "$DRY_RUN" = "1" ]; then
    echo "${DIM}  [dry-run] would: $desc${RST}"
    return 0
  fi
  if "$@" >/dev/null 2>&1; then ok "$desc"; else skip "$desc"; fi
}

# ── Preflight ─────────────────────────────────────────────────────────────────
ACCOUNT=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "UNKNOWN")
echo "${BOLD}${RED}This will PERMANENTLY DELETE the occupancy-forecast demo from AWS.${RST}"
echo "  Account : $ACCOUNT"
echo "  Region  : $REGION"
echo "  Stacks  : $REGIONAL_STACK, $EVENTSTORE_STACK"
echo "  SageMaker endpoint/config/model : $ENDPOINT_NAME"
echo "  IAM role: $SM_ROLE"
echo "  S3 model: s3://$ASSET_BUCKET/$MODEL_KEY"
[ "$DRY_RUN" = "1" ] && echo "${YEL}  (DRY RUN — nothing will actually be deleted)${RST}"

if [ "${FORCE:-0}" != "1" ] && [ "$DRY_RUN" != "1" ]; then
  echo
  read -r -p "Type ${BOLD}destroy${RST} to proceed: " CONFIRM
  if [ "$CONFIRM" != "destroy" ]; then echo "Aborted."; exit 1; fi
fi

# ── 1. SageMaker (endpoint -> config -> model) ────────────────────────────────
step "1. Deleting SageMaker resources"
run "delete endpoint $ENDPOINT_NAME" \
  aws sagemaker delete-endpoint --endpoint-name "$ENDPOINT_NAME" --region "$REGION"
run "delete endpoint-config $ENDPOINT_CONFIG" \
  aws sagemaker delete-endpoint-config --endpoint-config-name "$ENDPOINT_CONFIG" --region "$REGION"
run "delete model $MODEL_NAME" \
  aws sagemaker delete-model --model-name "$MODEL_NAME" --region "$REGION"

# ── 2. CDK stacks (dependent stack first) ─────────────────────────────────────
step "2. Destroying CDK stacks (this is the slow part: NAT + Redis)"
if [ "$DRY_RUN" = "1" ]; then
  echo "${DIM}  [dry-run] would: npx cdk destroy $REGIONAL_STACK $EVENTSTORE_STACK --force${RST}"
else
  # cdk destroy resolves cross-stack dependency order itself, but we name both.
  npx cdk destroy "$REGIONAL_STACK" "$EVENTSTORE_STACK" --force
  # cdk destroy exit status reflects success; surface it.
  if [ $? -eq 0 ]; then ok "cdk stacks destroyed"; else echo "${RED}  ! cdk destroy reported an error — check the CloudFormation console${RST}"; fi
fi

# ── 3. IAM role (detach managed + delete inline, then the role) ───────────────
step "3. Deleting IAM role $SM_ROLE"
if [ "$DRY_RUN" = "1" ]; then
  echo "${DIM}  [dry-run] would: detach policies + delete role $SM_ROLE${RST}"
else
  # Detach managed policies.
  for arn in $(aws iam list-attached-role-policies --role-name "$SM_ROLE" \
        --query "AttachedPolicies[].PolicyArn" --output text 2>/dev/null); do
    run "detach $arn" aws iam detach-role-policy --role-name "$SM_ROLE" --policy-arn "$arn"
  done
  # Delete inline policies.
  for name in $(aws iam list-role-policies --role-name "$SM_ROLE" \
        --query "PolicyNames[]" --output text 2>/dev/null); do
    run "delete inline policy $name" aws iam delete-role-policy --role-name "$SM_ROLE" --policy-name "$name"
  done
  run "delete role $SM_ROLE" aws iam delete-role --role-name "$SM_ROLE"
fi

# ── 4. S3 model artifact (best-effort) ────────────────────────────────────────
step "4. Removing S3 model artifact"
run "delete s3://$ASSET_BUCKET/$MODEL_KEY" \
  aws s3 rm "s3://$ASSET_BUCKET/$MODEL_KEY"

# ── Done ──────────────────────────────────────────────────────────────────────
step "Teardown complete"
if [ "$DRY_RUN" = "1" ]; then
  echo "Dry run only — nothing was deleted."
else
  echo "All demo resources removed (or already gone). Verify in the console:"
  echo "  - CloudFormation: $REGIONAL_STACK / $EVENTSTORE_STACK should be gone"
  echo "  - SageMaker: no endpoint '$ENDPOINT_NAME'"
  echo "  - The CDK bootstrap stack (CDKToolkit) and assets bucket are LEFT IN PLACE"
  echo "    on purpose — they're shared infra, not part of this demo."
fi
