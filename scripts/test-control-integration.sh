#!/usr/bin/env bash
set -euo pipefail

# A successful unit run with skipped database tests is not a publication gate.
: "${CONTROL_TEST_DATABASE_URL:?CONTROL_TEST_DATABASE_URL is required}"
: "${CONTROL_DB_CONTRACT_ADMIN_URL:?shared V1 test database admin URL required}"
: "${CONTROL_DB_CONTRACT_MIGRATION_URL:?V1 control_migrator URL required}"
: "${CONTROL_DB_CONTRACT_RUNTIME_URL:?V1 control_runtime URL required}"
: "${GATEWAY_V1_TEST_MIGRATION_DATABASE_URL:?other-workload gateway_migrator test URL required}"
: "${GATEWAY_V1_TEST_DATABASE_URL:?other-workload gateway_runtime test URL required}"
cd "$(dirname "$0")/.."
log=$(mktemp "${TMPDIR:-/tmp}/control-integration.XXXXXX")
trap 'rm -f "$log"' EXIT

go test -count=1 -json \
  ./services/control-api/integration \
  ./services/control-api/internal/bootstrap \
  ./services/control-api/internal/deployment/adapter/outbound/postgres \
  ./services/control-api/internal/runtimeprofile/adapter/outbound/postgres | tee "$log"

if grep -q '"Action":"skip"' "$log"; then
  echo 'CONTROL_INTEGRATION_GATE=FAIL skipped test' >&2
  exit 1
fi
for required in \
  TestV1DatabaseContractControlRuntimeIsolation \
  TestV1DatabaseContractControlConcurrentFirstMigration \
  TestV1DatabaseContractControlRejectsTargetsBeforeDDL \
  TestV1DatabaseContractControlRejectsNonOwnerMigratorBeforeDDL \
  TestV1DatabaseContractControlRejectsOtherWorkloadBeforeDDL \
  TestV1DatabaseContractControlRejectsAssumedRolesBeforeDDL \
  TestControlAPIV1AgainstPostgreSQL \
  TestPublicationTransactionAgainstPostgreSQL \
  TestPublicationWriteFailuresRollbackEveryRecordAgainstPostgreSQL \
  TestConcurrentSameKeyPublicationReturnsOneWinnerAgainstPostgreSQL \
  TestConcurrentDifferentKeyPublicationHasOneCASWinnerAgainstPostgreSQL \
  TestPublicationDatabaseImmutabilityAgainstPostgreSQL \
  TestRevisionWithoutManifestIsIntegrityFailureAgainstPostgreSQL \
  TestProfileCredentialConsumerRoundTripAgainstPostgreSQL; do
  if ! grep -q "\"Action\":\"pass\".*\"Test\":\"$required\"" "$log"; then
    echo "CONTROL_INTEGRATION_GATE=FAIL missing passing test: $required" >&2
    exit 1
  fi
done
echo 'CONTROL_INTEGRATION_GATE=PASS zero skipped tests'
