#!/usr/bin/env bash
# Provider-specific actions run through reviewed executable wrappers, never shell text.
set -euo pipefail

: "${BACKUP_COMMAND:?BACKUP_COMMAND is required}"
: "${RESTORE_COMMAND:?RESTORE_COMMAND is required}"
: "${VERIFY_COMMAND:?VERIFY_COMMAND is required}"

require_absolute_executable() {
  local label="$1"
  local command_path="$2"
  case "$command_path" in
    /*) ;;
    *)
      echo "$label must be an absolute executable path, not shell text" >&2
      exit 2
      ;;
  esac
  if [[ ! -x "$command_path" ]]; then
    echo "$label is not executable: $command_path" >&2
    exit 2
  fi
}

require_absolute_executable BACKUP_COMMAND "$BACKUP_COMMAND"
require_absolute_executable RESTORE_COMMAND "$RESTORE_COMMAND"
require_absolute_executable VERIFY_COMMAND "$VERIFY_COMMAND"

backup_ref="$(mktemp)"
trap 'rm -f "$backup_ref"' EXIT
"$BACKUP_COMMAND" >"$backup_ref"
test -s "$backup_ref"
export BACKUP_ARTIFACT="$(cat "$backup_ref")"
"$RESTORE_COMMAND"
"$VERIFY_COMMAND"
echo "backup/restore smoke passed"
