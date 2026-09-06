#!/usr/bin/env bash
set -euo pipefail

# Keep the converter's working directory and log location identical to the
# original manual command. No Agent configuration or credentials are changed.
WORKBUDDY_DIR="${WORKBUDDY2API_DIR:-$HOME/workbuddy2api}"

if [[ ! -d "$WORKBUDDY_DIR" ]]; then
  echo "workbuddy2api directory not found: $WORKBUDDY_DIR" >&2
  echo "Set WORKBUDDY2API_DIR to the directory containing converter.py." >&2
  exit 1
fi

if [[ ! -f "$WORKBUDDY_DIR/converter.py" ]]; then
  echo "converter.py not found in: $WORKBUDDY_DIR" >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv not found in PATH; make sure uv is installed and available." >&2
  exit 1
fi

cd -- "$WORKBUDDY_DIR"
echo "Starting workbuddy2api in the foreground; press Ctrl+C to stop."
echo "Log: $PWD/converter.log"
exec uv run converter.py --desensitize --log converter.log --api-key 0
