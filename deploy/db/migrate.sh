#!/usr/bin/env bash
# Apply the incremental schema migrations in deploy/db/migrations.
#
# The genesis baseline (deploy/db/init.sql) is NOT run from here — it belongs to
# empty-database provisioning (compose initdb.d, the k8s db-init Job, CI). This
# script only moves an already-provisioned database forward, and stamps nothing
# by hand: init.sql leaves schema_migrations at version 1, so the first `up`
# here applies 000002.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
MIGRATIONS="$ROOT/deploy/db/migrations"

# Same default as config.go's TRPC_PG_DSN, so `./migrate.sh up` works against a
# fresh `docker compose up -d` without repeating the DSN.
DSN="${TRPC_PG_DSN:-postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable}"

usage() {
	cat <<EOF
usage: $(basename "$0") <command>

  up [N]     apply all pending migrations, or just the next N
  version    print the schema version the database is at
  new NAME   create the next NNNNNN_name.up.sql / .down.sql pair
  force V    record version V without running any SQL
             (clear a dirty state, or adopt a database provisioned by the
              genesis script before it stamped schema_migrations: force 1)
  down 1     roll back one migration; refused unless CONFIRM_DOWN=yes

DSN: \$TRPC_PG_DSN
     default ${DSN}
EOF
}

# Checked here rather than at the top of the script: `new` and a usage error
# only touch files and stdout, so they must still work on a machine that has
# never deployed anything. Only the commands that talk to the database need it.
run() {
	if ! command -v migrate >/dev/null 2>&1; then
		cat >&2 <<'EOF'
golang-migrate CLI not found. Install it once:

  go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest

It is deliberately absent from go.mod: the service never imports it, and adding
it would drag the whole migrate module tree into the binary's module graph for
a tool that only runs at deploy time.
EOF
		exit 1
	fi
	migrate -path "$MIGRATIONS" -database "$DSN" "$@"
}

case "${1:-}" in
up)
	shift
	if [ $# -gt 0 ]; then run up "$1"; else run up; fi
	;;
version)
	run version
	;;
force)
	[ -n "${2:-}" ] || { usage >&2; exit 2; }
	run force "$2"
	;;
down)
	# Down migrations are destructive by definition, and production rolls
	# forward: schema changes stay additive (add a column, dual write, then
	# drop the old one). One step at a time behind an explicit
	# opt-in, so a fat-fingered `migrate.sh down` cannot drop a column.
	{ [ -z "${2:-}" ] || [ "$2" = "1" ]; } || { usage >&2; exit 2; }
	if [ "${CONFIRM_DOWN:-}" != "yes" ]; then
		echo "refusing: down migrations are destructive. Re-run with CONFIRM_DOWN=yes." >&2
		exit 1
	fi
	run down 1
	;;
new)
	[ -n "${2:-}" ] || { usage >&2; exit 2; }
	name=$(printf '%s' "$2" | tr '[:upper:] ' '[:lower:]_' | tr -cd 'a-z0-9_')
	[ -n "$name" ] || { echo "new: NAME has no usable characters: $2" >&2; exit 2; }
	# Baseline lives in init.sql as version 1, so an empty migrations/ starts
	# the sequence at 000002 — never at 000001, which would collide with it.
	# `|| true`: an empty migrations/ dir makes grep exit 1, and under
	# `set -euo pipefail` that would abort the script silently right here —
	# which is exactly the first-run case this line exists to handle.
	last=$(ls "$MIGRATIONS" 2>/dev/null | grep -E '^[0-9]{6}_.*\.up\.sql$' | sort | tail -1 | cut -d_ -f1 || true)
	next=$(printf '%06d' $((10#${last:-1} + 1)))
	up="$MIGRATIONS/${next}_${name}.up.sql"
	down="$MIGRATIONS/${next}_${name}.down.sql"
	if [ -e "$up" ] || [ -e "$down" ]; then
		echo "new: $next already exists" >&2
		exit 1
	fi
	printf -- '-- %s: <what this changes and why>\n' "${next}_${name}" >"$up"
	printf -- '-- Rollback for %s. Production rolls forward; this exists so a\n-- failed rollout in a lower environment can be reverted.\n' "${next}_${name}" >"$down"
	echo "created:"
	echo "  ${up#"$ROOT"/}"
	echo "  ${down#"$ROOT"/}"
	echo "both files are required: migrate refuses to run a version with only one side."
	;;
*)
	usage >&2
	exit 2
	;;
esac
