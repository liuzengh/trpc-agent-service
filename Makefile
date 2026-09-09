# trpc-service developer entrypoints.
#
# The *.sh scripts are the source of truth (README 代码目录 lists them as the
# contract); these targets are thin wrappers so both entry points stay usable.
# Dependencies (PG/Redis/MinIO) come from docker-compose.yml.

BIN := bin/trpc-service

.PHONY: build start stop clean test cover fmt lint deps migrate

build:
	./build.sh

start: build
	./start.sh

stop:
	./stop.sh

clean:
	./clean.sh

test:
	go test ./...

cover:
	./coverage.sh

fmt:
	./format.sh

lint:
	./lint.sh

# Start the dev dependency stack (postgres/redis/minio/jaeger/prometheus), not
# the service itself — the binary runs locally via `make start` for easier
# debugging.
deps:
	docker compose up -d

# Apply pending incremental schema migrations. The genesis baseline
# (deploy/db/init.sql) is not run from here — compose's initdb.d, the k8s
# db-init Job and CI each apply it to an empty database. See
# deploy/db/migrations/README.md.
migrate:
	./deploy/db/migrate.sh up
