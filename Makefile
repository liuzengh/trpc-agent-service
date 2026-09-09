SHELL := /bin/bash

.PHONY: verify build test race integration compose-config compose-up compose-down

# The source bundle intentionally omits .git. Keep VCS stamping disabled so
# builds are reproducible from either a clone or an extracted delivery archive.
build:
	go build -buildvcs=false ./...

test:
	go test -buildvcs=false -count=1 ./...

race:
	go test -buildvcs=false -race -count=1 ./...

integration:
	./scripts/validate.sh

verify:
	./scripts/validate.sh

compose-config:
	POSTGRES_PASSWORD=validation-only-password \
	MASTER_KEY=validation-only-master-key-32-bytes-minimum \
	SERVICE_AUTH_SECRET=validation-only-service-secret-32-bytes \
	ADMIN_API_TOKEN=validation-only-admin-token-32-bytes \
	GRAFANA_PASSWORD=validation-only-grafana-password \
	docker compose -f deploy/docker-compose.yml config

compose-up:
	docker compose -f deploy/docker-compose.yml up --build -d

compose-down:
	docker compose -f deploy/docker-compose.yml down
