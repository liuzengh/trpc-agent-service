#!/bin/sh
set -eu
bucket=${MANAGED_S3_BUCKET:-agent-artifacts}
collection=${MANAGED_QDRANT_COLLECTION:-agent-knowledge}
vector=${MANAGED_QDRANT_VECTOR:-embedding}
python3 /tooling/backendctl.py minio-init --endpoint http://minio:9000 --bucket "$bucket" --access-key-file /run/secrets/minio_user --secret-key-file /run/secrets/minio_password --timeout 10 --wait-seconds 60
python3 /tooling/backendctl.py minio-account-init --endpoint http://minio:9000 --bucket "$bucket" --access-key-file /run/secrets/minio_user --secret-key-file /run/secrets/minio_password --app-access-key-file /run/secrets/minio_app_user --app-secret-key-file /run/secrets/minio_app_password --timeout 15 --wait-seconds 60
set -- --endpoint http://qdrant:6333 --api-key-file /run/secrets/qdrant_api_key --collection "$collection" --vector-name "$vector" --distance "${MANAGED_QDRANT_DISTANCE:-Cosine}" --timeout 10 --wait-seconds 60
if [ -n "${MANAGED_QDRANT_DIMENSIONS:-}" ]; then
    set -- "$@" --dimensions "$MANAGED_QDRANT_DIMENSIONS"
fi
python3 /tooling/backendctl.py qdrant-init "$@"
