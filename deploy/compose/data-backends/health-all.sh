#!/bin/sh
set -eu
python3 /tooling/backendctl.py health --service redis --host redis --password-file /run/secrets/redis_admin --timeout 5 --wait-seconds 15
python3 /tooling/backendctl.py health --service minio --endpoint http://minio:9000 --timeout 5 --wait-seconds 15
python3 /tooling/backendctl.py health --service qdrant --endpoint http://qdrant:6333 --api-key-file /run/secrets/qdrant_api_key --timeout 5 --wait-seconds 15
