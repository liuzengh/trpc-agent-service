# Runtime evidence

`scripts/kind-e2e.sh` replaces the text files in this directory with evidence
from the latest local or CI run:

- `pods.txt`: Pod readiness, node placement, and IPs.
- `jobs.txt`: migration and MinIO bucket initialization completion.
- `readyz.txt`: the application readiness response through port-forwarding.
- `smoke.txt`: the reusable Kubernetes smoke-gate output.
