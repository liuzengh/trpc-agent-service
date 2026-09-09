# Own Backend Lifecycle In The Server

Backend construction, tenant selection, and adapter closure are owned by a server-side registry rather than individual request handlers. Requests acquire leases on the selected Storage Adapter, and a retiring adapter closes only after its leases are released. This keeps tenant routing server-owned while preventing a backend switch or service shutdown from closing a database or Redis connection that an in-flight request still needs.

## Considered Options

- Close the previous backend immediately after selection: rejected because an in-flight request can still hold that adapter.
- Keep every backend open for process lifetime: rejected because it allows leaked adapters and obscures service shutdown.
