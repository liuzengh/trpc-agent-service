# Keep Routing Server-Owned And Event Contracts Stable

Status: accepted

Tenant access, Active Deployment selection and Session routing are server-owned decisions; browser state, request bodies and arbitrary headers cannot grant tenant access. Workers remain stateless and depend on shared Session/Memory storage, while the Stage 3 SSE envelope (`event_id`, `request_id`, `session_id`, monotonic `sequence`, `type`, `data`) is a stable platform contract translated from runtime events. This keeps authorization and correctness independent of clients or node placement and allows `trpc-agent-go` internals to evolve behind adapters.

## Considered Options

- Sticky sessions as the correctness mechanism: rejected because node loss and rescheduling would break conversations.
- Exposing upstream runtime event types to the browser: rejected because it couples the management console to framework internals.
