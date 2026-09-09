# Framework Runtime Integration Boundary

Status: accepted

Stage 3.5 integrates the published `trpc.group/trpc-go/trpc-agent-go v1.11.2` module through an explicit `AgentFactory` and streaming `RunnerAdapter`. The platform owns tenant routing, deployment lifecycle, persistence, governance, and public event contracts; the upstream framework owns Agent execution and runtime events. A Runner is cached per active Deployment Version, while local source checkouts are limited to inspection or temporary debugging and are not delivery dependencies.
