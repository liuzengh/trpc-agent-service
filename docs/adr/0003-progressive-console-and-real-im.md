# Use One Progressive Console Without Replacing Real IM Providers

Status: accepted

Starting in Stage 1, the platform will build one React/TypeScript management console and extend it with each phase's management surface; Stage 3 adds a Chat Workspace that visualizes and tests the Mock IM flow. This avoids a late, disconnected frontend and gives every phase a user-verifiable vertical result, while Enterprise WeChat and Telegram remain the two required real Channel Adapter implementations because a browser chat client does not exercise real-provider protocols, credentials, callbacks, limits, or delivery behavior.
