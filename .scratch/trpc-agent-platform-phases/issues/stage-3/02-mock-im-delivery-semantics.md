# 02: Mock IM Delivery Semantics

**What to build:** Make the Mock IM provider deterministic and fault-capable across timeout, retry, rate-limit, message-length, and attachment semantics. Users can exercise channel delivery behavior locally and see whether each reply is accepted, retried, rejected, or permanently failed without relying on an external provider.

**Blocked by:** 01: Channel Adapter And Binding Baseline

**Status:** resolved

- [x] Mock IM models deterministic timeout, retry, rate-limit, message-length, and attachment outcomes for inbound and outbound delivery.
- [x] Retry behavior is bounded, cancellable, and does not duplicate an accepted message or corrupt event history.
- [x] Rate-limit, length, and attachment failures use stable public outcomes without exposing provider secrets or internal diagnostics.
- [x] Automated tests cover each delivery semantic, including duplicate, out-of-order, timeout, retry-exhaustion, and cancellation paths.
- [x] The Mock IM remains a local validation surface and is not represented as one of the required real IM providers.

## Answer

Implemented tenant-scoped Mock IM fault configuration with deterministic timeout, bounded retry, rate-limit, message-length, and attachment semantics. Channel failures now map to stable public error codes, outbound failures persist a terminal delivery event, and tests cover fault outcomes, bounded retry, cancellation, duplicate suppression, and stable callback errors.
