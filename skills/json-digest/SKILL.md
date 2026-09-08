---
name: json-digest
description: Compute the byte count and SHA-256 of a JSON object in an isolated sandbox.
---

# JSON digest

Use this skill when the user requests a reproducible digest of a JSON object.
Call `skill_run` with `skill: "json-digest"` and the object in `input`.
The platform asks for approval before executing. Do not claim execution before
receiving a successful tool result. The digest covers the serialized input.json
bytes, including JSON structure, not just a field's text.

The fixed entrypoint is run.sh. It has no network or host filesystem access.
Report the returned byte count and digest; do not invent a result on failure.
