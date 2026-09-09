# 07: Tenant Memory And Knowledge

**What to build:** Two Tenants can select their own data backends, write durable
Memory, ingest Knowledge, and use those records in later Agent responses without
retrieving or modifying each other's data.

**Blocked by:** 05: Signed Remote Worker Run.

**Status:** resolved

- [x] The complete Agent workflow routes Session, Memory, and Knowledge through
  distinct tenant-scoped store ports selected only from server-owned Backend
  Selection.
- [x] Memory authority is committed before eventually consistent vector
  indexing, with observable retry/checkpoint state for index failures.
- [x] InMemory reference implementations and PostgreSQL metadata implementations
  support the automated workflow.
- [x] S3, Qdrant, and Milvus integration boundaries and consistency behavior are
  documented as adapter designs, not claimed as production integrations.
- [x] Black-box tests prove retrieval affects a later response, index delay does
  not lose authoritative data, and cross-Tenant guesses do not leak existence.
- [x] The ticket documents and runs its own Memory/Knowledge acceptance command.

## Comments

租户化 Memory/Knowledge 工作流及一致性行为已由总门禁验证，设计边界见中文存储文档。
