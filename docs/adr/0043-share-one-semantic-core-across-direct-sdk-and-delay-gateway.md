# Share one semantic core across Direct SDK and Delay Gateway

Status: Accepted
Spec revision: `V1-FROZEN-2026-08-13`

Nereus Delay V1 supports two production entry shapes: a Direct Java SDK for applications that can hold Broker credentials and need the shortest enqueue path, and an optional Delay Gateway for multi-language access and centralized authentication, quota, audit, and credential custody. Neither entry is the semantic authority. Both invoke one versioned Semantic Core that performs Route selection, self-routing identity construction, canonical Command encoding, Command Hash calculation, and `AUTO_FAST` branch freezing. A shared post-preparation Submission Coordinator owns transport selection, physical-attempt ownership, and closed NDR1 outcome projection.

## Decision

- `delay-semantic-core` owns all zero-I/O preparation logic. It consumes only an authenticated tenant context, an immutable verified `RouteSnapshotV1`, Registry-shaped intent values, trusted time, and deterministic identity/hash services. It has no Kafka, Pulsar, gRPC, Oxia-network, RocksDB, or Worker dependency.
- A background control-plane component may refresh immutable Route snapshots from Oxia, but `prepare*` reads only the local verified snapshot. Registry §6.6 fixes its canonical resource/partition/policy fields, digest and signature. Cache miss, watch gap, expired signature, tenant mismatch, semantic drift within one Route Incarnation, or invalid lifecycle fails closed before transport I/O.
- A new Schedule chooses an `ACTIVE_FOR_NEW` Route. Cancel, Reschedule, Payload Commit, and query decode the original self-routing identity and use that exact historical Route and partition; they never rehash against the current active Route.
- The Direct Java SDK is the default high-performance entry. It composes the Semantic Core with bounded client admission, an exact-resource transport registry, query clients, and an optional durable outbox. It does not provide a constructor that silently creates a stock name-only Kafka or Pulsar Producer.
- The Delay Gateway is optional. It derives tenant authority from mTLS/JWT/service-account authentication, applies separate data/control/query quotas, persists request idempotency, then invokes the same Semantic Core and post-preparation Submission Coordinator/transport registry. The request body does not carry authoritative `tenantId`.
- Gateway idempotency persists the exact prepared bytes before any Broker I/O. Equal `(tenant scope, RPC kind, idempotency key, canonical request body)` returns the same prepared identity. Reusing a key for a different body is a conflict. A crash after submission ownership but before a durable outcome is exposed as uncertainty and never triggers a fresh preparation against a newer Route.
- Both entries return the same Registry-defined `SubmissionOutcomeMessageV1`/NDR1 union. Broker durability means queued, not applied; Gateway success is not a second receipt meaning.
- The existing Worker consumes identical NDL1 bytes from the same Command Topic. It has no Direct/Gateway branch.

The code-level module, API, state-machine, configuration, and test mapping is frozen in [`V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md`](../V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md).

## Consequences and trade-offs

- Direct SDK avoids a network hop and centralized bottleneck, but every application process must run the signed Route cache, bounded transport pools, and credential lifecycle correctly.
- Gateway centralizes policy and supports light clients, but adds one RPC hop, a highly available idempotency store, its own admission limits, and an operational failure domain.
- Sharing preparation logic prevents byte drift and incompatible retry identities. It also makes the Semantic Core a deliberately narrow compatibility boundary that requires cross-entry golden tests.
- Gateway request idempotency and Command idempotency are separate. The former deduplicates client RPC ownership; the latter remains the authoritative Worker rule based on `commandId + commandHash`.
- This ADR does not add recurring schedules, a second Message ID format, caller-supplied tenant authority, or a Gateway-only Command envelope.
