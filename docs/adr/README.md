# Architecture Decision Records

This file is the authoritative index for Nereus Delay V1 ADRs 0001–0044.

- Status: `Accepted`
- Spec revision: `V1-FROZEN-2026-08-13`
- Normative numeric registry: [`V1-PROTOCOL-REGISTRY.md`](../V1-PROTOCOL-REGISTRY.md)
- Cross-document audit and release-evidence checklist: [`V1-DESIGN-AUDIT.md`](../V1-DESIGN-AUDIT.md)

## Governance

Every ADR listed below is an accepted part of spec revision `V1-FROZEN-2026-08-13`. A more specific ADR does not silently override the main design or Protocol Registry: if they conflict, the conflict must be resolved through an explicit revision to every affected artifact.

When an ADR is superseded, this index must be updated in the same change to record its new status and identify the superseding ADR.

## Index

| ADR | Status | Spec revision |
| --- | --- | --- |
| [0001 — Define `deliverAt` as earliest consumer visibility](0001-define-deliver-at-as-earliest-consumer-visibility.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0002 — Require opt-in for native fast delivery](0002-require-opt-in-for-native-fast-delivery.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0003 — Scope `ownerEpoch` to local fencing](0003-scope-owner-epoch-to-local-fencing.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0004 — Use one RocksDB database per Delay Shard](0004-use-one-rocksdb-database-per-delay-shard.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0005 — Separate Command queuing from authoritative application](0005-separate-command-queuing-from-application.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0006 — Stabilize Command identity before enqueue](0006-stabilize-command-identity-before-enqueue.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0007 — Use Source Position as the sole Command order](0007-use-source-position-as-the-sole-command-order.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0008 — Isolate Destination Lanes from Command application](0008-isolate-destination-lanes-from-command-application.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0009 — Pin versioned Destination Profiles at Schedule application](0009-pin-versioned-destination-profiles-at-schedule-application.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0010 — Use reserve, upload, and commit for large payloads](0010-use-reserve-upload-commit-for-large-payloads.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0011 — Tie recovery and garbage collection to a checkpoint floor](0011-tie-recovery-and-garbage-collection-to-a-checkpoint-floor.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0012 — Bound Command idempotency with Broker-time fences](0012-bound-command-idempotency-with-broker-time-fences.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0013 — Make Publish Admission the control point of no return](0013-make-publish-admission-the-control-point-of-no-return.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0014 — Make destination guarantees explicit capability Profiles](0014-make-destination-guarantees-explicit-capability-profiles.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0015 — Make dead-letter state internal and replay explicit](0015-make-dead-letter-state-internal-and-replay-explicit.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0016 — Pin ordered delivery to one source and target partition](0016-pin-ordered-delivery-to-one-source-and-target-partition.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0017 — Require Source Assignment and an Oxia Owner Lease](0017-require-source-assignment-and-an-oxia-owner-lease.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0018 — Route queries by receipt and read through a Source barrier](0018-route-queries-by-receipt-and-read-through-a-source-barrier.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0019 — Use atomic Shard Quota Grants and reserve control capacity](0019-use-atomic-shard-quota-grants-and-reserve-control-capacity.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0020 — Derive tenant authority from isolated Ingress Routes](0020-derive-tenant-authority-from-isolated-ingress-routes.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0021 — Use bounded time and certify Pulsar delayed delivery](0021-use-bounded-time-and-certify-pulsar-delayed-delivery.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0022 — Classify publish outcomes by side-effect evidence](0022-classify-publish-outcomes-by-side-effect-evidence.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0023 — Make Command Topics replayable non-compacted logs](0023-make-command-topics-replayable-non-compacted-logs.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0024 — Use versioned self-routing identities and a fixed hash](0024-use-versioned-self-routing-identities-and-a-fixed-hash.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0025 — Use seven Column Families with versioned key namespaces](0025-use-seven-column-families-with-versioned-key-namespaces.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0026 — Hash a versioned Canonical Command Body](0026-hash-a-versioned-canonical-command-body.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0027 — Use checkpoint/replay recovery without warm standby](0027-use-checkpoint-replay-recovery-without-warm-standby.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0028 — Bound multi-DB Workers with shared resources and weighted placement](0028-bound-multi-db-workers-with-shared-resources-and-weighted-placement.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0029 — Use immutable control versions and narrow operator actions](0029-use-immutable-control-versions-and-narrow-operator-actions.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0030 — Limit V1 to one active recovery cell](0030-limit-v1-to-one-active-recovery-cell.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0031 — Prepare the `AUTO_FAST` branch before I/O and return an outcome union](0031-choose-auto-fast-before-io-and-return-a-receipt-union.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0032 — Use two-level bounded deficit round robin](0032-use-two-level-bounded-deficit-round-robin.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0033 — Treat payloads as opaque and metadata as Adapter-specific](0033-treat-payloads-as-opaque-and-metadata-as-adapter-specific.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0034 — Make Command application deterministic under replay](0034-make-command-application-deterministic-under-replay.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0035 — Scope strong destination channels to Destination Lanes](0035-scope-strong-destination-channels-to-lanes.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0036 — Isolate Adapter admission and buffers by Destination Lane](0036-isolate-adapter-admission-and-buffers-by-lane.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0037 — Journal Pulsar sequence mappings before target send](0037-journal-pulsar-sequence-mappings-before-send.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0038 — Pin Broker resource incarnations](0038-pin-broker-resource-incarnations.md) | Accepted; Producer enforcement amended by 0044 | `V1-FROZEN-2026-08-13` |
| [0039 — Serialize command-affecting runtime mutations in the Shard Log](0039-serialize-command-affecting-runtime-mutations-in-the-shard-log.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0040 — Use ancestry-bound Recovery Lineages and recovery pins](0040-use-ancestry-bound-recovery-lineages-and-pins.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0041 — Persist SLO samples before they can be lost](0041-persist-slo-samples-before-they-can-be-lost.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0042 — Separate immutable credential policy from rotatable secret bindings](0042-separate-credential-policy-from-secret-bindings.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0043 — Share one semantic core across Direct SDK and Delay Gateway](0043-share-one-semantic-core-across-direct-sdk-and-delay-gateway.md) | Accepted | `V1-FROZEN-2026-08-13` |
| [0044 — Use first-class guarded Broker transports](0044-use-first-class-guarded-broker-transports.md) | Accepted | `V1-FROZEN-2026-08-13` |
