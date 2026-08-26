# Use bounded time and certify Pulsar delayed delivery

Nereus Delay treats `deliverAt` as a UTC not-before boundary and never compares it directly with an unqualified Worker wall clock. Every Worker maintains a Trusted UTC Interval from an approved time-synchronization source, measured uncertainty, configured drift bound, and monotonic elapsed time. A new Publish Admission is eligible only when `earliestUtcNow >= actionAt`; it is permitted before expiration only while `latestUtcNow < expireAt`.

## Worker clock guard

- `deliverAt`, `actionAt`, `expireAt`, and persisted retry times are signed 64-bit Unix epoch milliseconds in UTC. Client clocks choose requested business time but are not trusted as current server time.
- Schedule/Reschedule horizon validation uses the ingress Broker persistence timestamp `bp` and pinned bounds: `expireAt >= max(deliverAt, bp) + minDeliveryWindow`, `deliverAt <= bp + maxDelayHorizon`, and `expireAt <= bp + maxMessageLifetime`. A past `deliverAt` with a remaining valid window is immediately due. A record persisted in time but applied after expiration deterministically creates its `SCHEDULED` Generation; a separate Trusted-Time runtime transition terminalizes it `EXPIRED`, so source lag does not turn it into a rejection.
- A forward or backward wall-clock step, synchronization loss, uncertainty above the configured bound, suspend/GC pause that invalidates the estimate, or inconsistent Broker-time sample closes Publish Admission on that Worker. Command application may continue if durability and ownership remain safe.
- The scheduler resumes only after a stabilization window of healthy samples. A forward jump never releases a mass of messages merely because raw wall time crossed them; a backward jump produces lateness rather than early delivery.
- `expireAt` must be later than `deliverAt` by the policy's minimum delivery window. In the uncertainty interval where due eligibility is proven but pre-expiry admission is not, the message waits; once expiration is definitely reached it terminalizes without a new external request.
- Configuration proves `minDeliveryWindow > maxTrustedUtcIntervalWidth + maxHealthyAdmissionDecisionDelay`; strict inequality follows from the `latestUtcNow < expireAt` gate. The delay budget includes event-loop arbitration and the Admission System Mutation round trip. An infeasible timing policy or Worker fails activation instead of accepting messages that cannot be admitted even on the certified healthy path.

This contract promises no early eligibility, not exact-time delivery. Queueing, target throttling, retries, Broker dispatch, and consumer availability can all make visibility later.

## Kafka

Kafka managed delivery sets `actionAt = deliverAt` and creates no Producer request until the Worker lower time bound reaches that instant. Record timestamp is preserved business metadata and is not used as a visibility gate. Transactional delivery also begins and commits no earlier than this gate.

## Pulsar

Ordinary managed Pulsar delivery also uses `actionAt = deliverAt` and sends without Broker delay, which works independently of subscription type. Early handoff is a separate `PULSAR_DELAYED_HANDOFF` capability and is permitted only when the Destination Profile certifies all of the following:

- Broker delayed delivery is enabled. Protected physical-topic policies permit only `Shared` or `Key_Shared`, the Broker rejects Exclusive/Failover at subscribe, and activation proves no incompatible Consumer is already connected; Pulsar may deliver delayed messages immediately for the excluded types.
- `isDelayedDeliveryDeliverAtTimeStrict=true`; the default non-strict tracker can release up to one tick early.
- Every eligible Broker runs source-locked `PULSAR_DELAY_VISIBILITY_GUARD`. A Nereus delayed record carries business `deliverAt` and guard version; the delayed tracker/dispatcher releases it only when the Broker's Trusted UTC lower bound reaches that business instant. Forward steps, lost synchronization, or excess uncertainty hold delivery. A signed all-Broker attestation fixes the binary, config generation, time policy, and protected subscription policy.
- Target Broker clock-ahead error is bounded and continuously monitored, and topic retention/TTL cannot remove the message before its delayed visibility.
- The fixed Profile-version handoff lead and Target Early-Delivery Bound are known.

For that capability, the service uses `actionAt = deliverAt - handoffLead` and writes Pulsar `deliverAt = businessDeliverAt + targetClockAheadBound`, plus guard metadata carrying the unshifted business time. The shift may add bounded lateness; the dispatch guard closes post-ACK clock/config/subscription drift. Resource-controller ACL and the guard keep strictness, subscription policy, guard binary/config, and retention immutable while any outstanding guarded record has not crossed business `deliverAt`. Loss of a prerequisite removes READY and marks affected handoff Lanes runtime `BLOCKED`; it never writes `ADMIN_PAUSED` or falls back to an unsafe native timestamp. Bypassing the protected Broker control plane is a capability TCB violation, not a recoverable timing mode. `HANDED_OFF` means Pulsar durably accepted responsibility, not that a consumer has received the message.

`AUTO_FAST` is subject to the same Broker-enforced subscription, strictness, clock, and per-record visibility guard and exposes the guard/profile version in its Native Delivery Receipt. A client-side probe alone is insufficient. Without certification, the SDK selects the managed branch before any submission I/O; once native I/O starts it can never fall back to managed.

## Evidence and tests

Metrics expose time uncertainty, last synchronized sample age, wall/monotonic divergence, clock-gate pauses, action lateness, target clock bound, and Pulsar strict/visibility-guard certification. Deterministic and real-Broker cuts run both before and after handoff ACK: clock steps, Broker failover, guard/config rollout, subscription-policy changes, long pauses, uncertainty growth, non-strict ticks, and incompatible subscribe attempts. The oracle is exact: ordinary managed has no Admission/Producer call before `deliverAt`; certified handoff has no Admission before `actionAt`; neither handoff nor AUTO_FAST permits consumer eligibility before business `deliverAt`.
