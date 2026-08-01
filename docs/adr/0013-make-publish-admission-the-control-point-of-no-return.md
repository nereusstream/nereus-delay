# Make Publish Admission the control point of no return

Nereus Delay V1 treats Claim as a reversible internal reservation and makes durable Publish Admission the exact cancellation and rescheduling point of no return. Admission is an authenticated `PUBLISH_ADMISSION_V1` System Mutation in the same Shard Log as tenant Commands. The shard must consume it and commit `PUBLISHING` with one `publishAttemptId`, Message Generation, Owner identity, and runtime revision before invoking a destination Producer. A Cancel or Reschedule ordered before that Source Position revokes the Claim and succeeds; one ordered after admission returns `TOO_LATE` because the external result can no longer be controlled.

## Message state machine

```text
SCHEDULED / RETRY_WAIT
    -> CLAIMED
        -> SCHEDULED / RETRY_WAIT     claim revoked
        -> CANCELED                   Cancel wins
        -> SUPERSEDED + SCHEDULED(new generation)  Reschedule wins
        -> EXPIRED                    expireAt reached
        -> PUBLISHING                 durable Publish Admission

PUBLISHING
    -> PUBLISHED / HANDED_OFF
    -> RETRY_WAIT                     definitive retriable failure
    -> DEAD_LETTER                    permanent failure or exhausted policy
    -> UNCERTAIN                      target acceptance cannot be determined

UNCERTAIN
    -> PUBLISHED / HANDED_OFF         capability resolves success
    -> RETRY_WAIT                     every admitted attempt is proven absent/retired
    -> UNCERTAIN + current work        unordered bounded baseline retry
    -> DEAD_LETTER + order barrier     explicit terminal policy where required
```

Large-payload `PAYLOAD_RESERVED` is a pre-message reservation and becomes `SCHEDULED` only through Payload Commit.

## Linearization and recovery

- Lane message/byte permits are acquired before Claim. A claimed record is persisted in `inflight_cf` and materialized as a `PreparedPublishTemplate`, but no Producer call is allowed until a second shard event durably applies Publish Admission.
- Each Store WAL-syncs a checked monotonic Claim sequence with the reversible Claim. The Protocol Registry derives `claimId` from Store/Owner/sequence/message/generation/Lane revision and derives `publishAttemptId` from that exact Claim plus generation/attempt number. A capacity-gated or stale Admission revokes the Claim without consuming attempt number, so a later Claim cannot collide with the earlier mutation; an uncertain enqueue reuses the original IDs and bytes.
- The executor freezes the exact Claim precondition, Ready Certificate, Trusted-UTC decision evidence, full descriptor, and hash, then prepares and retries the exact signed Admission System Mutation. Consumption validates those immutable facts and the record's Broker persistence time, allocates/finalizes the attempt, and persists its reproducible descriptor with `PUBLISHING`. Cancel, Reschedule, and logged expiration are ordered against admission by physical Shard Log Source Position, not unrecoverable local callback order.
- `PUBLISHING durable happens-before producer.sendAsync`. A crash after Admission never authorizes a “harmless” assumption: replay first reconstructs `PUBLISHING`, then absence of the live first-send gate produces the exact logged recovery-unknown Outcome and `UNCERTAIN`; capability evidence may later prove non-publication, while a baseline retry may duplicate. A call without prior durable Admission is a protocol violation.
- Recovery safely requeues `CLAIMED` only when no source-ordered Close overlay owns it, preserving its semantic timeline kind/authority/candidate/digest and prior obligation set while issuing a new runtime revision/instance digest; aggregate state can therefore remain `UNCERTAIN`. The source Admission precondition deliberately excludes that local instance digest. Recovery replays every Admission/Outcome/Expiry System Mutation. A valid, on-time Admission always reconstructs the same durable `PUBLISHING`, even when its local Claim was omitted by an older checkpoint; Owner/Store change, apply-time wall clock, or absence of the ephemeral locally-authored token cannot change that log result. Only the matching live Owner/Store/Claim/token/certificate/time gate can issue a first send. Without it, recovery appends an exact initial `UNKNOWN` Outcome at a later Shard Log position and then follows capability evidence or the pinned baseline policy.
- Every callback carries exact attempt, generation, Owner Epoch, Store Incarnation, and Admission revision. A callback for the current attempt must match current runtime state. A retained earlier `UNKNOWN` attempt under the same Owner/Store may append its exact ledger outcome even after the aggregate runtime revision advanced; applying that outcome still revalidates the current Generation. Owner, Store, Generation, or attempt-ledger mismatch is audit-only; cross-Owner resolution requires the Profile's authenticated external-evidence path.
- Every Admission also creates a Publish Attempt ledger with immutable attempt/Admission identity and registered monotonic Outcome/evidence/retirement transitions, and adds its exact-key `AttemptObligationRefV1` to the canonical `GenerationRuntimeIndexV1` set. A baseline retry does not overwrite a prior `UNKNOWN` attempt that may still complete remotely; the Generation has at most one current new send while retaining all unresolved admitted attempts up to its pinned attempt bound. While any such ledger remains UNCERTAIN, the aggregate state remains `UNCERTAIN`, while the distinct current-work oneof may be TIMELINE, CLAIMED, PUBLISHING, or NONE—never ordinary `RETRY_WAIT`.
- `UNKNOWN + SCHEDULED` creates unordered `UNCERTAIN_RETRY` timeline work but does not consume the uncertain-retry count. Its Claim freezes the replay-stable semantic-work digest/kind, both Admission counters, and the obligation-set digest while separately retaining a local work-instance digest; only the later durable Admission consumes the count if an older UNCERTAIN ledger is still present. Intervening semantic evidence revokes that reversible Claim or makes its Admission stale, while a pure runtime requeue does not invalidate a persisted record.
- An explicit `ResolveUncertain` retry uses a distinct source-ordered `CONTROL_OVERRIDE` authority embedded in the timeline semantic-work digest. It may exceed the automatic uncertain-retry budget only within the pinned total Admission/time/expiry bounds; it cannot run on an ordered, broken, or closed Lane, and transient capacity is still enforced at Claim/Admission.
- A verified late success removes reversible timeline/Claim work and terminalizes, but cannot revoke another already admitted PUBLISHING attempt; that attempt remains an open terminal obligation and duplicate risk becomes explicit. If all old attempts prove absent instead, stale-digest Claim is revoked before the current work normalizes to definitive retry. Terminal callbacks can release only their own evidence/charge and cannot rewrite the terminal decision.

## Control outcomes

- Cancel and Reschedule succeed in `SCHEDULED`, `CLAIMED`, or `RETRY_WAIT` only when the obligation set certifies all admitted attempts as not published and retired. They remain `TOO_LATE` whenever any UNCERTAIN obligation exists, including when current work is a reversible timeline or Claim. Claim revocation, timeline/id update, command result, capacity update, and Source Position advance are one WriteBatch.
- In `PUBLISHING`, `UNCERTAIN`, or `HANDED_OFF`, they return `TOO_LATE`. A known `PUBLISHED` returns `ALREADY_PUBLISHED`; other terminal states return a stable terminal outcome. V1 removes ambiguous `IN_FLIGHT` as a result that might imply later cancellation can succeed.
- `expectedStateVersion` compares the client-visible Control Version. Successful Schedule, Reschedule, and Cancel advance it; internal Claim, retry, circuit, and callback writes use a separate runtime revision. State eligibility is checked even when the expected version matches.
- Initial Schedule creates Message Generation `0` and Control Version `1`. Reschedule and Dead Letter Replay use checked increment to create the next generation; overflow is a permanent protocol limit. Reschedule writes an immutable `SUPERSEDED` history record for the old generation and a new timeline entry while preserving payload, Destination Binding, ordering mode, and Retry Policy in V1. Publish retries keep the same generation and Control Version.
- Each internal Claim, Admission, retry, resolution, and callback advances a separate runtime revision. Publish Attempt numbering starts at `1` for each generation; only durable Admission consumes a number.
- `expireAt` blocks a new Publish Admission from qualifying at Broker persistence time. Apply/replay lag may cross that wall-clock boundary without changing an already persisted Admission, and `expireAt` cannot revoke a request already admitted, so first send or completion after expiration remains possible and follows the normal publish outcome.
- `PUBLISHED` and `HANDED_OFF` mean destination-Broker durability under the selected capability, never consumer processing. `HANDED_OFF` is the terminal result for certified early Pulsar handoff.
- A new Cancel against `CANCELED` returns stable `ALREADY_CANCELED`; against `PUBLISHED`/`HANDED_OFF` it returns `ALREADY_PUBLISHED`; against `EXPIRED`, `DEAD_LETTER`, or an explicitly referenced old `SUPERSEDED` generation it returns `ALREADY_EXPIRED`, `ALREADY_DEAD_LETTERED`, or `GENERATION_SUPERSEDED`. There is no free-form terminal-conflict code. A new Reschedule is permitted only for the current generation in `SCHEDULED`, `RETRY_WAIT`, or reversible `CLAIMED`.
