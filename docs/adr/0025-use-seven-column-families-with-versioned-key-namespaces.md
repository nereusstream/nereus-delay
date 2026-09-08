# Use seven Column Families with versioned key namespaces

Nereus Delay keeps exactly seven **application** Column Families per shard database: `timeline_cf`, `id_cf`, `inflight_cf`, `dedupe_cf`, `terminal_cf`, `gc_cf`, and `meta_cf`. RocksDB's mandatory `default` Column Family is opened as an eighth physical CF, stays application-empty and read/write-forbidden, and is still included in descriptors, cache/memtable/file budgets, checkpoint manifests and open-time validation. Missing/unknown CFs or a non-empty default CF fail activation. New protocol concepts use versioned one-byte key namespaces inside the seven application CFs rather than creating another memtable/tuning surface. Every authoritative mutation uses one WAL-enabled RocksDB WriteBatch across them.

## Key encoding

Keys begin with record-type and key-format bytes. The normative [`Current Protocol Registry`](../PROTOCOL-REGISTRY.md) fixes every CF tag, component order/width, typed Source Position variant and conformance vector. Ordered numeric components are fixed-width unsigned big-endian; identities use their exact fixed canonical width; truly variable bytes are u32-length-prefixed and bounded. Values use the registry's typed version envelope with required-field validation and CRC32C in addition to RocksDB block/file checksums. Unknown key or required value versions fail shard activation. Java serialization, delimiter-concatenated strings, native-endian numbers, and wall-clock TTL compaction filters are forbidden.

## Column Family namespaces

- `timeline_cf`
  - unordered due key: `[DUE=0x01][format=0x01][destinationLaneId][eligibleAt][sourceOrderToken][delayMessageId][generation]`;
  - ordered key: `[ORDERED=0x02][format=0x01][destinationLaneId][deliverAt][sourceOrderToken][delayMessageId][generation]`, whose value carries canonical `actionAt`, current retry eligibility and head-blocking state. `deliverAt` selects the strict business-order head; the READY time is that head's `max(actionAt, retryEligibilityAt)`. Certified Pulsar handoff uses one fixed Profile-version lead per Lane, so `actionAt=deliverAt-handoffLead` is order-preserving; Nereus Delay forbids a per-message lead;
  - every DUE/ORDERED value is exact `TimelineWorkRef`, whose closed kind is initial schedule, definitive retry, or unordered uncertain retry; its full embedded key/hash, candidate attempt number, runtime revision and times must match the RocksDB key and `id_cf` current-work copy;
  - ready key: `[READY=0x03][format=0x01][nextEligibleAt][destinationLaneId][laneVersion]`; Lane state stores its exact current key, and every replacement atomically deletes the old key and inserts the new one;
  - expiry key: `[EXPIRY=0x04][format=0x01][expireAt][destinationLaneId][delayMessageId][generation]`, independent of Lane readiness/circuit and removed only by generation replacement or terminal/close;
  - reservation expiry key: `[RESERVATION_EXPIRY=0x05][format=0x01][reservationExpireAt][reservationId]`; scanner never guesses reservation versus Message Generation from value/length;
  - bounded system-Lane keys for DLQ export and maintenance admission.
- `id_cf`
  - current message index by Delay Message Identity, including generation, Control Version, binding/payload references, and exact `GenerationRuntimeIndex`: public aggregate state, the zero-or-one current send work, canonical admitted-attempt obligation ref set, Admissions/uncertain-retry counters, duplicate risk and runtime digest. Each ref stores the exact inflight key/hash and ledger state for direct old-Owner lookup;
  - compact `RETIRED_IDENTITY` tombstone after full entity history is reclaimable and until the ID's maximum first-seen freshness deadline is source-fenced and checkpoint-safe;
  - payload reservation/reference by reservation identity;
  - immutable binding/value references needed to find active state without scanning.
- `inflight_cf`
  - full `CLAIMED`, `PUBLISHING`, and `UNCERTAIN` records, including owner/store identity, claim/admission deadlines, the complete canonical Prepared Publish descriptor plus hash, attempt number, capability evidence identity, and permit accounting;
  - one reversible current Claim may coexist with older UNCERTAIN ledgers; one current PUBLISHING ledger may likewise coexist with older UNCERTAIN ledgers. Every admitted ledger remains in the runtime index's bounded obligation set until exact outcome/evidence/charge retirement.
- `dedupe_cf`
  - `[COMMAND=0x01][format=0x01][commandId]` compact Client Command identity/hash/outcome and retry window;
  - `[RESULT=0x02][format=0x01][commandId]` full public query result;
  - `[POSITION=0x03][format=0x01][typedCanonicalSourcePosition]` position audit/quarantine;
  - `[FENCE=0x04][format=0x01][fenceProofId]` fence/reclamation evidence;
  - `[SYSTEM_MUTATION=0x05][format=0x01][systemMutationId]` signed mutation hash/type/author/scope/deadline/stable result;
  - full query result with shorter retention;
  - Source Position and System Mutation namespaces are never exposed as bare Command IDs.
- `terminal_cf`
  - immutable per-generation `SUPERSEDED`, `PUBLISHED`, `HANDED_OFF`, `CANCELED`, `EXPIRED`, and `DEAD_LETTER` history;
  - independently updated DLQ Export outcome and retained target evidence.
- `gc_cf`
  - time-ordered guarded deletion tasks for payloads, terminal/result/dedupe/retired-identity state, checkpoints, and orphan uploads;
  - every task carries exact expected identity/version/checksum and Checkpoint Safety Barrier requirement.
- `meta_cf`
  - fixed shard/store/DB identity, format and schema versions, Route Incarnation and partition, applied Shard Log position and typed evidence cursors, checkpoint/catalog identity, last opened Owner Epoch, clean-close marker, and the checked per-Store next-Claim sequence used by the Protocol Registry's collision-free Claim/Publish Attempt ID derivation;
  - Lane state/version/circuit/fairness counters;
  - Shard Quota Grant usage and Worker reconciliation metadata;
  - destination producer sequences/channels and bounded GC/checkpoint progress.
  - bounded `SLO_OUTBOX=0x08` entries keyed by exact sample ID. These are observational, reconstructible/monotonic-conservative records outside the command-derived state digest; their budget is disjoint from correctness/outcome reserve.

Lane and fixed identifiers are collision-resistant canonical hashes, but their values retain enough canonical source identity to detect a hash collision before merging state.

## Atomic mutations

- Shard Log apply writes dedupe/result/position audit, message or reservation state, timeline/inflight/terminal changes, old/new READY replacement, Lane/quota counters, and final `appliedShardLogPosition` atomically.
- Claim moves one exact timeline record to `inflight_cf` and updates the ID locator and Lane permits atomically.
- Publish Admission replaces reversible claim state with exact `PUBLISHING` evidence before the Adapter call.
- Publish Admission also validates the Claim's source-work kind/counters/obligation-set digest, checked-increments Admissions and, only when an older UNCERTAIN ledger exists, uncertain-retry Admissions, then adds the new attempt to the canonical obligation set in that same batch.
- Callback/outcome, retry, terminal, Cancel, Reschedule, signed Replay mutation, payload commit, and GC compare their exact generation/runtime/owner/store or immutable attempt-ledger tokens and update every duplicate index in one batch. A retained prior `UNKNOWN` attempt can append evidence without pretending it is the current runtime revision. Late success deletes reversible current work but cannot erase another admitted attempt; terminal history retains that open obligation and later callbacks only retire its evidence/charge.

All correctness batches keep WAL enabled. `sync=true` is group-amortized for command-application batches and for Claim/Admission/outcome groups only where the state machine allows grouping; a Producer call never starts before the exact Admission batch is durably synced. Source acknowledgements likewise follow the command batch sync.

## Scan and consistency rules

The shard event loop is the single writer. Scheduler scans use bounded RocksDB snapshots and iterate Lane-ready/due prefixes with upper bounds; the event loop revalidates READY version, exact ID locator, and runtime revision before Claim, so an old key visible in a concurrent snapshot cannot publish. Persistent orphan/missing/version-mismatched READY keys fence scheduling and require deterministic rebuild rather than ordinary GC. Ordered Lane heads are selected by Delivery-Time FIFO and remain blocking through retry or uncertainty. Unordered Lanes scan by eligibility.

Invariant audits verify one runtime index for the Message Identity's current Generation or protected retired-identity tombstone; zero or one current TIMELINE/CLAIMED/PUBLISHING work; zero through pinned-maximum admitted attempt ledgers covered by that nonterminal/current-terminal runtime set or an older terminal open-obligation summary; matching work/attempt counters and digests; counter sums, Lane head/readiness, Source Position monotonicity, and terminal-decision immutability. A current terminal record mirrors retained PUBLISHING/UNCERTAIN obligations with current work NONE; after Replay, its summary is their sole Generation locator and the new runtime index starts empty. Any ambiguity or cross-generation reference is fail-closed; repair requires a fenced shard and deterministic rebuild from authoritative records.

## Local layout

Each installation lives at:

```text
<root>/shards/<routeIncarnation>/<partition>/
  ACTIVE
  incarnations/<storeIncarnation>/db/
  checkpoint-tmp/<checkpointId>/
  restore-tmp/<checkpointId>-<nonce>/db/
```

Restore populates a unique temp directory, verifies manifest/file checksums, DB and shard identity, format, and replay availability, then generates and WAL-syncs a fresh Store Incarnation. It closes install mode before atomically replacing and fsyncing the checksummed `ACTIVE` pointer, then opens normally. The checkpoint creator's Store token is never reused, files are never copied over an open database, and two shard identities never share one DB directory.


## NDIP-3 Target format work in progress

The [Target identity and key contract](../ndip/NDIP-3/05-Target身份与索引契约.md) reserves
separate DUE/NATIVE/EXPIRY and Target-state tags inside the existing application CFs.
Candidate keys include domain slot and nonzero generation from the first single-domain
implementation. Store format 2 is reserved as the old-reader refusal boundary; new tags
alone do not prove refusal because legacy readers can ignore unknown prefixes. Current
application writers remain on format 1 until the complete schema and migration gates close.


The Target format reserves an immutable physical identity record separately from the
hot Target queue state. Queue state contains bounded domain/head summaries and control
versions; released slots retain generation history. FIFO business-order and eligible-head
keys remain distinct. The [B1 field contract](../ndip/NDIP-3/05-Target身份与索引契约.md)
reserves NV types 12/13, which the active Lane-format envelope reader continues to reject.
These codecs do not activate Store format 2 or prove the external obligation-release gates.


The Target format also reserves NV type 14 for a complete reversible work value shared
by ordinary and optional Native indexes. Its locator is embedded in Message/work rather
than maintained as an extra independently updated key. Full Source Positions retain their
registered wire identity with explicit Target-format text limits. The byte bounds and
semantic/instance digest rules are fixed in the B1 contract; they neither activate the
format nor replace Message, binding, source, Owner and obligation checks during C1.


The Target Message uses a separate id tag 05 and reserved NV type 15; its bounded runtime
is the sole aggregate-state projection and retains exact open-attempt references even
when terminal. Reserved NV type 16 carries a stable generation/expiry projection. Inline
and committed object payloads have closed byte bounds. None of these reservations alters
the active Lane reader or removes obligations; capacity before Admission and exact
retirement evidence remain runtime/migration gates.

ORDER_STATE reserves NV type 17 under the existing Target order-state key. It binds
the source Shard, execution slot/generation, accounting incarnation, ordering contract,
revisions and gate, with mutually exclusive serviceable-head and exact runtime barrier
projections. ORDER_HEAD reuses the complete NV 14 work body. Terminal status cannot
remove a barrier while attempts remain unresolved. The bounded codecs preserve old
strict semantics under contract 1; contract 2 is reserved for the explicit Admission
watermark. B5/E5 define and activate late-insertion behavior, with B6/F1 proving the
old-data transition. These reservations remain inactive and do not add a column family.


B2 additionally reserves immutable execution/control contracts at meta tags 0c/0d,
with keys `tag + 01 + digest[32]` and NV types 18/19. Their closed canonical byte
bounds are 1049187 and 2396. Domain summaries reference the entire verified objects;
content equality and offered-requirement coverage are distinct. The bounded registration
planner validates all non-VACANT references and keeps slot generation history, without
writing source results or granting execution authority. Group membership, Schedule binding,
channel freezing and teardown are specified in the following B2 contracts. The [B2 contract](../ndip/NDIP-3/06-执行与控制兼容契约.md)
records these obligations; the active format and existing seven CFs remain unchanged.


B2 reserves exact Target Schedule bindings at id tag 06 / NV 21 and immutable channel
identities at meta tag 0e / NV 20; both keys use `tag + 01 + digest[32]`. Store decoders
also verify the source Shard. Binding bodies retain exact Schedule/Prepare bytes and the
original authorization source, with bounded preflight before legacy nested decoders.
Channel renewals preserve the producer/sequence domain while advancing channel generation
and replacing the complete lease; a new identity cannot overwrite an old frozen attempt.
Canonical bounds are 20976072 bytes per binding and 1279 per channel identity. The
[record contract](../ndip/NDIP-3/07-Schedule绑定与通道身份契约.md) fixes lifetime/teardown rules;
complete grants are specified below, while actual reference retirement and format activation
remain C1/C2/B6/F1 work. No column family or active Lane format is changed.

B2 reserves full Target membership grants at meta tag 0f / NV 22, using the same
34-byte content-addressed key shape. The canonical bound is 3149936 bytes; its
pre-append registration bound is 2101194. Full source identity is added after
authenticated source apply; decoding is not issuer authentication. The
[membership contract](../ndip/NDIP-3/08-成员授权与source关联契约.md) preserves
historical binding/attempt protection; the issuance/closure wire and complete policy are
defined by the [authenticated Control contract](../ndip/NDIP-3/09-成员策略与认证控制契约.md). The active seven-CF Lane format remains unchanged.


The immutable membership policy reserves meta tag 10 / NV 23, with a 1052039-byte
canonical bound. Authenticated Control operations 16/17 use ApplyShardControl kinds
15/16, exact target registration and two source-authorized signatures. The pure verifier
returns first-application grant/closure changes; C1 must provide protected authority
snapshots and atomically commit state with Result/SourceAdvance after deduplication.
The active Lane Store still rejects these kinds and NV 23, including after local reopen.
These contracts do not activate the new format or certify a deployed authority backend.


NDIP-3 B3 reserves complete common Native scopes at meta tag 11 / NV 24 and signed
snapshots at meta tag 12 / NV 25, both with 34-byte digest keys. Their bounds are
397/646 bytes. The new artifact/snapshot/head generations and precise authority
requirements are fixed in the [B3 contract](../ndip/NDIP-3/10-Native共同策略与签名契约.md).
Current Lane readers continue rejecting these envelopes; no CF or active format changes.

NDIP-3 B4 reserves independent quota counters at meta tag 13 / NV 26 and a shard
aggregate at meta tag 14 / NV 27. Their full identity keys, local revisions and
source stamps are described in the [B4 contract](../ndip/NDIP-3/11-局部Quota与增量计费契约.md).
Tenant counters are mirrors; only primary counters contribute to the aggregate.
Target cardinalities have explicit new fields and do not reinterpret legacy Lane
capacity dimensions. This is an in-progress contract foundation: full billing/grant
rules and C4 atomic runtime integration remain required. The current reader rejects
these types, and no new column family or active quota writer is introduced.

B4 additionally reserves the attempt accounting lifecycle at meta tag 15 / NV 28,
keyed by the exact PublishAttemptId. Its complete locator, frozen accounting artifact,
commitment/allocation and source/Floor stamps separate definitive logical completion
from checkpoint-safe reserve transfer and guarded retained deletion. Per-attempt
reserves exclude Message-owned retained payload. No active writer or old reader
changes; the [B4 contract](../ndip/NDIP-3/11-局部Quota与增量计费契约.md) remains in progress.


B4 also reserves a derived all-incarnation Target total at meta tag 16 / NV 29:
`16 01 | 02 | sourceShard[20] | tenantRoutingScope[32] | targetId[32]` (87 bytes).
Only affected primary leaves update their Target total in the same batch. This total
is never added again to the shard aggregate; tenant mirrors remain projections.
The new complete quota grant artifact has no standalone NV/meta reservation and
is not wired into the legacy grant control branch. Its new authenticated control
contract is described below; full bookkeeping/retirement remain incomplete. The
old reader still rejects NV 29.

B4 reserves the full quota grant activation projection at meta tag 17 / NV 30:
`17 01 | scopeKind[1] | sourceShard[20] | tenantRoutingScope[32] | [targetId[32]]`
(55/87 bytes). Control operation 18 and APPLY ControlKind/payload 17 bind full
next/prior grants, exact registration and optional parent transfer plan. The
source stamp hashes the complete accepted signed envelope. No recursive prior
activation is embedded. A pure verifier requires source-valid signing/Route/
capacity authority and exact Store read-set guards; it does not implement the
production authority backend or C4 atomic writer. Legacy readers reject both the
new body and NV 30; old Lane Store rejection survives reopen. No CF is added.

B4 reserves one source-local bookkeeping anchor at meta tag 18 / NV 31:
`18 01 | sourceShard[20]` (22 bytes). A frozen SHARD root and its tenant mirror
hold the fixed storage commitments for counter/aggregate/total/grant-activation
projections and the anchor itself. Schema bounds use the exact immutable source
shape, including optional Kafka leader epoch space, without recursive usage
encoding. Retired counters and zero totals keep their slots until protected
actual deletion. Attempt budget record storage remains with its frozen Target
owner outside its internal reserve, even in RELEASED. The actual aggregate must
match the root incarnation and source stamps. No active reader/writer or CF is
changed; C4 still owns atomic inventory integration and independent recovery.

B4 reserves a unique Message payload owner at meta tag 19 / NV 32:
`19 01 | DelayMessageId[41]` (43 bytes). The original Target incarnation,
tenant and complete accounting artifact remain frozen across generations.
Reservation, active, retained and released phases move one payload charge;
attempt execution budgets do not duplicate it. The owner's STATE record fee
persists after payload release until protected actual record deletion. Complete
binding/source/object identities and mandatory transition authority are required.
The codec does not activate a reader/writer, add a CF or certify Store recovery.
See NDIP-3 quota contract section 13 for the closed field table and obligations.

B4 binds existing Message-family records (NV 14/15/16/21/32) to their frozen
Message payload owner through TargetQuotaMessageRecords. Every physical key pays
one STATE fee from its validated full key and typed payload. Native/ordinary
copies pay separate record storage, while embedded runtime/head structures do
not become invented independent records. The supplied-subset limit is six and
exact CF/type/key/value rechecks remain part of C4's Store guard. This adds no
new namespace, CF or active reader/writer; complete recovery is still required.

B4 reserves a source-derived accounting descriptor at meta tag 1a / NV 33.
The complete primary identity suffix yields 39-byte SHARD or 71-byte TARGET keys.
Full scope/accounting/lineage/allocation source derive the 16-byte incarnation;
one optional later drain stamp closes new ingress without releasing retained costs.
The descriptor pays its own fixed two-source storage envelope and one incarnation;
QueueState and OrderState provide unique local key cardinalities. Protected actual
descriptor deletion leaves zero counter tombstones and existing root slot fees.
The active Store root cannot retire independently. No CF or active reader/writer
changes; rotation/legacy handover and C4 source/ledger/delete backends remain required.

B4 draft NV 30 now retains the immutable OPEN first Target allocation in field 7;
its digest is field 8. The preceding signed grant request contains no derived ID,
so allocation does not depend on later membership/channel registrations. Updates
preserve the snapshot, including zero limits, without reviving the descriptor.
Activation root slots reserve two bounded sources; NV 33 still owns its distinct
record fee and single incarnation. No new tag/CF or active Lane format change.

B4 binds ten existing Target META record types to immutable quota owners without
adding a namespace. Records without an incarnation retain the first Target grant
allocation; queue/order/channel/Native records use their embedded incarnation or
full scope. A maximum of four exact META point reads rechecks record and attribution
bytes; dependencies do not add charges. C4 still proves actual before/absence and
atomic source/ledger changes, while physical resource authority remains separate.

B4 draft quota mutation field 4 carries an optional nonzero local Claim ordinal.
Local Claim/revoke preserves the source sequence and complete SourcePosition;
counter/total/aggregate revisions advance separately. Source allocation IDs retain
the source-only stamp, independent of local Claim history. Their three quota slot
bounds each reserve eleven additional bytes; source-only projections do not.
A Floor without an ordinal must strictly advance beyond a local stamp. This adds
no CF/tag or active Lane writer; C4 still supplies actual Claim authority and atomicity.

B4 reserves TargetQuotaClaimCharge at META tag 1b / NV 34. The 62-byte key binds
Source Shard, raw Owner epoch and Claim ID. It retains the original Target work,
immutable accounting owner/artifact, Owner/Store, original execution charge and
local creation stamp. Its STATE bytes are measured once; execution is separate.
Exact business Claim authority and atomic consumption remain required. Existing
Lane Claim NV 9 and active readers are unchanged; this introduces no new CF.

B4 reserves NV 35 for immutable Target result records in DEDUPE tags 06–09.
First Command evidence, query results and System results retain exact source and
accounting ownership. Physical POSITION audits use a Shard owner and reference
the original logical evidence; retries do not rewrite first outcomes. Successful
allocation control results may retain the complete OPEN origin. Record categories
remain distinct from System Mutation outboxes. Active Lane tags/readers and the
seven CFs are unchanged; C4 atomic application and protected recovery remain required.

B4 result recovery now folds existing DEDUPE 06–09 records and exact META 1a
owners without consuming persisted quota usage. First references and source/event
history are checked before primary and tenant subtotals are returned. Accounting
artifacts compare by canonical value across independent decodes. This adds no CF,
namespace or writer; actual complete Store traversal and full-ledger recovery
remain separate C4 obligations.


The implementation-first NDIP-3 batch adds an explicit format 2 open path and a
real Target Store transaction adapter. Default open and existing recovery paths
still require format 1; the selected reader rejects a foreign durable format before
rewriting Store identity. The seven CFs remain unchanged. Target NV supports the
registered types through 35 while the Lane NV reader retains its closed 1–11 set.
Business edits, local counters, Target totals, aggregate and source advancement
now share a guarded synchronous RocksDB batch. Complete result-range traversal
uses the same bounded Store view as its META dependencies. Production Worker
composition, complete business authority, all-ledger recovery and Target checkpoint
restore remain unfinished. Historical receipts do not certify these shared Store
changes; centralized validation follows implementation completion.


TargetMessageStore now derives Message, live Expiry and current ordinary/native/
ORDERED edits from complete before/after records, recomputes strict serviceable
heads and updates affected Target queues in the same prepared Store transaction.
Bounded range minima merge an exact overlay with at most touched-range-key count
plus one persisted entry. Incomplete reads yield instead of declaring an empty
prefix. Existing heads are validated before mutation, and full generated edits
reach accounting before commit. Production semantic/accounting authority and
Worker source/Claim/Admission/Producer composition remain integration work.


Actual Target record accounting now reads frozen owners and complete dependencies
from the same before/after Store views. Source accounting derives touched counter
changes and automatically reserves newly created counter/total/grant projection
storage through bookkeeping inventory in the same batch. Unchanged inventory
retains its bytes; zero counter/total records remain retained. Mirror resources
are equal to primary resources while cardinality is primary-only. Local Claim
accounting retains the source frontier and requires one exact charge transition.
The source smoke proves limited numeric Store behavior, not authentication,
complete business semantics, protected retirement, recovery or Worker activation.


#### NDIP-3 reversible Target Claim integration

INFLIGHT adds `04 01 + ClaimId[32]` with NV36/schema1 for an exact reversible
Target Claim. This uses the same seven CFs and the explicit Target Store reader.
Message.currentWork resolves this key directly; the retained Owner/Store and
local mutation bind the separate META1b charge. The Claim/revoke writer submits
Message, timeline/head, strict barrier and all fee projections in one batch.
Materialization/Admission, source-consumption/recovery and real Worker authority
remain pending. This entry grants no format migration or production activation.


#### NDIP-3 actual grant reads before commit

The logical gate now reads existing Shard/Target activation and root/first-owner
descriptors in the same bounded view as the actual business/accounting batch.
It checks primary totals, forbids self-authorizing grant edits and rejects stale
views after a grant update. Claim/revoke use the gate automatically. The CF and
wire layout is unchanged; source acceptance/rejection, physical admission and
production composition still require their complete runtime implementation.


#### NDIP-3 first grant source batch

TargetQuotaGrantStore now prepares actual signed first grant-control application:
META activation/optional first descriptor and DEDUPE SYSTEM/POSITION join derived
quota and source in one guarded batch. A verifier-owned rejection writes the
immutable rejection/audit only; external failures do not advance source. Existing
logical/physical keys are not overwritten. Controlled bootstrap, duplicate/replay
routing, production capacity and Worker authority remain separate incomplete
implementation obligations; the seven CFs and registered formats are unchanged.
