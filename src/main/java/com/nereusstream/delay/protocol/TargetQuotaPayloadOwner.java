package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.TargetMessageRecord;
import com.nereusstream.delay.runtime.TargetPayloadObligationSummaryBootstrap;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Objects;

/** One frozen payload accounting owner per Message identity, independent of generation and attempt budgets. */
public final class TargetQuotaPayloadOwner {
    public static final int VERSION = 1;
    public static final int SUMMARY_VERSION = 2;
    public static final int VALUE_TYPE = 32;
    /** Frozen schema-1 ceiling; expanding the reader must not reprice an existing allocation. */
    public static final int V1_MAX_CANONICAL_BYTES = 2
            + 43
            + 3
            + TargetQuotaIdentity.MAX_CANONICAL_BYTES
            + 34
            + 2
            + TargetQuotaAccounting.MAX_CANONICAL_BYTES
            + 34
            + 2
            + 11
            + 3 * 34
            + 5
            + TargetPayloadReference.MAX_CANONICAL_BYTES
            + 2
            + 11
            + 4
            + TargetQuotaMutation.MAX_CANONICAL_BYTES
            + 19
            + 2 * 35;
    public static final int MAX_CANONICAL_BYTES = V1_MAX_CANONICAL_BYTES
            + 11 + 5 + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 3 + 35
            + 12 + 5 + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-quota-payload-owner\0");

    public enum Kind {
        INLINE(1),
        OBJECT(2);
        private final int wire;

        Kind(final int wire) {
            this.wire = wire;
        }

        public int wireValue() {
            return wire;
        }
    }

    public enum Phase {
        RESERVED(1),
        ACTIVE(2),
        RETAINED(3),
        RELEASED(4);
        private final int wire;

        Phase(final int wire) {
            this.wire = wire;
        }

        public int wireValue() {
            return wire;
        }
    }

    /** The original initialization path is retained after every later Source delta. */
    public enum SummaryOrigin {
        FRESH(1),
        IMPORTED(2),
        BOOTSTRAP(3);

        private final int wire;

        SummaryOrigin(final int wire) {
            this.wire = wire;
        }

        public int wireValue() {
            return wire;
        }

        private static SummaryOrigin decode(final long wire) {
            return switch ((int) wire) {
                case 1 -> FRESH;
                case 2 -> IMPORTED;
                case 3 -> BOOTSTRAP;
                default -> throw new IllegalArgumentException("unknown payload summary initialization origin");
            };
        }
    }

    /**
     * Verifies the exact accepted source body and actual before/after business ledger, grants and protection.
     * For release this includes all remaining attempts/writers, latest reference sources, catalog ancestry,
     * pins, query/replay retention and actual provider/Store deletion. Remains valid through atomic commit.
     * A callback, source digest or Floor DTO is not itself the production implementation of this authority.
     */
    @FunctionalInterface
    public interface TransitionAuthority {
        void requireAuthorized(TargetQuotaPayloadOwner prior, TargetQuotaPayloadOwner next, RecoveryFloorRef floor);
    }

    private final DelayMessageId messageId;
    private final TargetQuotaIdentity owner;
    private final byte[] tenantScope;
    private final TargetQuotaAccounting accounting;
    private final byte[] bindingDigest;
    private final Kind kind;
    private final long length;
    private final byte[] payloadSha256;
    private final byte[] reservationId;
    private final byte[] objectStoreProfileHash;
    private final PayloadReference committed;
    private final Phase phase;
    private final long revision;
    private final TargetQuotaMutation mutation;
    private final byte[] recoveryLineage;
    private final byte[] releaseFloorDigest;
    private final int schemaVersion;
    private final long unresolvedCount;
    private final TargetQuotaMutation initializationSource;
    private final SummaryOrigin summaryOrigin;
    private final byte[] summaryOriginDigest;
    private final long summaryRevision;
    private final TargetQuotaMutation summaryMutation;
    private final byte[] digest;

    private TargetQuotaPayloadOwner(
            final DelayMessageId messageId,
            final TargetQuotaIdentity owner,
            final byte[] tenantScope,
            final TargetQuotaAccounting accounting,
            final byte[] bindingDigest,
            final Kind kind,
            final long length,
            final byte[] payloadSha256,
            final byte[] reservationId,
            final byte[] objectStoreProfileHash,
            final PayloadReference committed,
            final Phase phase,
            final long revision,
            final TargetQuotaMutation mutation,
            final byte[] recoveryLineage,
            final byte[] releaseFloorDigest) {
        this(messageId, owner, tenantScope, accounting, bindingDigest, kind, length, payloadSha256,
                reservationId, objectStoreProfileHash, committed, phase, revision, mutation, recoveryLineage,
                releaseFloorDigest, VERSION, -1, null, null, null, 0, null);
    }

    private TargetQuotaPayloadOwner(
            final DelayMessageId messageId,
            final TargetQuotaIdentity owner,
            final byte[] tenantScope,
            final TargetQuotaAccounting accounting,
            final byte[] bindingDigest,
            final Kind kind,
            final long length,
            final byte[] payloadSha256,
            final byte[] reservationId,
            final byte[] objectStoreProfileHash,
            final PayloadReference committed,
            final Phase phase,
            final long revision,
            final TargetQuotaMutation mutation,
            final byte[] recoveryLineage,
            final byte[] releaseFloorDigest,
            final int schemaVersion,
            final long unresolvedCount,
            final TargetQuotaMutation initializationSource,
            final SummaryOrigin summaryOrigin,
            final byte[] summaryOriginDigest,
            final long summaryRevision,
            final TargetQuotaMutation summaryMutation) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.tenantScope = TargetCompatibilityCodec.assigned(tenantScope, 32, "tenantRoutingScope");
        this.accounting = Objects.requireNonNull(accounting, "accounting");
        this.bindingDigest = TargetCompatibilityCodec.assigned(bindingDigest, 32, "initialBindingDigest");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.phase = Objects.requireNonNull(phase, "phase");
        this.mutation = Objects.requireNonNull(mutation, "mutation");
        if (mutation.isLocalMutation()
                && (!(mutation.reservationExpiry() || mutation.reservationClosure())
                        || kind != Kind.OBJECT
                        || phase != Phase.RETAINED
                        || committed != null)) {
            throw new IllegalArgumentException(
                    "local reservation terminalization can only retain an uncommitted object reservation");
        }
        this.recoveryLineage = TargetCompatibilityCodec.assigned(recoveryLineage, 16, "recoveryLineage");
        Bytes.requireLength(payloadSha256, 32, "payloadSha256");
        if (owner.kind() != TargetQuotaIdentity.Kind.TARGET
                || !owner.shard().equals(messageId.routingId().shardId())
                || !owner.shard().equals(mutation.source().shardId())
                || length < 0
                || revision == 0
                || Long.compareUnsigned(revision, mutation.sequence()) > 0) {
            throw new IllegalArgumentException("invalid payload owner identity/length/revision/source");
        }
        if (kind == Kind.INLINE) {
            if (length > TargetScheduleBody.MAX_INLINE_BYTES
                    || phase == Phase.RESERVED
                    || reservationId != null
                    || objectStoreProfileHash != null
                    || committed != null) {
                throw new IllegalArgumentException("invalid inline payload owner branch");
            }
        } else {
            TargetCompatibilityCodec.assigned(reservationId, 32, "reservationId");
            TargetCompatibilityCodec.assigned(objectStoreProfileHash, 32, "objectStoreProfileHash");
            if ((phase == Phase.RESERVED && committed != null) || (phase == Phase.ACTIVE && committed == null)) {
                throw new IllegalArgumentException("object payload owner lacks the correct commit phase");
            }
            if (committed != null) {
                TargetPayloadReference.requireBounded(committed);
                if (length != committed.length()
                        || !Arrays.equals(payloadSha256, committed.payloadSha256())
                        || !Arrays.equals(reservationId, committed.reservationId())
                        || !Arrays.equals(objectStoreProfileHash, committed.objectStoreProfileHash())) {
                    throw new IllegalArgumentException("committed object differs from its frozen reservation");
                }
            }
        }
        if ((phase == Phase.RELEASED) != (releaseFloorDigest != null)) {
            throw new IllegalArgumentException("released payload must retain its Floor digest");
        }
        this.releaseFloorDigest = releaseFloorDigest == null
                ? null
                : TargetCompatibilityCodec.assigned(releaseFloorDigest, 32, "releaseFloorDigest");
        this.length = length;
        this.payloadSha256 = Bytes.copy(payloadSha256);
        this.reservationId = reservationId == null ? null : Bytes.copy(reservationId);
        this.objectStoreProfileHash = objectStoreProfileHash == null ? null : Bytes.copy(objectStoreProfileHash);
        this.committed = committed;
        this.revision = revision;
        this.schemaVersion = schemaVersion;
        this.unresolvedCount = unresolvedCount;
        this.initializationSource = initializationSource;
        this.summaryOrigin = summaryOrigin;
        this.summaryRevision = summaryRevision;
        this.summaryMutation = summaryMutation;
        if (schemaVersion == VERSION) {
            if (unresolvedCount != -1 || initializationSource != null || summaryOrigin != null || summaryOriginDigest != null
                    || summaryRevision != 0 || summaryMutation != null) {
                throw new IllegalArgumentException("schema-1 payload owner cannot carry a qualified summary");
            }
            this.summaryOriginDigest = null;
        } else if (schemaVersion == SUMMARY_VERSION) {
            if (unresolvedCount < 0 || initializationSource == null || summaryOrigin == null || summaryMutation == null
                    || summaryRevision == 0 || Long.compareUnsigned(summaryRevision, summaryMutation.sequence()) > 0
                    || !messageId.routingId().shardId().equals(summaryMutation.source().shardId())
                    || ((phase == Phase.RETAINED || phase == Phase.RELEASED) && unresolvedCount != 0)) {
                throw new IllegalArgumentException("invalid qualified payload obligation summary");
            }
            initializationSource.requireSourceApplied();
            summaryMutation.requireSourceApplied();
            initializationSource.requireAtOrBefore(summaryMutation);
            this.summaryOriginDigest = TargetCompatibilityCodec.assigned(summaryOriginDigest, 32, "summaryOriginDigest");
        } else {
            throw new IllegalArgumentException("unknown payload owner version");
        }
        digest = Bytes.sha256(DIGEST_DOMAIN, digestFields());
    }

    /** Caller first authenticates the binding and resolves the Message/Command allocation against the Store. */
    public static TargetQuotaPayloadOwner scheduled(
            final TargetScheduleBinding binding,
            final byte[] tenantScope,
            final TargetQuotaAccounting accounting,
            final byte[] lineage,
            final TargetQuotaMutation stamp) {
        requireInitialBinding(binding, CommandType.SCHEDULE, stamp);
        final var intent = binding.intent();
        final var object =
                intent.hasInlinePayload() ? null : PayloadReference.fromDescriptor(intent.committedPayload());
        return new TargetQuotaPayloadOwner(
                binding.messageId(),
                identity(binding),
                tenantScope,
                accounting,
                binding.digest(),
                object == null ? Kind.INLINE : Kind.OBJECT,
                object == null ? intent.inlinePayload().length : object.length(),
                object == null ? Bytes.sha256(intent.inlinePayload()) : object.payloadSha256(),
                object == null ? null : object.reservationId(),
                object == null ? null : object.objectStoreProfileHash(),
                object,
                Phase.ACTIVE,
                1,
                stamp,
                lineage,
                null);
    }

    /** Explicit migration baseline allocation. Complete original binding/raw custody is retained by schema2 and Native66. */
    public static TargetQuotaPayloadOwner importedScheduled(
            final TargetScheduleBinding binding, final TargetMigrationPlan plan,
            final byte[] tenantScope, final TargetQuotaAccounting accounting, final byte[] lineage) {
        Objects.requireNonNull(binding); Objects.requireNonNull(plan);
        if (binding.schemaVersion() != 2 || binding.commandType() != CommandType.SCHEDULE
                || !Arrays.equals(binding.migrationPlanDigest(), plan.digest())
                || !Arrays.equals(tenantScope, plan.scope().tenantScope()) || !Arrays.equals(lineage, plan.newLineage())
                || binding.bindingSource().compareTo(plan.baseline().source()) > 0) {
            throw new IllegalArgumentException("imported payload allocation changed its complete original binding/plan/baseline");
        }
        return importedScheduledAt(binding, tenantScope, accounting, lineage, TargetMigrationRoot.baselineFor(plan));
    }

    /** A newly accepted legacy-prefix Schedule retains its actual Source S, never a provisional A or fake first. */
    public static TargetQuotaPayloadOwner importedPrefixScheduled(
            final TargetScheduleBinding binding, final TargetMigrationPlan plan,
            final byte[] tenantScope, final TargetQuotaAccounting accounting, final byte[] lineage) {
        Objects.requireNonNull(binding); Objects.requireNonNull(plan);
        if (binding.schemaVersion() != 3 || binding.commandType() != CommandType.SCHEDULE
                || !Arrays.equals(binding.migrationPlanDigest(), plan.digest())
                || !Arrays.equals(tenantScope, plan.scope().tenantScope()) || !Arrays.equals(lineage, plan.newLineage())) {
            throw new IllegalArgumentException("prefix payload allocation changed its complete original Source binding/plan");
        }
        binding.migrationSource().requireAfter(plan.baseline());
        return importedScheduledAt(binding, tenantScope, accounting, lineage, binding.migrationSource());
    }

    /** Complete original Prepare/receipt remains in migration custody; this is an explicit C allocation. */
    public static TargetQuotaPayloadOwner importedReserved(TargetScheduleBinding binding, TargetMigrationPlan plan,
            byte[] reservationId, byte[] tenantScope, TargetQuotaAccounting accounting, byte[] lineage) {
        return importedReservedAt(binding, plan, reservationId, tenantScope, accounting, lineage, false);
    }

    public static TargetQuotaPayloadOwner importedPrefixReserved(TargetScheduleBinding binding, TargetMigrationPlan plan,
            byte[] reservationId, byte[] tenantScope, TargetQuotaAccounting accounting, byte[] lineage) {
        return importedReservedAt(binding, plan, reservationId, tenantScope, accounting, lineage, true);
    }

    private static TargetQuotaPayloadOwner importedReservedAt(TargetScheduleBinding binding, TargetMigrationPlan plan,
            byte[] reservationId, byte[] tenantScope, TargetQuotaAccounting accounting, byte[] lineage, boolean prefix) {
        Objects.requireNonNull(binding); Objects.requireNonNull(plan);
        if (binding.schemaVersion() != (prefix ? 3 : 2) || binding.commandType() != CommandType.PREPARE_LARGE_SCHEDULE
                || !Arrays.equals(binding.migrationPlanDigest(), plan.digest())
                || !Arrays.equals(tenantScope, plan.scope().tenantScope()) || !Arrays.equals(lineage, plan.newLineage())
                || !prefix && binding.bindingSource().compareTo(plan.baseline().source()) > 0) {
            throw new IllegalArgumentException("imported reservation payload changed complete original Prepare/plan");
        }
        final var stamp = prefix ? binding.migrationSource() : TargetMigrationRoot.baselineFor(plan);
        if (prefix) stamp.requireAfter(plan.baseline());
        final var prepare = PrepareLargeScheduleBody.decode(binding.canonicalBody());
        return new TargetQuotaPayloadOwner(binding.messageId(), identity(binding), tenantScope, accounting, binding.digest(),
                Kind.OBJECT, prepare.expectedPayloadLength(), prepare.payloadSha256(), reservationId,
                prepare.objectStoreProfile().semanticHash(), null, Phase.RESERVED, 1, stamp, lineage, null);
    }

    public static TargetQuotaPayloadOwner importedCommitted(TargetScheduleBinding binding, TargetMigrationPlan plan,
            com.nereusstream.delay.runtime.PayloadReservation original, byte[] tenantScope,
            TargetQuotaAccounting accounting, byte[] lineage) {
        return importedCommittedAt(binding, plan, original, tenantScope, accounting, lineage, false);
    }

    public static TargetQuotaPayloadOwner importedPrefixCommitted(TargetScheduleBinding binding, TargetMigrationPlan plan,
            com.nereusstream.delay.runtime.PayloadReservation original, byte[] tenantScope,
            TargetQuotaAccounting accounting, byte[] lineage) {
        return importedCommittedAt(binding, plan, original, tenantScope, accounting, lineage, true);
    }

    private static TargetQuotaPayloadOwner importedCommittedAt(TargetScheduleBinding binding, TargetMigrationPlan plan,
            com.nereusstream.delay.runtime.PayloadReservation original, byte[] tenantScope,
            TargetQuotaAccounting accounting, byte[] lineage, boolean prefix) {
        Objects.requireNonNull(original);
        final var reserved = importedReservedAt(binding, plan, original.reservationId(), tenantScope, accounting, lineage, prefix);
        final var prepare = PrepareLargeScheduleBody.decode(binding.canonicalBody());
        if (original.status() != com.nereusstream.delay.runtime.PayloadReservationStatus.COMMITTED
                || !original.delayMessageId().equals(binding.messageId()) || original.committedPayload() == null
                || original.intent().expectedPayloadLength() != reserved.length
                || !Arrays.equals(original.intent().payloadSha256(), reserved.payloadSha256)
                || original.intent().deliverAtEpochMs() != prepare.intentWithoutPayload().deliverAtEpochMs()
                || original.intent().expireAtEpochMs() != prepare.intentWithoutPayload().expireAtEpochMs()
                || original.intent().orderingMode() != prepare.intentWithoutPayload().orderingMode()
                || original.intent().reservationTtlMs() != prepare.reservationTtlMs()
                || original.intent().payloadProofTrustSetVersion() != prepare.trustSet().version()) {
            throw new IllegalArgumentException("imported committed payload lost its complete original reservation/object");
        }
        final var object = TargetPayloadReference.requireBounded(original.committedPayload());
        if (!Arrays.equals(object.objectStoreProfileHash(), reserved.objectStoreProfileHash)
                || object.length() != reserved.length || !Arrays.equals(object.payloadSha256(), reserved.payloadSha256)) {
            throw new IllegalArgumentException("imported committed object differs from the original full Prepare declaration");
        }
        return new TargetQuotaPayloadOwner(reserved.messageId, reserved.owner, reserved.tenantScope, reserved.accounting,
                reserved.bindingDigest, Kind.OBJECT, reserved.length, reserved.payloadSha256, reserved.reservationId,
                reserved.objectStoreProfileHash, object, Phase.ACTIVE, 1, reserved.mutation, reserved.recoveryLineage, null);
    }

    private static TargetQuotaPayloadOwner importedScheduledAt(TargetScheduleBinding binding, byte[] tenantScope,
            TargetQuotaAccounting accounting, byte[] lineage, TargetQuotaMutation source) {
        final var intent = binding.intent();
        final var object = intent.hasInlinePayload() ? null : PayloadReference.fromDescriptor(intent.committedPayload());
        return new TargetQuotaPayloadOwner(binding.messageId(), identity(binding), tenantScope, accounting, binding.digest(),
                object == null ? Kind.INLINE : Kind.OBJECT,
                object == null ? intent.inlinePayload().length : object.length(),
                object == null ? Bytes.sha256(intent.inlinePayload()) : object.payloadSha256(),
                object == null ? null : object.reservationId(), object == null ? null : object.objectStoreProfileHash(),
                object, Phase.ACTIVE, 1, source, lineage, null);
    }

    /** Reservation ID must come from the authenticated Prepare/receipt authority, not an arbitrary caller value. */
    public static TargetQuotaPayloadOwner reserved(
            final TargetScheduleBinding binding,
            final byte[] reservationId,
            final byte[] tenantScope,
            final TargetQuotaAccounting accounting,
            final byte[] lineage,
            final TargetQuotaMutation stamp) {
        requireInitialBinding(binding, CommandType.PREPARE_LARGE_SCHEDULE, stamp);
        final var prepare = PrepareLargeScheduleBody.decode(binding.canonicalBody());
        return new TargetQuotaPayloadOwner(
                binding.messageId(),
                identity(binding),
                tenantScope,
                accounting,
                binding.digest(),
                Kind.OBJECT,
                prepare.expectedPayloadLength(),
                prepare.payloadSha256(),
                reservationId,
                prepare.objectStoreProfile().semanticHash(),
                null,
                Phase.RESERVED,
                1,
                stamp,
                lineage,
                null);
    }

    private static TargetQuotaIdentity identity(final TargetScheduleBinding binding) {
        return new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TARGET,
                binding.messageId().routingId().shardId(),
                binding.accountingIncarnation(),
                binding.target(),
                null);
    }

    private static void requireInitialBinding(
            final TargetScheduleBinding binding, final CommandType type, final TargetQuotaMutation stamp) {
        if (binding.imported() || binding.commandType() != type
                || !Arrays.equals(
                        binding.bindingSource().canonicalBytes(), stamp.source().canonicalBytes())) {
            throw new IllegalArgumentException("payload allocation does not match its initial binding source/type");
        }
    }

    public DelayMessageId messageId() {
        return messageId;
    }

    public TargetQuotaIdentity primaryIdentity() {
        return owner;
    }

    public TargetQuotaIdentity tenantIdentity() {
        return new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TENANT_TARGET,
                owner.shard(),
                owner.accountingIncarnation(),
                owner.target(),
                tenantScope);
    }

    public byte[] tenantScope() {
        return Bytes.copy(tenantScope);
    }

    public TargetQuotaAccounting accounting() {
        return accounting;
    }

    public byte[] initialBindingDigest() {
        return Bytes.copy(bindingDigest);
    }

    public Kind kind() {
        return kind;
    }

    public long length() {
        return length;
    }

    public byte[] payloadSha256() {
        return Bytes.copy(payloadSha256);
    }

    public byte[] reservationId() {
        return reservationId == null ? null : Bytes.copy(reservationId);
    }

    public byte[] objectStoreProfileHash() {
        return objectStoreProfileHash == null ? null : Bytes.copy(objectStoreProfileHash);
    }

    public PayloadReference committedPayload() {
        return committed;
    }

    public Phase phase() {
        return phase;
    }

    public int schemaVersion() {
        return schemaVersion;
    }

    public boolean summaryQualified() {
        return schemaVersion == SUMMARY_VERSION;
    }

    public void requireQualifiedSummary() {
        if (!summaryQualified()) {
            throw new IllegalStateException("payload owner requires its original complete obligation-summary initialization");
        }
    }

    public long unresolvedCount() {
        requireQualifiedSummary();
        return unresolvedCount;
    }

    public TargetQuotaMutation initializationSource() {
        requireQualifiedSummary();
        return initializationSource;
    }

    public SummaryOrigin summaryOrigin() {
        requireQualifiedSummary();
        return summaryOrigin;
    }

    public byte[] summaryOriginDigest() {
        requireQualifiedSummary();
        return Bytes.copy(summaryOriginDigest);
    }

    public long summaryRevision() {
        requireQualifiedSummary();
        return summaryRevision;
    }

    public TargetQuotaMutation summaryMutation() {
        requireQualifiedSummary();
        return summaryMutation;
    }

    public void requireSameSummaryOrigin(final TargetQuotaPayloadOwner other) {
        Objects.requireNonNull(other, "payloadOwner");
        if (schemaVersion != other.schemaVersion
                || !Objects.equals(initializationSource, other.initializationSource)
                || summaryOrigin != other.summaryOrigin
                || !Arrays.equals(summaryOriginDigest, other.summaryOriginDigest)) {
            throw new IllegalStateException("payload owner changed its original obligation-summary qualification");
        }
    }

    public ProtocolTuple requiredTuple() {
        return summaryQualified() ? ProtocolTuple.targetPayloadObligationSummary() : ProtocolTuple.currentSystemMutation();
    }

    public long revision() {
        return revision;
    }

    public TargetQuotaMutation mutation() {
        return mutation;
    }

    public byte[] recoveryLineage() {
        return Bytes.copy(recoveryLineage);
    }

    public byte[] releaseFloorDigest() {
        return releaseFloorDigest == null ? null : Bytes.copy(releaseFloorDigest);
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public CapacityVector payloadCharge() {
        return switch (phase) {
            case RESERVED -> TargetQuotaAccounting.reservedPayload(length);
            case ACTIVE -> TargetQuotaAccounting.activePayload(length);
            case RETAINED -> TargetQuotaAccounting.retainedPayload(length);
            case RELEASED -> CapacityVector.empty();
        };
    }

    /** The record has no embedded usage, so its encoded record fee is computed once without recursion. */
    public CapacityVector recordCharge() {
        return accounting.recordCharge(TargetQuotaAccounting.RecordClass.STATE, key().length, canonicalBytes().length);
    }

    /** Only the complete protected fold/fresh-birth verifier can mint this initialization qualification. */
    public TargetQuotaPayloadOwner initializeSummary(
            final TargetPayloadObligationSummaryBootstrap.Qualification qualification,
            final TargetQuotaMutation stamp,
            final TransitionAuthority authority) {
        if (summaryQualified()) {
            throw new IllegalStateException("payload obligation summary is already initialized");
        }
        Objects.requireNonNull(qualification, "summaryQualification").requireOwner(this);
        Objects.requireNonNull(stamp, "initializationSource").requireSourceApplied();
        if (!stamp.equals(qualification.initializedAt())
                || qualification.birth() != (qualification.origin() != SummaryOrigin.BOOTSTRAP)) {
            throw new IllegalStateException("payload summary changed its qualified initialization Source/origin");
        }
        if (qualification.birth()) {
            if (revision != 1 || !mutation.equals(stamp)) {
                throw new IllegalStateException("payload summary birth must share the original owner allocation batch");
            }
        } else {
            stamp.requireAfter(mutation);
        }
        if (qualification.origin() == SummaryOrigin.FRESH && qualification.unresolvedCount() != 0) {
            throw new IllegalStateException("fresh payload birth cannot contain a previous attempt obligation");
        }
        final var next = new TargetQuotaPayloadOwner(messageId, owner, tenantScope, accounting, bindingDigest,
                kind, length, payloadSha256, reservationId, objectStoreProfileHash, committed, phase,
                revision, mutation, recoveryLineage, releaseFloorDigest, SUMMARY_VERSION,
                qualification.unresolvedCount(), stamp, qualification.origin(), qualification.originDigest(), 1, stamp);
        Objects.requireNonNull(authority, "transitionAuthority").requireAuthorized(this, next, null);
        return next;
    }

    /** Source Native validation must independently prove a genuinely first APPLIED Admission in this batch. */
    public TargetQuotaPayloadOwner admitAttempt(
            final TargetQuotaAttemptBudget admitted,
            final TargetQuotaMutation stamp,
            final TransitionAuthority authority) {
        requireQualifiedSummary();
        requireAttemptIdentity(admitted);
        if (phase != Phase.ACTIVE || admitted.phase() != TargetQuotaAttemptBudget.Phase.ADMITTED
                || admitted.resolvedAt() != null || !admitted.mutation().equals(stamp)) {
            throw new IllegalStateException("payload summary increment requires the original first Admission budget");
        }
        final long nextCount = Math.addExact(unresolvedCount, 1);
        return transition(phase, committed, stamp, null, authority, nextCount, true);
    }

    /** One definitive Source resolution and optional RETAINED phase share one final owner successor. */
    public TargetQuotaPayloadOwner resolveAttempt(
            final TargetQuotaAttemptBudget before,
            final TargetQuotaAttemptBudget resolved,
            final boolean retain,
            final TargetQuotaMutation stamp,
            final TransitionAuthority authority) {
        requireQualifiedSummary();
        requireAttemptIdentity(before);
        requireAttemptIdentity(resolved);
        if (phase != Phase.ACTIVE || before.resolvedAt() != null
                || resolved.phase() != TargetQuotaAttemptBudget.Phase.RESOLVED_AWAITING_FLOOR
                || !stamp.equals(resolved.resolvedAt()) || !stamp.equals(resolved.mutation())
                || !Arrays.equals(before.locator().canonicalBytes(), resolved.locator().canonicalBytes())
                || !Arrays.equals(before.publishAttemptId(), resolved.publishAttemptId())
                || !Arrays.equals(before.admissionDigest(), resolved.admissionDigest())
                || before.executionBytes() != resolved.executionBytes()
                || !before.commitment().equals(resolved.commitment())
                || TargetQuotaMutation.increment(before.revision()) != resolved.revision()) {
            throw new IllegalStateException("payload summary decrement changed its original unresolved attempt identity");
        }
        stamp.requireAfter(before.mutation());
        if (unresolvedCount == 0) {
            throw new IllegalStateException("payload obligation summary underflow");
        }
        final long nextCount = Math.subtractExact(unresolvedCount, 1);
        if (retain && nextCount != 0) {
            throw new IllegalStateException("payload with unresolved attempts cannot become retained");
        }
        return transition(retain ? Phase.RETAINED : phase, committed, stamp, null, authority, nextCount, true);
    }

    private void requireAttemptIdentity(final TargetQuotaAttemptBudget budget) {
        Objects.requireNonNull(budget, "attemptBudget");
        if (!messageId.equals(budget.locator().messageId()) || !owner.equals(budget.primaryIdentity())
                || !Arrays.equals(bindingDigest, budget.locator().scheduleBindingDigest())
                || !Arrays.equals(tenantScope, budget.tenantScope())
                || !Arrays.equals(recoveryLineage, budget.recoveryLineage())
                || !accounting.equals(budget.accounting())) {
            throw new IllegalStateException("payload obligation summary crossed its frozen owner/binding/tenant/lineage");
        }
    }

    public TargetQuotaPayloadOwner commit(
            final PayloadReference payload, final TargetQuotaMutation stamp, final TransitionAuthority authority) {
        if (phase != Phase.RESERVED) {
            throw new IllegalStateException("payload owner is not reserved");
        }
        return transition(Phase.ACTIVE, Objects.requireNonNull(payload, "payload"), stamp, null, authority);
    }

    public TargetQuotaPayloadOwner retain(final TargetQuotaMutation stamp, final TransitionAuthority authority) {
        if (phase != Phase.ACTIVE && phase != Phase.RESERVED) {
            throw new IllegalStateException("payload owner cannot become retained from this phase");
        }
        return transition(Phase.RETAINED, committed, stamp, null, authority);
    }

    public TargetQuotaPayloadOwner replay(final TargetQuotaMutation stamp, final TransitionAuthority authority) {
        if (phase != Phase.RETAINED || (kind == Kind.OBJECT && committed == null)) {
            throw new IllegalStateException("only an existing committed/inline retained payload may be replayed");
        }
        return transition(Phase.ACTIVE, committed, stamp, null, authority);
    }

    public TargetQuotaPayloadOwner release(
            final RecoveryFloorRef floor, final TargetQuotaMutation stamp, final TransitionAuthority authority) {
        if (phase != Phase.RETAINED) {
            throw new IllegalStateException("only retained payload may be released");
        }
        Objects.requireNonNull(floor, "floor");
        stamp.requireAfter(mutation);
        TargetSourcePosition.requireBounded(floor.appliedSourcePosition());
        mutation.requireCoveredByFloor(floor);
        if (summaryQualified()) {
            summaryMutation.requireCoveredByFloor(floor);
        }
        if (!Arrays.equals(recoveryLineage, floor.recoveryLineageId())
                || floor.appliedSourcePosition().compareTo(stamp.source()) >= 0
                || Long.compareUnsigned(floor.includedMutationSequence(), stamp.sequence()) >= 0) {
            throw new IllegalStateException("payload release Floor does not cover its latest owner mutation");
        }
        return transition(Phase.RELEASED, committed, stamp, floor, authority);
    }

    private TargetQuotaPayloadOwner transition(
            final Phase nextPhase,
            final PayloadReference payload,
            final TargetQuotaMutation stamp,
            final RecoveryFloorRef floor,
            final TransitionAuthority authority) {
        return transition(nextPhase, payload, stamp, floor, authority, unresolvedCount, false);
    }

    private TargetQuotaPayloadOwner transition(
            final Phase nextPhase,
            final PayloadReference payload,
            final TargetQuotaMutation stamp,
            final RecoveryFloorRef floor,
            final TransitionAuthority authority,
            final long nextUnresolvedCount,
            final boolean updateSummary) {
        if (stamp.reservationExpiry() || stamp.reservationClosure()) {
            if (phase != Phase.RESERVED || nextPhase != Phase.RETAINED || payload != null || floor != null) {
                throw new IllegalStateException(
                        "local reservation terminalization cannot replay, commit or release payload");
            }
            stamp.requireStoreSuccessorOf(mutation);
            if (summaryQualified()) {
                stamp.requireStoreSuccessorOf(summaryMutation);
            }
        } else {
            stamp.requireAfter(mutation);
            if (summaryQualified()) {
                stamp.requireAfter(summaryMutation);
            }
        }
        if (updateSummary) {
            stamp.requireSourceApplied();
        }
        final boolean businessChanged = nextPhase != phase || !Objects.equals(payload, committed);
        final var next = new TargetQuotaPayloadOwner(
                messageId,
                owner,
                tenantScope,
                accounting,
                bindingDigest,
                kind,
                length,
                payloadSha256,
                reservationId,
                objectStoreProfileHash,
                payload,
                nextPhase,
                businessChanged ? TargetQuotaMutation.increment(revision) : revision,
                businessChanged ? stamp : mutation,
                recoveryLineage,
                floor == null ? null : floor.floorDigest(),
                schemaVersion, nextUnresolvedCount, initializationSource, summaryOrigin, summaryOriginDigest,
                updateSummary ? TargetQuotaMutation.increment(summaryRevision) : summaryRevision,
                updateSummary ? stamp : summaryMutation);
        Objects.requireNonNull(authority, "transitionAuthority").requireAuthorized(this, next, floor);
        return next;
    }

    /** Read-only correlation of persisted Source state. This neither authorizes nor executes a release. */
    public void requireReleasedFrom(final TargetQuotaPayloadOwner expected, final RecoveryFloorRef floor) {
        Objects.requireNonNull(expected, "expected");Objects.requireNonNull(floor, "floor");
        if(expected.phase!=Phase.RETAINED||phase!=Phase.RELEASED)throw new IllegalStateException("payload release phase mismatch");
        mutation.requireSourceApplied();mutation.requireAfter(expected.mutation);expected.mutation.requireCoveredByFloor(floor);
        if(!Arrays.equals(expected.recoveryLineage,floor.recoveryLineageId())
                ||floor.appliedSourcePosition().compareTo(mutation.source())>=0
                ||Long.compareUnsigned(floor.includedMutationSequence(),mutation.sequence())>=0)
            throw new IllegalStateException("payload release did not follow its complete Floor");
        final var projected=new TargetQuotaPayloadOwner(expected.messageId,expected.owner,expected.tenantScope,
                expected.accounting,expected.bindingDigest,expected.kind,expected.length,expected.payloadSha256,
                expected.reservationId,expected.objectStoreProfileHash,expected.committed,Phase.RELEASED,
                TargetQuotaMutation.increment(expected.revision),mutation,expected.recoveryLineage,floor.floorDigest());
        if(!Arrays.equals(businessCanonicalBytes(),projected.canonicalBytes()))throw new IllegalStateException("released payload changed frozen owner/value");
        if(expected.summaryQualified()){
            requireSameSummaryOrigin(expected);
            if(unresolvedCount!=0||summaryRevision!=expected.summaryRevision||!summaryMutation.equals(expected.summaryMutation))
                throw new IllegalStateException("payload release changed its original obligation summary");
            expected.summaryMutation.requireCoveredByFloor(floor);
        }
    }

    private byte[] businessCanonicalBytes() {
        if (!summaryQualified()) {
            return canonicalBytes();
        }
        return new TargetQuotaPayloadOwner(messageId, owner, tenantScope, accounting, bindingDigest, kind,
                length, payloadSha256, reservationId, objectStoreProfileHash, committed, phase, revision,
                mutation, recoveryLineage, releaseFloorDigest).canonicalBytes();
    }

    /** Rebinds decoded owner data to the full retained first Schedule/Prepare, not just its digest. */
    public void requireInitialBinding(final TargetScheduleBinding binding) {
        if (!messageId.equals(binding.messageId())
                || !owner.equals(identity(binding))
                || !Arrays.equals(bindingDigest, binding.digest())
                || binding.bindingSource().compareTo(mutation.source()) > 0
                || (binding.bindingSource().compareTo(mutation.source()) == 0
                        && !Arrays.equals(
                                binding.bindingSource().canonicalBytes(),
                                mutation.source().canonicalBytes()))) {
            throw new IllegalStateException("payload owner differs from its initial full binding");
        }
        if (binding.commandType() == CommandType.PREPARE_LARGE_SCHEDULE) {
            final var prepare = PrepareLargeScheduleBody.decode(binding.canonicalBody());
            if (kind != Kind.OBJECT
                    || length != prepare.expectedPayloadLength()
                    || !Arrays.equals(payloadSha256, prepare.payloadSha256())
                    || !Arrays.equals(
                            objectStoreProfileHash, prepare.objectStoreProfile().semanticHash())) {
                throw new IllegalStateException("payload owner differs from its original reservation intent");
            }
        } else {
            final var intent = binding.intent();
            if (intent.hasInlinePayload()
                    ? kind != Kind.INLINE
                            || length != intent.inlinePayload().length
                            || !Arrays.equals(payloadSha256, Bytes.sha256(intent.inlinePayload()))
                    : kind != Kind.OBJECT
                            || !Objects.equals(committed, PayloadReference.fromDescriptor(intent.committedPayload()))) {
                throw new IllegalStateException("payload owner differs from its original Schedule payload");
            }
        }
    }

    /** Checks payload identity; the committer separately validates the current generation, state and grants. */
    public void requireMessagePayload(final TargetMessageRecord message) {
        if (phase == Phase.RESERVED
                || phase == Phase.RELEASED
                || !messageId.equals(message.locator().messageId())
                || !owner.target().equals(message.locator().target())
                || length != message.payloadLength()
                || (kind == Kind.INLINE
                        ? message.inlinePayload() == null
                                || !Arrays.equals(payloadSha256, Bytes.sha256(message.inlinePayload()))
                        : committed == null || !Objects.equals(committed, message.payloadReference()))) {
            throw new IllegalStateException("Message does not refer to the frozen payload owner");
        }
    }

    public byte[] key() {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, TargetKeyCodec.KEY_FORMAT}, messageId.bytes());
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, schemaVersion);
            CanonicalProtobuf.bytes(out, 2, messageId.bytes());
            CanonicalProtobuf.bytes(out, 3, owner.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, tenantScope);
            CanonicalProtobuf.bytes(out, 5, accounting.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, bindingDigest);
            CanonicalProtobuf.uint32(out, 7, kind.wireValue());
            CanonicalProtobuf.uint64(out, 8, length);
            CanonicalProtobuf.bytes(out, 9, payloadSha256);
            if (reservationId != null) {
                CanonicalProtobuf.bytes(out, 10, reservationId);
            }
            if (objectStoreProfileHash != null) {
                CanonicalProtobuf.bytes(out, 11, objectStoreProfileHash);
            }
            if (committed != null) {
                CanonicalProtobuf.bytes(out, 12, committed.encode());
            }
            CanonicalProtobuf.uint32(out, 13, phase.wireValue());
            CanonicalProtobuf.uint64Bits(out, 14, revision);
            CanonicalProtobuf.bytes(out, 15, mutation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 16, recoveryLineage);
            if (releaseFloorDigest != null) {
                CanonicalProtobuf.bytes(out, 17, releaseFloorDigest);
            }
        });
    }

    private void writeSummary(final ByteArrayOutputStream out) {
        if (summaryQualified()) {
            CanonicalProtobuf.uint64(out, 19, unresolvedCount);
            CanonicalProtobuf.bytes(out, 20, initializationSource.canonicalBytes());
            CanonicalProtobuf.uint32(out, 21, summaryOrigin.wireValue());
            CanonicalProtobuf.bytes(out, 22, summaryOriginDigest);
            CanonicalProtobuf.uint64Bits(out, 23, summaryRevision);
            CanonicalProtobuf.bytes(out, 24, summaryMutation.canonicalBytes());
        }
    }

    private byte[] digestFields() {
        if (!summaryQualified()) {
            return fields();
        }
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            writeSummary(out);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 18, digest);
            writeSummary(out);
        });
    }

    public static TargetQuotaPayloadOwner decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 24, false, "Target payload owner");
        final byte[][] bytes = new byte[25][];
        final long[] numbers = new long[25];
        final boolean[] present = new boolean[25];
        for (var field : fields) {
            final int n = field.number();
            if (n < 1 || n > 24 || present[n]) {
                throw new IllegalArgumentException("invalid payload owner fields");
            }
            present[n] = true;
            if (n == 1 || n == 7 || n == 13 || n == 21) {
                numbers[n] = QueryCodecSupport.uint32(field, n);
            } else if (n == 8 || n == 19) {
                numbers[n] = QueryCodecSupport.uint(field, n);
            } else if (n == 14 || n == 23) {
                numbers[n] = QueryCodecSupport.uint64Bits(field, n);
            } else {
                bytes[n] = QueryCodecSupport.bytes(field, n);
            }
        }
        for (int n : new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14, 15, 16, 18}) {
            if (!present[n]) {
                throw new IllegalArgumentException("missing payload owner field");
            }
        }
        final boolean qualified = numbers[1] == SUMMARY_VERSION;
        if (numbers[1] != VERSION && !qualified) {
            throw new IllegalArgumentException("unknown payload owner version");
        }
        if (!qualified && encoded.length > V1_MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("schema-1 payload owner exceeds its frozen byte bound");
        }
        for (int n : new int[] {19, 20, 21, 22, 23, 24}) {
            if (present[n] != qualified) {
                throw new IllegalArgumentException("payload owner version and qualified summary fields disagree");
            }
        }
        final Kind decodedKind =
                switch ((int) numbers[7]) {
                    case 1 -> Kind.INLINE;
                    case 2 -> Kind.OBJECT;
                    default -> throw new IllegalArgumentException("unknown payload owner kind");
                };
        final Phase decodedPhase =
                switch ((int) numbers[13]) {
                    case 1 -> Phase.RESERVED;
                    case 2 -> Phase.ACTIVE;
                    case 3 -> Phase.RETAINED;
                    case 4 -> Phase.RELEASED;
                    default -> throw new IllegalArgumentException("unknown payload owner phase");
                };
        final var value = new TargetQuotaPayloadOwner(
                new DelayMessageId(bytes[2]),
                TargetQuotaIdentity.decode(bytes[3]),
                bytes[4],
                TargetQuotaAccounting.decode(bytes[5]),
                bytes[6],
                decodedKind,
                numbers[8],
                bytes[9],
                bytes[10],
                bytes[11],
                bytes[12] == null ? null : TargetPayloadReference.decode(bytes[12]),
                decodedPhase,
                numbers[14],
                TargetQuotaMutation.decode(bytes[15]),
                bytes[16],
                bytes[17],
                (int) numbers[1],
                qualified ? numbers[19] : -1,
                qualified ? TargetQuotaMutation.decode(bytes[20]) : null,
                qualified ? SummaryOrigin.decode(numbers[21]) : null,
                bytes[22],
                qualified ? numbers[23] : 0,
                qualified ? TargetQuotaMutation.decode(bytes[24]) : null);
        if (!Arrays.equals(value.digest, bytes[18])) {
            throw new IllegalArgumentException("payload owner digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target payload owner");
        return value;
    }

    public static TargetQuotaPayloadOwner decodeForStore(
            final byte[] key, final byte[] encoded, final ShardId shard, final byte[] authenticatedTenantRoutingScope) {
        final var value = decode(encoded);
        if (!value.owner.shard().equals(shard)
                || !Arrays.equals(value.tenantScope, authenticatedTenantRoutingScope)
                || !Arrays.equals(key, value.key())) {
            throw new IllegalArgumentException("payload owner key/Shard/tenant mismatch");
        }
        return value;
    }
}
