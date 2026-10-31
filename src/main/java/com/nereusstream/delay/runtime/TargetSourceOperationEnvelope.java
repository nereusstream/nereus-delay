package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetChannelControlBody;
import com.nereusstream.delay.protocol.TargetChannelControlRecord;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetChannelSlot;
import com.nereusstream.delay.protocol.TargetCloseBody;
import com.nereusstream.delay.protocol.TargetCloseCursorRecord;
import com.nereusstream.delay.protocol.TargetCloseRecord;
import com.nereusstream.delay.protocol.TargetControlScope;
import com.nereusstream.delay.protocol.TargetDispatchCompatibility;
import com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord;
import com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody;
import com.nereusstream.delay.protocol.TargetEvidenceRetirementContext;
import com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord;
import com.nereusstream.delay.protocol.TargetExpireGenerationBody;
import com.nereusstream.delay.protocol.TargetExternalEvidenceOwner;
import com.nereusstream.delay.protocol.TargetExternalEvidenceReference;
import com.nereusstream.delay.protocol.TargetInitialControlSnapshot;
import com.nereusstream.delay.protocol.TargetKafkaReceiptMapping;
import com.nereusstream.delay.protocol.TargetKafkaReceiptSlot;
import com.nereusstream.delay.protocol.TargetMembershipClosureRecord;
import com.nereusstream.delay.protocol.TargetMembershipControlBody;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetMembershipPolicy;
import com.nereusstream.delay.protocol.TargetMigrationActivateRequest;
import com.nereusstream.delay.protocol.TargetMigrationActivationAttempt;
import com.nereusstream.delay.protocol.TargetMigrationArchiveEntry;
import com.nereusstream.delay.protocol.TargetMigrationArchiveRequest;
import com.nereusstream.delay.protocol.TargetMigrationArchiveRow;
import com.nereusstream.delay.protocol.TargetMigrationArchiveSourceFrame;
import com.nereusstream.delay.protocol.TargetMigrationArchiveState;
import com.nereusstream.delay.protocol.TargetMigrationControlBody;
import com.nereusstream.delay.protocol.TargetMigrationCustodyGuard;
import com.nereusstream.delay.protocol.TargetMigrationOrigin;
import com.nereusstream.delay.protocol.TargetMigrationPrefixRecord;
import com.nereusstream.delay.protocol.TargetMigrationRetirementConfirmRequest;
import com.nereusstream.delay.protocol.TargetMigrationRetirementIntentRequest;
import com.nereusstream.delay.protocol.TargetMigrationRetirementRecord;
import com.nereusstream.delay.protocol.TargetMigrationRoot;
import com.nereusstream.delay.protocol.TargetMigrationSettlementRequest;
import com.nereusstream.delay.protocol.TargetNativePolicyControlBody;
import com.nereusstream.delay.protocol.TargetNativePolicyControlRecord;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetNativePolicySnapshot;
import com.nereusstream.delay.protocol.TargetParentControlCredit;
import com.nereusstream.delay.protocol.TargetParentControlCreditBody;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord;
import com.nereusstream.delay.protocol.TargetProtocolActivationBody;
import com.nereusstream.delay.protocol.TargetProtocolActivationDescriptor;
import com.nereusstream.delay.protocol.TargetProtocolActivationRecord;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaClaimCharge;
import com.nereusstream.delay.protocol.TargetQuotaCounter;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaTotal;
import com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner;
import com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference;
import com.nereusstream.delay.protocol.TargetResourceReclaimBody;
import com.nereusstream.delay.protocol.TargetRetirementHistoryTipRecord;
import com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.protocol.TargetScopeControlObligation;
import com.nereusstream.delay.protocol.TargetScopeControlObligationBody;
import com.nereusstream.delay.protocol.TargetSloCustodyRecord;
import com.nereusstream.delay.protocol.TargetSloObservationControlBody;
import com.nereusstream.delay.protocol.TargetSloObservationEventBody;
import com.nereusstream.delay.protocol.TargetSloObservationEventRecord;
import com.nereusstream.delay.protocol.TargetSloObservationGeneration;
import com.nereusstream.delay.protocol.TargetSloObservationPolicy;
import com.nereusstream.delay.protocol.TargetSloObservationSample;
import com.nereusstream.delay.protocol.TargetSloObservationState;
import com.nereusstream.delay.protocol.TargetSloWriterControlBody;
import com.nereusstream.delay.protocol.TargetSloWriterEpoch;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.protocol.TargetTimeFenceBody;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;

/** Closed Target Source costs. These computed numbers grant neither history nor physical allocation authority. */
public final class TargetSourceOperationEnvelope {
    private TargetSourceOperationEnvelope() {}

    /** Computed only here; callers can combine calculations but cannot assert a cheaper cost. */
    public static final class Bounds {
        private final long readRecords,
                readBytes,
                writeRecords,
                writeBytes,
                allocatedBytes,
                retainedBytes,
                maximumFrameBytes;
        private final int maximumCounters, maximumTargets, maximumDomains;
        private final boolean writerOriginRequired;
        private final Capabilities capabilities;
        private final com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.SourceFrameContract
                frameContract;

        private Bounds(
                long readRecords,
                long readBytes,
                long writeRecords,
                long writeBytes,
                long allocatedBytes,
                long retainedBytes,
                int counters,
                int targets,
                int domains,
                boolean writerOriginRequired,
                long maximumFrameBytes,
                Capabilities capabilities,
                com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.SourceFrameContract frameContract) {
            this.readRecords = readRecords;
            this.readBytes = readBytes;
            this.writeRecords = writeRecords;
            this.writeBytes = writeBytes;
            this.allocatedBytes = allocatedBytes;
            this.retainedBytes = retainedBytes;
            maximumCounters = counters;
            maximumTargets = targets;
            maximumDomains = domains;
            this.writerOriginRequired = writerOriginRequired;
            this.maximumFrameBytes = maximumFrameBytes;
            this.capabilities = capabilities.copy();
            this.frameContract = frameContract;
        }

        public long readRecords() {
            return readRecords;
        }

        public long readBytes() {
            requireNativeFrameQualification();
            return readBytes;
        }

        public long writeRecords() {
            return writeRecords;
        }

        public long writeBytes() {
            requireNativeFrameQualification();
            return writeBytes;
        }
        /** Cumulative explicit byte-array material/copy work, not simultaneous heap or a nanosecond estimate. */
        public long allocatedBytes() {
            requireFrameQualification();
            return allocatedBytes;
        }
        /** Simultaneous explicit Source-plan material; original provider Held material is a separate owner. */
        public long retainedBytes() {
            requireFrameQualification();
            return retainedBytes;
        }

        public long maximumFrameBytes() {
            requireFrameQualification();
            return maximumFrameBytes;
        }
        /** The fixed portion only; it is not the complete Source input/decode reservation. */
        public long fixedRetainedBytes() {
            return retainedBytes;
        }

        public long fixedMaximumFrameBytes() {
            return maximumFrameBytes;
        }

        public long fixedReadBytes() {
            return readBytes;
        }

        public long fixedWriteBytes() {
            return writeBytes;
        }

        public boolean requiresSourceFrameQualification() {
            return frameContract == null;
        }

        private void requireFrameQualification() {
            if (requiresSourceFrameQualification()) {
                throw unavailable(("Source service still needs its original frozen prefix and continuous writer "
                        + "frame qualification"));
            }
            frameContract.requireLocalCurrent();
        }

        private void requireNativeFrameQualification() {
            if (capabilities.generation >= 21) {
                requireFrameQualification();
            }
        }

        public int maximumCounters() {
            return maximumCounters;
        }

        public int maximumTargets() {
            return maximumTargets;
        }

        public int maximumDomains() {
            return maximumDomains;
        }

        public boolean writerOriginRequired() {
            return writerOriginRequired;
        }

        public boolean includes(Bounds computed) {
            Objects.requireNonNull(computed);
            if (frameContract != null) {
                requireFrameQualification();
            }
            if (computed.frameContract != null) {
                computed.requireFrameQualification();
            }
            return !requiresSourceFrameQualification()
                    && !computed.requiresSourceFrameQualification()
                    && frameContract == computed.frameContract
                    && readRecords >= computed.readRecords
                    && readBytes >= computed.readBytes
                    && writeRecords >= computed.writeRecords
                    && writeBytes >= computed.writeBytes
                    && allocatedBytes >= computed.allocatedBytes
                    && retainedBytes >= computed.retainedBytes
                    && maximumCounters >= computed.maximumCounters
                    && maximumTargets >= computed.maximumTargets
                    && maximumDomains >= computed.maximumDomains
                    && maximumFrameBytes >= computed.maximumFrameBytes
                    && (!computed.writerOriginRequired || writerOriginRequired);
        }
        /** Monotonic reservation union, including an uncommitted prospective activation. */
        public Bounds covering(Bounds computed) {
            Objects.requireNonNull(computed);
            if (frameContract != null) {
                requireFrameQualification();
            }
            if (computed.frameContract != null) {
                computed.requireFrameQualification();
            }
            if (frameContract != null && computed.frameContract != null && frameContract != computed.frameContract) {
                throw unavailable("Source reservation cannot combine different original frame qualifications");
            }
            final var combined = capabilities.covering(computed.capabilities);
            final var original = computed.frameContract != null ? computed.frameContract : frameContract;
            if (original != null) {
                combined.sourceFrameBytes = original.maximumFrameBytes();
            }
            final var resolved = original != null ? closedFamilies(combined, original) : null;
            return new Bounds(
                            Math.max(readRecords, computed.readRecords),
                            Math.max(readBytes, computed.readBytes),
                            Math.max(writeRecords, computed.writeRecords),
                            Math.max(writeBytes, computed.writeBytes),
                            Math.max(allocatedBytes, computed.allocatedBytes),
                            Math.max(retainedBytes, computed.retainedBytes),
                            Math.max(maximumCounters, computed.maximumCounters),
                            Math.max(maximumTargets, computed.maximumTargets),
                            Math.max(maximumDomains, computed.maximumDomains),
                            writerOriginRequired || computed.writerOriginRequired,
                            Math.max(maximumFrameBytes, computed.maximumFrameBytes),
                            combined,
                            original)
                    .coverResolved(resolved);
        }
        /** Bind only the original provider's opaque retained-prefix and continuous-writer qualification. */
        public Bounds withSourceFrames(
                com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.SourceFrameContract genuineM) {
            Objects.requireNonNull(genuineM).requireLocalCurrent();
            if (frameContract != null && frameContract != genuineM) {
                throw unavailable("Source reservation changed its original frame qualification");
            }
            final var qualified = capabilities.copy();
            qualified.sourceFrameBytes = genuineM.maximumFrameBytes();
            return closedFamilies(qualified, genuineM);
        }
        /** The original owner replaces a drained generation with a freshly captured opaque qualification. */
        public Bounds forSourceGeneration(
                com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.SourceFrameContract genuineM) {
            Objects.requireNonNull(genuineM).requireLocalCurrent();
            final var qualified = capabilities.copy();
            qualified.sourceFrameBytes = genuineM.maximumFrameBytes();
            return closedFamilies(qualified, genuineM).coverResolved(this);
        }

        private Bounds coverResolved(Bounds resolved) {
            if (resolved == null) {
                return this;
            }
            return new Bounds(
                    Math.max(readRecords, resolved.readRecords),
                    Math.max(readBytes, resolved.readBytes),
                    Math.max(writeRecords, resolved.writeRecords),
                    Math.max(writeBytes, resolved.writeBytes),
                    Math.max(allocatedBytes, resolved.allocatedBytes),
                    Math.max(retainedBytes, resolved.retainedBytes),
                    Math.max(maximumCounters, resolved.maximumCounters),
                    Math.max(maximumTargets, resolved.maximumTargets),
                    Math.max(maximumDomains, resolved.maximumDomains),
                    writerOriginRequired || resolved.writerOriginRequired,
                    Math.max(maximumFrameBytes, resolved.maximumFrameBytes),
                    capabilities,
                    frameContract);
        }
    }

    /* Key reservations come from TargetKeyCodec and the fixed-width scope/identity codecs. No read/write limit
    is used to derive a legal row or operation size. FIRST includes both logical result namespaces; POSITION
    retains the complete physical Source key. Sample primary/reference are distinct stored projections. */
    enum Row {
        INITIAL(1, KeyCodec.metaFixed(10).length, TargetInitialControlSnapshot.MAX_CANONICAL_BYTES),
        FENCE(1, KeyCodec.metaFixed(4).length, com.nereusstream.delay.store.IngressFenceState.MAX_CANONICAL_BYTES),
        CURSORS(1, KeyCodec.metaFixed(6).length, com.nereusstream.delay.store.StoreRuntimeMetadata.MAX_CANONICAL_BYTES),
        QUEUE(12, 2 + TargetPartitionId.LENGTH, TargetQueueState.MAX_CANONICAL_BYTES),
        IDENTITY(13, 2 + TargetPartitionId.LENGTH, CanonicalTargetPartition.MAX_CANONICAL_BYTES),
        WORK(
                14,
                TargetKeyCodec.CANDIDATE_PREFIX_BYTES + 8 + 21 + DelayMessageId.LENGTH + 4,
                TargetTimelineWorkRef.MAX_CANONICAL_BYTES),
        ORDER_HEAD(
                14,
                TargetKeyCodec.CANDIDATE_PREFIX_BYTES + 8 + 21 + DelayMessageId.LENGTH + 4,
                TargetTimelineWorkRef.MAX_CANONICAL_BYTES),
        MESSAGE(15, 2 + DelayMessageId.LENGTH, TargetMessageRecord.MAX_CANONICAL_BYTES),
        EXPIRY(16, 2 + 8 + TargetPartitionId.LENGTH + DelayMessageId.LENGTH + 4, TargetExpiryRef.MAX_CANONICAL_BYTES),
        ORDER(17, 2 + TargetPartitionId.LENGTH + 32, TargetOrderState.MAX_CANONICAL_BYTES),
        DISPATCH(18, 34, TargetDispatchCompatibility.MAX_CANONICAL_BYTES),
        CONTROL(19, 34, TargetControlScope.MAX_CANONICAL_BYTES),
        CHANNEL(20, 34, TargetChannelIdentity.MAX_CANONICAL_BYTES),
        BINDING(21, 34, TargetScheduleBinding.MAX_CANONICAL_BYTES),
        MEMBER(22, 34, TargetMembershipGrant.MAX_CANONICAL_BYTES),
        POLICY(23, 34, TargetMembershipPolicy.MAX_CANONICAL_BYTES),
        NATIVE_SCOPE(24, 34, TargetNativePolicyScope.MAX_CANONICAL_BYTES),
        NATIVE_SNAPSHOT(25, 34, TargetNativePolicySnapshot.MAX_CANONICAL_BYTES),
        COUNTER(26, 103, TargetQuotaCounter.MAX_CANONICAL_BYTES),
        AGGREGATE(27, 22, TargetQuotaAggregate.MAX_CANONICAL_BYTES),
        ATTEMPT(28, 34, TargetQuotaAttemptBudget.MAX_CANONICAL_BYTES),
        TOTAL(29, 87, TargetQuotaTotal.MAX_CANONICAL_BYTES),
        GRANT(30, 87, TargetQuotaGrantActivation.MAX_CANONICAL_BYTES),
        BOOKKEEPING(31, 22, TargetQuotaBookkeeping.MAX_CANONICAL_BYTES),
        PAYLOAD_V1(32, 2 + DelayMessageId.LENGTH, TargetQuotaPayloadOwner.V1_MAX_CANONICAL_BYTES),
        PAYLOAD(32, 2 + DelayMessageId.LENGTH, TargetQuotaPayloadOwner.MAX_CANONICAL_BYTES),
        DESCRIPTOR(33, 103, TargetQuotaIncarnation.MAX_CANONICAL_BYTES),
        CLAIM_CHARGE(34, 34, TargetQuotaClaimCharge.MAX_CANONICAL_BYTES),
        FIRST(35, 2 + Math.max(32, CommandId.LENGTH), TargetResultRecord.MAX_CANONICAL_BYTES),
        POSITION(35, 2 + TargetSourcePosition.MAX_CANONICAL_BYTES, TargetResultRecord.MAX_CANONICAL_BYTES),
        CLAIM(36, 34, TargetClaimRecord.MAX_CANONICAL_BYTES),
        TERMINAL(37, 2 + DelayMessageId.LENGTH + 4, TargetTerminalGenerationRecord.MAX_CANONICAL_BYTES),
        RESERVATION(
                38, 2 + TargetPartitionId.LENGTH + DelayMessageId.LENGTH, TargetReservationRecord.MAX_CANONICAL_BYTES),
        CLOSE(39, 2 + TargetPartitionId.LENGTH, TargetCloseRecord.MAX_CANONICAL_BYTES),
        CLOSE_CURSOR(40, 2 + TargetPartitionId.LENGTH, TargetCloseCursorRecord.MAX_CANONICAL_BYTES),
        MEMBER_CLOSE(41, 34, TargetMembershipClosureRecord.MAX_CANONICAL_BYTES),
        NATIVE_CONTROL(42, 66, TargetNativePolicyControlRecord.MAX_CANONICAL_BYTES),
        CHANNEL_CONTROL(43, 34, TargetChannelControlRecord.MAX_CANONICAL_BYTES),
        CHANNEL_SLOT(44, 34, TargetChannelSlot.MAX_CANONICAL_BYTES),
        KAFKA_MAPPING(45, 34, TargetKafkaReceiptMapping.MAX_CANONICAL_BYTES),
        KAFKA_SLOT(46, 34, TargetKafkaReceiptSlot.MAX_CANONICAL_BYTES),
        EVIDENCE_OWNER(47, 35, TargetExternalEvidenceOwner.MAX_CANONICAL_BYTES),
        EVIDENCE_REF(48, 35, TargetExternalEvidenceReference.MAX_CANONICAL_BYTES),
        RECOVERY_OWNER(49, 34, TargetRecoveryEvidenceOwner.MAX_CANONICAL_BYTES),
        RECOVERY_REF(50, 67, TargetRecoveryEvidenceReference.MAX_CANONICAL_BYTES),
        RETIRING(51, 34, TargetEvidenceRetiringRecord.MAX_CANONICAL_BYTES),
        CONFIRMED(52, 34, TargetEvidenceConfirmedRecord.MAX_CANONICAL_BYTES),
        ACTIVATION(53, 34, TargetProtocolActivationRecord.MAX_BYTES),
        HISTORY(54, 34, TargetRetirementSourceHistoryRecord.MAX_BYTES),
        HISTORY_TIP(55, 34, TargetRetirementHistoryTipRecord.MAX_BYTES),
        OBJECT_PENDING(56, 2 + DelayMessageId.LENGTH, TargetPayloadObjectRetiringRecord.MAX_CANONICAL_BYTES),
        SLO_STATE(57, 55, TargetSloObservationState.MAX_CANONICAL_BYTES),
        GENERATION(58, 63, TargetSloObservationGeneration.MAX_CANONICAL_BYTES),
        SAMPLE(59, 34, TargetSloObservationSample.MAX_CANONICAL_BYTES),
        SAMPLE_REF(59, 66, TargetSloObservationSample.MAX_CANONICAL_BYTES),
        COMMAND_SAMPLE(59, 34, TargetSloObservationSample.MAX_COMMAND_BYTES),
        COMMAND_SAMPLE_REF(59, 66, TargetSloObservationSample.MAX_COMMAND_BYTES),
        DUE_SAMPLE(59, 34, TargetSloObservationSample.MAX_CANONICAL_BYTES),
        DUE_SAMPLE_REF(59, 66, TargetSloObservationSample.MAX_CANONICAL_BYTES),
        EVENT(60, 42, TargetSloObservationEventRecord.MAX_CANONICAL_BYTES),
        CUSTODY(61, 66, TargetSloCustodyRecord.MAX_CANONICAL_BYTES),
        WRITER(62, 95, TargetSloWriterEpoch.MAX_CANONICAL_BYTES),
        PARENT_EMPTY(
                63,
                34,
                TargetParentControlCredit.MAX_CANONICAL_BYTES
                        - (long) TargetParentControlCredit.MAX_ALLOCATIONS
                                * (6L + TargetParentControlCredit.Allocation.MAX_CANONICAL_BYTES)),
        PARENT(63, 34, TargetParentControlCredit.MAX_CANONICAL_BYTES),
        SCOPE(64, 34, TargetScopeControlObligation.MAX_CANONICAL_BYTES),
        MIGRATION(65, 22, TargetMigrationRoot.MAX_CANONICAL_BYTES),
        MIGRATION_ORIGIN(66, 35, TargetMigrationOrigin.MAX_CANONICAL_BYTES),
        MIGRATION_GUARD(67, 2 + DelayMessageId.LENGTH, TargetMigrationCustodyGuard.MAX_CANONICAL_BYTES),
        MIGRATION_PREFIX(68, 42, TargetMigrationPrefixRecord.MAX_CANONICAL_BYTES),
        MIGRATION_ATTEMPT(69, 34, TargetMigrationActivationAttempt.MAX_CANONICAL_BYTES),
        RETIREMENT(71, 34, TargetMigrationRetirementRecord.MAX_CANONICAL_BYTES),
        ARCHIVE_HEAD_V2(71, 35, TargetMigrationArchiveState.V2_MAX_CANONICAL_BYTES),
        ARCHIVE_HEAD(71, 35, TargetMigrationArchiveState.MAX_CANONICAL_BYTES),
        ARCHIVE_STEP(71, 43, TargetMigrationArchiveState.V2_MAX_CANONICAL_BYTES),
        ARCHIVE_COPY(
                72,
                4 + TargetSourcePosition.MAX_CANONICAL_BYTES,
                TargetMigrationArchiveRow.maximumCanonicalBytes(TargetMigrationArchiveRow.SCHEMA_VERSION, 0)),
        ARCHIVE_ROW_V1(
                72, 4 + TargetSourcePosition.MAX_CANONICAL_BYTES, TargetMigrationArchiveRow.V1_MAX_CANONICAL_BYTES),
        ARCHIVE_ROW(72, 4 + TargetSourcePosition.MAX_CANONICAL_BYTES, TargetMigrationArchiveRow.MAX_CANONICAL_BYTES);
        final int type;
        final long key, payload;

        Row(int type, long key, long payload) {
            this.type = type;
            this.key = key;
            this.payload = payload;
        }

        long nv() {
            return Math.addExact(12L, payload);
        }

        long stored() {
            return Math.addExact(key, nv());
        }

        long payload(long frameBytes) {
            return switch (this) {
                case ARCHIVE_HEAD ->
                    Math.max(
                            TargetMigrationArchiveState.V2_MAX_CANONICAL_BYTES,
                            TargetMigrationArchiveState.maximumCanonicalBytes(
                                    TargetMigrationArchiveState.SCHEMA_VERSION, frameBytes));
                case ARCHIVE_STEP ->
                    Math.max(
                            payload,
                            TargetMigrationArchiveState.maximumCanonicalBytes(
                                    TargetMigrationArchiveState.SCHEMA_VERSION, 0));
                case ARCHIVE_ROW ->
                    Math.max(
                            TargetMigrationArchiveRow.V1_MAX_CANONICAL_BYTES,
                            TargetMigrationArchiveRow.maximumCanonicalBytes(
                                    TargetMigrationArchiveRow.SCHEMA_VERSION, frameBytes));
                default -> payload;
            };
        }

        long nv(long frameBytes) {
            return TargetMigrationArchiveSourceFrame.nativeValueBytes(payload(frameBytes));
        }

        long stored(long frameBytes) {
            return Math.addExact(key, nv(frameBytes));
        }
    }

    /** Logical projections retain multiplicity; only an explicit overlay hit removes a Native read. */
    static final class Reads {
        private final EnumMap<Row, long[]> rows = new EnumMap<>(Row.class);
        private final EnumMap<Row, Long> creations = new EnumMap<>(Row.class);
        private final List<Reads[]> alternatives = new ArrayList<>();
        private long parentOriginMaterials;
        private long localAllocated, localRetained;
        private long retirementRangeCalls;

        Reads add(Row row, long count) {
            return add(row, count, true);
        }

        Reads add(Row row, long count, boolean present) {
            if (count < 0) {
                throw new IllegalArgumentException("negative repository read multiplicity");
            }
            if (count != 0) {
                final var values = rows.computeIfAbsent(row, ignored -> new long[3]);
                final int at = present ? 1 : 0;
                values[at] = Math.addExact(values[at], count);
            }
            return this;
        }
        /** A mandatory same-key overlay hit still clones and decodes its real NV, but performs no Native get. */
        Reads local(Row row, long count) {
            if (count < 0) {
                throw new IllegalArgumentException("negative actual local projection multiplicity");
            }
            if (count != 0) {
                final var values = rows.computeIfAbsent(row, ignored -> new long[3]);
                values[2] = Math.addExact(values[2], count);
            }
            return this;
        }

        Reads created(Row row, long count) {
            if (count < 0) {
                throw new IllegalArgumentException("negative actual local construction multiplicity");
            }
            if (count != 0) {
                creations.merge(row, count, Math::addExact);
            }
            return this;
        }

        Reads add(Reads other) {
            for (var entry : other.rows.entrySet()) {
                add(entry.getKey(), entry.getValue()[0], false);
                add(entry.getKey(), entry.getValue()[1], true);
                local(entry.getKey(), entry.getValue()[2]);
            }
            for (var entry : other.creations.entrySet()) {
                created(entry.getKey(), entry.getValue());
            }
            alternatives.addAll(other.alternatives);
            parentOriginMaterials = Math.addExact(parentOriginMaterials, other.parentOriginMaterials);
            localAllocated = Math.addExact(localAllocated, other.localAllocated);
            localRetained = Math.addExact(localRetained, other.localRetained);
            retirementRangeCalls = Math.addExact(retirementRangeCalls, other.retirementRangeCalls);
            return this;
        }
        /** Local actual array construction/accessors, derived by the owning helper; no Native read credit. */
        Reads material(long allocated, long retained) {
            if (allocated < 0 || retained < 0) {
                throw new IllegalArgumentException("negative repository material");
            }
            localAllocated = Math.addExact(localAllocated, allocated);
            localRetained = Math.addExact(localRetained, retained);
            return this;
        }
        /** Only proof.view.first's actual local overlay walk; no backing-store row is fabricated. */
        Reads retirementRangeCalls(long calls) {
            if (calls < 0) {
                throw new IllegalArgumentException("negative actual proof range calls");
            }
            retirementRangeCalls = Math.addExact(retirementRangeCalls, calls);
            return this;
        }

        static Reads either(Reads... choices) {
            if (choices.length < 2) {
                throw new IllegalArgumentException("an actual branch choice needs alternatives");
            }
            final var result = new Reads();
            result.alternatives.add(choices.clone());
            return result;
        }

        long records() {
            long total = 0;
            for (var values : rows.values()) {
                total = Math.addExact(total, Math.addExact(values[0], values[1]));
            }
            for (var choices : alternatives) {
                long maximum = 0;
                for (var choice : choices) {
                    maximum = Math.max(maximum, choice.records());
                }
                total = Math.addExact(total, maximum);
            }
            return total;
        }

        long bytes() {
            return bytes(0);
        }

        long bytes(long frameBytes) {
            long total = 0;
            for (var entry : rows.entrySet()) {
                final var values = entry.getValue();
                total = Math.addExact(
                        total,
                        Math.addExact(
                                Math.multiplyExact(values[0], entry.getKey().key),
                                Math.multiplyExact(values[1], entry.getKey().stored(frameBytes))));
            }
            for (var choices : alternatives) {
                long maximum = 0;
                for (var choice : choices) {
                    maximum = Math.max(maximum, choice.bytes(frameBytes));
                }
                total = Math.addExact(total, maximum);
            }
            return total;
        }

        long rawArrays() {
            return rawArrays(0);
        }

        long rawArrays(long frameBytes) {
            long total = 0;
            for (var entry : rows.entrySet()) {
                total = Math.addExact(
                        total,
                        Math.multiplyExact(
                                Math.addExact(entry.getValue()[1], entry.getValue()[2]),
                                entry.getKey().nv(frameBytes)));
            }
            for (var choices : alternatives) {
                long maximum = 0;
                for (var choice : choices) {
                    maximum = Math.max(maximum, choice.rawArrays(frameBytes));
                }
                total = Math.addExact(total, maximum);
            }
            return total;
        }
    }

    private record Edit(Row row, boolean before, boolean after, long count, long aggregatePayloadBytes) {
        private Edit(Row row, boolean before, boolean after, long count) {
            this(row, before, after, count, -1);
        }

        private Edit {
            if (!before && !after
                    || count < 1
                    || aggregatePayloadBytes < -1
                    || aggregatePayloadBytes >= 0 && (row != Row.ARCHIVE_COPY || before || !after)) {
                throw new IllegalArgumentException("invalid repository edit shape");
            }
        }

        long payloadBytes(long frameBytes) {
            return aggregatePayloadBytes >= 0
                    ? aggregatePayloadBytes
                    : Math.multiplyExact(count, row.payload(frameBytes));
        }

        long nativeBytes(long frameBytes) {
            return Math.addExact(Math.multiplyExact(count, 12L), payloadBytes(frameBytes));
        }

        long retained(long frameBytes) {
            return Math.addExact(
                    Math.multiplyExact(count, row.key),
                    Math.multiplyExact((before ? 1L : 0) + (after ? 1L : 0), nativeBytes(frameBytes)));
        }

        long copied(long frameBytes) {
            return Math.addExact(
                    retained(frameBytes),
                    Math.multiplyExact(
                            Math.multiplyExact(2L, (before ? 1L : 0) + (after ? 1L : 0)), payloadBytes(frameBytes)));
        }

        long written(long frameBytes) {
            return Math.addExact(Math.multiplyExact(count, row.key), after ? nativeBytes(frameBytes) : 0);
        }
    }

    /** One actual assemble invocation. Earlier invocations are discarded plans, never extra committed edits. */
    private record Assembly(List<Edit> business, int scopeReferences, boolean scopeSource, boolean rootInOverlay) {
        private Assembly {
            business = List.copyOf(business);
            if (scopeReferences < 0) {
                throw new IllegalArgumentException("negative actual Scope reference count");
            }
        }
    }

    /** One mutually exclusive dispatch branch, including its actual earlier accounting attempts. */
    private static final class Plan {
        final String source;
        final long frameBytes;
        final int ownerPairs, targets, domains;
        final Reads direct = new Reads();
        final List<Assembly> assemblies = new ArrayList<>();
        boolean fence,
                cursors,
                activation,
                nativeClaim,
                migrationActivation,
                migrationSettlement,
                migrationNoArchiveTail;
        boolean migrationRetirementIntent, migrationRetirementConfirmation;
        long sourceFrameEncodings, sourceFrameCopies;

        Plan(String source, long frameBytes, int ownerPairs, int targets, int domains) {
            this.source = Objects.requireNonNull(source);
            this.frameBytes = frameBytes;
            if (ownerPairs < 1 || targets < 0 || domains < 1) {
                throw new IllegalArgumentException("invalid closed family identities");
            }
            this.ownerPairs = ownerPairs;
            this.targets = targets;
            this.domains = domains;
        }

        Plan reads(Reads reads) {
            direct.add(reads);
            return this;
        }

        Plan assembly(int references, boolean source, boolean rootOverlay, Edit... business) {
            assemblies.add(new Assembly(Arrays.asList(business), references, source, rootOverlay));
            return this;
        }

        Plan assembly(int references, boolean source, boolean rootOverlay, List<Edit> business) {
            assemblies.add(new Assembly(business, references, source, rootOverlay));
            return this;
        }
    }

    private static Edit create(Row row, long count) {
        return new Edit(row, false, true, count);
    }

    private static Edit copyBatch(long count) {
        return new Edit(
                Row.ARCHIVE_COPY,
                false,
                true,
                count,
                TargetRecordAccounting.envelopeArchiveCopyCanonicalBytes(count, copyBatchRaw(count)));
    }

    private static long copyBatchRaw(long count) {
        return Math.min(
                TargetMigrationArchiveRequest.MAX_CANONICAL_BYTES,
                Math.multiplyExact(count, TargetMigrationArchiveEntry.MAX_RAW_BYTES));
    }

    private static Edit replace(Row row, long count) {
        return new Edit(row, true, true, count);
    }

    private static Edit delete(Row row, long count) {
        return new Edit(row, true, false, count);
    }

    private static List<Edit> edits(Edit... edits) {
        return new ArrayList<>(Arrays.asList(edits));
    }

    /** Fresh replay probe, native first-absence and result CreationAuthority; replace-before is separate. */
    private static Reads systemPrelude() {
        return new Reads()
                .add(Row.FIRST, 3, false)
                .add(Row.POSITION, 3, false)
                .add(Row.AGGREGATE, 1)
                .add(Row.DESCRIPTOR, 1);
    }

    private static Reads commandPrelude() {
        return new Reads()
                .add(Row.FIRST, 6, false)
                .add(Row.POSITION, 3, false)
                .add(Row.AGGREGATE, 1)
                .add(Row.DESCRIPTOR, 1);
    }

    private static List<Edit> systemResults() {
        return edits(create(Row.FIRST, 1), create(Row.POSITION, 1));
    }

    private static List<Edit> commandResults() {
        return edits(create(Row.FIRST, 2), create(Row.POSITION, 1));
    }

    private static long systemFrame(long body) {
        return TargetMigrationArchiveSourceFrame.systemFrameBytes(body, TargetResultRecord.MAX_AUTHOR_BYTES);
    }

    private static long controlFrame(long body) {
        return TargetMigrationArchiveSourceFrame.controlFrameBytes(body);
    }

    /** Reader.replace performs a real before get even when the caller already obtained the same key. */
    private static Reads replacementReads(List<Edit> business) {
        final var reads = new Reads();
        for (var edit : business) {
            reads.add(edit.row, edit.count, edit.before);
        }
        return reads;
    }

    /** Weighted form of the established head bound. All three minima can coexist in one domain.
     * Each OPEN side reads due(native sibling), ordered(order+ordinary), native(ordinary sibling):
     * 6 Work + 3 Message + 1 Order. Only the new side skips input Work replacements in Reader.first.
     * A newly CLOSED queue has no new-side minima; its retained old physical candidates are still checked. */
    private static Reads headReads(List<Edit> input, int targets, int domains, boolean newClosed) {
        final var reads = replacementReads(input).created(Row.QUEUE, targets);
        long queues = 0, identities = 0;
        for (var edit : input) {
            if (edit.row == Row.QUEUE) {
                queues = Math.addExact(queues, edit.count);
                reads.add(Row.QUEUE, edit.count, edit.before).local(Row.QUEUE, Math.multiplyExact(2L, edit.count));
            }
            if (edit.row == Row.IDENTITY) {
                identities = Math.addExact(identities, edit.count);
            }
        }
        reads.add(Row.QUEUE, Math.multiplyExact(2L, Math.max(0L, targets - queues)));
        reads.add(Row.IDENTITY, Math.max(0L, targets - identities)).local(Row.IDENTITY, Math.min(targets, identities));
        final long sides = Math.multiplyExact(newClosed ? 1L : 2L, domains);
        reads.add(Row.WORK, Math.multiplyExact(6L, sides))
                .add(Row.MESSAGE, Math.multiplyExact(3L, sides))
                .add(Row.ORDER, sides);
        if (!newClosed) {
            for (var edit : input) {
                if (edit.row.type == TargetTimelineWorkRef.VALUE_TYPE) {
                    reads.add(Row.WORK, edit.count);
                }
            }
        }
        return reads;
    }

    /** TargetProtocolActivationStore.requireApplied: original first, payer descriptor, physical position;
     * extended Roots add one of the original META10 or actual A/Native69 proof branches, never both. */
    private static Reads activationApplied(Capabilities c) {
        final var reads = new Reads().add(Row.FIRST, 1).add(Row.DESCRIPTOR, 1).add(Row.POSITION, 1);
        if (c.generation >= 15) {
            if (c.migration) {
                reads.add(Row.MIGRATION, 1)
                        .add(Row.FIRST, 1)
                        .add(Row.POSITION, 1)
                        .add(Row.MIGRATION_ATTEMPT, 1);
            } else {
                reads.add(Row.INITIAL, 1).add(Row.FIRST, 1).add(Row.POSITION, 1).add(Row.DESCRIPTOR, 1);
            }
        }
        return reads;
    }

    private static Reads activationOrigin(Capabilities c) {
        return new Reads().add(Row.ACTIVATION, 1).add(activationApplied(c));
    }

    private static Reads scopeLocalOrigin(Capabilities c) {
        return new Reads().add(Row.FIRST, 1).add(Row.POSITION, 1).add(activationOrigin(c));
    }

    private static Reads parentOrigin(Capabilities c) {
        final var reads = new Reads().add(Row.FIRST, 1).add(Row.POSITION, 1).add(activationOrigin(c));
        for (int i = 0; i < TargetParentControlCredit.MAX_ALLOCATIONS; i++) {
            reads.add(Row.SCOPE, 1).add(scopeLocalOrigin(c));
        }
        reads.parentOriginMaterials = 1;
        return reads;
    }

    private static Reads scopeOrigin(Capabilities c) {
        return new Reads().add(scopeLocalOrigin(c)).add(Row.PARENT, 1).add(parentOrigin(c));
    }

    private static Reads paidScope(Capabilities c) {
        return new Reads()
                .add(Row.AGGREGATE, 1)
                .add(Row.SCOPE, 1)
                .add(Row.DESCRIPTOR, 1)
                .add(Row.BOOKKEEPING, 1)
                .add(scopeOrigin(c));
    }
    /** Protected WriterOrigin still point-checks the current node and actual predecessor including later CLOSEs. */
    private static Reads writerHot(Capabilities c) {
        final var reads = new Reads().add(Row.WRITER, 2).add(Row.DESCRIPTOR, 1).add(Row.POSITION, 1);
        for (int node = 0; node < 2; node++) {
            reads.add(Row.WRITER, 1).add(activationOrigin(c)).add(Row.FIRST, 1).add(Row.POSITION, 1);
            reads.add(activationOrigin(c)).add(Row.FIRST, 1).add(Row.POSITION, 1);
            reads.add(Row.DESCRIPTOR, 1).add(Row.FIRST, 1).add(Row.POSITION, 1).add(Row.FENCE, 1);
        }
        return reads;
    }

    private static Reads repeat(Reads reads, long count) {
        if (count < 0) {
            throw new IllegalArgumentException("negative repository call count");
        }
        final var result = new Reads();
        for (long i = 0; i < count; i++) {
            result.add(reads);
        }
        return result;
    }

    private static long count(List<Edit> business, int type) {
        long count = 0;
        for (var edit : business) {
            if (edit.row.type == type) {
                count = Math.addExact(count, edit.count);
            }
        }
        return count;
    }

    private static boolean hasAfter(List<Edit> business, int type) {
        for (var edit : business) {
            if (edit.row.type == type && edit.after) {
                return true;
            }
        }
        return false;
    }

    private static Reads self(Row row, boolean after, long copies) {
        return after ? new Reads().local(row, copies) : new Reads().add(row, copies);
    }

    /** Actual typed dependency calls. Mandatory own-key after projections are local; other identities
     * retain a backing-read allowance until the assembly fixes an exact same-key overlay dependency. */
    private static Reads chargeReads(Row row, boolean after, Assembly assembly, Plan plan, Capabilities c) {
        final var reads = self(row, after, 1);
        final var business = assembly.business;
        final boolean importedPayload =
                c.migration && (!after || !hasAfter(business, TargetQuotaPayloadOwner.VALUE_TYPE));
        switch (row) {
            case INITIAL -> reads.add(Row.DESCRIPTOR, 1);
            case RESERVATION -> {
                reads.add(Row.RESERVATION, 4)
                        .add(Row.BINDING, 1)
                        .add(Row.PAYLOAD, 1)
                        .add(Row.DESCRIPTOR, 1);
                if (importedPayload) {
                    reads.add(Row.MIGRATION, 1).add(Row.BINDING, 1);
                }
            }
            case MESSAGE, TERMINAL, BINDING -> {
                reads.add(Row.PAYLOAD, 1).add(Row.DESCRIPTOR, 1);
                if (importedPayload) {
                    reads.add(Row.MIGRATION, 1).add(Row.BINDING, 1);
                }
            }
            case WORK, ORDER_HEAD, EXPIRY -> {
                reads.add(Row.MESSAGE, 1).add(Row.PAYLOAD, 1).add(Row.DESCRIPTOR, 1);
                if (row == Row.ORDER_HEAD) {
                    reads.add(Row.ORDER, 1);
                }
                if (importedPayload) {
                    reads.add(Row.MIGRATION, 1).add(Row.BINDING, 1);
                }
            }
            case PAYLOAD, PAYLOAD_V1 -> {
                reads.add(Row.DESCRIPTOR, 1).add(retirementPayloadReclaim(c));
                if (importedPayload) {
                    reads.add(Row.MIGRATION, 1).add(Row.BINDING, 1);
                }
            }
            case DESCRIPTOR -> reads.add(retirementIncarnationOrigin(c));
            case CLOSE -> reads.add(Row.QUEUE, 1).add(Row.DESCRIPTOR, 1);
            case CLOSE_CURSOR -> reads.add(Row.CLOSE, 1).add(Row.QUEUE, 1).add(Row.DESCRIPTOR, 1);
            case QUEUE -> reads.add(self(row, after, 1)).add(Row.DESCRIPTOR, 2).add(Row.IDENTITY, 2);
            case ORDER, CHANNEL, NATIVE_SCOPE -> reads.add(self(row, after, 1)).add(Row.DESCRIPTOR, 2);
            case NATIVE_SNAPSHOT ->
                reads.add(self(row, after, 1)).add(Row.DESCRIPTOR, 2).add(Row.NATIVE_SCOPE, 2);
            case NATIVE_CONTROL ->
                reads.add(self(row, after, 1))
                        .add(Row.DESCRIPTOR, 2)
                        .add(Row.NATIVE_SCOPE, 2)
                        .add(Row.MEMBER, 1)
                        .add(Row.POLICY, 1)
                        .add(Row.MEMBER_CLOSE, 1);
            case IDENTITY ->
                reads.add(self(row, after, 2)).add(Row.DESCRIPTOR, 2).add(Row.GRANT, 2);
            case DISPATCH, CONTROL, MEMBER, POLICY, MEMBER_CLOSE -> {
                reads.add(self(row, after, 1))
                        .add(Row.DESCRIPTOR, 2)
                        .add(Row.GRANT, 2)
                        .add(Row.IDENTITY, 2);
                if (row == Row.MEMBER) {
                    reads.add(Row.POLICY, 1);
                }
                if (row == Row.MEMBER_CLOSE) {
                    reads.add(Row.MEMBER, 1);
                }
            }
            case CHANNEL_CONTROL, CHANNEL_SLOT, KAFKA_MAPPING, KAFKA_SLOT -> reads.add(Row.DESCRIPTOR, 1);
            case FIRST, POSITION -> reads.add(Row.DESCRIPTOR, 1).add(retirementFirstHistory(c));
            case CLAIM -> {
                reads.add(Row.CLAIM_CHARGE, 1).add(Row.MESSAGE, 1).add(Row.DESCRIPTOR, 1);
                if (plan.nativeClaim) {
                    reads.add(Row.NATIVE_SCOPE, 1).add(Row.BINDING, 1).add(Row.NATIVE_SNAPSHOT, 1);
                }
            }
            case CLAIM_CHARGE -> reads.add(Row.CLAIM, 1).add(Row.MESSAGE, 1).add(Row.DESCRIPTOR, 1);
            case EVIDENCE_OWNER -> reads.add(Row.DESCRIPTOR, 1);
            case EVIDENCE_REF -> reads.add(Row.DESCRIPTOR, 1).add(Row.EVIDENCE_OWNER, 1);
            case RECOVERY_OWNER ->
                reads.add(Row.DESCRIPTOR, 1).add(Row.RECOVERY_REF, 2).add(Row.ATTEMPT, 1);
            case RECOVERY_REF -> reads.add(Row.DESCRIPTOR, 1).add(Row.RECOVERY_OWNER, 1);
            case OBJECT_PENDING -> reads.add(Row.DESCRIPTOR, 1).add(retirementPending(c));
            case RETIRING -> reads.add(retirementRetiring(c));
            case CONFIRMED -> reads.add(Row.DESCRIPTOR, 1).add(retirementConfirmed(c));
            case HISTORY -> reads.add(Row.DESCRIPTOR, 1).add(retirementFirst(c));
            case HISTORY_TIP -> reads.add(Row.DESCRIPTOR, 1).add(retirementTip(c));
            case ACTIVATION -> reads.add(Row.DESCRIPTOR, 1).add(activationApplied(c));
            case SLO_STATE -> reads.add(Row.DESCRIPTOR, 1).add(controlStateOrigin(c));
            case GENERATION -> reads.add(Row.DESCRIPTOR, 1).add(controlGenerationOrigin(c));
            case SAMPLE, SAMPLE_REF, COMMAND_SAMPLE, COMMAND_SAMPLE_REF, DUE_SAMPLE, DUE_SAMPLE_REF ->
                reads.add(Row.DESCRIPTOR, 1).add(controlSampleOrigin(c));
            case EVENT -> reads.add(Row.DESCRIPTOR, 1).add(controlEventOrigin(c));
            case CUSTODY -> reads.add(Row.DESCRIPTOR, 1).add(controlCustodyOrigin(c));
            case WRITER -> reads.add(Row.DESCRIPTOR, 1).add(controlWriterOrigin(c));
            case PARENT_EMPTY ->
                reads.add(Row.DESCRIPTOR, 1)
                        .add(Row.FIRST, 1)
                        .add(Row.POSITION, 1)
                        .add(activationOrigin(c));
            case PARENT -> reads.add(Row.DESCRIPTOR, 1).add(parentOrigin(c));
            case SCOPE -> reads.add(Row.DESCRIPTOR, 1).add(scopeOrigin(c));
            case ATTEMPT ->
                reads.add(Row.DESCRIPTOR, 1).add(retirementAttemptReclaim(c)).add(retirementRemainingAttempt(c));
            case GRANT -> {
                if (plan.ownerPairs > 1) {
                    reads.add(Row.DESCRIPTOR, 1);
                }
            }
            case MIGRATION, MIGRATION_ORIGIN, MIGRATION_PREFIX, MIGRATION_ATTEMPT -> {
                if (row == Row.MIGRATION && after && plan.migrationActivation) {
                    reads.local(Row.MIGRATION, 1);
                } else {
                    reads.add(Row.MIGRATION, 1);
                }
                reads.add(Row.DESCRIPTOR, 1);
                if (row == Row.MIGRATION && (!plan.migrationActivation || after)) {
                    reads.add(Row.FIRST, 1).add(Row.POSITION, 1).add(Row.MIGRATION_ATTEMPT, 1);
                }
            }
            case MIGRATION_GUARD -> {
                reads.add(Row.MIGRATION, 1).add(Row.DESCRIPTOR, 1);
                if (!plan.migrationSettlement || after) {
                    reads.add(Row.FIRST, 1).add(Row.POSITION, 1);
                }
            }
            case RETIREMENT ->
                reads.add(f2Activated())
                        .add(f2Selected(
                                !plan.migrationRetirementIntent && (!plan.migrationRetirementConfirmation || after)))
                        .add(Row.DESCRIPTOR, 1);
            case ARCHIVE_STEP ->
                reads.add(f2Activated())
                        .add(f2Selected(true))
                        .add(Row.FIRST, 1)
                        .add(Row.POSITION, 1)
                        .add(Row.DESCRIPTOR, 1)
                        .add(f2FullFormat(c));
            case ARCHIVE_HEAD, ARCHIVE_HEAD_V2 ->
                reads.add(f2Activated())
                        .add(f2Selected(true))
                        .add(f2Head(c))
                        .add(Row.DESCRIPTOR, 1)
                        .add(row == Row.ARCHIVE_HEAD ? f2FullFormat(c) : new Reads());
            case ARCHIVE_COPY, ARCHIVE_ROW, ARCHIVE_ROW_V1 ->
                reads.add(f2Activated())
                        .add(f2Selected(true))
                        .add(
                                row == Row.ARCHIVE_COPY
                                        ? new Reads()
                                                .add(Row.ARCHIVE_STEP, 1)
                                                .add(Row.FIRST, 1)
                                                .add(Row.POSITION, 1)
                                        : Reads.either(
                                                new Reads()
                                                        .add(Row.ARCHIVE_STEP, 1)
                                                        .add(Row.FIRST, 1)
                                                        .add(Row.POSITION, 1),
                                                f2TailApplied(c)))
                        .add(Row.DESCRIPTOR, 1)
                        .add(row == Row.ARCHIVE_ROW || row == Row.ARCHIVE_COPY ? f2FullFormat(c) : new Reads());
            case COUNTER, AGGREGATE, TOTAL, BOOKKEEPING, FENCE, CURSORS ->
                throw new IllegalArgumentException("backend projection is not a business typed-charge row");
        }
        if (after && (row.type == 57 || row.type == 58 || row.type == 59)) {
            reads.add(controlGenerationDelta(business));
        }
        return reads;
    }

    /* Command/Expiry families: their graph projection runs once before the first accounting attempt.
    Certification can assemble the same graph again; quota rejection instead projects new results. */
    private static List<Plan> commandFamilies(Capabilities c) {
        final var plans = new ArrayList<Plan>();
        final int slots = TargetQueueState.MAX_DOMAIN_SLOTS;
        for (int registration = 0; registration < 3; registration++) {
            for (int path = 0; path < 3; path++) {
                final boolean strict = path == 1, nativeWork = path == 2;
                commandIngressFamilies(c, plans, registration, strict, nativeWork, false);
                commandIngressFamilies(c, plans, registration, strict, nativeWork, true);
            }
        }
        final var ownerChoices = new Row[] {Row.PAYLOAD_V1, Row.PAYLOAD};
        for (var owner : ownerChoices) {
            for (int path = 0; path < 3; path++) {
                final boolean strict = path == 1, nativeWork = path == 2;
                final var commit = edits(
                        replace(Row.RESERVATION, 2),
                        delete(Row.RESERVATION, 2),
                        replace(owner, 1),
                        create(Row.MESSAGE, 1),
                        create(Row.EXPIRY, 1),
                        create(Row.WORK, nativeWork ? 2 : 1));
                if (strict) {
                    commandStrictEdits(commit, false, false);
                }
                final var commitReads = new Reads()
                        .add(Row.RESERVATION, 4)
                        .add(owner, 1)
                        .add(Row.DESCRIPTOR, 1)
                        .add(Row.BINDING, 1)
                        .add(Row.QUEUE, 1)
                        .add(Row.MESSAGE, 1, false)
                        .add(commandReservationControls());
                if (c.migration) {
                    commitReads.add(commandPayloadAttribution(c)).add(commandImportedReservation(c));
                }
                if (strict) {
                    commandFinish(
                            c,
                            plans,
                            "command/commit-strict-rejected-after-reservation-projection/" + owner,
                            List.of(),
                            new Reads()
                                    .add(commitReads)
                                    .add(Row.ORDER, 1)
                                    .add(replacementReads(
                                            edits(replace(Row.RESERVATION, 2), delete(Row.RESERVATION, 2)))),
                            2,
                            0,
                            1,
                            false,
                            false);
                }
                if (strict) {
                    commitReads.add(Row.ORDER, 1).add(commandStrictReads(false, false, 0));
                }
                commandFinish(
                        c,
                        plans,
                        "command/commit/" + owner + "/" + path,
                        commit,
                        commitReads,
                        2,
                        1,
                        slots,
                        !nativeWork,
                        false);
            }
            final var reservationCancel =
                    edits(replace(Row.RESERVATION, 2), delete(Row.RESERVATION, 2), replace(owner, 1));
            final var reservationReads = new Reads()
                    .add(Row.MESSAGE, 1, false)
                    .add(owner, 1)
                    .add(Row.RESERVATION, 4)
                    .add(Row.DESCRIPTOR, 1)
                    .add(Row.BINDING, 1)
                    .add(Row.QUEUE, 1)
                    .add(commandReservationControls());
            if (c.migration) {
                reservationReads.add(commandPayloadAttribution(c)).add(commandImportedReservation(c));
            }
            commandFinish(
                    c,
                    plans,
                    "command/cancel-reservation/" + owner,
                    reservationCancel,
                    reservationReads,
                    2,
                    1,
                    slots,
                    false,
                    false);
            commandFinish(
                    c,
                    plans,
                    "command/reservation-unchanged/" + owner,
                    List.of(),
                    reservationReads,
                    2,
                    0,
                    1,
                    false,
                    false);
            for (int path = 0; path < 3; path++) {
                for (boolean claimed : new boolean[] {false, true}) {
                    final boolean strict = path == 1, nativeWork = path == 2;
                    for (boolean retain : new boolean[] {false, true}) {
                        final var cancel =
                                edits(replace(Row.MESSAGE, 1), delete(Row.EXPIRY, 1), create(Row.TERMINAL, 1));
                        if (!claimed) {
                            cancel.add(delete(Row.WORK, nativeWork ? 2 : 1));
                        }
                        if (claimed) {
                            cancel.add(delete(Row.CLAIM, 1));
                            cancel.add(delete(Row.CLAIM_CHARGE, 1));
                        }
                        if (retain) {
                            cancel.add(replace(owner, 1));
                        }
                        if (strict) {
                            commandStrictEdits(cancel, false, claimed);
                        }
                        final var reads = commandCurrentReads(c, owner).add(Row.TERMINAL, 1, false);
                        if (claimed) {
                            reads.add(commandClaimReads());
                        }
                        if (retain) {
                            reads.add(Row.MIGRATION_GUARD, 1);
                        }
                        if (strict) {
                            reads.add(Row.ORDER, 1).add(commandStrictReads(false, claimed, claimed ? 0 : 1));
                        }
                        commandFinish(
                                c,
                                plans,
                                "command/cancel/" + owner + "/" + path + "/" + claimed + "/" + retain,
                                cancel,
                                reads,
                                2,
                                1,
                                slots,
                                false,
                                false);
                        commandExpiry(c, plans, owner, path, claimed, retain, cancel);
                    }
                    final var reschedule = edits(
                            replace(Row.MESSAGE, 1),
                            delete(Row.EXPIRY, 1),
                            create(Row.EXPIRY, 1),
                            create(Row.WORK, nativeWork ? 2 : 1),
                            create(Row.TERMINAL, 1));
                    if (claimed) {
                        reschedule.add(delete(Row.CLAIM, 1));
                        reschedule.add(delete(Row.CLAIM_CHARGE, 1));
                    } else {
                        reschedule.add(delete(Row.WORK, nativeWork ? 2 : 1));
                    }
                    if (strict) {
                        commandStrictEdits(reschedule, false, claimed);
                    }
                    final var rescheduleReads =
                            commandCurrentReads(c, owner).add(Row.IDENTITY, 1).add(Row.TERMINAL, 1, false);
                    if (claimed) {
                        rescheduleReads.add(commandClaimReads());
                    }
                    if (strict) {
                        rescheduleReads.add(Row.ORDER, 2).add(commandStrictReads(false, claimed, claimed ? 0 : 1));
                    }
                    commandFinish(
                            c,
                            plans,
                            "command/reschedule/" + owner + "/" + path + "/" + claimed,
                            reschedule,
                            rescheduleReads,
                            2,
                            1,
                            slots,
                            !nativeWork,
                            false);
                }
            }
            commandFinish(
                    c,
                    plans,
                    "command/terminal-current/" + owner,
                    List.of(),
                    commandCurrentReads(c, owner).add(Row.TERMINAL, 1),
                    2,
                    0,
                    1,
                    false,
                    false);
            if (c.generation >= 7) {
                commandFinish(
                        c,
                        plans,
                        "command/released-current/" + owner,
                        List.of(),
                        new Reads()
                                .add(Row.MESSAGE, 1, false)
                                .add(owner, 1)
                                .add(Row.DESCRIPTOR, 1)
                                .add(retirementPayloadReclaim(c))
                                .add(Row.TERMINAL, 1)
                                .add(Row.BINDING, 1)
                                .add(Row.QUEUE, 1),
                        2,
                        0,
                        1,
                        false,
                        false);
            }
        }
        commandFinish(
                c,
                plans,
                "command/rejected-or-not-found",
                List.of(),
                new Reads().add(Row.MESSAGE, 1, false).add(Row.PAYLOAD, 1, false),
                1,
                0,
                1,
                false,
                false);
        final var expiryResults = systemResults();
        plans.add(new Plan(
                        "expiry/rejected-or-unchanged",
                        systemFrame(TargetExpireGenerationBody.MAX_CANONICAL_BYTES),
                        1,
                        0,
                        1)
                .reads(systemPrelude())
                .reads(new Reads().add(Row.MESSAGE, 1))
                .reads(replacementReads(expiryResults))
                .reads(headReads(expiryResults, 0, 0, false))
                .assembly(0, false, false, expiryResults));
        commandReplayFamilies(c, plans);
        return plans;
    }

    private static void commandIngressFamilies(
            Capabilities c, List<Plan> plans, int registration, boolean strict, boolean nativeWork, boolean prepare) {
        final int slots = TargetQueueState.MAX_DOMAIN_SLOTS;
        final boolean freshTarget = registration == 0, newDomain = registration != 2;
        final int domains = freshTarget ? 0 : newDomain ? slots - 1 : slots;
        final var rows = edits(create(Row.BINDING, 1), create(Row.PAYLOAD, 1));
        if (freshTarget) {
            rows.add(create(Row.IDENTITY, 1));
        }
        if (newDomain) {
            rows.add(create(Row.DISPATCH, 2));
            rows.add(create(Row.CONTROL, 1));
            rows.add(freshTarget ? create(Row.QUEUE, 1) : replace(Row.QUEUE, 1));
        }
        final var reads = new Reads()
                .add(Row.MESSAGE, 1, false)
                .add(Row.PAYLOAD, 1, false)
                .add(Row.RESERVATION, 1, false)
                .add(Row.DESCRIPTOR, 2)
                .add(Row.GRANT, 1)
                .add(Row.MEMBER, 1)
                .add(Row.MEMBER_CLOSE, 1)
                .add(Row.POLICY, 1)
                .add(Row.IDENTITY, 1, !freshTarget)
                .add(Row.QUEUE, 1, !freshTarget)
                .add(Row.DISPATCH, domains)
                .add(Row.CONTROL, domains)
                .add(Row.NATIVE_SCOPE, domains)
                .add(Row.BINDING, 1, false)
                .add(Row.IDENTITY, 1, !freshTarget)
                .add(Row.DISPATCH, 2, !newDomain)
                .add(Row.CONTROL, 1, !newDomain)
                // Initial owner replacement is followed by genuine qualification and its second replacement.
                .add(Row.PAYLOAD, 2, false)
                .add(Row.MESSAGE, 1, false);
        if (freshTarget) {
            reads.created(Row.QUEUE, 1);
        }
        if (nativeWork) {
            reads.add(Row.NATIVE_SCOPE, 1).add(commandNativeBindingReads());
        } else {
            reads.add(Row.NATIVE_SCOPE, 1);
        }
        if (prepare) {
            rows.add(create(Row.RESERVATION, 4));
            reads.add(Row.RESERVATION, 4, false);
            if (strict) {
                reads.add(Row.ORDER, 1, !newDomain);
            }
        } else {
            rows.add(create(Row.MESSAGE, 1));
            rows.add(create(Row.EXPIRY, 1));
            rows.add(create(Row.WORK, nativeWork ? 2 : 1));
            if (strict) {
                commandStrictEdits(rows, newDomain, false);
                reads.add(Row.ORDER, 1, !newDomain).add(commandStrictReads(newDomain, false, 0));
            }
        }
        commandFinish(
                c,
                plans,
                "command/" + (prepare ? "prepare" : "schedule") + "/" + registration + "/" + strict + "/" + nativeWork,
                rows,
                reads,
                2,
                1,
                slots,
                !prepare && !nativeWork,
                true);
    }

    private static Reads commandCurrentReads(Capabilities c, Row owner) {
        final var reads = new Reads()
                .add(Row.MESSAGE, 1)
                .add(owner, 1)
                .add(Row.DESCRIPTOR, 1)
                .add(Row.BINDING, 1)
                .add(Row.QUEUE, 1)
                .add(commandReservationControls());
        if (c.migration) {
            reads.add(commandPayloadAttribution(c));
        }
        return reads;
    }

    private static Reads commandClaimReads() {
        return new Reads().add(Row.MESSAGE, 1).add(Row.CLAIM, 1).add(Row.CLAIM_CHARGE, 1);
    }

    private static Reads commandReservationControls() {
        return Reads.either(
                new Reads().add(Row.CLOSE, 1, false),
                new Reads().add(Row.CLOSE, 1).add(Row.QUEUE, 1));
    }

    private static Reads commandNativeBindingReads() {
        // firstBinding calls only member(); publisher/activation trust is a later Admission operation.
        return new Reads().add(Row.NATIVE_SCOPE, 1).add(Row.NATIVE_CONTROL, 2);
    }

    private static Reads commandImportedBinding(Capabilities c, boolean prefix) {
        final var reads = new Reads()
                .add(Row.MIGRATION, 1)
                .add(Row.FIRST, 1)
                .add(Row.POSITION, 1)
                .add(Row.BINDING, 1)
                .add(Row.MIGRATION_ORIGIN, 5)
                .add(Row.IDENTITY, 1)
                .add(Row.DISPATCH, 2)
                .add(Row.CONTROL, 1);
        if (prefix) {
            reads.add(Reads.either(new Reads(), new Reads().add(Row.MIGRATION_PREFIX, 1)));
        }
        return reads;
    }

    private static Reads commandPayloadAttribution(Capabilities c) {
        return new Reads()
                .add(Row.BINDING, 1)
                .add(commandImportedBinding(c, false))
                .add(Row.MIGRATION, 1);
    }

    private static Reads commandImportedReservation(Capabilities c) {
        return commandImportedBinding(c, true)
                .add(Row.MIGRATION, 1)
                .add(Row.RESERVATION, 4)
                .add(Row.MIGRATION_ORIGIN, 4);
    }

    private static void commandStrictEdits(List<Edit> rows, boolean fresh, boolean claimed) {
        rows.add(fresh ? create(Row.ORDER, 1) : replace(Row.ORDER, 1));
        if (!fresh && !claimed) {
            rows.add(delete(Row.ORDER_HEAD, 1));
        }
        rows.add(create(Row.ORDER_HEAD, 1));
    }

    private static Reads commandStrictReads(boolean fresh, boolean claimed, int deletedOrdinary) {
        final var afterOpen = new Reads()
                .created(Row.ORDER, 1)
                .add(Row.WORK, 1L + deletedOrdinary)
                .add(Row.MESSAGE, 1)
                .add(Row.WORK, 1, false);
        if (fresh) {
            return afterOpen.add(Row.QUEUE, 1);
        }
        if (claimed) {
            return afterOpen.add(Row.MESSAGE, 1).add(Row.QUEUE, 1);
        }
        return new Reads().add(Row.WORK, 2).add(Row.MESSAGE, 1).add(afterOpen).add(Row.QUEUE, 1);
    }

    private static void commandExpiry(
            Capabilities c,
            List<Plan> plans,
            Row owner,
            int path,
            boolean claimed,
            boolean retain,
            List<Edit> commandRows) {
        final var rows = new ArrayList<>(commandRows);
        rows.addAll(systemResults());
        final var reads = new Reads()
                .add(Row.MESSAGE, 1)
                .add(owner, 1)
                .add(Row.DESCRIPTOR, 1)
                .add(Row.BINDING, 1)
                .add(Row.QUEUE, 1)
                .add(Row.TERMINAL, 1, false);
        if (claimed) {
            reads.add(commandClaimReads());
        }
        if (retain) {
            reads.add(Row.MIGRATION_GUARD, 1);
        }
        if (path == 1) {
            reads.add(Row.ORDER, 1).add(commandStrictReads(false, claimed, claimed ? 0 : 1));
        }
        final var input = new ArrayList<>(rows);
        commandQueue(rows, false);
        final var expiry = new Plan(
                        "expiry/current/" + owner + "/" + path + "/" + claimed + "/" + retain,
                        systemFrame(TargetExpireGenerationBody.MAX_CANONICAL_BYTES),
                        2,
                        1,
                        TargetQueueState.MAX_DOMAIN_SLOTS)
                .reads(systemPrelude())
                .reads(reads)
                .reads(replacementReads(input))
                .reads(headReads(input, 1, TargetQueueState.MAX_DOMAIN_SLOTS, false))
                .assembly(0, false, false, rows);
        expiry.nativeClaim = claimed && path != 1;
        plans.add(expiry);
        final var barrier = commandBarrierBusiness(input);
        if (barrier != null) {
            plans.add(new Plan(
                            "expiry/retained-other-barrier/" + owner + "/" + retain,
                            systemFrame(TargetExpireGenerationBody.MAX_CANONICAL_BYTES),
                            2,
                            1,
                            TargetQueueState.MAX_DOMAIN_SLOTS)
                    .reads(systemPrelude())
                    .reads(commandBarrierReads(reads, input))
                    .reads(replacementReads(barrier))
                    .reads(headReads(barrier, 1, TargetQueueState.MAX_DOMAIN_SLOTS, false))
                    .assembly(0, false, false, barrier));
        }
    }

    private static void commandFinish(
            Capabilities c,
            List<Plan> plans,
            String label,
            List<Edit> business,
            Reads direct,
            int owners,
            int targets,
            int domains,
            boolean dueSample,
            boolean quotaRejection) {
        commandFinishProjection(
                c, plans, label, business, direct, owners, targets, domains, dueSample, quotaRejection, false);
    }

    private static void commandFinishProjection(
            Capabilities c,
            List<Plan> plans,
            String label,
            List<Edit> business,
            Reads direct,
            int owners,
            int targets,
            int domains,
            boolean dueSample,
            boolean quotaRejection,
            boolean retainedBarrier) {
        final var input = new ArrayList<>(business);
        input.addAll(commandResults());
        final var complete = new ArrayList<>(input);
        if (!retainedBarrier) {
            commandQueue(complete, false);
        }
        final int headTargets = commandHeadTarget(input) ? targets : 0;
        final var base = commandPrelude()
                .add(direct)
                .add(replacementReads(input))
                .add(headReads(input, headTargets, headTargets == 0 ? 0 : domains, false));
        final long frame = TargetMigrationArchiveSourceFrame.MAX_COMMAND_FRAME_BYTES;
        commandObserved(c, plans, label, frame, owners, targets, domains, complete, base, dueSample, false);
        if (quotaRejection) {
            final var rejection = commandResults();
            final var rejectedReads = new Reads()
                    .add(base)
                    .add(Row.DESCRIPTOR, 1)
                    .add(Row.FIRST, 2, false)
                    .add(Row.POSITION, 1, false)
                    .add(replacementReads(rejection))
                    .add(headReads(rejection, 0, 0, false));
            commandObserved(
                    c,
                    plans,
                    label + "/quota-rejected",
                    frame,
                    owners,
                    targets,
                    domains,
                    rejection,
                    rejectedReads,
                    false,
                    true,
                    complete);
        }
        if (!retainedBarrier) {
            final var barrier = commandBarrierBusiness(business);
            if (barrier != null) {
                commandFinishProjection(
                        c,
                        plans,
                        label + "/retained-other-barrier",
                        barrier,
                        commandBarrierReads(direct, business),
                        owners,
                        targets,
                        domains,
                        dueSample,
                        quotaRejection,
                        true);
            }
        }
    }

    private static List<Edit> commandBarrierBusiness(List<Edit> business) {
        int order = -1;
        for (int i = 0; i < business.size(); i++) {
            final var edit = business.get(i);
            if (edit.row == Row.CLAIM) {
                return null;
            }
            if (edit.row == Row.ORDER) {
                if (!edit.before) {
                    return null;
                }
                order = i;
            }
        }
        if (order < 0) {
            return null;
        }
        final var result = new ArrayList<Edit>();
        for (int i = 0; i < business.size(); i++) {
            if (i < order || business.get(i).row != Row.ORDER_HEAD) {
                result.add(business.get(i));
            }
        }
        return result;
    }

    private static Reads commandBarrierReads(Reads original, List<Edit> business) {
        int deleted = 0;
        for (var edit : business) {
            if (edit.row == Row.ORDER) {
                break;
            }
            if (edit.row == Row.WORK && edit.before && !edit.after) {
                deleted = Math.addExact(deleted, Math.toIntExact(edit.count));
            }
        }
        final var open = commandStrictReads(false, false, deleted);
        final var result = new Reads().add(original);
        for (var entry : open.rows.entrySet()) {
            final var counts = result.rows.get(entry.getKey());
            if (counts == null) {
                throw new IllegalStateException("strict barrier branch lost its actual open projection");
            }
            for (int i = 0; i < 2; i++) {
                counts[i] = Math.subtractExact(counts[i], entry.getValue()[i]);
                if (counts[i] < 0) {
                    throw new IllegalStateException("strict barrier branch changed its actual read multiplicity");
                }
            }
        }
        return result.add(Row.MESSAGE, 2).add(Row.TERMINAL, 2).add(Row.QUEUE, 1);
    }

    private static void commandQueue(List<Edit> rows, boolean fresh) {
        boolean queue = false, work = false;
        for (var edit : rows) {
            queue |= edit.row == Row.QUEUE;
            work |= edit.row.type == TargetTimelineWorkRef.VALUE_TYPE;
        }
        if (work && !queue) {
            rows.add(fresh ? create(Row.QUEUE, 1) : replace(Row.QUEUE, 1));
        }
    }

    private static boolean commandHeadTarget(List<Edit> input) {
        for (var edit : input) {
            if (edit.row == Row.MESSAGE || edit.row.type == TargetTimelineWorkRef.VALUE_TYPE || edit.row == Row.QUEUE) {
                return true;
            }
        }
        return false;
    }

    @SafeVarargs
    private static void commandObserved(
            Capabilities c,
            List<Plan> plans,
            String label,
            long frame,
            int owners,
            int targets,
            int domains,
            List<Edit> business,
            Reads direct,
            boolean due,
            boolean rejected,
            List<Edit>... prior) {
        final var inactive = new Plan(label + "/no-sample", frame, owners, targets, domains)
                .reads(direct)
                .reads(new Reads().add(Row.SLO_STATE, 1, c.slo));
        inactive.nativeClaim =
                count(business, TargetClaimRecord.VALUE_TYPE) > 0 && count(business, TargetOrderState.VALUE_TYPE) == 0;
        for (var previous : prior) {
            inactive.assembly(0, false, false, previous);
        }
        inactive.assembly(0, false, false, business);
        plans.add(inactive);
        if (!c.slo) {
            return;
        }
        final var sampleReads = new Reads()
                .add(Row.SLO_STATE, 1)
                .add(Row.GENERATION, 1)
                .add(Row.AGGREGATE, 1)
                .add(Row.FIRST, 1, false);
        final var empty = new Plan(label + "/active-no-declaration", frame, owners, targets, domains)
                .reads(direct)
                .reads(sampleReads);
        empty.nativeClaim = inactive.nativeClaim;
        for (var previous : prior) {
            empty.assembly(0, false, false, previous);
        }
        empty.assembly(0, false, false, business);
        plans.add(empty);
        for (int samples = 1; samples <= (due ? 2 : 1); samples++) {
            final var additions =
                    edits(create(Row.COMMAND_SAMPLE, 1), create(Row.COMMAND_SAMPLE_REF, 2), replace(Row.GENERATION, 1));
            if (samples == 2) {
                additions.add(create(Row.DUE_SAMPLE, 1));
                additions.add(create(Row.DUE_SAMPLE_REF, 2));
            }
            final var observed = new ArrayList<>(business);
            observed.addAll(additions);
            final var reads = new Reads()
                    .add(direct)
                    .add(sampleReads)
                    .add(replacementReads(additions))
                    .add(Row.COMMAND_SAMPLE, 1, false)
                    .add(Row.COMMAND_SAMPLE_REF, 2, false);
            if (samples == 2) {
                reads.add(Row.DUE_SAMPLE, 1, false).add(Row.DUE_SAMPLE_REF, 2, false);
            }
            if (c.scoped) {
                reads.add(paidScope(c));
            }
            final var plan = new Plan(label + "/samples-" + samples, frame, owners, targets, domains).reads(reads);
            plan.nativeClaim = inactive.nativeClaim;
            for (var previous : prior) {
                plan.assembly(0, false, false, previous);
            }
            plan.assembly(0, false, false, business).assembly(c.scoped ? 1 : 0, false, false, observed);
            plans.add(plan);
        }
    }

    private static void commandReplayFamilies(Capabilities c, List<Plan> plans) {
        final long frame = TargetMigrationArchiveSourceFrame.MAX_COMMAND_FRAME_BYTES;
        final var position = edits(create(Row.POSITION, 1));
        plans.add(new Plan("command/later-logical-replay", frame, 2, 0, 1)
                .reads(new Reads().add(Row.FIRST, 4).add(Row.POSITION, 4, false).add(Row.DESCRIPTOR, 5))
                .assembly(0, false, false, position));
        final var expired =
                new Reads().add(Row.FIRST, 4, false).add(Row.POSITION, 4, false).add(Row.DESCRIPTOR, 1);
        plans.add(new Plan("command/expired-physical/no-sample", frame, 1, 0, 1)
                .reads(expired)
                .reads(new Reads().add(Row.SLO_STATE, 1, c.slo))
                .assembly(0, false, false, position));
        if (c.slo) {
            final var rows = edits(
                    create(Row.POSITION, 1),
                    create(Row.COMMAND_SAMPLE, 1),
                    create(Row.COMMAND_SAMPLE_REF, 1),
                    replace(Row.GENERATION, 1));
            final var reads = new Reads()
                    .add(expired)
                    .add(Row.SLO_STATE, 1)
                    .add(Row.GENERATION, 2)
                    .add(Row.AGGREGATE, 1)
                    .add(Row.FIRST, 2, false)
                    .add(Row.POSITION, 1, false)
                    .add(Row.COMMAND_SAMPLE, 2, false)
                    .add(Row.COMMAND_SAMPLE_REF, 2, false);
            if (c.scoped) {
                reads.add(paidScope(c));
            }
            plans.add(new Plan("command/expired-physical/sample", frame, 1, 0, 1)
                    .reads(reads)
                    .assembly(c.scoped ? 1 : 0, false, false, rows));
        }
        final var systemPosition = edits(create(Row.POSITION, 1));
        plans.add(new Plan(
                        "expiry/later-logical-replay",
                        systemFrame(TargetExpireGenerationBody.MAX_CANONICAL_BYTES),
                        1,
                        0,
                        1)
                .reads(new Reads().add(Row.FIRST, 2).add(Row.POSITION, 4, false).add(Row.DESCRIPTOR, 3))
                .assembly(0, false, false, systemPosition));
    }

    /* Admission / Outcome first-application families. Replay is owned by the common replay family. */
    private static List<Plan> attemptFamilies(Capabilities c) {
        final var plans = new ArrayList<Plan>();
        for (int transport = 0; transport < 4; transport++) {
            for (boolean strict : new boolean[] {false, true}) {
                if (transport == 2 && strict) {
                    continue;
                } // Native managed handoff has no strict-order admission.
                if (transport == 3) {
                    for (int fresh = 0; fresh < 4; fresh++) {
                        plans.add(attemptAdmission(c, transport, strict, fresh));
                    }
                } else {
                    plans.add(attemptAdmission(c, transport, strict, 0));
                }
            }
        }
        plans.addAll(attemptOutcomeFamilies(c, c.sourceFrameBytes, c.sourceFrameBytes));
        return plans;
    }

    /** transport: original v3, materialized ordinary v4, Native v5, or K2 v6. */
    private static Plan attemptAdmission(Capabilities c, int transport, boolean strict, int fresh) {
        final boolean materialized = transport != 0, nativeHandoff = transport == 2, kafka = transport == 3;
        final boolean protectionFresh = kafka && (fresh & 1) != 0, anchorFresh = kafka && (fresh & 2) != 0;
        final long body = transport == 0
                ? TargetPublishAdmissionBody.MAX_CANONICAL_BYTES
                : nativeHandoff
                        ? TargetPublishAdmissionBody.MAX_NATIVE_MATERIALIZED_BYTES
                        : kafka
                                ? TargetPublishAdmissionBody.MAX_KAFKA_RECEIPT_BYTES
                                : TargetPublishAdmissionBody.MAX_MATERIALIZED_BYTES;
        final var plan = new Plan(
                "admission-" + transport + "-strict-" + strict + "-new-artifacts-" + fresh,
                systemFrame(body),
                protectionFresh ? 3 : 2,
                protectionFresh ? 2 : 1,
                1);
        plan.nativeClaim = nativeHandoff;
        // The accounting sample callback obtains the same stamp again; the common prelude counts the first call.
        final var reads = systemPrelude()
                .add(Row.FIRST, 1, false)
                .add(Row.POSITION, 1, false)
                .add(Row.MESSAGE, 2)
                .add(Row.CLAIM, 2)
                .add(Row.CLAIM_CHARGE, 1)
                .add(Row.DESCRIPTOR, 2)
                .add(Row.ATTEMPT, 1, false)
                .add(attemptSendReads(c))
                .add(attemptSendReads(c));
        if (materialized) {
            reads.add(Row.BINDING, 1)
                    .add(Row.IDENTITY, 1)
                    .add(Row.CHANNEL, 1)
                    .add(Row.CHANNEL_SLOT, 1)
                    .add(Row.QUEUE, 1);
            if (c.migration) {
                reads.add(attemptImportedBindingReads());
            }
        }
        if (nativeHandoff) {
            reads.add(Row.QUEUE, 1).add(Row.NATIVE_SCOPE, 1).add(attemptNativeTrustReads());
        }
        if (strict) {
            reads.add(Row.ORDER, 1).add(Row.QUEUE, 1);
        }
        reads.add(attemptPayload(c), 1).add(attemptAdmissionSloReads(c));
        final var extra = systemResults();
        extra.add(delete(Row.CLAIM, 1));
        extra.add(delete(Row.CLAIM_CHARGE, 1));
        extra.add(create(Row.ATTEMPT, 1));
        if (c.payloadV2) {
            extra.add(replace(Row.PAYLOAD, 1));
        }
        if (kafka) {
            reads.add(Row.KAFKA_SLOT, 1)
                    .add(Row.KAFKA_MAPPING, 1, false)
                    .add(Row.CHANNEL_CONTROL, 1)
                    .add(Row.DESCRIPTOR, 2)
                    .add(Row.EVIDENCE_OWNER, 1, !protectionFresh)
                    .add(Row.EVIDENCE_OWNER, 1, !anchorFresh)
                    .add(Row.EVIDENCE_REF, 3, false);
            extra.add(create(Row.KAFKA_MAPPING, 1));
            // A retained settled slot is the larger legal branch; a first absent slot has the same after row.
            extra.add(replace(Row.KAFKA_SLOT, 1));
            extra.add(create(Row.EVIDENCE_REF, 3));
            if (protectionFresh) {
                extra.add(create(Row.EVIDENCE_OWNER, 1));
            }
            if (anchorFresh) {
                extra.add(create(Row.EVIDENCE_OWNER, 1));
            }
        }
        reads.add(replacementReads(extra));
        // CLAIMED -> PUBLISHING has no timeline on either side; its unchanged EXPIRY is still checked.
        reads.add(Row.MESSAGE, 1).add(Row.EXPIRY, 1);
        final var projected = new ArrayList<>(extra);
        projected.add(replace(Row.MESSAGE, 1));
        if (strict) {
            reads.add(Row.ORDER, 1).add(Row.MESSAGE, 1).add(Row.QUEUE, 1);
            projected.add(
                    replace(Row.ORDER, 1)); // Both exact barriers are nonempty, so no serviceable head is written.
        }
        return plan.reads(reads).reads(headReads(projected, 0, 0, false)).assembly(0, false, false, projected);
    }

    private static Row attemptPayload(Capabilities c) {
        return c.payloadV2 ? Row.PAYLOAD : Row.PAYLOAD_V1;
    }

    private static Reads attemptSendReads(Capabilities c) {
        return new Reads().add(Row.MIGRATION, 1, c.migration).add(Row.MIGRATION_GUARD, 1, c.migration);
    }

    private static Reads attemptImportedBindingReads() {
        // MembershipStoreAuthority: actual A/first/POS, the immutable binding and the four complete mapped execution
        // objects.
        return new Reads()
                .add(Row.MIGRATION, 1)
                .add(Row.FIRST, 1)
                .add(Row.POSITION, 1)
                .add(Row.BINDING, 1)
                .add(Row.MIGRATION_ORIGIN, 5)
                .add(Row.MIGRATION_PREFIX, 1)
                .add(Row.IDENTITY, 1)
                .add(Row.DISPATCH, 2)
                .add(Row.CONTROL, 1);
    }

    private static Reads attemptNativeTrustReads() {
        // requireTrusted -> activation(scope,event) and publisher(scope,installation,closure).
        return new Reads().add(Row.NATIVE_SCOPE, 2).add(Row.NATIVE_CONTROL, 3);
    }

    private static Reads attemptAdmissionSloReads(Capabilities c) {
        if (!c.slo) {
            return new Reads().add(Row.SLO_STATE, 1, false);
        }
        // Admission only verifies an existing first-attempt Start; it never appends samples or changes generation
        // counts.
        return new Reads()
                .add(Row.SLO_STATE, 1)
                .add(Row.GENERATION, 1)
                .add(Row.MESSAGE, 1)
                .add(Row.SAMPLE_REF, TargetSloObservationPolicy.MAX_DECLARATIONS);
    }

    /** The actual legal Source-frame bound is supplied separately from these unchanged business shapes. */
    private static List<Plan> attemptOutcomeFamilies(
            Capabilities c, long outcomeFrameBytes, long resolutionFrameBytes) {
        final var plans = new ArrayList<Plan>();
        for (boolean strict : new boolean[] {false, true}) {
            for (int phase = 0; phase < 4; phase++) {
                plans.add(attemptUnknown(c, outcomeFrameBytes, strict, phase));
            }
            for (int transport = 0; transport < 3; transport++) {
                if (strict && transport == 1) {
                    continue;
                } // Native handoff is BEST_EFFORT.
                final int recoveries = transport == 2 ? 2 : 1;
                for (int recovery = 0; recovery < recoveries; recovery++) {
                    for (int phase = 0; phase < 4; phase++) {
                        plans.add(attemptDefinitive(
                                c, outcomeFrameBytes, strict, transport, recovery != 0, false, phase));
                    }
                    for (int phase = 0; phase < 9; phase++) {
                        plans.add(attemptDefinitive(
                                c, resolutionFrameBytes, strict, transport, recovery != 0, true, phase));
                    }
                }
            }
        }
        return plans;
    }

    /** phase: current no retry, current uncertain retry, current terminal, older terminal generation. */
    private static Plan attemptUnknown(Capabilities c, long frameBytes, boolean strict, int phase) {
        final boolean retry = phase == 1, terminal = phase >= 2, historical = phase == 3;
        final var plan = new Plan("outcome-unknown-" + phase + "-strict-" + strict, frameBytes, 2, 1, 1);
        final var reads = systemPrelude()
                .add(Row.ATTEMPT, 1)
                .add(attemptGenerationReads(strict, terminal, historical))
                .add(attemptRetryReads());
        if (retry) {
            reads.add(Row.QUEUE, 1);
        }
        final var extra = systemResults();
        extra.add(replace(Row.ATTEMPT, 1));
        if (terminal) {
            reads.add(attemptPayload(c), 1);
            extra.add(replace(Row.TERMINAL, 1));
        }
        reads.add(replacementReads(extra));
        final var projected = new ArrayList<>(extra);
        if (!historical) {
            projected.add(replace(Row.MESSAGE, 1));
            reads.add(Row.MESSAGE, 1);
            if (!terminal) {
                reads.add(Row.EXPIRY, 1);
            }
        }
        if (retry) {
            projected.add(create(Row.WORK, 1));
            reads.add(Row.WORK, 1, false).add(Row.QUEUE, 1).add(Row.IDENTITY, 1);
        }
        if (strict) {
            reads.add(attemptOrderProjectionReads(historical, false));
            projected.add(replace(Row.ORDER, 1));
        }
        final int affected = retry && !strict ? 1 : 0;
        reads.add(headReads(projected, affected, affected, false));
        if (affected != 0) {
            projected.add(replace(Row.QUEUE, 1));
        }
        return plan.reads(reads).assembly(0, false, false, projected);
    }

    /** Definitive phases 0..3: terminalize, retry, already terminal, historical.
     * Resolution adds 4 claim/terminal, 5 timeline/terminal, 6 claim/retry, 7 timeline/retry, 8 preserve publisher. */
    private static Plan attemptDefinitive(
            Capabilities c,
            long frameBytes,
            boolean strict,
            int transport,
            boolean recovery,
            boolean resolution,
            int phase) {
        final boolean historical = phase == 3, wasTerminal = phase == 2 || historical;
        final boolean revokeClaim = resolution && (phase == 4 || phase == 6);
        final boolean beforeWork = resolution && (phase == 5 || phase == 7),
                afterWork = phase == 1 || phase == 6 || phase == 7;
        final boolean preservePublisher = resolution && phase == 8;
        final boolean newTerminal = !wasTerminal && !afterWork && !preservePublisher;
        final boolean clearBarrier =
                !preservePublisher; // This branch settles the final obligation; retained-barrier cases write fewer
        // heads.
        final var plan = new Plan(
                (resolution ? "resolution-" : "outcome-definitive-") + phase + "-strict-" + strict + "-transport-"
                        + transport + "-recovery-" + recovery,
                frameBytes,
                2,
                1,
                1);
        plan.cursors = resolution;
        final var reads = systemPrelude()
                .add(Row.ATTEMPT, 1)
                .add(attemptGenerationReads(strict, wasTerminal, historical))
                .add(attemptRetryReads())
                .add(Row.FIRST, 1)
                .add(Row.DESCRIPTOR, 1); // evidence Admission plus current/first retry Admission proofs
        if (resolution) {
            reads.add(Row.POSITION, 1).add(Row.FIRST, 1).add(Row.DESCRIPTOR, 1);
        }
        if (transport == 1) {
            reads.add(attemptNativeTrustReads());
        }
        // The accepted negative branch may read the current queue before a closed/permanent/retry decision.
        if (!wasTerminal) {
            reads.add(Row.QUEUE, 1);
        }
        final var extra = systemResults();
        extra.add(replace(Row.ATTEMPT, 1));
        if (revokeClaim) {
            reads.add(Row.MESSAGE, 1).add(Row.CLAIM, 1).add(Row.CLAIM_CHARGE, 1);
            extra.add(delete(Row.CLAIM, 1));
            extra.add(delete(Row.CLAIM_CHARGE, 1));
        }
        if (transport == 2) {
            reads.add(Row.KAFKA_SLOT, 1).add(Row.KAFKA_MAPPING, 1);
            extra.add(replace(Row.KAFKA_SLOT, 1));
            if (recovery) {
                reads.add(attemptRecoveryAdoptionReads())
                        .add(Row.RECOVERY_OWNER, 1, false)
                        .add(Row.RECOVERY_REF, 2, false);
                extra.add(create(Row.RECOVERY_OWNER, 1));
                extra.add(create(Row.RECOVERY_REF, 2));
            }
        }
        reads.add(attemptPayload(c), 1);
        if (c.payloadV2) {
            if (newTerminal || wasTerminal) {
                reads.add(Row.MIGRATION_GUARD, 1, c.migration);
            }
            extra.add(replace(Row.PAYLOAD, 1));
        } else if (newTerminal || wasTerminal) {
            // The original finite v1-prefix path may perform its actual last retention, without creating a summary.
            extra.add(replace(Row.PAYLOAD_V1, 1));
        }
        if (wasTerminal) {
            reads.add(attemptPayload(c), 1);
            extra.add(replace(Row.TERMINAL, 1));
        } else if (newTerminal) {
            reads.add(Row.TERMINAL, 1, false);
            extra.add(create(Row.TERMINAL, 1));
        }
        reads.add(replacementReads(extra));
        final var projected = new ArrayList<>(extra);
        if (!historical) {
            projected.add(replace(Row.MESSAGE, 1));
            reads.add(Row.MESSAGE, 1);
            if (!wasTerminal) {
                reads.add(Row.EXPIRY, 1);
                if (newTerminal) {
                    projected.add(delete(Row.EXPIRY, 1));
                }
            }
        }
        if (beforeWork) {
            projected.add(delete(Row.WORK, 1));
            reads.add(Row.WORK, 1).add(Row.QUEUE, 1).add(Row.IDENTITY, 1);
        }
        if (afterWork) {
            projected.add(create(Row.WORK, 1));
            reads.add(Row.WORK, 1, false).add(Row.QUEUE, 1).add(Row.IDENTITY, 1);
        }
        if (preservePublisher) {
            reads.add(attemptSendReads(c));
        }
        if (strict) {
            reads.add(attemptOrderProjectionReads(historical, clearBarrier));
            projected.add(replace(Row.ORDER, 1));
            if (clearBarrier) {
                projected.add(create(Row.ORDER_HEAD, 1));
            } // one serviceable ORDER_HEAD, not another Message
        }
        final int affected = (beforeWork || afterWork) && !strict || strict && clearBarrier ? 1 : 0;
        reads.add(headReads(projected, affected, affected, false));
        if (affected != 0) {
            projected.add(replace(Row.QUEUE, 1));
        }
        return plan.reads(reads).assembly(0, false, false, projected);
    }

    private static Reads attemptGenerationReads(boolean strict, boolean terminal, boolean historical) {
        final var reads = new Reads().add(Row.MESSAGE, 1);
        if (terminal) {
            reads.add(Row.TERMINAL, 1);
        }
        if (strict && historical) {
            reads.add(Row.ORDER, 1).add(Row.QUEUE, 1);
        }
        return reads;
    }

    private static Reads attemptRetryReads() {
        return new Reads().add(Row.FIRST, 2).add(Row.DESCRIPTOR, 2).add(Row.BINDING, 1);
    }

    private static Reads attemptOrderProjectionReads(boolean historical, boolean clearBarrier) {
        // Store order decode + MessageStore order-before validation; the old exact barrier has no head.
        final var reads = new Reads().add(Row.ORDER, 2).add(Row.QUEUE, 2).add(Row.MESSAGE, 1);
        if (historical) {
            reads.add(Row.TERMINAL, 1);
        }
        if (clearBarrier) {
            reads.add(Row.WORK, 1).add(Row.MESSAGE, 1).add(Row.ORDER_HEAD, 1, false);
        } else if (historical) {
            reads.add(Row.MESSAGE, 1);
        } // Current primary is not the historical terminal overlay.
        return reads;
    }

    private static Reads attemptRecoveryAdoptionReads() {
        // ExternalEvidenceStore.readAdoption: exact mapping/Attempt, original Admission first/POS and both
        // owner/reference chains.
        return new Reads()
                .add(Row.KAFKA_MAPPING, 1)
                .add(Row.ATTEMPT, 1)
                .add(Row.FIRST, 1)
                .add(Row.POSITION, 1)
                .add(Row.DESCRIPTOR, 4)
                .add(Row.EVIDENCE_REF, 3)
                .add(Row.EVIDENCE_OWNER, 2);
    }

    private static final List<ProtocolTuple> ACTIVATIONS = List.of(
            ProtocolTuple.targetEvidenceRetirement(),
            ProtocolTuple.targetResourceReclaim(),
            ProtocolTuple.targetInlinePayloadReclaim(),
            ProtocolTuple.targetSelfContainedInlinePayloadReclaim(),
            ProtocolTuple.targetNativeRetainedReclaim(),
            ProtocolTuple.targetObjectPayloadReclaim(),
            ProtocolTuple.targetUncommittedObjectReclaim(),
            ProtocolTuple.targetAnchorAllocationReclaim(),
            ProtocolTuple.targetIncarnationReclaim(),
            ProtocolTuple.targetDuplicatePositionReclaim(),
            ProtocolTuple.targetMigrationObjectPayloadReclaim(),
            ProtocolTuple.targetMigrationUncommittedObjectReclaim(),
            ProtocolTuple.targetMigrationRetireIntent(),
            ProtocolTuple.targetMigrationRetireConfirm(),
            ProtocolTuple.targetPayloadObligationSummary(),
            ProtocolTuple.targetMigrationLogicalFirstArchive(),
            ProtocolTuple.targetMigrationFullSourceArchive(),
            ProtocolTuple.targetSloObservation(),
            ProtocolTuple.targetSloWriterEpoch(),
            ProtocolTuple.targetSloScopedWriterEpoch(),
            ProtocolTuple.targetSloObservationEpochPolicy(),
            ProtocolTuple.targetSloScopedObservation(),
            ProtocolTuple.targetParentControlCredit(),
            ProtocolTuple.targetScopeControlObligation(),
            ProtocolTuple.targetScopeControlObligationFrozen(),
            ProtocolTuple.targetScopeControlObligationExposure());

    private static final class Capabilities {
        int generation;
        boolean migration, writer, slo, scoped, payloadV2;
        /** -1 is an unbound formal F variable, never a claim that a legal input is empty or small. */
        long sourceFrameBytes = -1;

        Capabilities copy() {
            final var copy = new Capabilities();
            copy.generation = generation;
            copy.migration = migration;
            copy.writer = writer;
            copy.slo = slo;
            copy.scoped = scoped;
            copy.payloadV2 = payloadV2;
            copy.sourceFrameBytes = sourceFrameBytes;
            return copy;
        }

        Capabilities covering(Capabilities other) {
            final var copy = copy();
            copy.generation = Math.max(generation, other.generation);
            copy.migration |= other.migration;
            copy.writer |= other.writer;
            copy.slo |= other.slo;
            copy.scoped |= other.scoped;
            copy.payloadV2 |= other.payloadV2;
            copy.sourceFrameBytes = Math.max(sourceFrameBytes, other.sourceFrameBytes);
            return copy;
        }

        Capabilities include(ProtocolTuple tuple) {
            generation = Math.max(generation, tupleGeneration(tuple));
            writer |= writerTuple(tuple);
            slo |= writerTuple(tuple)
                    || tuple.equals(ProtocolTuple.targetSloObservation())
                    || tuple.equals(ProtocolTuple.targetSloScopedObservation())
                    || tuple.equals(ProtocolTuple.targetSloObservationEvent())
                    || tuple.equals(ProtocolTuple.targetSloObservationEventEvidence())
                    || tuple.equals(ProtocolTuple.targetSloObservationEventAssigned())
                    || tuple.equals(ProtocolTuple.targetSloObservationCustody());
            scoped |= tuple.equals(ProtocolTuple.targetSloScopedWriterEpoch())
                    || tuple.equals(ProtocolTuple.targetSloScopedObservation())
                    || tuple.equals(ProtocolTuple.targetParentControlCredit())
                    || tuple.equals(ProtocolTuple.targetScopeControlObligation())
                    || tuple.equals(ProtocolTuple.targetScopeControlObligationFrozen())
                    || tuple.equals(ProtocolTuple.targetScopeControlObligationExposure());
            payloadV2 |= tuple.equals(ProtocolTuple.targetPayloadObligationSummary());
            return this;
        }
    }

    public static Bounds basic() {
        return closedFamilies(new Capabilities());
    }

    /** Reads actual immutable Native permissions, never the optional producer/provider configuration. */
    public static Bounds required(TargetStoreBackend.Reader actual) {
        return closedFamilies(selectCapabilities(actual));
    }

    public static Bounds required(
            TargetStoreBackend.Reader actual,
            com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.SourceFrameContract genuineM) {
        Objects.requireNonNull(genuineM).requireCurrent(actual);
        final var capabilities = selectCapabilities(actual);
        capabilities.sourceFrameBytes = genuineM.maximumFrameBytes();
        genuineM.requireCurrent(actual);
        return closedFamilies(capabilities, genuineM);
    }

    private static Capabilities selectCapabilities(TargetStoreBackend.Reader actual) {
        Objects.requireNonNull(actual);
        final var capabilities = new Capabilities();
        // This is a same-view root/bookkeeping/accounting join, not a whole-ledger audit.
        final var selected = actual.retirementRootProof();
        final var initialRaw = actual.get(ColumnFamily.META, KeyCodec.metaFixed(10));
        if (initialRaw != null) {
            final var initial = TargetInitialControlSnapshot.decode(
                    TargetValueEnvelope.decode(initialRaw, 1).payload());
            if (!initial.root().equals(selected.root().identity())
                    || !Arrays.equals(initial.input().lineage(), selected.root().recoveryLineage())) {
                throw new IllegalStateException("Source envelope changed actual Root/accounting/lineage");
            }
            for (var tuple : initial.input().protocolTuples()) {
                capabilities.include(tuple);
            }
        }
        for (var tuple : ACTIVATIONS) {
            final byte[] key = TargetProtocolActivationRecord.key(tuple), raw = actual.get(ColumnFamily.META, key);
            if (raw == null) {
                continue;
            }
            final var activation = TargetProtocolActivationRecord.decode(
                    TargetValueEnvelope.decode(raw, TargetProtocolActivationRecord.VALUE_TYPE)
                            .payload());
            if (!Arrays.equals(key, activation.key())
                    || !actual.shardId()
                            .equals(activation.body().payload().scope().shard())) {
                throw new IllegalStateException("Source service selector changed actual activation key/Shard");
            }
            capabilities.generation = Math.max(
                    capabilities.generation, activation.readers().descriptor().schemaVersion());
            capabilities.include(tuple);
        }
        final byte[] migrationRaw = actual.get(ColumnFamily.META, TargetMigrationRoot.keyFor(actual.shardId()));
        if (migrationRaw != null) {
            final var root =
                    TargetMigrationRoot.decode(TargetValueEnvelope.decode(migrationRaw, TargetMigrationRoot.VALUE_TYPE)
                            .payload());
            if (!root.plan().scope().shard().equals(actual.shardId())) {
                throw new IllegalStateException("Source envelope changed actual migration Root");
            }
            if (!root.owner().identity().equals(selected.root().identity())
                    || !root.owner().accounting().equals(selected.root().accounting())
                    || !Arrays.equals(root.plan().newLineage(), selected.root().recoveryLineage())) {
                throw new IllegalStateException("Source envelope changed original migration Root");
            }
            capabilities.migration = true;
            if (root.activated()) {
                includeActivation(capabilities, root.activation());
            }
            // PREPARED always has Root schema1. Only its actual staged A, not that outer version,
            // determines A1/A2, PayloadOwner reader schema and original Root issuer extensions.
            final byte[] attemptRaw = actual.get(
                    ColumnFamily.META,
                    TargetMigrationActivationAttempt.keyFor(root.plan().migrationId()));
            if (attemptRaw != null) {
                final var attempt = TargetMigrationActivationAttempt.decode(
                        TargetValueEnvelope.decode(attemptRaw, TargetMigrationActivationAttempt.VALUE_TYPE)
                                .payload());
                attempt.requirePlan(root.plan());
                includeActivation(capabilities, attempt.input());
            }
        }
        requireWriterKernel(actual, capabilities.writer);
        return capabilities;
    }

    private static void includeActivation(Capabilities capabilities, SystemMutation input) {
        final var body = TargetMigrationControlBody.decode(input.canonicalBody());
        body.requireInput(input);
        if (!(body.request() instanceof TargetMigrationActivateRequest request)) {
            throw new IllegalStateException("Source service selector did not read original A");
        }
        capabilities.payloadV2 |= request.requiredPayloadOwnerSchema() == 2;
        if (request.rootIssuerAuthorization() != null) {
            for (var tuple : request.rootIssuerAuthorization().allowedTuples()) {
                capabilities.include(tuple);
            }
        }
    }

    private static void requireWriterKernel(TargetStoreBackend.Reader actual, boolean writer) {
        if (writer && actual.writerOriginContext() == null) {
            throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable(
                    "actual Source writer service requires its protected hot-origin kernel");
        }
    }

    public static Bounds activating(TargetStoreBackend.Reader actual, TargetProtocolActivationDescriptor descriptor) {
        final var prospective =
                new Capabilities().include(Objects.requireNonNull(descriptor).tuple());
        prospective.generation = descriptor.schemaVersion();
        final var computed = required(actual).covering(closedFamilies(prospective));
        requireWriterKernel(actual, computed.writerOriginRequired());
        return computed;
    }

    public static Bounds activating(
            TargetStoreBackend.Reader actual, TargetProtocolActivationDescriptor descriptor, Bounds originalComputed) {
        final var original = Objects.requireNonNull(originalComputed);
        original.requireFrameQualification();
        final var selected = required(actual, original.frameContract);
        final var prospective = selected.capabilities
                .copy()
                .include(Objects.requireNonNull(descriptor).tuple());
        prospective.generation = Math.max(prospective.generation, descriptor.schemaVersion());
        requireWriterKernel(actual, prospective.writer);
        return selected.covering(closedFamilies(prospective, original.frameContract));
    }

    private static com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable unavailable(
            String message) {
        return new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable(message);
    }

    private static boolean writerTuple(ProtocolTuple tuple) {
        return tuple.equals(ProtocolTuple.targetSloWriterEpoch())
                || tuple.equals(ProtocolTuple.targetSloScopedWriterEpoch())
                || tuple.equals(ProtocolTuple.targetSloObservationEpochPolicy())
                || tuple.equals(ProtocolTuple.targetSloObservationEpochEvent());
    }

    /** Actual mutually exclusive ordinary Control and certification Source branches. */
    private static List<Plan> controlFamilies(Capabilities c) {
        final var plans = new ArrayList<Plan>();
        final var results = systemResults();
        final int domains = TargetQueueState.MAX_DOMAIN_SLOTS;

        final var close = edits(replace(Row.QUEUE, 1), create(Row.CLOSE, 1), create(Row.CLOSE_CURSOR, 1));
        close.addAll(results);
        final var closeReads = new Reads()
                .add(Row.FENCE, 1)
                .add(Row.QUEUE, 1)
                .add(Row.CLOSE, 1)
                .add(Row.CLOSE, 1, false)
                .add(Row.CLOSE_CURSOR, 1, false);
        plans.add(controlPlan(
                "control/close",
                controlFrame(TargetCloseBody.MAX_CANONICAL_BYTES),
                2,
                1,
                domains,
                0,
                false,
                close,
                closeReads.add(headReads(close, 1, domains, true))));
        plans.add(controlPlan(
                "control/close-rejected",
                controlFrame(TargetCloseBody.MAX_CANONICAL_BYTES),
                1,
                0,
                1,
                0,
                false,
                results,
                new Reads().add(Row.FENCE, 1).add(Row.QUEUE, 1).add(Row.CLOSE, 1)));

        final var member = edits(create(Row.IDENTITY, 1), create(Row.POLICY, 1), create(Row.MEMBER, 1));
        member.addAll(results);
        plans.add(controlPlan(
                "control/membership-issue",
                controlFrame(TargetMembershipControlBody.MAX_CANONICAL_BYTES),
                2,
                1,
                1,
                0,
                false,
                member,
                new Reads()
                        .add(Row.IDENTITY, 1, false)
                        .add(Row.POLICY, 1, false)
                        .add(Row.MEMBER, 1, false)));
        final var memberClose = edits(create(Row.MEMBER_CLOSE, 1));
        memberClose.addAll(results);
        plans.add(controlPlan(
                "control/membership-close",
                controlFrame(TargetMembershipControlBody.MAX_CANONICAL_BYTES),
                2,
                1,
                1,
                0,
                false,
                memberClose,
                controlMembershipReads().add(Row.IDENTITY, 1).add(Row.POLICY, 1).add(Row.MEMBER_CLOSE, 1, false)));
        plans.add(controlPlan(
                "control/membership-already-closed-or-rejected",
                controlFrame(TargetMembershipControlBody.MAX_CANONICAL_BYTES),
                1,
                0,
                1,
                0,
                false,
                results,
                controlMembershipReads().add(Row.IDENTITY, 1).add(Row.POLICY, 1)));

        final long nativeFrame = controlFrame(TargetNativePolicyControlBody.MAX_CANONICAL_BYTES);
        final var nativeNew = edits(create(Row.NATIVE_SCOPE, 1), create(Row.NATIVE_CONTROL, 1));
        nativeNew.addAll(results);
        final var nativeScope = controlNativeScopeReads().add(Row.NATIVE_SCOPE, 1, false);
        final var nativeApprove = new Reads()
                .add(Row.NATIVE_CONTROL, 2, false)
                .add(controlMembershipReads())
                .add(Row.IDENTITY, 1);
        plans.add(controlPlan(
                "control/native-new-scope-member",
                nativeFrame,
                2,
                1,
                1,
                0,
                false,
                nativeNew,
                nativeScope.add(nativeApprove).add(Row.NATIVE_SCOPE, 1, false).add(Row.NATIVE_CONTROL, 1, false)));
        final var nativeRecord = edits(create(Row.NATIVE_CONTROL, 1));
        nativeRecord.addAll(results);
        plans.add(controlPlan(
                "control/native-existing-scope",
                nativeFrame,
                2,
                1,
                1,
                0,
                false,
                nativeRecord,
                controlNativeScopeReads()
                        .add(Row.NATIVE_SCOPE, 1)
                        .add(Reads.either(
                                new Reads().add(Row.NATIVE_CONTROL, 2),
                                new Reads()
                                        .add(Row.NATIVE_CONTROL, 2, false)
                                        .add(controlMembershipReads())
                                        .add(Row.IDENTITY, 1),
                                new Reads()
                                        .add(Row.NATIVE_CONTROL, 1, false)
                                        .add(Row.NATIVE_SCOPE, 1)
                                        .add(Row.NATIVE_CONTROL, 2)))
                        .add(Row.NATIVE_CONTROL, 1, false)));
        plans.add(controlPlan(
                "control/native-already-closed-or-rejected",
                nativeFrame,
                1,
                0,
                1,
                0,
                false,
                results,
                controlNativeScopeReads()
                        .add(Row.NATIVE_SCOPE, 1)
                        .add(Row.NATIVE_CONTROL, 2)
                        .add(controlMembershipReads())));

        final long channelFrame = controlFrame(TargetChannelControlBody.MAX_CANONICAL_BYTES);
        final var channel = edits(create(Row.CHANNEL_CONTROL, 1), replace(Row.CHANNEL_SLOT, 1), create(Row.CHANNEL, 1));
        channel.addAll(results);
        final var channelReads = controlChannelReads()
                .add(Row.QUEUE, 1)
                .add(Row.CHANNEL, 1, false)
                .add(Row.CHANNEL_CONTROL, 1, false)
                .add(Row.CHANNEL, 1, false);
        plans.add(controlPlan("control/channel-open-or-renew", channelFrame, 2, 1, 1, 0, false, channel, channelReads));
        final var channelStop = edits(create(Row.CHANNEL_CONTROL, 1), replace(Row.CHANNEL_SLOT, 1));
        channelStop.addAll(results);
        plans.add(controlPlan(
                "control/channel-stop",
                channelFrame,
                2,
                1,
                1,
                0,
                false,
                channelStop,
                controlChannelReads().add(Row.CHANNEL_CONTROL, 1, false)));
        plans.add(controlPlan(
                "control/channel-rejected",
                channelFrame,
                1,
                0,
                1,
                0,
                false,
                results,
                controlChannelReads().add(Row.QUEUE, 1).add(Row.CHANNEL, 1)));

        final var activation = edits(create(Row.ACTIVATION, 1));
        activation.addAll(results);
        final long activationFrame = controlFrame(TargetProtocolActivationBody.MAX_BYTES);
        final var activationProof = controlActivationRootReads(c);
        final var activated = controlPlan(
                "control/protocol-activate",
                activationFrame,
                1,
                0,
                1,
                0,
                false,
                activation,
                new Reads().add(activationProof).add(Row.ACTIVATION, 1, false));
        activated.activation = true;
        plans.add(activated);
        plans.add(controlPlan(
                "control/protocol-rejected",
                activationFrame,
                1,
                0,
                1,
                0,
                false,
                results,
                new Reads().add(activationProof).add(Row.ACTIVATION, 1).add(activationApplied(c))));

        final long grantFrame = controlFrame(TargetQuotaGrantControlBody.MAX_CANONICAL_BYTES);
        final var grant = edits(replace(Row.GRANT, 1));
        grant.addAll(results);
        plans.add(controlPlan(
                "control/root-grant-update", grantFrame, 1, 0, 1, 0, false, grant, new Reads().add(Row.GRANT, 2)));
        plans.add(controlPlan(
                "control/target-grant-update",
                grantFrame,
                1,
                0,
                1,
                0,
                false,
                grant,
                new Reads().add(Row.GRANT, 2).add(Row.TOTAL, 2)));
        final var allocate = edits(create(Row.DESCRIPTOR, 1), create(Row.GRANT, 1));
        allocate.addAll(results);
        plans.add(controlPlan(
                "control/first-target-grant",
                grantFrame,
                2,
                1,
                1,
                0,
                false,
                allocate,
                new Reads().add(Row.GRANT, 2, false).add(Row.TOTAL, 2, false).add(Row.DESCRIPTOR, 1, false)));
        plans.add(controlPlan(
                "control/grant-rejected",
                grantFrame,
                1,
                0,
                1,
                0,
                false,
                results,
                new Reads().add(Row.GRANT, 1).add(Row.TOTAL, 1)));
        final var bootstrap = edits(create(Row.DESCRIPTOR, 1), create(Row.GRANT, 1), create(Row.INITIAL, 1));
        bootstrap.addAll(results);
        // The original whole-Store emptiness proof is qualified separately; its initial row shape remains closed.
        plans.add(new Plan("control/root-bootstrap-with-initial", grantFrame, 1, 0, 1)
                .reads(new Reads()
                        .add(Row.FIRST, 1, false)
                        .add(Row.POSITION, 1, false)
                        .add(replacementReads(bootstrap)))
                .assembly(0, false, true, bootstrap));

        if (c.slo) {
            controlSloFamilies(c, plans);
        }
        if (c.scoped) {
            controlFundingFamilies(c, plans);
        }
        return plans;
    }

    /** Root plus an actual Target allocation is two primary/tenant pairs, never four guessed counters. */
    private static Plan controlPlan(
            String label,
            long frame,
            int owners,
            int targets,
            int domains,
            int references,
            boolean scopedSource,
            List<Edit> business,
            Reads direct) {
        return new Plan(label, frame, owners, targets, domains)
                .reads(systemPrelude())
                .reads(direct)
                .reads(replacementReads(business))
                .assembly(references, scopedSource, false, business);
    }

    private static Reads controlMembershipReads() {
        return new Reads().add(Row.MEMBER, 1).add(Row.MEMBER_CLOSE, 1).add(Row.POLICY, 1);
    }

    private static Reads controlNativeScopeReads() {
        return new Reads().add(Row.IDENTITY, 1).add(Row.DESCRIPTOR, 1).add(Row.GRANT, 1);
    }

    private static Reads controlChannelReads() {
        return new Reads().add(Row.CHANNEL_SLOT, 1).add(Row.CHANNEL_CONTROL, 1).add(Row.CHANNEL, 1);
    }

    private static Reads controlActivationRootReads(Capabilities c) {
        return c.migration
                ? new Reads()
                        .add(Row.MIGRATION, 1)
                        .add(Row.MIGRATION_ATTEMPT, 1)
                        .add(Row.FIRST, 1)
                        .add(Row.POSITION, 1)
                : new Reads()
                        .add(Row.INITIAL, 1)
                        .add(Row.DESCRIPTOR, 1)
                        .add(Row.FIRST, 1)
                        .add(Row.POSITION, 1);
    }

    private static void controlSloFamilies(Capabilities c, List<Plan> plans) {
        final var results = systemResults();
        final int references = c.scoped ? 2 : 0;
        final long modeFrame = controlFrame(
                c.writer || c.scoped
                        ? TargetSloObservationControlBody.MAX_CANONICAL_BYTES
                        : TargetSloObservationControlBody.MAX_V1_CANONICAL_BYTES);
        final var modeReads =
                new Reads().add(activationOrigin(c)).add(Row.SLO_STATE, 1).add(Row.GENERATION, 1);
        if (c.scoped) {
            modeReads.add(paidScope(c));
        }
        if (c.writer) {
            modeReads.add(Row.WRITER, 1).add(writerHot(c));
        }
        final var enable = edits(create(Row.GENERATION, 1), replace(Row.SLO_STATE, 1));
        enable.addAll(results);
        plans.add(controlPlan(
                "slo/enable",
                modeFrame,
                1,
                0,
                1,
                references,
                true,
                enable,
                new Reads().add(modeReads).add(Row.GENERATION, 1, false)));
        final var stop = edits(replace(Row.GENERATION, 1), replace(Row.SLO_STATE, 1));
        stop.addAll(results);
        plans.add(controlPlan(
                "slo/stop",
                modeFrame,
                1,
                0,
                1,
                references,
                true,
                stop,
                new Reads()
                        .add(activationOrigin(c))
                        .add(Row.SLO_STATE, 1)
                        .add(Row.GENERATION, 1)
                        .add(c.scoped ? paidScope(c) : new Reads())));
        plans.add(controlPlan("slo/mode-rejected", modeFrame, 1, 0, 1, c.scoped ? 1 : 0, true, results, modeReads));

        if (c.writer) {
            final long writerFrame = controlFrame(TargetSloWriterControlBody.MAX_CANONICAL_BYTES);
            final var writer = edits(create(Row.WRITER, 1));
            writer.addAll(results);
            final var registerReads = new Reads()
                    .add(activationOrigin(c))
                    .add(Row.WRITER, 1, false)
                    .add(Row.WRITER, 1)
                    .add(writerHot(c));
            if (c.scoped) {
                registerReads.add(paidScope(c));
            }
            plans.add(controlPlan(
                    "slo/writer-register", writerFrame, 1, 0, 1, c.scoped ? 1 : 0, true, writer, registerReads));
            final var close = edits(replace(Row.WRITER, 1));
            close.addAll(results);
            final var closeReads = new Reads()
                    .add(activationOrigin(c))
                    .add(Row.WRITER, 1)
                    .add(writerHot(c))
                    .add(Row.DESCRIPTOR, 1)
                    .add(Row.FIRST, 1)
                    .add(Row.POSITION, 1)
                    .add(Row.FENCE, 1);
            if (c.scoped) {
                closeReads.add(paidScope(c));
            }
            plans.add(controlPlan("slo/writer-close", writerFrame, 1, 0, 1, references, true, close, closeReads));
            plans.add(controlPlan(
                    "slo/writer-rejected",
                    writerFrame,
                    1,
                    0,
                    1,
                    c.scoped ? 1 : 0,
                    true,
                    results,
                    Reads.either(registerReads, closeReads)));
        }

        final var eventPrelude = new Reads()
                .add(Row.GENERATION, 1)
                .add(Row.SAMPLE, 1)
                .add(activationOrigin(c))
                .add(activationOrigin(c));
        final int eventReferences = c.scoped ? (c.writer ? 2 : 1) : 0;
        if (c.writer) {
            eventPrelude.add(Row.WRITER, 2).add(writerHot(c));
        }
        final var finalRows = edits(
                replace(Row.SAMPLE, 1), replace(Row.SAMPLE_REF, 3), create(Row.EVENT, 1), replace(Row.GENERATION, 1));
        finalRows.addAll(results);
        final long finalFrame = systemFrame(
                c.writer
                        ? TargetSloObservationEventBody.MAX_EPOCH_FINAL_CANONICAL_BYTES
                        : Math.max(
                                TargetSloObservationEventBody.MAX_FINAL_CANONICAL_BYTES,
                                TargetSloObservationEventBody.MAX_SOURCE_ASSIGNED_CANONICAL_BYTES));
        final var finalEvidence = controlEvidenceOrigin(c);
        plans.add(controlPlan(
                "slo/final",
                finalFrame,
                1,
                0,
                1,
                eventReferences,
                true,
                finalRows,
                new Reads()
                        .add(eventPrelude)
                        .add(finalEvidence)
                        .add(Row.CUSTODY, 1)
                        .add(Row.EVENT, 1, false)));
        // TIMEOUT's eligibility proof is a separate branch; recovery-gap and positive-Admission proofs are not added to
        // it.
        plans.add(controlPlan(
                "slo/final-timeout",
                finalFrame,
                1,
                0,
                1,
                eventReferences,
                true,
                finalRows,
                new Reads()
                        .add(eventPrelude)
                        .add(controlTimeoutReads(c))
                        .add(new Reads().add(Row.POSITION, 2))
                        .add(Row.CUSTODY, 1)
                        .add(Row.EVENT, 1, false)));
        final var custody = edits(create(Row.CUSTODY, 1), replace(Row.GENERATION, 1));
        custody.addAll(results);
        final long custodyFrame = systemFrame(
                c.writer
                        ? TargetSloObservationEventBody.MAX_EPOCH_CUSTODY_CANONICAL_BYTES
                        : TargetSloObservationEventBody.MAX_CUSTODY_CANONICAL_BYTES);
        final var custodyReads = new Reads()
                .add(eventPrelude)
                .add(Row.EVENT, 1)
                .add(Row.POSITION, 1)
                .add(controlEventOrigin(c))
                .add(Row.CUSTODY, 1, false)
                .add(Row.CUSTODY, 1)
                .add(Row.CUSTODY, 1, false);
        plans.add(controlPlan("slo/custody", custodyFrame, 1, 0, 1, eventReferences, true, custody, custodyReads));
        plans.add(controlPlan(
                "slo/event-rejected",
                Math.max(finalFrame, custodyFrame),
                1,
                0,
                1,
                c.scoped ? 1 : 0,
                true,
                results,
                Reads.either(
                        new Reads().add(eventPrelude).add(finalEvidence),
                        new Reads().add(eventPrelude).add(controlTimeoutReads(c)),
                        custodyReads)));
    }

    private static void controlFundingFamilies(Capabilities c, List<Plan> plans) {
        final var results = systemResults();
        final var view = new Reads()
                .add(Row.AGGREGATE, 1)
                .add(Row.DESCRIPTOR, 1)
                .add(Row.BOOKKEEPING, 1)
                .add(Row.GRANT, 1);
        final var parent = edits(create(Row.PARENT_EMPTY, 1));
        parent.addAll(results);
        final var parentReads = new Reads()
                .add(view)
                .add(view)
                .add(activationOrigin(c))
                .add(Row.PARENT, 1, false)
                .add(Row.PARENT, 1, false);
        final long parentFrame = controlFrame(TargetParentControlCreditBody.MAX_CANONICAL_BYTES);
        plans.add(controlPlan("control/parent-credit", parentFrame, 1, 0, 1, 0, false, parent, parentReads));
        // requireComplete can reject only after the complete successful accounting attempt was measured.
        plans.add(new Plan("control/parent-credit-complete-rejected", parentFrame, 1, 0, 1)
                .reads(systemPrelude())
                .reads(parentReads)
                .reads(replacementReads(parent))
                .reads(replacementReads(results))
                .reads(new Reads().add(Row.FIRST, 1, false).add(Row.POSITION, 1, false))
                .assembly(0, false, false, parent)
                .assembly(0, false, false, results));
        final var scope = edits(replace(Row.PARENT, 1), create(Row.SCOPE, 1));
        scope.addAll(results);
        final var scopeReads = new Reads()
                .add(view)
                .add(view)
                .add(activationOrigin(c))
                .add(Row.PARENT, 1)
                .add(parentOrigin(c))
                .add(Row.SCOPE, 1, false);
        final long scopeFrame = controlFrame(TargetScopeControlObligationBody.MAX_CANONICAL_BYTES);
        plans.add(controlPlan("control/scope-fund", scopeFrame, 1, 0, 1, 0, false, scope, scopeReads));
        plans.add(new Plan("control/scope-complete-rejected", scopeFrame, 1, 0, 1)
                .reads(systemPrelude())
                .reads(scopeReads)
                .reads(replacementReads(scope))
                .reads(replacementReads(results))
                .reads(new Reads().add(Row.FIRST, 1, false).add(Row.POSITION, 1, false))
                .assembly(0, false, false, scope)
                .assembly(0, false, false, results));
    }

    /** Structural generation linkage. Overlay-dependent delta reads are computed from that actual business below. */
    private static Reads controlGenerationOrigin(Capabilities c) {
        final var reads = new Reads()
                .add(activationOrigin(c))
                .add(Row.FIRST, 3)
                .add(Row.POSITION, 3)
                .add(Row.GENERATION, 1);
        if (c.writer) {
            reads.add(Row.WRITER, 1).add(writerHot(c));
        }
        return reads;
    }

    private static Reads controlStateOrigin(Capabilities c) {
        return new Reads().add(Row.GENERATION, 1).add(controlGenerationOrigin(c));
    }

    private static Reads controlWriterOrigin(Capabilities c) {
        return writerHot(c);
    }

    private static Reads controlEvidenceOrigin(Capabilities c) {
        // Inspected POSITION + birth POSITION, followed by either a known gap first/POS or a positive Admission
        // first/POS.
        return new Reads().add(Row.POSITION, 2).add(Row.FIRST, 1).add(Row.POSITION, 1);
    }

    private static Reads controlEventOrigin(Capabilities c) {
        final var reads = new Reads()
                .add(activationOrigin(c))
                .add(Row.FIRST, 1)
                .add(Row.POSITION, 1)
                .add(Row.SAMPLE, 1)
                .add(Row.GENERATION, 1)
                .add(controlEvidenceOrigin(c));
        if (c.writer) {
            reads.add(Row.WRITER, 1).add(writerHot(c));
        }
        return reads;
    }

    private static Reads controlSampleOrigin(Capabilities c) {
        final var common = new Reads()
                .add(Row.GENERATION, 1)
                .add(controlGenerationOrigin(c))
                .add(Row.SAMPLE, 1)
                .add(Row.FIRST, 1)
                .add(Row.POSITION, 1);
        // Admission's four stored projections and eligibility's actual birth Message are different branches.
        return common.add(Reads.either(
                new Reads()
                        .add(Row.SAMPLE, 1)
                        .add(Row.SAMPLE_REF, 3)
                        .add(Row.ATTEMPT, 1)
                        .add(Row.EVENT, 1)
                        .add(controlEventOrigin(c)),
                new Reads()
                        .add(Row.SAMPLE, 1)
                        .add(Row.SAMPLE_REF, 2)
                        .add(Row.EVENT, 1)
                        .add(controlEventOrigin(c)),
                new Reads()
                        .add(Row.SAMPLE, 1)
                        .add(Row.SAMPLE_REF, 2)
                        .add(Row.MESSAGE, 1)
                        .add(Row.FIRST, 1, false)));
    }

    private static Reads controlCustodyOrigin(Capabilities c) {
        final var reads = new Reads()
                .add(activationOrigin(c))
                .add(Row.FIRST, 1)
                .add(Row.POSITION, 2)
                .add(Row.SAMPLE, 1)
                .add(Row.EVENT, 1)
                .add(controlEventOrigin(c))
                .add(Row.GENERATION, 1);
        if (c.writer) {
            reads.add(Row.WRITER, 1).add(writerHot(c));
        }
        return reads;
    }

    private static Reads controlGenerationDelta(List<Edit> business) {
        final var reads = new Reads();
        for (var edit : business) {
            if (edit.row == Row.GENERATION && edit.after) {
                reads.add(Row.GENERATION, edit.count, edit.before);
            }
            if (edit.row == Row.SAMPLE) {
                // The new primary is in this overlay; only its actual prior primary is a Native read.
                reads.add(Row.SAMPLE, edit.count, edit.before).add(Row.CUSTODY, edit.count);
                if (edit.before) {
                    reads.add(Row.CUSTODY, edit.count);
                }
            }
            if (edit.row == Row.CUSTODY) {
                // An older exported version may leave today's primary and exact custody key outside the overlay.
                reads.add(Row.SAMPLE, Math.multiplyExact(2L, edit.count))
                        .add(Row.CUSTODY, Math.multiplyExact(2L, edit.count));
            }
        }
        return reads;
    }

    private static Reads controlTimeoutReads(Capabilities c) {
        // A successful no-known-Admission timeout is the existing open eligibility branch: no Admission
        // budget, latest Final event or newly born Message can coexist with this origin check.
        final var openEligibility = new Reads()
                .add(Row.GENERATION, 1)
                .add(controlGenerationOrigin(c))
                .add(Row.SAMPLE, 2)
                .add(Row.SAMPLE_REF, 2)
                .add(Row.FIRST, 1)
                .add(Row.POSITION, 1);
        final var retained = new Reads()
                .add(Row.AGGREGATE, 1)
                .add(Row.POSITION, 1)
                .add(Row.SAMPLE, 1)
                .add(openEligibility)
                .add(Row.MESSAGE, 1)
                .add(c.payloadV2 ? Row.PAYLOAD : Row.PAYLOAD_V1, 1)
                .add(Row.BINDING, 1)
                .add(Row.POSITION, 1)
                .add(Row.FIRST, 1);
        // Terminal proof and current Timeline/Claim proof are mutually exclusive actual Message runtime states.
        return retained.add(Reads.either(
                new Reads().add(Row.TERMINAL, 1).add(Row.POSITION, 1).add(Row.FIRST, 1),
                new Reads().add(Row.WORK, 1),
                new Reads().add(Row.CLAIM, 1)));
    }

    private static int tupleGeneration(ProtocolTuple tuple) {
        if (tuple.equals(ProtocolTuple.targetMigrationFullSourceArchive())) {
            return 21;
        }
        if (tuple.equals(ProtocolTuple.targetMigrationLogicalFirstArchive())) {
            return 20;
        }
        if (tuple.equals(ProtocolTuple.targetPayloadObligationSummary())) {
            return 19;
        }
        if (tuple.equals(ProtocolTuple.targetMigrationRetireConfirm())) {
            return 18;
        }
        if (tuple.equals(ProtocolTuple.targetMigrationRetireIntent())) {
            return 17;
        }
        if (tuple.equals(ProtocolTuple.targetMigrationUncommittedObjectReclaim())) {
            return 16;
        }
        if (tuple.equals(ProtocolTuple.targetMigrationObjectPayloadReclaim())) {
            return 15;
        }
        if (tuple.equals(ProtocolTuple.targetDuplicatePositionReclaim())) {
            return 14;
        }
        if (tuple.equals(ProtocolTuple.targetIncarnationReclaim())) {
            return 13;
        }
        if (tuple.equals(ProtocolTuple.targetAnchorAllocationReclaim())) {
            return 12;
        }
        if (tuple.equals(ProtocolTuple.targetUncommittedObjectReclaim())) {
            return 11;
        }
        if (tuple.equals(ProtocolTuple.targetObjectPayloadReclaim())) {
            return 10;
        }
        if (tuple.equals(ProtocolTuple.targetNativeRetainedReclaim())) {
            return 9;
        }
        if (tuple.equals(ProtocolTuple.targetSelfContainedInlinePayloadReclaim())) {
            return 8;
        }
        if (tuple.equals(ProtocolTuple.targetInlinePayloadReclaim())) {
            return 7;
        }
        if (tuple.equals(ProtocolTuple.targetResourceReclaim())) {
            return 6;
        }
        if (tuple.equals(ProtocolTuple.targetEvidenceRetirement())) {
            return 5;
        }
        return 0;
    }

    /** Actual Source codec material. This value grants no Source, reader or allocation authority. */
    public static final class FrameDecodeBounds {
        private final long maximumFrameBytes, allocatedBytes, retainedBytes, fieldVisits, arrayObjects;

        private FrameDecodeBounds(long frame, TargetRecordAccounting.EnvelopeMaterial material) {
            maximumFrameBytes = frame;
            allocatedBytes = Math.addExact(frame, material.allocated());
            retainedBytes = Math.addExact(frame, material.retained());
            fieldVisits = material.fieldVisits();
            arrayObjects = Math.addExact(1L, material.arrayObjects());
        }

        public long maximumFrameBytes() {
            return maximumFrameBytes;
        }

        public long allocatedBytes() {
            return allocatedBytes;
        }

        public long retainedBytes() {
            return retainedBytes;
        }

        public long fieldVisits() {
            return fieldVisits;
        }

        public long arrayObjects() {
            return arrayObjects;
        }
    }

    public static FrameDecodeBounds frameDecode(long actualFrameBytes) {
        if (actualFrameBytes < 1 || actualFrameBytes > TargetMigrationArchiveSourceFrame.MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("actual Source frame is outside its NDL1 representation");
        }
        return new FrameDecodeBounds(actualFrameBytes, TargetRecordAccounting.envelopeSourceMaterial(actualFrameBytes));
    }

    private static Row f2Row(Capabilities c) {
        return c.generation >= 21 ? Row.ARCHIVE_ROW : Row.ARCHIVE_ROW_V1;
    }

    private static Row f2HeadRow(Capabilities c) {
        return c.generation >= 21 ? Row.ARCHIVE_HEAD : Row.ARCHIVE_HEAD_V2;
    }

    private static Reads f2Activated() {
        return new Reads().add(Row.MIGRATION, 1).add(Row.FIRST, 1).add(Row.POSITION, 1);
    }

    private static Reads f2Selected(boolean confirmation) {
        return new Reads()
                .add(Row.RETIREMENT, 1)
                .add(Row.FIRST, confirmation ? 2 : 1)
                .add(Row.POSITION, confirmation ? 2 : 1);
    }

    private static Reads f2OverlayActivationApplied(Capabilities c) {
        final var reads = new Reads().local(Row.FIRST, 1).local(Row.POSITION, 1).add(Row.DESCRIPTOR, 1);
        if (c.generation >= 15) {
            if (c.migration) {
                reads.add(Row.MIGRATION, 1)
                        .add(Row.FIRST, 1)
                        .add(Row.POSITION, 1)
                        .add(Row.MIGRATION_ATTEMPT, 1);
            } else {
                reads.add(Row.INITIAL, 1).add(Row.FIRST, 1).add(Row.POSITION, 1).add(Row.DESCRIPTOR, 1);
            }
        }
        return reads;
    }
    /** Native53 and original first/POS; the enabling batch's exact mandatory overlay hits stay local. */
    private static Reads f2FullFormat(Capabilities c) {
        if (c.generation < 21) {
            return new Reads();
        }
        final var existing = activationOrigin(c);
        final var enabling = new Reads()
                .local(Row.ACTIVATION, 2)
                .add(Row.ACTIVATION, 3, false)
                .add(Row.FIRST, 1, false)
                .add(Row.POSITION, 1, false)
                .add(f2OverlayActivationApplied(c))
                .add(f2OverlayActivationApplied(c))
                .add(Row.AGGREGATE, 1);
        return Reads.either(existing, enabling);
    }

    private static Reads f2Writer(Capabilities c) {
        final var existing =
                new Reads().add(Row.ACTIVATION, 3).add(activationApplied(c)).add(Row.AGGREGATE, 2);
        final var enabling = new Reads()
                .add(Row.ACTIVATION, 3, false)
                .add(Row.FIRST, 1, false)
                .add(Row.POSITION, 1, false)
                .local(Row.ACTIVATION, 2)
                .add(f2OverlayActivationApplied(c))
                .add(Row.AGGREGATE, 1);
        return Reads.either(existing, enabling);
    }
    /** Prefix, ordinary Target position and archive physical proof are exclusive original provenance branches. */
    private static Reads f2TailApplied(Capabilities c) {
        return Reads.either(
                new Reads().add(Row.MIGRATION_PREFIX, 1),
                new Reads().add(Row.MIGRATION_PREFIX, 1, false).add(Row.POSITION, 1),
                new Reads()
                        .add(Row.MIGRATION_PREFIX, 1, false)
                        .add(Row.POSITION, 1, false)
                        .add(f2Row(c), 2));
    }

    private static Reads f2Head(Capabilities c) {
        return new Reads()
                .add(f2HeadRow(c), 1)
                .add(Row.ARCHIVE_STEP, 1)
                .add(Row.FIRST, 1)
                .add(Row.POSITION, 1)
                .add(f2FullFormat(c))
                .add(f2FullFormat(c))
                .add(Reads.either(new Reads(), f2TailApplied(c)));
    }

    private static Reads f2Takeover(Capabilities c) {
        return new Reads().add(f2Head(c)).add(Row.ARCHIVE_STEP, 1);
    }

    private static Reads f2CurrentTakeover(Capabilities c) {
        return new Reads()
                .add(f2Activated())
                .add(f2Selected(true))
                .add(f2Head(c))
                .add(activationOrigin(c))
                .add(f2Takeover(c))
                .add(Row.AGGREGATE, 6);
    }

    private static Material f2FrameMaterial(long frame, long encoded, long copied) {
        return Material.typed(TargetRecordAccounting.envelopeSourceCanonicalMaterial(frame))
                .repeat(encoded)
                .hold(Material.array(frame).repeat(copied));
    }

    private static List<Plan> f2Families(Capabilities c) {
        final var plans = new ArrayList<Plan>();
        if (!c.migration || c.generation < 21) {
            return plans;
        }
        final long frame = c.sourceFrameBytes;
        // COPY retains the original 32-MiB request/32-row schema; full-inline Source does not enlarge it.
        for (boolean takeover : new boolean[] {false, true}) {
            for (boolean prior : new boolean[] {false, true}) {
                if (takeover && !prior) {
                    continue;
                }
                final long rows = takeover ? 0 : TargetMigrationArchiveRequest.MAX_ROWS;
                final var business = systemResults();
                if (rows > 0) {
                    business.add(copyBatch(rows));
                }
                business.add(create(Row.ARCHIVE_STEP, 1));
                business.add(prior ? replace(Row.ARCHIVE_HEAD, 1) : create(Row.ARCHIVE_HEAD, 1));
                final var direct = new Reads()
                        .add(repeat(f2Activated(), 2))
                        .add(repeat(f2Selected(false), 2))
                        .add(repeat(f2Writer(c), 2))
                        .add(activationOrigin(c))
                        .add(Row.DESCRIPTOR, 1)
                        .add(prior ? repeat(f2Head(c), 2) : new Reads().add(Row.ARCHIVE_HEAD, 2, false))
                        .add(Row.ARCHIVE_COPY, rows, false)
                        .add(Row.FIRST, 2, false)
                        .add(Row.POSITION, 2, false)
                        .add(Row.AGGREGATE, 11)
                        .add(Row.FENCE, 1)
                        .add(replacementReads(business));
                plans.add(new Plan(
                                "f2/" + (takeover ? "takeover" : "copy") + "/prior-" + prior,
                                controlFrame(TargetMigrationControlBody.MAX_CANONICAL_BYTES),
                                1,
                                0,
                                1)
                        .reads(direct)
                        .assembly(0, false, false, business));
            }
        }
        // prepareReplay and prepareClosed consume separately protected actual material. These are the point joins
        // of the Native consumer; whole-shadow COPY/retirement acquisition is never silently charged as one get.
        for (boolean closed : new boolean[] {false, true}) {
            for (int rows = 1; rows <= (closed ? 3 : 1); rows++) {
                final var business = edits(create(Row.ARCHIVE_ROW, rows), replace(Row.ARCHIVE_HEAD, 1));
                final var direct = new Reads()
                        .add(f2CurrentTakeover(c))
                        .add(Row.FENCE, closed ? 3 : 2)
                        .add(Row.AGGREGATE, 7)
                        .add(f2Activated())
                        .add(f2Selected(true))
                        .add(f2Writer(c))
                        .add(f2Takeover(c))
                        .add(f2Head(c))
                        .add(Row.ARCHIVE_ROW, Math.multiplyExact(2L, rows), false)
                        .add(replacementReads(business));
                if (!closed) {
                    direct.add(Row.ARCHIVE_ROW, 1)
                            .add(f2FullFormat(c))
                            .add(Reads.either(
                                    new Reads()
                                            .add(Row.ARCHIVE_STEP, 1)
                                            .add(Row.FIRST, 1)
                                            .add(Row.POSITION, 1),
                                    f2TailApplied(c)));
                }
                final var replay = new Plan(
                                "f2/closed-or-physical-replay/closed-" + closed + "/rows-" + rows, frame, 1, 0, 1)
                        .reads(direct)
                        .assembly(0, false, false, business);
                // The exact compare/retained-frame and sourceDigest sites of each actual planner are distinct.
                replay.sourceFrameEncodings = closed ? 9 : 4;
                plans.add(replay);
            }
        }
        plans.add(new Plan("f2/already-applied-physical", frame, 1, 0, 1)
                .reads(new Reads()
                        .add(Row.ARCHIVE_ROW, 1)
                        .add(Row.AGGREGATE, 3)
                        .add(f2Activated())
                        .add(f2Selected(true))
                        .add(f2FullFormat(c))
                        .add(f2TailApplied(c))));
        return plans;
    }

    private static List<Plan> sourceReplayFamilies(Capabilities c) {
        final var plans = new ArrayList<Plan>();
        final long frame = c.sourceFrameBytes;
        final var replayReads = new Reads()
                .add(Row.FIRST, 2)
                .add(Row.POSITION, 4, false)
                .add(Row.DESCRIPTOR, 3)
                .add(Row.FENCE, 2);
        plans.add(new Plan("source/later-system-replay", frame, 2, 0, 1)
                .reads(replayReads)
                .assembly(0, false, false, create(Row.POSITION, 1)));
        if (c.scoped) {
            plans.add(new Plan(
                            "source/later-scoped-system-replay",
                            Math.max(
                                    Math.max(
                                            controlFrame(TargetSloObservationControlBody.MAX_CANONICAL_BYTES),
                                            controlFrame(TargetSloWriterControlBody.MAX_CANONICAL_BYTES)),
                                    systemFrame(TargetSloObservationEventBody.MAX_CANONICAL_BYTES)),
                            2,
                            0,
                            1)
                    .reads(replayReads)
                    .assembly(1, true, false, create(Row.POSITION, 1)));
        }
        plans.add(new Plan("source/same-system-position", frame, 2, 0, 1)
                .reads(new Reads()
                        .add(Row.FIRST, 1)
                        .add(Row.POSITION, 1)
                        .add(Row.DESCRIPTOR, 2)
                        .add(Row.FENCE, 1)));
        final var rejected = systemResults();
        final var stale = Reads.either(
                new Reads(),
                activationOrigin(c),
                new Reads().add(Row.CONFIRMED, 1).add(retirementConfirmed(c)),
                new Reads()
                        .add(Row.CONFIRMED, 1, false)
                        .add(Row.RETIRING, 1)
                        .add(activationOrigin(c))
                        .add(Row.FIRST, 1)
                        .add(Row.POSITION, 1));
        plans.add(new Plan("source/closed-or-stale-first", frame, 1, 0, 1)
                .reads(new Reads()
                        .add(Row.FIRST, 4, false)
                        .add(Row.POSITION, 4, false)
                        .add(Row.DESCRIPTOR, 1)
                        .add(Row.FENCE, 2)
                        .add(repeat(stale, 2)))
                .assembly(0, false, false, rejected));
        // The fallback still consumes/decodes the real Source before a typed control verifier may reject it.
        plans.add(new Plan("source/decoded-generic-control-fallback", frame, 1, 0, 1)
                .reads(new Reads().add(Row.FIRST, 1, false).add(Row.POSITION, 1, false)));
        for (boolean applied : new boolean[] {false, true}) {
            final var fence = new Plan(
                            "source/time-fence/" + applied,
                            systemFrame(TargetTimeFenceBody.MAX_CANONICAL_BYTES),
                            1,
                            0,
                            1)
                    .reads(systemPrelude())
                    .reads(new Reads().add(Row.FENCE, 1))
                    .reads(replacementReads(rejected))
                    .assembly(0, false, false, rejected);
            fence.fence = applied;
            plans.add(fence);
        }
        return plans;
    }

    /* COMMON_ENVELOPE_REGION: all numbers below are projections of the closed branches above and below.
    F=0 is evaluation of their fixed term only. It never qualifies an empty/small Source service. */
    private static Bounds closedFamilies(Capabilities capabilities) {
        return closedFamilies(capabilities, null);
    }

    private static Bounds closedFamilies(
            Capabilities capabilities,
            com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.SourceFrameContract original) {
        final var c = capabilities.copy();
        if (original != null) {
            original.requireLocalCurrent();
            c.sourceFrameBytes = original.maximumFrameBytes();
        }
        final long frame = Math.max(0, c.sourceFrameBytes);
        final var families = new ArrayList<Plan>();
        families.addAll(controlFamilies(c));
        families.addAll(commandFamilies(c));
        families.addAll(attemptFamilies(c));
        families.addAll(retirementFamilies(c));
        families.addAll(migrationFamilies(c));
        families.addAll(f2Families(c));
        families.addAll(sourceReplayFamilies(c));
        long records = 0, bytes = 0, writes = 0, written = 0, allocated = 0, retained = 0, maximumFrame = 0;
        int counters = 2, targets = 1, domains = 1;
        for (var plan : families) {
            final var cost = planCost(plan, c, frame);
            records = Math.max(records, cost.reads.records());
            bytes = Math.max(bytes, cost.reads.bytes(frame));
            writes = Math.max(writes, cost.writeRecords);
            written = Math.max(written, cost.writeBytes);
            allocated = Math.max(allocated, cost.material.allocated);
            retained = Math.max(retained, cost.material.peak);
            counters = Math.max(counters, Math.multiplyExact(2, plan.ownerPairs));
            targets = Math.max(targets, plan.targets);
            domains = Math.max(domains, plan.domains);
            maximumFrame = Math.max(
                    maximumFrame,
                    plan.frameBytes < 0
                            ? frame
                            : original == null ? plan.frameBytes : Math.min(frame, plan.frameBytes));
        }
        return new Bounds(
                records,
                bytes,
                writes,
                written,
                allocated,
                retained,
                counters,
                targets,
                domains,
                c.writer,
                maximumFrame,
                c,
                original);
    }

    /** held tracks explicit returned arrays; peak includes call temporaries while earlier held values remain live. */
    private record Material(long allocated, long peak, long held, long visits, long arrays) {
        private Material {
            if (allocated < peak || peak < held || held < 0 || visits < 0 || arrays < 0) {
                throw new IllegalArgumentException("invalid Source material projection");
            }
        }

        static Material empty() {
            return new Material(0, 0, 0, 0, 0);
        }

        static Material array(long bytes) {
            return new Material(bytes, bytes, bytes, 0, 1);
        }

        static Material arrays(long bytes, long count) {
            return new Material(bytes, bytes, bytes, 0, count);
        }

        static Material typed(TargetRecordAccounting.EnvelopeMaterial value) {
            return new Material(
                    value.allocated(), value.retained(), value.held(), value.fieldVisits(), value.arrayObjects());
        }

        Material hold(Material next) {
            return new Material(
                    Math.addExact(allocated, next.allocated),
                    Math.max(peak, Math.addExact(held, next.peak)),
                    Math.addExact(held, next.held),
                    Math.addExact(visits, next.visits),
                    Math.addExact(arrays, next.arrays));
        }

        Material drop(Material next) {
            return new Material(
                    Math.addExact(allocated, next.allocated),
                    Math.max(peak, next.peak),
                    next.held,
                    Math.addExact(visits, next.visits),
                    Math.addExact(arrays, next.arrays));
        }

        Material temporary(Material next) {
            return new Material(
                    Math.addExact(allocated, next.allocated),
                    Math.max(peak, Math.addExact(held, next.peak)),
                    held,
                    Math.addExact(visits, next.visits),
                    Math.addExact(arrays, next.arrays));
        }

        Material repeat(long count) {
            if (count < 0) {
                throw new IllegalArgumentException("negative Source material multiplicity");
            }
            if (count == 0) {
                return empty();
            }
            return new Material(
                    Math.multiplyExact(allocated, count),
                    Math.addExact(Math.multiplyExact(held, count - 1), peak),
                    Math.multiplyExact(held, count),
                    Math.multiplyExact(visits, count),
                    Math.multiplyExact(arrays, count));
        }

        Material alternative(Material other) {
            return new Material(
                    Math.max(allocated, other.allocated),
                    Math.max(peak, other.peak),
                    Math.max(held, other.held),
                    Math.max(visits, other.visits),
                    Math.max(arrays, other.arrays));
        }
    }

    private record PlanCost(Reads reads, long writeRecords, long writeBytes, Material material) {}

    private static Material readMaterial(Reads reads, long frame) {
        return readMaterial(reads, frame, null);
    }

    private static Material readMaterial(Reads reads, long frame, Edit copyCohort) {
        var material = Material.empty();
        for (var entry : reads.rows.entrySet()) {
            final var row = entry.getKey();
            final var counts = entry.getValue();
            material = material.hold(Material.array(row.key).repeat(counts[0]));
            final long present =
                    Math.addExact(counts[1], row == Row.ARCHIVE_COPY && copyCohort != null ? 0 : counts[2]);
            if (present != 0) {
                final long payload = row.payload(frame);
                final var wrappers = new Material(
                        TargetRecordAccounting.envelopeDecodedPayloadCopies(payload, 1),
                        Math.multiplyExact(3L, payload),
                        payload,
                        0,
                        3);
                final var projection = Material.array(row.key)
                        .hold(Material.array(row.nv(frame)))
                        .hold(wrappers)
                        .hold(Material.typed(
                                TargetRecordAccounting.envelopeDecodeMaterial(materialRow(row, frame), frame)));
                material = material.hold(projection.repeat(present));
                if (row == Row.FIRST) {
                    material =
                            material.temporary(Material.typed(TargetRecordAccounting.envelopeSystemResultFromMaterial())
                                    .repeat(present));
                }
            }
            if (row == Row.ARCHIVE_COPY && copyCohort != null && counts[2] > 0) {
                final long repetitions = Math.addExact(counts[2], copyCohort.count - 1L) / copyCohort.count;
                final long payload = copyCohort.payloadBytes(frame);
                final var wrappers = new Material(
                        Math.multiplyExact(3L, payload),
                        Math.multiplyExact(3L, payload),
                        payload,
                        0,
                        Math.multiplyExact(3L, copyCohort.count));
                final var group = Material.arrays(Math.multiplyExact(row.key, copyCohort.count), copyCohort.count)
                        .hold(Material.arrays(copyCohort.nativeBytes(frame), copyCohort.count))
                        .hold(wrappers)
                        .hold(Material.typed(TargetRecordAccounting.envelopeArchiveCopyDecodeMaterial(
                                copyCohort.count, copyBatchRaw(copyCohort.count))));
                material = material.hold(group.repeat(repetitions));
            }
        }
        for (var choices : reads.alternatives) {
            var selected = Material.empty();
            for (var choice : choices) {
                selected = selected.alternative(readMaterial(choice, frame, copyCohort));
            }
            material = material.hold(selected);
        }
        for (var entry : reads.creations.entrySet()) {
            material = material.temporary(Material.typed(TargetRecordAccounting.envelopeCreationUpperBound(
                            materialRow(entry.getKey(), frame), frame))
                    .repeat(entry.getValue()));
        }
        material = material.temporary(Material.typed(TargetRecordAccounting.envelopeParentOriginMaterial())
                .repeat(reads.parentOriginMaterials));
        return material.temporary(new Material(reads.localAllocated, reads.localRetained, 0, 0, 0));
    }

    private static Row projectionRow(int type) {
        return switch (type) {
            case TargetQuotaAggregate.VALUE_TYPE -> Row.AGGREGATE;
            case TargetQuotaIncarnation.VALUE_TYPE -> Row.DESCRIPTOR;
            case TargetQuotaBookkeeping.VALUE_TYPE -> Row.BOOKKEEPING;
            case TargetQuotaCounter.VALUE_TYPE -> Row.COUNTER;
            case TargetQuotaTotal.VALUE_TYPE -> Row.TOTAL;
            default -> throw new IllegalArgumentException("unknown Source accounting projection");
        };
    }

    private static long editCount(List<Edit> edits) {
        long count = 0;
        for (var edit : edits) {
            count = Math.addExact(count, edit.count);
        }
        return count;
    }

    private static Row materialRow(Row row, long frame) {
        if (frame != 0) {
            return row;
        }
        // Unqualified bounds expose only fixed legacy material, never a fabricated full-inline F.
        return switch (row) {
            case ARCHIVE_HEAD -> Row.ARCHIVE_HEAD_V2;
            case ARCHIVE_ROW -> Row.ARCHIVE_ROW_V1;
            default -> row;
        };
    }

    private static Reads beforeChecks(List<Edit> edits) {
        return replacementReads(edits);
    }

    private static Material editMaterial(Edit edit, long frame) {
        final var row = edit.row;
        final long before = edit.before ? row.nv(frame) : 0, after = edit.after ? row.nv(frame) : 0;
        if (edit.aggregatePayloadBytes >= 0) {
            final long raw = copyBatchRaw(edit.count);
            var group = Material.arrays(Math.multiplyExact(row.key, edit.count), edit.count);
            group = group.hold(
                            Material.typed(TargetRecordAccounting.envelopeArchiveCopyDecodeMaterial(edit.count, raw)))
                    .hold(Material.typed(TargetRecordAccounting.envelopeArchiveCopyCanonicalMaterial(edit.count, raw)))
                    .hold(Material.arrays(edit.nativeBytes(frame), edit.count));
            group = group.temporary(new Material(
                    edit.copied(frame),
                    edit.copied(frame),
                    edit.retained(frame),
                    0,
                    Math.multiplyExact(4L, edit.count)));
            return new Material(group.allocated, group.peak, edit.retained(frame), group.visits, group.arrays);
        }
        var constructed = Material.array(row.key);
        if (edit.after && (row == Row.MIGRATION || row == Row.MIGRATION_GUARD || row == Row.RETIREMENT)) {
            constructed =
                    constructed.hold(Material.typed(TargetRecordAccounting.envelopeMigrationCreationMaterial(row)));
        } else if (edit.after && creationMajorantRow(row)) {
            constructed = constructed.hold(
                    Material.typed(TargetRecordAccounting.envelopeCreationUpperBound(materialRow(row, frame), frame)));
        }
        if (edit.after && (row == Row.FIRST || row == Row.POSITION)) {
            constructed = constructed.hold(Material.typed(TargetRecordAccounting.envelopeResultFactoryUpperBound()));
        }
        if (edit.after) {
            constructed = constructed
                    .hold(Material.typed(
                            TargetRecordAccounting.envelopeCanonicalMaterial(materialRow(row, frame), frame)))
                    .hold(Material.array(after));
        }
        final long copies = TargetRecordAccounting.envelopeEditCopies(row.key, before, after);
        final long held = Math.addExact(row.key, Math.addExact(before, after));
        // Edit owns all three arrays; decode validation retains no additional payload after construction.
        constructed = constructed.temporary(
                new Material(copies, copies, held, 0, (edit.before ? 3 : 0) + (edit.after ? 3 : 0) + 1));
        return new Material(constructed.allocated, constructed.peak, held, constructed.visits, constructed.arrays)
                .repeat(edit.count);
    }

    private static boolean creationMajorantRow(Row row) {
        return switch (row) {
            case INITIAL,
                    QUEUE,
                    WORK,
                    ORDER_HEAD,
                    ORDER,
                    EXPIRY,
                    BINDING,
                    RESERVATION,
                    MEMBER,
                    CHANNEL_CONTROL,
                    CHANNEL_SLOT,
                    MEMBER_CLOSE,
                    CLOSE,
                    CLOSE_CURSOR,
                    NATIVE_CONTROL,
                    EVIDENCE_OWNER,
                    EVIDENCE_REF,
                    RECOVERY_OWNER,
                    RECOVERY_REF,
                    KAFKA_MAPPING,
                    KAFKA_SLOT,
                    TERMINAL,
                    ATTEMPT,
                    MESSAGE,
                    PAYLOAD,
                    PAYLOAD_V1,
                    SCOPE,
                    GRANT,
                    DESCRIPTOR,
                    ACTIVATION,
                    HISTORY,
                    HISTORY_TIP,
                    OBJECT_PENDING,
                    SLO_STATE,
                    GENERATION,
                    SAMPLE,
                    SAMPLE_REF,
                    COMMAND_SAMPLE,
                    COMMAND_SAMPLE_REF,
                    DUE_SAMPLE,
                    DUE_SAMPLE_REF,
                    EVENT,
                    CUSTODY,
                    WRITER,
                    PARENT_EMPTY,
                    PARENT,
                    MIGRATION,
                    MIGRATION_GUARD,
                    RETIREMENT,
                    ARCHIVE_COPY,
                    ARCHIVE_ROW,
                    ARCHIVE_ROW_V1,
                    ARCHIVE_HEAD,
                    ARCHIVE_HEAD_V2,
                    ARCHIVE_STEP -> true;
            default -> false;
        };
    }

    private static Material editAccessors(
            List<Edit> edits, long frame, long keyCalls, long beforeCalls, long afterCalls) {
        long allocated = 0, largest = 0, arrays = 0;
        for (var edit : edits) {
            final long bytes = Math.addExact(
                    Math.multiplyExact(Math.multiplyExact(keyCalls, edit.row.key), edit.count),
                    Math.addExact(
                            edit.before ? Math.multiplyExact(beforeCalls, edit.nativeBytes(frame)) : 0,
                            edit.after ? Math.multiplyExact(afterCalls, edit.nativeBytes(frame)) : 0));
            allocated = Math.addExact(allocated, bytes);
            largest = Math.max(
                    largest,
                    Math.addExact(
                            Math.multiplyExact(keyCalls, edit.row.key),
                            Math.multiplyExact(
                                    (edit.before ? beforeCalls : 0) + (edit.after ? afterCalls : 0),
                                    edit.row.nv(frame))));
            arrays = Math.addExact(
                    arrays,
                    Math.multiplyExact(
                            edit.count, keyCalls + (edit.before ? beforeCalls : 0) + (edit.after ? afterCalls : 0)));
        }
        return new Material(allocated, largest, 0, 0, arrays);
    }

    private static boolean scopedRow(Row row) {
        return row.type >= 57 && row.type <= 62;
    }

    private static boolean metadataRow(Row row) {
        return row != Row.WORK
                && row != Row.ORDER_HEAD
                && row != Row.MESSAGE
                && row != Row.EXPIRY
                && row != Row.FIRST
                && row != Row.POSITION
                && row != Row.CLAIM
                && row != Row.TERMINAL
                && row != Row.RESERVATION;
    }
    /** Separate actual public-accessor sites in Scope settlement, Source accounting, summary and permanent guards.
     * Branch-only sites reserve their larger reachable branch; no array copies are inferred from Native fees. */
    private static Material sourceAccessorMaterial(
            List<Edit> business, long frame, Capabilities c, boolean last, boolean ownReplay) {
        var material = Material.empty();
        for (var edit : business) {
            final var one = List.of(edit);
            final long sides = (edit.before ? 1L : 0) + (edit.after ? 1L : 0);
            // Scope duplicate key, exact before, and after-presence checks.
            material = material.temporary(editAccessors(one, frame, 2, 1, 1));
            if (metadataRow(edit.row)) {
                // rejectExternalSettlement's two probes per side, then the origin probes and decode argument.
                material = material.temporary(editAccessors(one, frame, sides, 4, 4));
                if (c.scoped && scopedRow(edit.row)) {
                    material = material.temporary(editAccessors(one, frame, 2L * sides, 2, 2));
                }
                if (edit.row == Row.SCOPE) {
                    material = material.temporary(editAccessors(one, frame, 0, 0, 1));
                }
            } else if (edit.row == Row.FIRST || edit.row == Row.POSITION) {
                material = material.temporary(editAccessors(one, frame, 0, 2, edit.before ? 2 : 1));
            }
            // Accounting key uniqueness/exact before, charge presence and charge arguments.
            material =
                    material.temporary(editAccessors(one, frame, 2L + sides + (metadataRow(edit.row) ? 1 : 0), 3, 2));
            if ((edit.row == Row.FIRST || edit.row == Row.POSITION) && edit.before) {
                material = material.temporary(editAccessors(one, frame, 0, 2, 2));
            }
            if (edit.row == Row.GRANT) {
                material = material.temporary(editAccessors(one, frame, 1, 1, 1));
            }
            // Payload summary visits every key; its selected raw decode arguments are already local projections.
            material = material.temporary(editAccessors(one, frame, 1, 0, 0));
            if (edit.row == Row.ATTEMPT
                    || edit.row == Row.MESSAGE
                    || edit.row == Row.TERMINAL
                    || edit.row == Row.PAYLOAD
                    || edit.row == Row.PAYLOAD_V1) {
                material = material.temporary(editAccessors(one, frame, 1, 1, 1));
            }
            // Migration guard once, permanent-retirement guard before Scope and again in backend.prepare.
            if (metadataRow(edit.row)) {
                material = material.temporary(editAccessors(one, frame, 3, 0, 0))
                        .temporary(editAccessors(one, frame, 3, 0, 0).repeat(last ? 2 : 1));
            }
            if ((edit.row == Row.FIRST || edit.row == Row.POSITION) && edit.before) {
                material = material.temporary(editAccessors(one, frame, 2, 2, 1).repeat(last ? 2 : 1));
            }
            // Archive guard after Scope, at the backend, and once more for an authenticated OWN_REPLAY batch.
            final long archiveCalls = 1L + (last ? 1 : 0) + (ownReplay ? 1 : 0);
            if (metadataRow(edit.row)) {
                material = material.temporary(editAccessors(one, frame, 3, 0, 0).repeat(archiveCalls));
            }
            if (edit.row.type == 71 || edit.row.type == 72) {
                material = material.temporary(editAccessors(one, frame, 1, 1, 2).repeat(archiveCalls));
            }
            if ((edit.row == Row.FIRST || edit.row == Row.POSITION) && edit.before) {
                material = material.temporary(editAccessors(one, frame, 1, 3, 1).repeat(archiveCalls));
            }
        }
        return material;
    }

    private static Reads scopeReads(Assembly assembly, Plan plan, Capabilities c) {
        final var reads = beforeChecks(assembly.business);
        for (var edit : assembly.business) {
            if (scopedRow(edit.row)) {
                final long sides = (edit.before ? 1L : 0) + (edit.after ? 1L : 0);
                if (edit.before) {
                    reads.add(edit.row, edit.count);
                }
                if (edit.after) {
                    reads.local(edit.row, edit.count);
                }
                if (edit.row.type == 57 || edit.row.type == 59) {
                    reads.add(Row.GENERATION, Math.multiplyExact(sides, edit.count));
                }
                if (edit.row.type == 60 || edit.row.type == 61) {
                    reads.add(Row.WRITER, Math.multiplyExact(sides, edit.count));
                }
                if (c.scoped && assembly.scopeReferences > 0) {
                    if (edit.before) {
                        reads.add(repeat(chargeReads(edit.row, false, assembly, plan, c), edit.count));
                    }
                    if (edit.after) {
                        reads.add(repeat(chargeReads(edit.row, true, assembly, plan, c), edit.count));
                    }
                    if (edit.after && edit.row.type == 59) {
                        reads.local(edit.row, edit.count);
                    }
                }
            }
        }
        if (assembly.scopeReferences > 0) {
            reads.add(repeat(paidScope(c), assembly.scopeReferences)).add(Row.SCOPE, assembly.scopeReferences);
            if (assembly.scopeSource) {
                boolean firstInOverlay = false;
                for (var edit : assembly.business) {
                    if (edit.row == Row.FIRST && edit.after) {
                        firstInOverlay = true;
                    }
                }
                if (firstInOverlay) {
                    reads.local(Row.FIRST, 1);
                } else {
                    reads.add(Row.FIRST, 1);
                }
                reads.local(Row.POSITION, 1)
                        .add(Row.WRITER, 1)
                        .add(Row.SCOPE, 1)
                        .add(paidScope(c));
            }
        }
        return reads;
    }
    /** Exact Source guard point joins. Duplicate Message/generation identities can only remove these projections. */
    private static Reads payloadSummaryReads(List<Edit> business) {
        final var reads = new Reads();
        long generations = 0, messages = 0, attempts = 0;
        for (var edit : business) {
            final boolean candidate = edit.row == Row.ATTEMPT
                    || edit.row == Row.MESSAGE
                    || edit.row == Row.TERMINAL
                    || edit.row == Row.PAYLOAD
                    || edit.row == Row.PAYLOAD_V1;
            if (!candidate) {
                continue;
            }
            reads.add(edit.row, edit.count, edit.before);
            if (edit.before) {
                reads.local(edit.row, edit.count);
            }
            if (edit.after) {
                reads.local(edit.row, edit.count);
            }
            if (edit.row == Row.ATTEMPT) {
                attempts = Math.addExact(attempts, edit.count);
                generations = Math.addExact(generations, edit.count);
            }
            if (edit.row == Row.MESSAGE || edit.row == Row.TERMINAL) {
                generations = Math.addExact(
                        generations, Math.multiplyExact(edit.count, (edit.before ? 1L : 0) + (edit.after ? 1L : 0)));
            }
            messages = Math.addExact(messages, edit.count);
        }
        reads.add(Row.MESSAGE, Math.multiplyExact(2L, generations))
                .add(Row.TERMINAL, Math.multiplyExact(2L, generations));
        reads.add(Row.PAYLOAD, Math.multiplyExact(2L, messages));
        if (attempts != 0) {
            reads.add(Row.MESSAGE, attempts)
                    .add(Row.CLAIM, attempts)
                    .add(Row.CLAIM_CHARGE, attempts)
                    .add(Row.MIGRATION_GUARD, messages)
                    .local(Row.FIRST, attempts)
                    .local(Row.MESSAGE, attempts)
                    .local(Row.CLAIM, attempts)
                    .local(Row.CLAIM_CHARGE, attempts)
                    .local(Row.TERMINAL, messages);
        }
        return reads;
    }

    private static Reads permanentGuardReads(List<Edit> business, Capabilities c) {
        final var reads = new Reads();
        boolean dedupeRemoval = false, archive = false;
        for (var edit : business) {
            if ((edit.row == Row.FIRST || edit.row == Row.POSITION) && edit.before) {
                dedupeRemoval = true;
            }
            if (edit.row == Row.RETIREMENT) {
                reads.add(Row.MIGRATION, edit.count);
            }
            if (edit.row.type == 71 || edit.row.type == 72) {
                archive = true;
            }
        }
        if (dedupeRemoval) {
            reads.add(Row.MIGRATION, 1, c.migration);
            if (c.migration) {
                reads.add(Row.RETIREMENT, 1)
                        .add(Row.MIGRATION, 1)
                        .add(Row.ARCHIVE_STEP, Math.addExact(count(business, TargetResultRecord.VALUE_TYPE), 0));
            }
        }
        if (archive) {
            reads.add(Row.MIGRATION, 1).add(Row.RETIREMENT, 1);
            for (var edit : business) {
                if (edit.row.type == 71 || edit.row.type == 72) {
                    reads.local(edit.row, edit.count);
                    if (c.generation >= 21) {
                        reads.add(repeat(f2FullFormat(c), edit.count));
                    }
                    if (edit.row == Row.ARCHIVE_HEAD || edit.row == Row.ARCHIVE_HEAD_V2) {
                        if (edit.before) {
                            reads.local(edit.row, edit.count);
                        }
                        reads.add(Reads.either(
                                new Reads().local(Row.ARCHIVE_STEP, edit.count),
                                new Reads().local(f2Row(c), count(business, TargetMigrationArchiveRow.VALUE_TYPE))));
                    }
                }
            }
        }
        return reads;
    }

    private static PlanCost planCost(Plan plan, Capabilities c, long frame) {
        final var ordinary = planCost(plan, c, frame, false);
        if (!c.migration || c.generation < 21 || plan.migrationNoArchiveTail || plan.source.startsWith("f2/")) {
            return ordinary;
        }
        final var tail = planCost(plan, c, frame, true);
        return new PlanCost(
                Reads.either(ordinary.reads, tail.reads),
                Math.max(ordinary.writeRecords, tail.writeRecords),
                Math.max(ordinary.writeBytes, tail.writeBytes),
                ordinary.material.alternative(tail.material));
    }

    private static PlanCost planCost(Plan plan, Capabilities c, long frame, boolean tail) {
        final var reads = new Reads().add(plan.direct);
        var material = readMaterial(plan.direct, frame);
        final var constructed = new IdentityHashMap<Edit, Boolean>();
        for (var assembly : plan.assemblies) {
            for (var edit : assembly.business) {
                if (constructed.put(edit, Boolean.TRUE) == null) {
                    material = material.hold(editMaterial(edit, frame));
                }
            }
        }
        long writeRecords = 0, writeBytes = 0;
        for (int index = 0; index < plan.assemblies.size(); index++) {
            final var original = plan.assemblies.get(index);
            final boolean last = index == plan.assemblies.size() - 1;
            final var business = new ArrayList<>(original.business);
            final var assemblyReads = new Reads();
            // Ordinary Source batches before T append their privately prepared actual tail in this same pass.
            long tailRows = 0;
            if (c.migration && !plan.source.startsWith("f2/")) {
                assemblyReads.add(Row.MIGRATION, 1);
                if (!plan.migrationActivation) {
                    assemblyReads.add(f2HeadRow(c), 1, c.generation >= 20);
                    if (tail) {
                        final long rows = count(original.business, TargetResultRecord.VALUE_TYPE);
                        tailRows = rows;
                        assemblyReads
                                .add(f2Activated())
                                .add(f2Selected(false))
                                .add(f2Head(c))
                                .add(f2Writer(c))
                                .add(Row.AGGREGATE, 4);
                        assemblyReads
                                .add(f2Row(c), Math.multiplyExact(3L, rows), false)
                                .add(f2HeadRow(c), 1);
                        if (rows > 0) {
                            business.add(create(f2Row(c), rows));
                        }
                        business.add(replace(f2HeadRow(c), 1));
                    }
                }
            } else if (!plan.source.startsWith("f2/")) {
                assemblyReads.add(Row.MIGRATION, 1, false);
            } else if (plan.source.startsWith("f2/copy") || plan.source.startsWith("f2/takeover")) {
                assemblyReads.add(Row.MIGRATION, 1).add(f2HeadRow(c), 1);
            }
            final var actualAssembly =
                    new Assembly(business, original.scopeReferences, original.scopeSource, original.rootInOverlay);
            assemblyReads.add(scopeReads(actualAssembly, plan, c));
            if (original.scopeReferences > 0) {
                business.add(replace(Row.SCOPE, original.scopeReferences));
            }
            assemblyReads.add(payloadSummaryReads(business)).add(permanentGuardReads(business, c));
            final boolean ownReplay = plan.source.startsWith("f2/closed-or-physical");
            if (ownReplay) {
                assemblyReads.add(permanentGuardReads(business, c));
                for (var edit : business) {
                    if (edit.row.type == 71 || edit.row.type == 72) {
                        assemblyReads.local(edit.row, edit.count);
                    }
                }
            }
            for (var edit : business) {
                if (edit.before) {
                    assemblyReads.add(repeat(chargeReads(edit.row, false, actualAssembly, plan, c), edit.count));
                }
                if (edit.after) {
                    assemblyReads.add(repeat(chargeReads(edit.row, true, actualAssembly, plan, c), edit.count));
                }
            }
            final long touchedCounters = Math.multiplyExact(2L, plan.ownerPairs), changedCounters = touchedCounters;
            final long touchedTargets = plan.targets, changedTargets = plan.targets;
            TargetSourceAccounting.envelopeProjectionReads(
                    (type, count) -> assemblyReads.add(projectionRow(type), count),
                    original.rootInOverlay,
                    touchedCounters,
                    last ? changedCounters : 0,
                    touchedTargets,
                    last ? changedTargets : 0);
            if (!last) {
                assemblyReads.add(Row.TOTAL, changedTargets);
            }
            // SourceAccounting checks B before images; backend.prepare later checks E=B+bookkeeping.
            assemblyReads.add(beforeChecks(business));
            final var complete = new ArrayList<>(business);
            complete.add(replace(Row.BOOKKEEPING, 1));
            if (last) {
                assemblyReads.add(beforeChecks(complete));
                if (plan.fence) {
                    assemblyReads.add(Row.FENCE, 2);
                }
                if (plan.cursors) {
                    assemblyReads.add(Row.CURSORS, 2);
                }
                assemblyReads.add(permanentGuardReads(complete, c));
            }
            reads.add(assemblyReads);
            Edit copyCohort = null;
            for (var edit : business) {
                if (edit.aggregatePayloadBytes >= 0) {
                    copyCohort = edit;
                }
            }
            var pass = readMaterial(assemblyReads, frame, copyCohort);
            // requireTail and appendTail each compare both original frames, then each new row/head encodes once.
            if (tail && c.sourceFrameBytes >= 0) {
                pass = pass.hold(f2FrameMaterial(frame, tailRows + 5L, 0));
            }
            for (var edit : complete) {
                if (!constructed.containsKey(edit)) {
                    pass = pass.hold(editMaterial(edit, frame));
                }
            }
            pass = pass.temporary(sourceAccessorMaterial(business, frame, c, last, ownReplay));
            if (assemblyReads.retirementRangeCalls != 0) {
                pass = pass.temporary(
                        editAccessors(business, frame, 1, 1, 1).repeat(assemblyReads.retirementRangeCalls));
            }
            material = last ? material.hold(pass) : material.temporary(pass);
            var arithmetic = Material.typed(TargetRecordAccounting.envelopeUsageArithmeticMaterial());
            // Per-counter Source subtraction/addition, per-primary aggregate delta, and per-target-counter total delta.
            final long primaryCounters = plan.ownerPairs;
            final long targetCounters = Math.max(0L, primaryCounters - 1L);
            long chargeSides = 0;
            for (var edit : business) {
                chargeSides = Math.addExact(
                        chargeSides, Math.multiplyExact(edit.count, (edit.before ? 1L : 0) + (edit.after ? 1L : 0)));
            }
            final long mergeCalls = Math.addExact(
                    Math.multiplyExact(2L, chargeSides), 4L + (plan.fence ? 4 : 0) + (plan.cursors ? 4 : 0));
            final long arithmeticCalls = Math.addExact(
                    mergeCalls,
                    Math.addExact(
                            Math.multiplyExact(2L, touchedCounters),
                            Math.addExact(
                                    Math.multiplyExact(2L, primaryCounters), Math.multiplyExact(2L, targetCounters))));
            material = material.temporary(arithmetic.repeat(arithmeticCalls));
            material = material.temporary(Material.typed(TargetRecordAccounting.envelopeUsageEmptyMaterial())
                    .repeat(Math.addExact(
                            Math.multiplyExact(3L, touchedCounters), Math.addExact(primaryCounters, changedTargets))));
            // The Source summary independently reconstructs the exact expected successor in addition to its final Edit.
            final long expectedPayloads = Math.min(
                    count(business, TargetQuotaAttemptBudget.VALUE_TYPE),
                    count(business, TargetQuotaPayloadOwner.VALUE_TYPE));
            if (expectedPayloads > 0) {
                material = material.temporary(
                        Material.typed(TargetRecordAccounting.envelopeCreationUpperBound(Row.PAYLOAD, frame))
                                .repeat(expectedPayloads));
            }
            if (plan.source.startsWith("outcome-") || plan.source.startsWith("resolution-")) {
                material = material.temporary(
                        Material.typed(TargetRecordAccounting.envelopeCreationUpperBound(Row.ATTEMPT, frame))
                                .repeat(count(business, TargetQuotaAttemptBudget.VALUE_TYPE)));
            }
            if (plan.source.startsWith("admission-")) {
                material = material.temporary(
                        Material.typed(TargetRecordAccounting.envelopeCreationUpperBound(Row.MESSAGE, frame)));
            }
            for (var row : new Row[] {Row.COUNTER, Row.AGGREGATE, Row.TOTAL}) {
                final long count = row == Row.COUNTER ? changedCounters : row == Row.TOTAL ? changedTargets : 1;
                material = material.temporary(Material.typed(TargetRecordAccounting.envelopeProjectionMaterial(row))
                        .repeat(count));
            }
            if (last) {
                writeRecords = TargetSourceAccounting.envelopeWriteRecords(
                        editCount(complete), changedCounters, changedTargets, plan.fence, plan.cursors);
                for (var edit : complete) {
                    writeBytes = Math.addExact(writeBytes, edit.written(frame));
                }
                writeBytes = Math.addExact(
                        writeBytes,
                        Math.addExact(
                                Math.multiplyExact(changedCounters, Row.COUNTER.stored(frame)),
                                Math.addExact(
                                        Math.multiplyExact(changedTargets, Row.TOTAL.stored(frame)),
                                        Row.AGGREGATE.stored(frame))));
                writeBytes = Math.addExact(
                        writeBytes,
                        KeyCodec.metaFixed(3).length
                                + 12L
                                + TargetSourcePosition.MAX_CANONICAL_BYTES
                                + KeyCodec.metaFixed(5).length
                                + 12L
                                + 8L);
                if (plan.fence) {
                    writeBytes = Math.addExact(writeBytes, Row.FENCE.stored(frame));
                }
                if (plan.cursors) {
                    writeBytes = Math.addExact(writeBytes, Row.CURSORS.stored(frame));
                }
                var projections = Material.empty();
                for (var row : new Row[] {Row.COUNTER, Row.TOTAL, Row.AGGREGATE, Row.FENCE, Row.CURSORS}) {
                    final long count = row == Row.COUNTER
                            ? changedCounters
                            : row == Row.TOTAL
                                    ? changedTargets
                                    : row == Row.FENCE
                                            ? (plan.fence ? 1 : 0)
                                            : row == Row.CURSORS ? (plan.cursors ? 1 : 0) : 1;
                    if (count == 0) {
                        continue;
                    }
                    final var canonical = Material.typed(TargetRecordAccounting.envelopeCanonicalMaterial(row, frame));
                    // prepare.measure serializes once; commit serializes again and constructs its actual NV array.
                    projections = projections.temporary(canonical
                            .drop(canonical.hold(Material.array(row.nv(frame))))
                            .repeat(count));
                }
                projections = projections.temporary(Material.array(TargetSourcePosition.MAX_CANONICAL_BYTES)
                        .hold(Material.array(12L + TargetSourcePosition.MAX_CANONICAL_BYTES)));
                projections = projections.temporary(Material.array(8).hold(Material.array(20)));
                material = material.temporary(projections);
            }
        }
        if (c.sourceFrameBytes >= 0) {
            final var input =
                    Material.array(frame).hold(Material.typed(TargetRecordAccounting.envelopeSourceMaterial(frame)));
            // Work-class sizing encodes once. Cursor sameEntry compares two separately encoded complete frames.
            final var submission = f2FrameMaterial(frame, 1, 0)
                    .hold(Material.array(TargetSourcePosition.MAX_CANONICAL_BYTES))
                    .temporary(Material.array(Math.addExact(
                                    frame,
                                    Math.addExact(
                                            TargetSourcePosition.MAX_CANONICAL_BYTES,
                                            "nereus-delay-source-apply-task\0".length())))
                            .hold(Material.array(32)));
            final var cursor = f2FrameMaterial(frame, 2, 0)
                    .hold(Material.arrays(2L * TargetSourcePosition.MAX_CANONICAL_BYTES, 2))
                    .temporary(Material.arrays(64, 2));
            material = input.hold(material)
                    .temporary(submission)
                    .temporary(cursor)
                    .temporary(f2FrameMaterial(frame, plan.sourceFrameEncodings, plan.sourceFrameCopies))
                    .temporary(f2FrameMaterial(frame, plan.assemblies.isEmpty() ? 0 : 1, 0))
                    .temporary(Material.typed(TargetRecordAccounting.envelopeSystemResultFromMaterial(frame)));
        }
        return new PlanCost(reads, writeRecords, writeBytes, material);
    }

    /* Retirement's complete namespace/ledger preparation has its own genuine cumulative loan. These
    plans include only the exact completed-proof consumer, its repeated point joins and accounting. */
    private static List<Plan> retirementFamilies(Capabilities c) {
        final var plans = new ArrayList<Plan>();
        final int q = TargetResourceReclaimBody.MAX_OPERATIONS, u = TargetEvidenceRetirementContext.MAX_USES;
        final long reclaimFrame = systemFrame(TargetResourceReclaimBody.MAX_NATIVE_CANONICAL_BYTES);
        final long evidenceFrame = systemFrame(TargetEvidenceRetireIntentBody.MAX_CANONICAL_BYTES);
        final var rejected = systemResults();
        plans.add(new Plan("retirement rejected before authority", evidenceFrame, 1, 0, 1)
                .reads(systemPrelude())
                .reads(new Reads().add(Row.FENCE, 2).add(activationOrigin(c)))
                .reads(replacementReads(rejected))
                .assembly(0, false, false, rejected));
        if (c.generation >= 5) {
            for (int provenance = 0; provenance < 4; provenance++) {
                final boolean recovery = provenance == 1, control = provenance >= 2, anchor = provenance == 3;
                final var intent = retirementBusiness();
                intent.add(create(Row.RETIRING, 1));
                final var intentReads = retirementEvidencePrelude(c, true)
                        .add(Row.RETIRING, 1, false)
                        .add(Row.CONFIRMED, 1, false)
                        .add(Row.FIRST, 1, false)
                        .add(Row.POSITION, 1, false)
                        .add(retirementUses(c, u, provenance, true));
                if (recovery) {
                    intentReads
                            .add(Row.RECOVERY_OWNER, 1)
                            .add(Row.RECOVERY_REF, 2)
                            .add(Row.KAFKA_MAPPING, 1)
                            .add(retirementAdoption())
                            .add(Row.ATTEMPT, 1);
                }
                plans.add(new Plan(
                                "retire intent provenance " + provenance,
                                evidenceFrame,
                                provenance == 0 ? 1 : 2,
                                provenance == 0 ? 0 : 1,
                                provenance == 0 ? 1 : 64)
                        .reads(intentReads)
                        .reads(replacementReads(intent))
                        .assembly(0, false, false, intent));

                final var confirmed = retirementBusiness();
                if (recovery) {
                    confirmed.add(delete(Row.RECOVERY_REF, 2));
                    confirmed.add(delete(Row.RECOVERY_OWNER, 1));
                }
                if (control) {
                    confirmed.add(delete(Row.EVIDENCE_REF, Math.multiplyExact(anchor ? 2L : 1L, u)));
                    confirmed.add(delete(Row.EVIDENCE_OWNER, 1));
                }
                confirmed.add(delete(Row.RETIRING, 1));
                confirmed.add(create(Row.CONFIRMED, 1));
                final var confirmReads = retirementEvidencePrelude(c, true)
                        .add(Row.RETIRING, 1)
                        .add(activationOrigin(c))
                        .add(retirementEvidenceRetained(c))
                        .add(retirementAppliedFirst())
                        .add(retirementUses(c, u, provenance, true))
                        .add(Row.CONFIRMED, 1, false)
                        .add(Row.FIRST, 1, false)
                        .add(Row.POSITION, 1, false)
                        .add(activationOrigin(c))
                        .add(activationOrigin(c));
                if (recovery) {
                    confirmReads.add(Row.RECOVERY_OWNER, 1).add(retirementCachedRange(Row.RECOVERY_REF, 2));
                }
                if (control) {
                    confirmReads
                            .add(retirementControlReferences(u, anchor))
                            .add(retirementCachedRange(Row.RECOVERY_OWNER, 0));
                }
                plans.add(new Plan(
                                "retire confirmation provenance " + provenance,
                                evidenceFrame,
                                control ? u + 2 : provenance == 0 ? 1 : 2,
                                provenance == 0 ? 0 : 1,
                                provenance == 0 ? 1 : 64)
                        .reads(confirmReads)
                        .reads(replacementReads(confirmed))
                        .assembly(0, false, false, confirmed));
            }
        }
        if (c.generation >= 6) {
            final var business = retirementBusiness();
            business.add(replace(Row.ATTEMPT, q));
            final var current = new Reads()
                    .add(Row.DESCRIPTOR, 1)
                    .add(Row.ATTEMPT, q)
                    .add(Row.FIRST, Math.multiplyExact(2L, q))
                    .add(Row.POSITION, Math.multiplyExact(2L, q))
                    .add(Row.FENCE, q);
            plans.add(new Plan(
                            "unused Attempt reserve",
                            systemFrame(TargetResourceReclaimBody.MAX_CANONICAL_BYTES),
                            q + 1,
                            q,
                            64)
                    .reads(retirementReclaimPrelude(c, new Reads().add(Row.ATTEMPT, q), new Reads()))
                    .reads(current)
                    .reads(new Reads().add(Row.ATTEMPT, q))
                    .reads(replacementReads(business))
                    .assembly(0, false, false, business));
        }
        if (c.generation >= 8) {
            final var business = retirementBusiness();
            business.add(delete(Row.MESSAGE, q));
            business.add(replace(retirementPayloadRow(c), q));
            final var expected = new Reads()
                    .add(retirementPayloadRow(c), q)
                    .add(Row.MESSAGE, q)
                    .add(Row.TERMINAL, q);
            plans.add(new Plan(
                            "self-contained INLINE release",
                            systemFrame(TargetResourceReclaimBody.MAX_CANONICAL_BYTES),
                            q + 1,
                            q,
                            64)
                    .reads(retirementReclaimPrelude(c, expected, retirementSourceFacts(c, q)))
                    .reads(retirementInlineCurrent(c, q))
                    .reads(new Reads().add(retirementPayloadRow(c), q))
                    .reads(replacementReads(business))
                    .assembly(0, false, false, business));
        }
        if (c.generation >= 9) {
            for (int kind = 0; kind < (c.generation >= 14 ? 3 : 2); kind++) {
                final Row row = kind == 0 ? Row.FIRST : Row.POSITION;
                final boolean referencesFirst = kind != 1;
                final var leaves = retirementBusiness();
                leaves.add(delete(row, q));
                plans.add(new Plan("retained Native result leaf kind " + kind, reclaimFrame, 1, 0, 1)
                        .reads(retirementReclaimPrelude(
                                c, new Reads().add(row, q), retirementNativeLeavesApplied(c, q)))
                        .reads(retirementFenceFirst())
                        .reads(new Reads()
                                .add(Row.FENCE, 1)
                                .add(row, q)
                                .add(Row.SAMPLE_REF, q, false)
                                .add(Row.FIRST, referencesFirst ? q : 0))
                        .reads(replacementReads(leaves))
                        .assembly(0, false, false, leaves));
            }
            final var allocations = retirementBusiness();
            allocations.add(replace(Row.ATTEMPT, q));
            plans.add(new Plan("retained Native allocation shares", reclaimFrame, q + 1, q, 64)
                    .reads(retirementReclaimPrelude(
                            c, new Reads().add(Row.ATTEMPT, q), retirementNativeAllocationsApplied(q)))
                    .reads(retirementNativeCurrent(c, q, true))
                    .reads(new Reads().add(Row.ATTEMPT, q))
                    .reads(replacementReads(allocations))
                    .assembly(0, false, false, allocations));
        }
        if (c.generation >= 10) {
            for (boolean confirmation : new boolean[] {false, true}) {
                final var business = retirementBusiness();
                if (confirmation) {
                    business.add(delete(Row.MESSAGE, 1));
                    business.add(delete(Row.OBJECT_PENDING, 1));
                    business.add(replace(retirementPayloadRow(c), 1));
                } else {
                    business.add(create(Row.OBJECT_PENDING, 1));
                }
                final var expected = new Reads()
                        .add(retirementPayloadRow(c), 1)
                        .add(Row.MESSAGE, 1)
                        .add(Row.TERMINAL, 1)
                        .add(Row.OBJECT_PENDING, 1, confirmation);
                final var direct = retirementReclaimPrelude(c, expected, retirementObjectApplied(c))
                        .add(retirementInlineCurrent(c, 1))
                        .add(retirementAcceptance(c))
                        .add(Row.OBJECT_PENDING, 1, confirmation);
                if (confirmation) {
                    direct.add(retirementObjectFirst()).add(retirementPayloadRow(c), 1);
                } else {
                    direct.add(Row.OBJECT_PENDING, 1, false);
                }
                plans.add(new Plan(
                                confirmation ? "OBJECT confirmed release" : "OBJECT retirement intent",
                                systemFrame(
                                        confirmation
                                                ? TargetResourceReclaimBody.MAX_NATIVE_CANONICAL_BYTES
                                                : TargetResourceReclaimBody.MAX_OBJECT_INTENT_BYTES),
                                2,
                                1,
                                64)
                        .reads(direct)
                        .reads(replacementReads(business))
                        .assembly(0, false, false, business));
            }
        }
        if (c.generation >= 11) {
            for (boolean confirmation : new boolean[] {false, true}) {
                final var business = retirementBusiness();
                if (confirmation) {
                    business.add(delete(Row.OBJECT_PENDING, 1));
                    business.add(replace(retirementPayloadRow(c), 1));
                } else {
                    business.add(create(Row.OBJECT_PENDING, 1));
                }
                final var expected = new Reads()
                        .add(retirementPayloadRow(c), 1)
                        .add(Row.MESSAGE, 1, false)
                        .add(Row.RESERVATION, 2)
                        .add(Row.OBJECT_PENDING, 1, confirmation);
                final var direct = retirementReclaimPrelude(c, expected, retirementUncommittedApplied(c))
                        .add(retirementUncommittedFacts(c))
                        .add(retirementPayloadRow(c), 1)
                        .add(Row.OBJECT_PENDING, 1, confirmation);
                if (confirmation) {
                    direct.add(retirementUncommittedPending(c)).add(retirementPayloadRow(c), 1);
                } else {
                    direct.add(Row.OBJECT_PENDING, 1, false);
                }
                plans.add(new Plan(
                                confirmation
                                        ? "uncommitted OBJECT confirmed release"
                                        : "uncommitted OBJECT retirement intent",
                                systemFrame(
                                        confirmation
                                                ? TargetResourceReclaimBody.MAX_NATIVE_CANONICAL_BYTES
                                                : TargetResourceReclaimBody.MAX_OBJECT_INTENT_BYTES),
                                2,
                                1,
                                64)
                        .reads(direct)
                        .reads(replacementReads(business))
                        .assembly(0, false, false, business));
            }
        }
        if (c.generation >= 13) {
            final var draining = retirementBusiness();
            draining.add(replace(Row.DESCRIPTOR, 1));
            final var drainExpected = new Reads().add(Row.DESCRIPTOR, 1).add(Row.CLOSE, 1);
            plans.add(new Plan("incarnation drain", reclaimFrame, 2, 1, 64)
                    .reads(retirementReclaimPrelude(
                            c, drainExpected, retirementFenceFirst().add(retirementIncarnationApplied(c))))
                    .reads(retirementFenceFirst())
                    .reads(new Reads().add(Row.FENCE, 1))
                    .reads(drainExpected)
                    .reads(drainExpected)
                    .reads(new Reads().add(Row.FIRST, 1).add(Row.POSITION, 1))
                    .reads(replacementReads(draining))
                    .assembly(0, false, false, draining));
            final var retired = retirementBusiness();
            retired.add(delete(Row.DESCRIPTOR, 1));
            retired.add(delete(Row.GRANT, 1));
            final var retireExpected = new Reads().add(Row.DESCRIPTOR, 1).add(Row.GRANT, 1);
            plans.add(new Plan("incarnation retirement", reclaimFrame, 2, 1, 64)
                    .reads(retirementReclaimPrelude(
                            c, retireExpected, retirementFenceFirst().add(retirementIncarnationApplied(c))))
                    .reads(retirementFenceFirst())
                    .reads(new Reads().add(Row.FENCE, 1))
                    .reads(retireExpected)
                    .reads(retireExpected)
                    .reads(retirementDrain())
                    .reads(new Reads().add(Row.COUNTER, 2).add(Row.BOOKKEEPING, 1))
                    .reads(replacementReads(retired))
                    .assembly(0, false, false, retired));
        }
        return plans;
    }

    private static List<Edit> retirementBusiness() {
        final var business = systemResults();
        business.add(create(Row.HISTORY, 1));
        business.add(replace(Row.HISTORY_TIP, 1));
        return business;
    }

    private static Row retirementPayloadRow(Capabilities c) {
        return Row.PAYLOAD;
    }

    private static Reads retirementRepeat(Reads reads, int count) {
        final var result = new Reads();
        for (int i = 0; i < count; i++) {
            result.add(reads);
        }
        return result;
    }

    private static Reads retirementReclaimPrelude(Capabilities c, Reads expected, Reads appliedHistory) {
        // prepareFirst: preflight and planner independently select the activation, tip and exact values.
        return systemPrelude()
                .add(Row.FENCE, 2)
                .add(retirementRepeat(activationOrigin(c), 3))
                .add(retirementRepeat(new Reads().add(Row.HISTORY_TIP, 1).add(retirementTip(c, appliedHistory)), 2))
                .add(Row.HISTORY, 1, false)
                .add(retirementRepeat(expected, 2));
    }

    private static Reads retirementEvidencePrelude(Capabilities c, boolean marker) {
        return systemPrelude()
                .add(Row.FENCE, 2)
                .add(retirementRepeat(activationOrigin(c), marker ? 5 : 3))
                .add(retirementRepeat(new Reads().add(Row.HISTORY_TIP, 1).add(retirementTip(c, new Reads())), 2))
                .add(Row.HISTORY, 1, false);
    }

    private static Reads retirementAppliedFirst() {
        return new Reads().add(Row.FIRST, 1).add(Row.DESCRIPTOR, 1).add(Row.POSITION, 1);
    }

    private static Reads retirementObjectFirst() {
        return new Reads().add(Row.FIRST, 1).add(Row.POSITION, 1).add(Row.HISTORY, 1);
    }

    private static Reads retirementFenceFirst() {
        return new Reads().add(Row.FIRST, 1).add(Row.POSITION, 1);
    }

    private static Reads retirementDrain() {
        return new Reads().add(Row.POSITION, 1).add(Row.HISTORY, 1).add(Row.FIRST, 1);
    }

    private static Reads retirementMigration() {
        return new Reads().add(Row.MIGRATION, 1).add(Row.FIRST, 1).add(Row.POSITION, 1);
    }

    private static Reads retirementMigrationPayload() {
        return retirementMigration().add(Row.MIGRATION_ORIGIN, 1).add(Row.MIGRATION_PREFIX, 1);
    }

    private static Reads retirementSourceFacts(Capabilities c, int retained) {
        final var identity = new Reads().add(Row.ATTEMPT, 1);
        final var settlement = new Reads()
                .add(Row.MIGRATION_GUARD, 1)
                .add(retirementMigration())
                .add(retirementFenceFirst());
        final var selected = Reads.either(new Reads(), identity, c.migration ? settlement : new Reads());
        return retirementFenceFirst()
                .add(retirementRepeat(retirementFenceFirst().add(selected), retained));
    }

    private static Reads retirementInlineCurrent(Capabilities c, int operations) {
        return new Reads()
                .add(retirementPayloadRow(c), operations)
                .add(Row.MESSAGE, operations)
                .add(Row.TERMINAL, operations)
                .add(retirementSourceFacts(c, operations))
                .add(Row.FENCE, 1)
                .add(Row.MESSAGE, operations)
                .add(Row.TERMINAL, operations);
    }

    private static Reads retirementAcceptance(Capabilities c) {
        final var direct = new Reads()
                .add(Row.BINDING, 1)
                .add(Row.FIRST, 1)
                .add(Row.POSITION, 1)
                .add(Row.DESCRIPTOR, 1);
        final var prepare = new Reads()
                .add(Row.BINDING, 1)
                .add(Row.RESERVATION, 2)
                .add(Row.FIRST, 2)
                .add(Row.POSITION, 2)
                .add(Row.DESCRIPTOR, 2);
        if (!c.migration) {
            return Reads.either(direct, prepare);
        }
        final var imported = new Reads()
                .add(Row.BINDING, 1)
                .add(Row.RESERVATION, 2)
                .add(retirementRepeat(retirementMigrationPayload(), 2))
                .add(Row.FIRST, 1)
                .add(Row.POSITION, 1)
                .add(Row.DESCRIPTOR, 1);
        return Reads.either(direct, prepare, imported);
    }

    private static Reads retirementUncommittedDecision() {
        return retirementFenceFirst().add(Row.DESCRIPTOR, 1);
    }

    private static Reads retirementUncommittedFacts(Capabilities c) {
        final var common = new Reads()
                .add(Row.RESERVATION, 2)
                .add(Row.BINDING, 1)
                .add(Row.MESSAGE, 1, false)
                .add(retirementRepeat(retirementUncommittedDecision(), 2));
        final var prepared = new Reads().add(Row.FIRST, 1).add(Row.POSITION, 2).add(Row.DESCRIPTOR, 1);
        return common.add(c.migration ? Reads.either(prepared, retirementMigrationPayload()) : prepared);
    }

    private static Reads retirementUncommittedPending(Capabilities c) {
        return retirementUncommittedFacts(c)
                .add(retirementUncommittedDecision())
                .add(retirementPayloadRow(c), 1);
    }

    private static Reads retirementPending(Capabilities c) {
        final var object = retirementObjectFirst()
                .add(retirementSourceFacts(c, 1))
                .add(retirementAcceptance(c))
                .add(retirementPayloadRow(c), 1)
                .add(Row.MESSAGE, 1)
                .add(Row.TERMINAL, 2);
        return c.generation >= 11 ? Reads.either(object, retirementUncommittedPending(c)) : object;
    }

    private static Reads retirementObjectApplied(Capabilities c) {
        final var common = retirementSourceFacts(c, 1)
                .add(retirementAcceptance(c))
                .add(Row.TERMINAL, 1)
                .add(retirementPayloadRow(c), 1)
                .add(retirementObjectFirst())
                .add(Row.OBJECT_PENDING, 1);
        final var retained =
                new Reads().add(retirementPayloadRow(c), 1).add(Row.MESSAGE, 1).add(Row.TERMINAL, 1);
        final var released = new Reads()
                .add(Row.MESSAGE, 1, false)
                .add(Row.POSITION, 1)
                .add(Row.HISTORY, 1)
                .add(retirementObjectFirst())
                .add(Row.TERMINAL, 1);
        return common.add(Reads.either(retained, released));
    }

    private static Reads retirementUncommittedApplied(Capabilities c) {
        return retirementUncommittedFacts(c)
                .add(retirementUncommittedDecision())
                .add(retirementPayloadRow(c), 1)
                .add(Row.OBJECT_PENDING, 1)
                .add(Reads.either(
                        new Reads(),
                        new Reads().add(Row.POSITION, 1).add(Row.HISTORY, 1).add(retirementUncommittedDecision())));
    }

    private static Reads retirementFirst(Capabilities c) {
        final var decisions = new ArrayList<Reads>();
        decisions.add(new Reads());
        if (c.generation >= 8) {
            decisions.add(retirementSourceFacts(c, TargetResourceReclaimBody.MAX_OPERATIONS));
        }
        if (c.generation >= 9) {
            decisions.add(retirementNativeApplied(c));
        }
        if (c.generation >= 10) {
            decisions.add(retirementObjectApplied(c));
        }
        if (c.generation >= 11) {
            decisions.add(retirementUncommittedApplied(c));
        }
        return retirementFirst(
                c, decisions.size() == 1 ? decisions.getFirst() : Reads.either(decisions.toArray(Reads[]::new)));
    }

    private static Reads retirementFirst(Capabilities c, Reads appliedDecision) {
        return retirementAppliedFirst()
                .add(Row.ACTIVATION, 1)
                .add(Row.HISTORY_TIP, 1)
                .add(activationOrigin(c))
                .add(appliedDecision);
    }

    private static Reads retirementRetained(Capabilities c) {
        return activationOrigin(c).add(Row.HISTORY, 1).add(retirementFirst(c));
    }

    private static Reads retirementEvidenceRetained(Capabilities c) {
        return activationOrigin(c).add(Row.HISTORY, 1).add(retirementFirst(c, new Reads()));
    }

    private static Reads retirementTip(Capabilities c) {
        return new Reads()
                .add(Row.HISTORY, 1)
                .add(retirementFirst(c))
                .add(activationOrigin(c))
                .add(Row.HISTORY_TIP, 1);
    }

    private static Reads retirementTip(Capabilities c, Reads appliedDecision) {
        return new Reads()
                .add(Row.HISTORY, 1)
                .add(retirementFirst(c, appliedDecision))
                .add(activationOrigin(c))
                .add(Row.HISTORY_TIP, 1);
    }

    private static Reads retirementFirstHistory(Capabilities c) {
        return c.generation < 5
                ? new Reads()
                : new Reads().add(Row.HISTORY, 1).add(Row.ACTIVATION, 1).add(retirementRetained(c));
    }

    private static Reads retirementAttemptReclaim(Capabilities c) {
        if (c.generation < 6) {
            return new Reads().add(Row.ACTIVATION, 3, false);
        }
        final int nativeActivations = c.generation >= 12 ? 2 : c.generation >= 9 ? 1 : 0;
        final var nativeSuccess = new Reads()
                .add(Row.ACTIVATION, 2)
                .add(Row.POSITION, 1)
                .add(Row.HISTORY, 1)
                .add(retirementFirst(c))
                .add(Row.FIRST, 1)
                .add(retirementRepeat(retirementConfirmation(), TargetResourceReclaimBody.MAX_OPERATIONS));
        final var ordinary = new Reads()
                .add(Row.ACTIVATION, nativeActivations + 1)
                .add(Row.ACTIVATION, 2 - nativeActivations, false)
                .add(Row.POSITION, nativeActivations + 1)
                .add(Row.HISTORY, nativeActivations + 1)
                .add(activationApplied(c))
                .add(retirementFirst(c))
                .add(Row.FIRST, 1);
        return nativeActivations == 0
                ? Reads.either(new Reads(), ordinary)
                : Reads.either(new Reads(), nativeSuccess, ordinary);
    }

    private static Reads retirementPayloadReclaim(Capabilities c) {
        final var inline = new Reads()
                .add(Row.POSITION, 1)
                .add(Row.HISTORY, 1)
                .add(retirementFirst(c))
                .add(Row.FIRST, 1)
                .add(Row.TERMINAL, 1)
                .add(Row.MESSAGE, 1, false);
        final var object = new Reads()
                .add(Row.POSITION, 1)
                .add(Row.HISTORY, 1)
                .add(retirementObjectFirst())
                .add(retirementFirst(c));
        final var uncommitted = new Reads()
                .add(Row.POSITION, 1)
                .add(Row.HISTORY, 1)
                .add(retirementUncommittedApplied(c))
                .add(retirementFirst(c));
        return c.generation >= 11
                ? Reads.either(new Reads(), inline, object, uncommitted)
                : Reads.either(new Reads(), inline, object);
    }

    private static Reads retirementRemainingAttempt(Capabilities c) {
        return Reads.either(
                new Reads().add(Row.EVIDENCE_REF, 1, false).add(Row.RECOVERY_REF, 1, false),
                new Reads().add(Row.EVIDENCE_REF, 1).add(Row.EVIDENCE_OWNER, 1).add(Row.RECOVERY_REF, 1, false),
                new Reads()
                        .add(Row.EVIDENCE_REF, 1)
                        .add(Row.EVIDENCE_OWNER, 1)
                        .add(Row.RECOVERY_REF, 2)
                        .add(Row.RECOVERY_OWNER, 1));
    }

    private static Reads retirementConfirmation() {
        final var common = new Reads().add(Row.CONFIRMED, 1).add(Row.FIRST, 1).add(Row.POSITION, 1);
        final var recovery = new Reads().add(Row.RECOVERY_OWNER, 1, false).add(Row.RECOVERY_REF, 2, false);
        final var anchor = new Reads()
                .add(Row.KAFKA_MAPPING, 1)
                .add(Row.FIRST, 1)
                .add(Row.POSITION, 1)
                .add(Row.EVIDENCE_OWNER, 1, false)
                .add(Row.EVIDENCE_REF, 2, false);
        return common.add(Reads.either(recovery, anchor));
    }

    private static Reads retirementNativeCurrent(Capabilities c, int operations, boolean allocations) {
        final var reads = retirementFenceFirst().add(Row.FENCE, 1);
        if (allocations) {
            return reads.add(Row.ATTEMPT, operations)
                    .add(retirementRepeat(
                            retirementConfirmation(),
                            Math.multiplyExact(operations, TargetResourceReclaimBody.MAX_OPERATIONS)));
        }
        return reads.add(Reads.either(
                new Reads()
                        .add(Row.FIRST, operations)
                        .add(Row.SAMPLE_REF, operations, false)
                        .add(Row.FIRST, operations),
                new Reads().add(Row.POSITION, operations).add(Row.SAMPLE_REF, operations, false)));
    }

    private static Reads retirementNativeApplied(Capabilities c) {
        final int q = TargetResourceReclaimBody.MAX_OPERATIONS;
        return c.generation >= 13
                ? Reads.either(
                        retirementNativeLeavesApplied(c, q),
                        retirementNativeAllocationsApplied(q),
                        retirementFenceFirst().add(retirementIncarnationApplied(c)))
                : Reads.either(retirementNativeLeavesApplied(c, q), retirementNativeAllocationsApplied(q));
    }

    private static Reads retirementNativeLeavesApplied(Capabilities c, int operations) {
        final var result = new Reads()
                .add(Row.SAMPLE_REF, operations, false)
                .add(Row.FIRST, operations, false)
                .add(Row.FIRST, operations);
        final var expired = new Reads().add(Row.SAMPLE_REF, operations, false).add(Row.POSITION, operations, false);
        final var position = new Reads()
                .add(Row.SAMPLE_REF, operations, false)
                .add(Row.POSITION, operations, false)
                .add(Row.FIRST, operations);
        return retirementFenceFirst()
                .add(c.generation >= 14 ? Reads.either(result, expired, position) : Reads.either(result, expired));
    }

    private static Reads retirementNativeAllocationsApplied(int operations) {
        return retirementFenceFirst()
                .add(Row.ATTEMPT, operations)
                .add(retirementRepeat(
                        retirementConfirmation(),
                        Math.multiplyExact(operations, TargetResourceReclaimBody.MAX_OPERATIONS)));
    }

    private static Reads retirementIncarnationApplied(Capabilities c) {
        final var drain = new Reads().add(Row.DESCRIPTOR, 1).add(retirementFenceFirst());
        final var deleted = new Reads()
                .add(Row.COUNTER, 2)
                .add(Row.POSITION, 1)
                .add(Row.HISTORY, 1)
                .add(Row.FIRST, 1);
        final var retire = new Reads()
                .add(Row.DESCRIPTOR, 1, false)
                .add(retirementDrain())
                .add(Row.GRANT, 1, false)
                .add(Row.COUNTER, 2)
                .add(Row.DESCRIPTOR, 1)
                .add(Row.GRANT, 1);
        return Reads.either(drain, new Reads().add(drain).add(deleted), retire);
    }

    private static Reads retirementIncarnationOrigin(Capabilities c) {
        return new Reads().add(Row.ACTIVATION, 1).add(retirementDrain());
    }

    private static Reads retirementMappingUse() {
        return new Reads()
                .add(Row.KAFKA_MAPPING, 1)
                .add(Row.FIRST, 1)
                .add(Row.POSITION, 1)
                .add(Row.ATTEMPT, 1)
                .add(Row.DESCRIPTOR, 2);
    }

    private static Reads retirementAdoption() {
        return retirementMappingUse()
                .add(Row.EVIDENCE_REF, 3)
                .add(Row.EVIDENCE_OWNER, 2)
                .add(Row.DESCRIPTOR, 2);
    }

    private static Reads retirementControlReferences(int uses, boolean anchor) {
        final var each = new Reads()
                .add(Row.EVIDENCE_REF, anchor ? 2 : 1)
                .add(Row.KAFKA_MAPPING, 1)
                .add(retirementMappingUse())
                .add(Row.DESCRIPTOR, 1);
        final int rows = Math.multiplyExact(anchor ? 2 : 1, uses);
        final long joined = Math.multiplyExact(uses, Row.EVIDENCE_REF.payload);
        return new Reads()
                .add(Row.EVIDENCE_OWNER, 1)
                .add(retirementRepeat(each, rows))
                .add(retirementCachedRange(Row.EVIDENCE_REF, rows))
                .material(joined, joined);
    }

    private static Reads retirementCachedRange(Row row, int returned) {
        // The private inventory's KeyValues are borrowed. first() clones candidate comparison keys;
        // the caller clones range/decode keys, the returned NV and its next lower bound. Overlay
        // clones are charged by common against this exact assembly's actual business list.
        final long calls = Math.addExact(returned, 1L);
        final long visits = Math.multiplyExact(returned, Math.addExact(returned, 3L)) / 2L;
        final long candidateKeys = Math.multiplyExact(Math.multiplyExact(2L, visits), row.key);
        final long decode = TargetRecordAccounting.envelopeDecodedPayloadCopies(row.payload, 1);
        final long returnedArrays = Math.multiplyExact(
                returned,
                Math.addExact(Math.addExact(Math.addExact(Math.multiplyExact(5L, row.key), row.nv()), 2L), decode));
        final long allocated =
                Math.addExact(Math.multiplyExact(4L, calls), Math.addExact(candidateKeys, returnedArrays));
        final long retained = returned == 0
                ? 4L
                : Math.addExact(
                        Math.multiplyExact(6L, row.key),
                        Math.addExact(Math.addExact(Math.multiplyExact(2L, row.nv()), row.payload), 6L));
        return new Reads().retirementRangeCalls(calls).material(allocated, retained);
    }

    private static Reads retirementUses(Capabilities c, int uses, int provenance, boolean retiring) {
        final var reads = new Reads().add(Row.FENCE, 1).add(retirementRepeat(retirementAppliedFirst(), uses));
        if (provenance == 0) {
            // Staging has no selected Native reference after the complete private inventory proved absence.
            reads.add(Row.DESCRIPTOR, 1)
                    .add(Reads.either(new Reads().add(Row.EVIDENCE_OWNER, 1), new Reads().add(Row.RECOVERY_OWNER, 1)));
            reads.add(Reads.either(
                    retirementCachedRange(Row.EVIDENCE_REF, 0), retirementCachedRange(Row.RECOVERY_REF, 0)));
            if (retiring) {
                reads.add(activationOrigin(c));
            }
        } else if (retiring && provenance >= 2) {
            reads.add(Row.EVIDENCE_OWNER, 1)
                    .add(Row.CHANNEL_CONTROL, 1)
                    .add(retirementControlReferences(uses, provenance == 3));
        }
        return reads;
    }

    private static Reads retirementRetiring(Capabilities c) {
        final int u = TargetEvidenceRetirementContext.MAX_USES;
        final var variants = new ArrayList<Reads>();
        variants.add(retirementUses(c, u, 0, true));
        variants.add(retirementUses(c, u, 1, true).add(Row.RECOVERY_OWNER, 1).add(Row.RECOVERY_REF, 2));
        variants.add(retirementUses(c, u, 2, true).add(Row.EVIDENCE_OWNER, 1));
        variants.add(retirementUses(c, u, 3, true).add(Row.EVIDENCE_OWNER, 1));
        return activationOrigin(c)
                .add(retirementEvidenceRetained(c))
                .add(retirementAppliedFirst())
                .add(Reads.either(variants.toArray(Reads[]::new)))
                .add(Row.DESCRIPTOR, 1);
    }

    private static Reads retirementConfirmed(Capabilities c) {
        final int u = TargetEvidenceRetirementContext.MAX_USES;
        final var staging = retirementUses(c, u, 0, false).add(activationOrigin(c));
        final var recovery = retirementUses(c, u, 1, false)
                .add(Row.RECOVERY_OWNER, 1, false)
                .add(retirementCachedRange(Row.RECOVERY_REF, 0));
        final var control = retirementUses(c, u, 2, false)
                .add(Row.EVIDENCE_OWNER, 1, false)
                .add(Row.FIRST, u)
                .add(retirementCachedRange(Row.EVIDENCE_REF, 0))
                .add(retirementCachedRange(Row.RECOVERY_OWNER, 0));
        return activationOrigin(c)
                .add(retirementEvidenceRetained(c))
                .add(retirementEvidenceRetained(c))
                .add(activationOrigin(c))
                .add(Reads.either(staging, recovery, control))
                .add(Row.RETIRING, 1, false)
                .add(retirementRepeat(retirementAppliedFirst(), 2));
    }

    /* MIGRATION_FAMILY_REGION: only actual Source A/settlement/I/R planners belong here. Native69 D
    uses a local preparation ordinal and direct quota delta; held legacy projections retain their
    original interpreter/loan instead of acquiring a fabricated fixed Source family. */
    private static List<Plan> migrationFamilies(Capabilities c) {
        final var plans = new ArrayList<Plan>();
        if (!c.migration) {
            return plans;
        }
        for (boolean issuer : new boolean[] {false, true}) {
            final var business = edits(replace(Row.MIGRATION, 1));
            business.addAll(systemResults());
            final var activation = new Plan("migration/A/" + (issuer ? 2 : 1), migrationFrame(34, issuer), 1, 0, 1)
                    .reads(migrationResultCreation(false))
                    .reads(migrationOperation(34, issuer))
                    .reads(new Reads()
                            .add(Row.MIGRATION, 1)
                            .add(Row.MIGRATION_ATTEMPT, 1)
                            .add(Row.AGGREGATE, 1)
                            .add(Row.FENCE, 1))
                    .reads(replacementReads(business))
                    .assembly(0, false, false, business);
            activation.migrationActivation = true;
            activation.migrationNoArchiveTail = true;
            plans.add(activation);
        }
        // An ordinary runtime cannot start a new A. The actual retained-first probe precedes that throw.
        plans.add(new Plan("migration/new-A-rejected", migrationFrame(34, true), 1, 0, 1)
                .reads(new Reads().add(Row.FIRST, 1, false).add(Row.POSITION, 1, false)));
        final long activationFrame = migrationFrame(34, true);
        final var checkedA = new Reads().add(f2Activated()).add(Row.AGGREGATE, 1);
        plans.add(new Plan("migration/A/same-physical-replay", activationFrame, 1, 0, 1)
                .reads(new Reads()
                        .add(checkedA)
                        .add(Row.FIRST, 1)
                        .add(Row.POSITION, 1)
                        .add(Row.AGGREGATE, 1)));
        final var duplicate = new Plan("migration/A/later-physical-replay", activationFrame, 1, 0, 1)
                .reads(new Reads()
                        .add(checkedA)
                        .add(Row.FIRST, 2)
                        .add(Row.POSITION, 4, false)
                        .add(Row.DESCRIPTOR, 3)
                        .add(Row.AGGREGATE, 5)
                        .add(Row.FENCE, 2))
                .assembly(0, false, false, create(Row.POSITION, 1));
        plans.add(duplicate);

        migrationSettlementFamilies(c, plans);
        if (c.generation >= 17) {
            migrationRetirementFamily(c, plans, false);
        }
        if (c.generation >= 18) {
            migrationRetirementFamily(c, plans, true);
        }
        return plans;
    }
    /** The runtime replay probe and appendResults CreationAuthority are distinct absence gets.
     * Reader.replace and subsequent accounting/backend before checks are added by their actual owners. */
    private static Reads migrationResultCreation(boolean replay) {
        return new Reads().add(Row.FIRST, replay ? 2 : 1, false).add(Row.POSITION, replay ? 2 : 1, false);
    }

    private static long migrationFrame(int kind, boolean issuer) {
        final long request =
                switch (kind) {
                    case 34 ->
                        issuer
                                ? TargetMigrationActivateRequest.MAX_CANONICAL_BYTES
                                : TargetMigrationActivateRequest.V1_MAX_CANONICAL_BYTES;
                    case 35 -> TargetMigrationSettlementRequest.MAX_CANONICAL_BYTES;
                    case 36 -> TargetMigrationRetirementIntentRequest.MAX_CANONICAL_BYTES;
                    case 37 -> TargetMigrationRetirementConfirmRequest.MAX_CANONICAL_BYTES;
                    default -> throw new IllegalArgumentException("not an actual migration Source planner");
                };
        return controlFrame(Math.addExact(request, 192L));
    }

    private static Reads migrationOperation(int kind, boolean issuer) {
        final var material = TargetRecordAccounting.envelopeMigrationOperationMaterial(kind, issuer);
        return new Reads().material(material.allocated(), material.retained());
    }

    private static Reads migrationSettlementPrelude() {
        return migrationResultCreation(true)
                .add(f2Activated())
                .add(Row.AGGREGATE, 1)
                .add(Row.FENCE, 1)
                .add(Row.MIGRATION_GUARD, 1)
                .add(migrationOperation(35, false));
    }

    private static void migrationSettlementFamilies(Capabilities c, List<Plan> plans) {
        final long frame = migrationFrame(35, false);
        final var guard = edits(replace(Row.MIGRATION_GUARD, 1));
        guard.addAll(systemResults());
        final var noOwner = migrationSettlementPlan(
                "migration/settlement/no-payload",
                frame,
                2,
                1,
                guard,
                migrationSettlementPrelude().add(Row.PAYLOAD, 1, false));
        plans.add(noOwner);
        // A qualified owner can stop at its current nonterminal Message; the owner-only gates are prefixes.
        plans.add(migrationSettlementPlan(
                "migration/settlement/current-payload-work",
                frame,
                2,
                1,
                guard,
                migrationSettlementPrelude()
                        .add(Row.PAYLOAD, 1)
                        .add(Row.AGGREGATE, 1)
                        .add(Row.MESSAGE, 1)));
        for (boolean legacy : new boolean[] {false, true}) {
            if (!legacy && !c.payloadV2) {
                continue;
            }
            final var payload = legacy ? Row.PAYLOAD_V1 : Row.PAYLOAD;
            final var retained = edits(replace(Row.MIGRATION_GUARD, 1), replace(payload, 1));
            retained.addAll(systemResults());
            final var reads = migrationSettlementPrelude()
                    .add(payload, 1)
                    .add(Row.AGGREGATE, 1)
                    .add(Row.MESSAGE, 1)
                    .add(Row.TERMINAL, 1)
                    .add(Row.BINDING, 1);
            // Both checks use the exact same-batch settled guard from the overlay, with no Native get.
            if (legacy) {
                reads.local(Row.MIGRATION_GUARD, 2);
            }
            // Native67 freezes its own payer. No protocol relation equates it with the payload payer or SourceRoot.
            plans.add(migrationSettlementPlan(
                    "migration/settlement/retain-payload-v" + (legacy ? 1 : 2), frame, 3, 2, retained, reads));
        }
    }

    private static Plan migrationSettlementPlan(
            String label, long frame, int owners, int targets, List<Edit> business, Reads direct) {
        final var plan = new Plan(label, frame, owners, targets, 1)
                .reads(direct)
                .reads(replacementReads(business))
                .assembly(0, false, false, business);
        plan.migrationSettlement = true;
        return plan;
    }

    private static void migrationRetirementFamily(Capabilities c, List<Plan> plans, boolean confirmation) {
        final int kind = confirmation ? 37 : 36;
        final var business = edits(confirmation ? replace(Row.RETIREMENT, 1) : create(Row.RETIREMENT, 1));
        business.addAll(systemResults());
        // prepareFirst captures the actual root/selection, then independently rereads both in prepare.
        final var direct = migrationResultCreation(true)
                .add(migrationOperation(kind, false))
                .add(repeat(f2Activated(), 2))
                .add(Row.DESCRIPTOR, 1)
                .add(Row.FENCE, 2)
                .add(Row.AGGREGATE, confirmation ? 8 : 6);
        if (confirmation) {
            direct.add(repeat(f2Selected(false), 2)).add(repeat(f2Takeover(c), 2));
        } else {
            direct.add(Row.RETIREMENT, 2, false);
        }
        final var plan = new Plan(
                        confirmation ? "migration/retire-confirm" : "migration/retire-intent",
                        migrationFrame(kind, false),
                        1,
                        0,
                        1)
                .reads(direct)
                .reads(replacementReads(business))
                .assembly(0, false, false, business);
        plan.migrationRetirementIntent = !confirmation;
        plan.migrationRetirementConfirmation = confirmation;
        // I precedes the first archive head; R requires the already-taken head and does not append tails.
        plan.migrationNoArchiveTail = true;
        plans.add(plan);
    }
}
