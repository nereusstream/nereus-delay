package com.nereusstream.delay.store;

import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplayOutcome;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalLaneTuple;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.LaneRecordEnvelope;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PrepareLargeScheduleBody;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.ScheduleBinding;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.TargetControlScope;
import com.nereusstream.delay.protocol.TargetDispatchCompatibility;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetMigrationCustodyGuard;
import com.nereusstream.delay.protocol.TargetMigrationObjectReference;
import com.nereusstream.delay.protocol.TargetMigrationPlan;
import com.nereusstream.delay.protocol.TargetMigrationPrefixRecord;
import com.nereusstream.delay.protocol.TargetMigrationRecordReference;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.runtime.AdmissionGate;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.ClaimRecord;
import com.nereusstream.delay.runtime.CommandResult;
import com.nereusstream.delay.runtime.CurrentSendWorkKind;
import com.nereusstream.delay.runtime.GenerationAggregateState;
import com.nereusstream.delay.runtime.LaneRecord;
import com.nereusstream.delay.runtime.MessageRecord;
import com.nereusstream.delay.runtime.MessageStatus;
import com.nereusstream.delay.runtime.PayloadReservation;
import com.nereusstream.delay.runtime.PayloadReservationStatus;
import com.nereusstream.delay.runtime.PublishAttemptLedger;
import com.nereusstream.delay.runtime.RetiredMessageIdentityRecord;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetExpiryRef;
import com.nereusstream.delay.runtime.TargetGenerationRuntimeIndex;
import com.nereusstream.delay.runtime.TargetMessageRecord;
import com.nereusstream.delay.runtime.TargetMigrationImporter;
import com.nereusstream.delay.runtime.TargetOrderState;
import com.nereusstream.delay.runtime.TargetPayloadObligationSummaryBootstrap;
import com.nereusstream.delay.runtime.TargetReservationRecord;
import com.nereusstream.delay.runtime.TargetTimelineWorkRef;
import com.nereusstream.delay.runtime.TerminalGenerationRecord;
import com.nereusstream.delay.runtime.TimelineWorkKind;
import com.nereusstream.delay.runtime.UncertainRetryAuthority;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Finite migration material. Neither construction nor publication activates a Store or retires custody. */
public final class LegacyMigrationBundle {
    private LegacyMigrationBundle() {}

    /**
     * An original Process/provider-owned maintenance flight, not a serializable authority DTO. Implementations
     * retain the actual writer/admission freeze, Source/checkpoint protection, SDK/Journal registrations and
     * resource reservations through sealing and activation. An SDK shutdown or TIME_FENCE is not noFuture.
     */
    public interface Held extends AutoCloseable {
        void requireCurrent();

        long retainedBytes();

        TargetQuotaMutation baseline(LegacyCheckpointStateInventory.CompleteView view);

        List<LaneMapping> mappings();

        List<ExternalRecord> externalRecords();

        void requireClosedWriterDomain(LegacyCheckpointStateInventory.CompleteView view, List<LaneMapping> mappings);

        void requireLegacyCustody(LegacyCheckpointStateInventory.CompleteView view, List<ExternalRecord> records);

        boolean noFuture(DelayMessageId id, MessageRecord message, ScheduleBinding binding);

        Conversion conversion(DelayMessageId id, MessageRecord message, ScheduleBinding binding, LaneMapping mapping);

        ReservationConversion reservation(PayloadReservation original, ScheduleBinding binding, LaneMapping mapping);

        void requireReservationCustody(PayloadReservation original, PayloadReservationReceipt exactReceipt);
        /** Audits actual old quota/payload owners, strict/control/head scope and all external responsibility. */
        void requireConservation(Material material);
        /** Rebinds the actual old applier result and original raw Source entry to this protected complete view. */
        void requirePrefixEvidence(
                SourceReplayEntry original,
                SourceReplayOutcome outcome,
                LegacyCheckpointStateInventory.LiveView actualAfter);
        /** Original legacy fees remain under their original payer; no missing proof is interpreted as zero. */
        CapacityVector retainedLegacyCharge(DelayMessageId id, LegacyCheckpointStateInventory.LiveView actualAfter);

        @Override
        void close();
    }

    /** Explicit missing proof; other failures are corrupt input or failed I/O and are not silently downgraded. */
    public static final class AuditUnavailable extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        public AuditUnavailable(final String message) {
            super(message);
        }
    }

    public enum Audit {
        FENCED_MAINTENANCE_AUTHORITY_NOT_AUDITED,
        COMPLETE_STATE_MAPPING_NOT_AUDITED,
        EXTERNAL_LEGACY_OBLIGATIONS_NOT_AUDITED,
        CONSERVATION_NOT_AUDITED
    }

    public enum Disposition {
        CONVERTED,
        RETAINED_LEGACY
    }

    public record Limits(int maxEntries, long maxRetainedBytes, int maxChunkBytes, int maxChunks) {
        public Limits {
            if (maxEntries <= 0
                    || maxEntries == Integer.MAX_VALUE
                    || maxRetainedBytes <= 0
                    || maxRetainedBytes == Long.MAX_VALUE
                    || maxChunkBytes < 1024
                    || maxChunkBytes == Integer.MAX_VALUE
                    || maxChunks <= 0
                    || maxChunks == Integer.MAX_VALUE) {
                throw new IllegalArgumentException("migration material requires finite positive limits");
            }
        }
    }

    /** Includes lanes allowed by the closed writer domain even when no row for them exists at C. */
    public record LaneMapping(
            byte[] canonicalLaneTuple,
            byte[] credentialScope,
            byte[] controlScope,
            TargetKeyCodec.Domain domain,
            byte[] accountingIncarnation,
            byte[] dispatchCompatibility,
            byte[] orderingDomain) {
        public LaneMapping {
            canonicalLaneTuple = copyBounded(canonicalLaneTuple, 1 << 20, "canonicalLaneTuple");
            CanonicalLaneTuple.project(canonicalLaneTuple);
            credentialScope = fixed(credentialScope, 32, "credentialScope");
            controlScope = fixed(controlScope, 32, "controlScope");
            Objects.requireNonNull(domain, "domain");
            if (domain.slot() >= TargetQueueState.MAX_DOMAIN_SLOTS) {
                throw new IllegalArgumentException("migration execution domain exceeds the frozen slot limit");
            }
            accountingIncarnation = fixed(accountingIncarnation, 16, "accountingIncarnation");
            dispatchCompatibility = fixed(dispatchCompatibility, 32, "dispatchCompatibility");
            orderingDomain = orderingDomain == null ? null : fixed(orderingDomain, 32, "orderingDomain");
            final var ordering = CanonicalLaneTuple.orderingProjection(canonicalLaneTuple);
            if (ordering.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO) {
                final byte[] legacyDomain =
                        DestinationLaneId.derive(canonicalLaneTuple).bytes();
                if (orderingDomain != null && !Arrays.equals(orderingDomain, legacyDomain)) {
                    throw new IllegalArgumentException(
                            "legacy strict migration must preserve its exact old per-Lane ordering isolation");
                }
                orderingDomain = legacyDomain;
            } else if (orderingDomain != null) {
                throw new IllegalArgumentException("unordered old Lane cannot acquire a strict ordering domain");
            }
        }

        public LaneMapping(
                byte[] canonicalLaneTuple,
                byte[] credentialScope,
                byte[] controlScope,
                TargetKeyCodec.Domain domain,
                byte[] accountingIncarnation,
                byte[] dispatchCompatibility) {
            this(
                    canonicalLaneTuple,
                    credentialScope,
                    controlScope,
                    domain,
                    accountingIncarnation,
                    dispatchCompatibility,
                    null);
        }

        @Override
        public byte[] canonicalLaneTuple() {
            return Bytes.copy(canonicalLaneTuple);
        }

        @Override
        public byte[] credentialScope() {
            return Bytes.copy(credentialScope);
        }

        @Override
        public byte[] controlScope() {
            return Bytes.copy(controlScope);
        }

        @Override
        public byte[] accountingIncarnation() {
            return Bytes.copy(accountingIncarnation);
        }

        @Override
        public byte[] dispatchCompatibility() {
            return Bytes.copy(dispatchCompatibility);
        }

        @Override
        public byte[] orderingDomain() {
            return orderingDomain == null ? null : Bytes.copy(orderingDomain);
        }

        public DestinationLaneId lane() {
            return DestinationLaneId.derive(canonicalLaneTuple);
        }

        public CanonicalTargetPartition physicalTarget() {
            return CanonicalTargetPartition.fromLaneTuple(canonicalLaneTuple);
        }

        public CanonicalLaneTuple.Projection legacyProjection() {
            return CanonicalLaneTuple.project(canonicalLaneTuple);
        }

        public byte[] canonicalBytes() {
            return Bytes.concat(
                    Bytes.lp32(canonicalLaneTuple),
                    credentialScope,
                    controlScope,
                    Bytes.u32be(domain.slot()),
                    Bytes.u64be(domain.generation()),
                    accountingIncarnation,
                    dispatchCompatibility,
                    Bytes.u8(orderingDomain == null ? 0 : 1),
                    orderingDomain == null ? new byte[0] : orderingDomain);
        }
    }

    /** Exact original Source/Admission/Journal/sequence/provider material supplied by the protected old domain. */
    public record ExternalRecord(String kind, byte[] owner, SourcePosition source, byte[] originalBytes) {
        public ExternalRecord {
            if (!List.of(
                            "SOURCE",
                            "ADMISSION",
                            "JOURNAL",
                            "SEQUENCE",
                            "PRODUCER",
                            "REGISTRATION",
                            "PROTECTION",
                            "PROFILE",
                            "CONTROL",
                            "RECEIPT")
                    .contains(kind)) {
                throw new IllegalArgumentException("unregistered migration custody material kind");
            }
            owner = copyBounded(owner, 4096, "owner");
            Objects.requireNonNull(source, "source");
            originalBytes = copyBounded(originalBytes, 1 << 26, "originalBytes");
        }

        @Override
        public byte[] owner() {
            return Bytes.copy(owner);
        }

        @Override
        public byte[] originalBytes() {
            return Bytes.copy(originalBytes);
        }

        public byte[] canonicalBytes() {
            return Bytes.concat(
                    Bytes.lp32(Bytes.utf8(kind)),
                    Bytes.lp32(owner),
                    Bytes.lp32(source.canonicalBytes()),
                    Bytes.lp32(originalBytes));
        }
    }

    /** Full typed Target row, with the value envelope retained exactly as it will be imported. */
    public record TargetRow(ColumnFamily family, byte[] key, byte[] value) {
        public TargetRow {
            Objects.requireNonNull(family, "family");
            key = copyBounded(key, 1 << 20, "Target key");
            value = copyBounded(value, 1 << 26, "Target value");
            TargetValueEnvelope.decodeAny(value);
        }

        @Override
        public byte[] key() {
            return Bytes.copy(key);
        }

        @Override
        public byte[] value() {
            return Bytes.copy(value);
        }

        public byte[] canonicalBytes() {
            return Bytes.concat(Bytes.u8(family.ordinal()), Bytes.lp32(key), Bytes.lp32(value));
        }
    }

    /** References are pinned by the Held provider to migration authority, never a fabricated normal grant/first. */
    public record Conversion(
            byte[] requiredDispatchRef,
            TargetQuotaAccounting accounting,
            ProfileSemanticEnvelope destination,
            ProfileSemanticEnvelope capability,
            ExternalRecord originalCommand,
            List<TargetRow> supportingRows) {
        public Conversion {
            requiredDispatchRef = fixed(requiredDispatchRef, 32, "requiredDispatchRef");
            Objects.requireNonNull(accounting, "accounting");
            Objects.requireNonNull(destination, "original destination Profile");
            Objects.requireNonNull(capability, "original capability Profile");
            if (originalCommand == null || !originalCommand.kind().equals("SOURCE")) {
                throw new IllegalArgumentException("conversion needs the complete original Source Command frame");
            }
            CommandCodec.decodeFrame(originalCommand.originalBytes());
            supportingRows = List.copyOf(supportingRows);
            for (var row : supportingRows) {
                final int type = TargetValueEnvelope.decodeAny(row.value()).valueType();
                if (type != TargetQuotaIncarnation.VALUE_TYPE
                        && type != TargetDispatchCompatibility.VALUE_TYPE
                        && type != TargetControlScope.VALUE_TYPE) {
                    throw new IllegalArgumentException(
                            "migration conversion support must be an actual C descriptor or frozen dispatch/control material");
                }
            }
        }

        @Override
        public byte[] requiredDispatchRef() {
            return Bytes.copy(requiredDispatchRef);
        }
    }

    public record ReservationConversion(Conversion conversion, ExternalRecord receipt) {
        public ReservationConversion {
            Objects.requireNonNull(conversion);
            if (receipt == null || !receipt.kind().equals("RECEIPT")) {
                throw new IllegalArgumentException(
                        "reservation conversion requires the complete original immutable receipt");
            }
            PayloadReservationReceipt.decodeFrame(receipt.originalBytes());
        }
    }

    public record Entity(
            DelayMessageId messageId,
            int generation,
            MessageStatus status,
            Disposition disposition,
            DestinationLaneId oldLane,
            long originalOrdinal,
            long payloadBytes,
            String reason) {
        public Entity {
            Objects.requireNonNull(messageId, "messageId");
            Objects.requireNonNull(disposition, "disposition");
            if (originalOrdinal < 0 || payloadBytes < 0) {
                throw new IllegalArgumentException("negative entity quantity");
            }
            Objects.requireNonNull(reason, "reason");
        }

        public byte[] canonicalBytes() {
            return Bytes.concat(
                    messageId.bytes(),
                    Bytes.u32beBits(generation),
                    Bytes.u8(status == null ? 0 : status.ordinal() + 1),
                    Bytes.u8(disposition.ordinal() + 1),
                    oldLane == null ? new byte[32] : oldLane.bytes(),
                    Bytes.u64be(originalOrdinal),
                    Bytes.u64be(payloadBytes),
                    Bytes.lp32(Bytes.utf8(reason)));
        }
    }

    public record RowDisposition(long originalOrdinal, Disposition disposition, String reason) {
        public RowDisposition {
            if (originalOrdinal < 0) {
                throw new IllegalArgumentException("negative original ordinal");
            }
            Objects.requireNonNull(disposition, "disposition");
            Objects.requireNonNull(reason, "reason");
        }

        public byte[] canonicalBytes() {
            return Bytes.concat(
                    Bytes.u64be(originalOrdinal), Bytes.u8(disposition.ordinal() + 1), Bytes.lp32(Bytes.utf8(reason)));
        }
    }

    public record Conservation(
            long originalRows,
            long originalBytes,
            long convertedRows,
            long legacyRows,
            long messages,
            long convertedMessages,
            long legacyMessages,
            long payloadBytes,
            long convertedPayloadBytes,
            long legacyPayloadBytes,
            long reservations,
            long convertedReservations,
            long legacyReservations,
            long reservedBytes,
            long convertedReservedBytes,
            long legacyReservedBytes) {
        public Conservation {
            if (Math.addExact(convertedRows, legacyRows) != originalRows
                    || Math.addExact(convertedMessages, legacyMessages) != messages
                    || Math.addExact(convertedPayloadBytes, legacyPayloadBytes) != payloadBytes
                    || Math.addExact(convertedReservations, legacyReservations) != reservations
                    || Math.addExact(convertedReservedBytes, legacyReservedBytes) != reservedBytes) {
                throw new IllegalArgumentException("converted and legacy sets do not conserve the original image");
            }
        }

        public byte[] canonicalBytes() {
            return Bytes.concat(
                    Bytes.u64be(originalRows),
                    Bytes.u64be(originalBytes),
                    Bytes.u64be(convertedRows),
                    Bytes.u64be(legacyRows),
                    Bytes.u64be(messages),
                    Bytes.u64be(convertedMessages),
                    Bytes.u64be(legacyMessages),
                    Bytes.u64be(payloadBytes),
                    Bytes.u64be(convertedPayloadBytes),
                    Bytes.u64be(legacyPayloadBytes),
                    Bytes.u64be(reservations),
                    Bytes.u64be(convertedReservations),
                    Bytes.u64be(legacyReservations),
                    Bytes.u64be(reservedBytes),
                    Bytes.u64be(convertedReservedBytes),
                    Bytes.u64be(legacyReservedBytes));
        }
    }

    public static final class Material {
        private final LegacyCheckpointStateInventory.CompleteView view;
        private final TargetQuotaMutation baseline;
        private final List<LaneMapping> mappings;
        private final List<ExternalRecord> external;
        private final List<Entity> entities;
        private final List<RowDisposition> dispositions;
        private final List<TargetRow> targetRows;
        private final Conservation conservation;
        private final EnumSet<Audit> blockers;
        private final List<String> conflicts;
        private final long retainedBytes;
        private final Map<String, Conversion> conversions;
        private final Map<String, ReservationConversion> reservationConversions;

        private Material(
                LegacyCheckpointStateInventory.CompleteView view,
                TargetQuotaMutation baseline,
                List<LaneMapping> mappings,
                List<ExternalRecord> external,
                List<Entity> entities,
                List<RowDisposition> dispositions,
                List<TargetRow> targetRows,
                Conservation conservation,
                EnumSet<Audit> blockers,
                List<String> conflicts,
                long retainedBytes,
                Map<String, Conversion> conversions,
                Map<String, ReservationConversion> reservationConversions) {
            this.view = view;
            this.baseline = baseline;
            this.mappings = List.copyOf(mappings);
            this.external = List.copyOf(external);
            this.entities = List.copyOf(entities);
            this.dispositions = List.copyOf(dispositions);
            this.targetRows = List.copyOf(targetRows);
            this.conservation = conservation;
            this.blockers = blockers.clone();
            this.conflicts = List.copyOf(conflicts);
            this.retainedBytes = retainedBytes;
            this.conversions = Map.copyOf(conversions);
            this.reservationConversions = Map.copyOf(reservationConversions);
        }

        public LegacyCheckpointStateInventory.CompleteView view() {
            return view;
        }

        public TargetQuotaMutation baseline() {
            return baseline;
        }

        public List<LaneMapping> mappings() {
            return mappings;
        }

        public List<ExternalRecord> externalRecords() {
            return external;
        }

        public List<Entity> entities() {
            return entities;
        }

        public List<RowDisposition> dispositions() {
            return dispositions;
        }

        public List<TargetRow> targetRows() {
            return targetRows;
        }

        public Conservation conservation() {
            return conservation;
        }

        public List<Audit> blockers() {
            return List.copyOf(blockers);
        }

        public List<String> conflicts() {
            return conflicts;
        }

        public long retainedBytes() {
            return retainedBytes;
        }
        /** READY means only that this material may be submitted to migration authorization. */
        public String summary() {
            return blockers.isEmpty() && conflicts.isEmpty() ? "READY" : "BLOCKED";
        }

        public boolean guarded(DelayMessageId id) {
            return entities.stream()
                    .anyMatch(e -> e.messageId().equals(id) && e.disposition() == Disposition.RETAINED_LEGACY);
        }
    }

    public enum Section {
        MAPPING,
        CUSTODY,
        CONSERVATION
    }

    /**
     * Uses the deployment's original checkpoint Object Store Profile/container/version semantics. The provider
     * charges every proposed object to the original root before I/O, retains UNKNOWN writes under that custody,
     * and owns exact cleanup. No list, delete, activation, or new detached resource budget is available here.
     */
    public interface ImmutableObjects {
        void requireReserved(long maximumEncodedBytes, int maximumObjects);

        CheckpointResource put(Section section, int chunk, boolean manifest, byte[] exactBytes);

        byte[] readExact(CheckpointResource object, int maximumBytes);
    }

    public record Chunk(long firstOrdinal, int records, CheckpointResource object) {
        public Chunk {
            if (firstOrdinal < 0 || records <= 0) {
                throw new IllegalArgumentException("invalid fixed chunk coverage");
            }
            Objects.requireNonNull(object, "object");
        }

        public byte[] canonicalBytes() {
            return Bytes.concat(Bytes.u64be(firstOrdinal), Bytes.u32be(records), Bytes.lp32(object.canonicalBytes()));
        }
    }

    public record Companion(Section section, long records, CheckpointResource manifest, List<Chunk> chunks) {
        public Companion {
            Objects.requireNonNull(section, "section");
            Objects.requireNonNull(manifest, "manifest");
            chunks = List.copyOf(chunks);
            long next = 0;
            for (var chunk : chunks) {
                if (chunk.firstOrdinal() != next) {
                    throw new IllegalArgumentException("companion chunk coverage has a gap or overlap");
                }
                next = Math.addExact(next, chunk.records());
            }
            if (next != records) {
                throw new IllegalArgumentException("companion chunks do not cover the full inventory");
            }
        }
        /** Locator addresses complete content inside one fixed chunk, never only a digest of an absent record. */
        public RecordLocation locate(long ordinal) {
            if (ordinal < 0 || ordinal >= records) {
                throw new IllegalArgumentException("companion ordinal outside inventory");
            }
            for (var chunk : chunks) {
                if (ordinal >= chunk.firstOrdinal() && ordinal - chunk.firstOrdinal() < chunk.records()) {
                    return new RecordLocation(chunk.object(), ordinal - chunk.firstOrdinal());
                }
            }
            throw new IllegalStateException("companion fixed inventory lost an ordinal");
        }
    }

    public record RecordLocation(CheckpointResource object, long ordinal) {
        public RecordLocation {
            Objects.requireNonNull(object, "object");
            if (ordinal < 0) {
                throw new IllegalArgumentException("negative immutable record ordinal");
            }
        }
    }

    public record Sealed(Material material, Companion mapping, Companion custody, Companion conservation) {
        public Sealed {
            Objects.requireNonNull(material, "material");
            Objects.requireNonNull(mapping, "mapping");
            Objects.requireNonNull(custody, "custody");
            Objects.requireNonNull(conservation, "conservation");
        }

        public RecordLocation originalRow(long ordinal) {
            if (ordinal >= material.view().entries().size()) {
                throw new IllegalArgumentException("not an original legacy row");
            }
            return custody.locate(ordinal);
        }

        public RecordLocation externalRecord(ExternalRecord exact) {
            final long first =
                    2L * material.view().entries().size() + material.entities().size();
            for (int index = 0; index < material.externalRecords().size(); index++) {
                if (Arrays.equals(material.externalRecords().get(index).canonicalBytes(), exact.canonicalBytes())) {
                    return custody.locate(first + index);
                }
            }
            throw new IllegalArgumentException("external record is outside the sealed complete custody inventory");
        }
    }

    public record RowKey(ColumnFamily family, byte[] key) {
        public RowKey {
            Objects.requireNonNull(family);
            key = Bytes.copy(Objects.requireNonNull(key));
        }

        @Override
        public byte[] key() {
            return Bytes.copy(key);
        }
    }

    public record RetainedEntity(
            DelayMessageId messageId,
            int generation,
            boolean identityOnly,
            TargetMigrationRecordReference original,
            byte[] resultDigest,
            CapacityVector retainedCharge) {
        public RetainedEntity {
            Objects.requireNonNull(messageId);
            Objects.requireNonNull(original);
            Objects.requireNonNull(retainedCharge);
            resultDigest = fixed(resultDigest, 32, "original result closure");
        }

        @Override
        public byte[] resultDigest() {
            return Bytes.copy(resultDigest);
        }
    }

    public record OriginInput(
            TargetRow row,
            LegacyCheckpointStateInventory.RawEntry original,
            TargetMigrationRecordReference originalRef,
            TargetQuotaIncarnation owner,
            CapacityVector retainedLegacyCharge) {
        public OriginInput {
            Objects.requireNonNull(row);
            Objects.requireNonNull(original);
            Objects.requireNonNull(originalRef);
            Objects.requireNonNull(owner);
            Objects.requireNonNull(retainedLegacyCharge);
        }
    }

    /**
     * Complete old responsibility is checked before a new Target summary is qualified. Old attempts are
     * never converted into a zero Target count merely because the current generation is reversible.
     */
    public static void requireSummaryImport(
            LegacyCheckpointStateInventory.LiveView complete,
            java.util.Collection<TargetRow> projected,
            TargetQuotaPayloadOwner owner,
            TargetScheduleBinding binding,
            TargetMigrationPlan plan) {
        Objects.requireNonNull(complete);
        Objects.requireNonNull(projected);
        Objects.requireNonNull(owner);
        Objects.requireNonNull(binding);
        Objects.requireNonNull(plan);
        owner.requireInitialBinding(binding);
        if (!binding.imported()
                || !Arrays.equals(binding.migrationPlanDigest(), plan.digest())
                || !Arrays.equals(complete.metadata().storeIncarnation(), plan.oldStore())
                || !complete.applied().sameSourceIdentity(plan.baseline().source())
                || !Arrays.equals(
                        owner.mutation().source().canonicalBytes(),
                        complete.applied().canonicalBytes())) {
            throw new IllegalArgumentException("payload summary import changed its complete original Store/Source");
        }
        requireNoLegacyObligations(complete.entries(), complete.applied(), owner.messageId());
        boolean actualOwner = false;
        for (var row : projected) {
            if (row.family() == ColumnFamily.META && Arrays.equals(row.key(), owner.key())) {
                if (actualOwner
                        || !Arrays.equals(
                                row.value(),
                                TargetValueEnvelope.encode(
                                        TargetQuotaPayloadOwner.VALUE_TYPE, owner.canonicalBytes()))) {
                    throw new IllegalArgumentException("payload summary import lost its exact proposed owner");
                }
                actualOwner = true;
            }
        }
        if (!actualOwner) {
            throw new IllegalArgumentException("payload summary import has no proposed owner");
        }
    }

    private static void requireNoLegacyObligations(
            List<LegacyCheckpointStateInventory.RawEntry> entries, SourcePosition frontier, DelayMessageId id) {
        final Map<String, com.nereusstream.delay.runtime.AttemptObligationRef> obligations = new TreeMap<>();
        final Map<String, PublishAttemptLedger> ledgers = new TreeMap<>();
        final Map<Integer, List<com.nereusstream.delay.runtime.AttemptObligationRef>> generations = new HashMap<>();
        for (var row : entries) {
            List<com.nereusstream.delay.runtime.AttemptObligationRef> references = null;
            int generation = 0;
            SourcePosition source = null;
            if (row.family() == ColumnFamily.ID && Arrays.equals(row.key(), KeyCodec.idMessage(id))) {
                final byte[] bytes = ValueEnvelope.decodeAny(row.value()).payload();
                if (RetiredMessageIdentityRecord.isEncoded(bytes)) {
                    continue;
                }
                final var message = MessageRecord.decode(bytes);
                references = message.runtimeIndex().attemptObligations();
                generation = message.generation();
                source = SourcePositionCodec.decode(message.scheduleSourcePosition());
            } else if (row.family() == ColumnFamily.TERMINAL && kind(row.key(), 1)) {
                final var terminal = TerminalGenerationRecord.decode(
                        ValueEnvelope.decodeAny(row.value()).payload());
                if (!terminal.messageId().equals(id)) {
                    continue;
                }
                if (!Arrays.equals(row.key(), KeyCodec.terminalGeneration(id, terminal.generation()))) {
                    throw new IllegalArgumentException("original terminal summary changed its exact generation key");
                }
                references = terminal.openObligations();
                generation = terminal.generation();
                source = SourcePositionCodec.decode(terminal.appliedSourcePosition());
            } else if (row.family() == ColumnFamily.INFLIGHT
                    && row.key().length >= 2
                    && row.key()[1] == 1
                    && (row.key()[0] == 2 || row.key()[0] == 3)) {
                final var ledger =
                        PublishAttemptLedger.decode(ValueEnvelope.decode(row.value(), PublishAttemptLedger.VALUE_TYPE)
                                .payload());
                if (!ledger.delayMessageId().equals(id)) {
                    continue;
                }
                if (!Arrays.equals(row.key(), ledger.encodedKey())
                        || ledgers.put(Bytes.hex(row.key()), ledger) != null) {
                    throw new IllegalArgumentException("original attempt ledger changed its exact identity");
                }
                source = SourcePositionCodec.decode(ledger.sourcePosition());
            }
            if (source != null && (!source.sameSourceIdentity(frontier) || source.compareTo(frontier) > 0)) {
                throw new IllegalArgumentException("original payload obligation exceeds its complete Source cut");
            }
            if (references == null) {
                continue;
            }
            final var mirror = generations.putIfAbsent(generation, references);
            if (mirror != null && !mirror.equals(references)) {
                throw new IllegalArgumentException("original current/terminal obligation mirrors disagree");
            }
            for (var reference : references) {
                if (reference.generation() != generation) {
                    throw new IllegalArgumentException("original payload obligation changed generation");
                }
                final String key = Bytes.hex(reference.encodedInflightKey());
                final var prior = obligations.putIfAbsent(key, reference);
                if (prior != null && !prior.equals(reference)) {
                    throw new IllegalArgumentException(
                            "original payload obligation identity has conflicting projections");
                }
            }
        }
        for (var entry : obligations.entrySet()) {
            final var ledger = Objects.requireNonNull(ledgers.get(entry.getKey()), "original obligation ledger");
            if (!entry.getValue().equals(ledger.obligationRef())) {
                throw new IllegalArgumentException(
                        "original payload obligation differs from its actual attempt ledger");
            }
        }
        if (!obligations.isEmpty() || !ledgers.isEmpty()) {
            throw new AuditUnavailable("original all-generation attempt responsibility remains in legacy custody for "
                    + Bytes.hex(id.bytes()));
        }
    }

    private static void qualifyImportedSummaries(
            LegacyCheckpointStateInventory.LiveView complete,
            Map<String, TargetRow> rows,
            Map<String, TargetRow> prior,
            TargetMigrationPlan plan,
            Held held) {
        for (var row : List.copyOf(rows.values())) {
            if (row.family() != ColumnFamily.META || prior.containsKey(rowId(row.family(), row.key()))) {
                continue;
            }
            final var value = TargetValueEnvelope.decodeAny(row.value());
            if (value.valueType() != TargetQuotaPayloadOwner.VALUE_TYPE) {
                continue;
            }
            final var owner = TargetQuotaPayloadOwner.decode(value.payload());
            if (owner.summaryQualified()) {
                continue;
            }
            final byte[] bindingKey = TargetKeyCodec.scheduleBinding(owner.initialBindingDigest());
            final var binding = TargetScheduleBinding.decodeForStore(
                    bindingKey,
                    TargetValueEnvelope.decode(
                                    Objects.requireNonNull(
                                            value(rows, ColumnFamily.ID, bindingKey),
                                            "imported payload summary binding"),
                                    TargetScheduleBinding.VALUE_TYPE)
                            .payload(),
                    plan.scope().shard());
            held.requireCurrent();
            final var qualification = TargetPayloadObligationSummaryBootstrap.imported(
                    complete, rows.values(), owner, binding, plan, owner.mutation());
            final var qualified = owner.initializeSummary(qualification, owner.mutation(), (before, after, floor) -> {
                held.requireCurrent();
                if (floor != null
                        || !Arrays.equals(before.canonicalBytes(), owner.canonicalBytes())
                        || !after.mutation().equals(owner.mutation())) {
                    throw new IllegalStateException("imported payload summary changed its original migration birth");
                }
                after.requireInitialBinding(binding);
            });
            rows.put(
                    rowId(row.family(), row.key()),
                    typed(
                            ColumnFamily.META,
                            qualified.key(),
                            TargetQuotaPayloadOwner.VALUE_TYPE,
                            qualified.canonicalBytes()));
            held.requireCurrent();
        }
    }

    /** Concrete, complete row provenance. Old fees are represented once at the Message anchor, never refunded. */
    public static List<OriginInput> baselineInputs(
            Material projected, Sealed sealed, TargetQuotaIncarnation root, Held held) {
        held.requireCurrent();
        if (projected.view() != sealed.material().view() || !projected.summary().equals("READY")) {
            throw new IllegalArgumentException("import projection does not come from this exact sealed complete view");
        }
        final Map<String, LegacyCheckpointStateInventory.RawEntry> oldRows = new HashMap<>();
        for (var row : projected.view().entries()) {
            oldRows.put(rowId(row.family(), row.key()), row);
        }
        final var metadata = Objects.requireNonNull(
                oldRows.get(rowId(ColumnFamily.META, KeyCodec.metaFixed(2))), "original Store identity row");
        final Map<String, TargetQuotaIncarnation> owners = new HashMap<>();
        owners.put(Bytes.hex(root.identity().accountingIncarnation()), root);
        for (var row : projected.targetRows()) {
            final var value = TargetValueEnvelope.decodeAny(row.value());
            if (value.valueType() == TargetQuotaIncarnation.VALUE_TYPE) {
                final var owner = TargetQuotaIncarnation.decode(value.payload());
                owners.put(Bytes.hex(owner.identity().accountingIncarnation()), owner);
            }
        }
        final var mappings = mappingIndex(projected.mappings());
        final var live = LegacyCheckpointStateInventory.baselineView(projected.view());
        final List<OriginInput> inputs = new ArrayList<>();
        for (var row : projected.targetRows()) {
            final var value = TargetValueEnvelope.decodeAny(row.value());
            DelayMessageId messageId = null;
            byte[] accounting = null;
            com.nereusstream.delay.protocol.TargetPartitionId physicalOwner = null;
            boolean bindingRow = false;
            byte[] reservationKey = null;
            DestinationLaneId controlLane = null;
            switch (value.valueType()) {
                case TargetMessageRecord.VALUE_TYPE -> {
                    final var message = TargetMessageRecord.decode(value.payload());
                    messageId = message.locator().messageId();
                    accounting = message.locator().accountingIncarnation();
                }
                case TargetTimelineWorkRef.VALUE_TYPE -> {
                    final var work = TargetTimelineWorkRef.decode(value.payload());
                    messageId = work.locator().messageId();
                    accounting = work.locator().accountingIncarnation();
                }
                case TargetExpiryRef.VALUE_TYPE -> {
                    final var expiry = TargetExpiryRef.decode(value.payload());
                    messageId = expiry.locator().messageId();
                    accounting = expiry.locator().accountingIncarnation();
                }
                case TargetScheduleBinding.VALUE_TYPE -> {
                    final var binding = TargetScheduleBinding.decode(value.payload());
                    messageId = binding.messageId();
                    accounting = binding.accountingIncarnation();
                    bindingRow = true;
                }
                case TargetQuotaPayloadOwner.VALUE_TYPE -> {
                    final var owner = TargetQuotaPayloadOwner.decode(value.payload());
                    messageId = owner.messageId();
                    accounting = owner.primaryIdentity().accountingIncarnation();
                }
                case TargetReservationRecord.VALUE_TYPE -> {
                    final var reservation = TargetReservationRecord.decode(value.payload());
                    messageId = reservation.locator().messageId();
                    accounting = reservation.locator().accountingIncarnation();
                    reservationKey = KeyCodec.idReservation(reservation.reservationId());
                }
                case TargetQueueState.VALUE_TYPE ->
                    accounting = TargetQueueState.decode(value.payload()).accountingIncarnation();
                case TargetOrderState.VALUE_TYPE -> {
                    final var order = TargetOrderState.decode(value.payload());
                    accounting = order.accountingIncarnation();
                    for (var mapping : mappings.values()) {
                        if (mapping.physicalTarget().id().equals(order.target())
                                && Arrays.equals(mapping.orderingDomain(), order.orderingDomain())) {
                            controlLane = mapping.lane();
                            break;
                        }
                    }
                    if (controlLane == null) {
                        throw new IllegalArgumentException(
                                "strict original control is outside frozen complete mapping");
                    }
                }
                case CanonicalTargetPartition.VALUE_TYPE -> {
                    physicalOwner =
                            CanonicalTargetPartition.decode(value.payload()).id();
                }
                case TargetDispatchCompatibility.VALUE_TYPE ->
                    physicalOwner =
                            TargetDispatchCompatibility.decode(value.payload()).target();
                case TargetControlScope.VALUE_TYPE ->
                    physicalOwner = TargetControlScope.decode(value.payload()).target();
                case TargetQuotaIncarnation.VALUE_TYPE ->
                    accounting = TargetQuotaIncarnation.decode(value.payload())
                            .identity()
                            .accountingIncarnation();
                default -> throw new IllegalArgumentException("unmapped imported Target value kind");
            }
            if (physicalOwner != null) {
                for (var mapping : mappings.values()) {
                    if (mapping.physicalTarget().id().equals(physicalOwner)) {
                        accounting = mapping.accountingIncarnation();
                        break;
                    }
                }
                if (accounting == null) {
                    throw new IllegalArgumentException("Target semantic material is outside frozen complete mapping");
                }
            }
            var original = controlLane != null
                    ? oldRows.get(rowId(ColumnFamily.META, KeyCodec.metaLane(controlLane)))
                    : messageId == null
                            ? metadata
                            : oldRows.get(rowId(
                                    ColumnFamily.ID,
                                    reservationKey != null
                                            ? reservationKey
                                            : bindingRow
                                                    ? KeyCodec.idScheduleBinding(messageId)
                                                    : KeyCodec.idMessage(messageId)));
            if (original == null && messageId != null && value.valueType() == TargetQuotaPayloadOwner.VALUE_TYPE) {
                for (var entity : projected.entities()) {
                    if (entity.messageId().equals(messageId)
                            && entity.status() == null
                            && entity.disposition() == Disposition.CONVERTED) {
                        original = projected.view().entries().get(Math.toIntExact(entity.originalOrdinal()));
                    }
                }
            }
            Objects.requireNonNull(original, "complete original Message/reservation/binding row");
            final var owner = accounting == null
                    ? root
                    : Objects.requireNonNull(owners.get(Bytes.hex(accounting)), "actual C allocated import owner");
            final CapacityVector oldCharge = value.valueType() == TargetMessageRecord.VALUE_TYPE
                            || value.valueType() == TargetReservationRecord.VALUE_TYPE
                                    && Arrays.equals(
                                            row.key(),
                                            TargetReservationRecord.decode(value.payload())
                                                    .key())
                    ? held.retainedLegacyCharge(messageId, live)
                    : CapacityVector.empty();
            inputs.add(new OriginInput(
                    row, original, reference(sealed.originalRow(original.ordinal())), owner, oldCharge));
        }
        held.requireCurrent();
        return List.copyOf(inputs);
    }

    /** Current legacy ownership wins over historical rows; a converted current generation never gets a fake guard. */
    public static List<TargetMigrationCustodyGuard> baselineGuards(
            Material projected, Sealed sealed, TargetMigrationPlan plan, TargetQuotaIncarnation root, Held held) {
        held.requireCurrent();
        if (projected.view() != sealed.material().view()) {
            throw new IllegalArgumentException("guard material changed complete old view");
        }
        final Map<String, Entity> selected = new TreeMap<>();
        final java.util.Set<String> converted = new java.util.HashSet<>();
        for (var entity : projected.entities()) {
            if (entity.disposition() == Disposition.CONVERTED) {
                converted.add(Bytes.hex(entity.messageId().bytes()));
            }
        }
        for (var entity : projected.entities()) {
            final String id = Bytes.hex(entity.messageId().bytes());
            if (entity.disposition() == Disposition.CONVERTED || converted.contains(id)) {
                continue;
            }
            final var prior = selected.get(id);
            if (prior == null
                    || entityPriority(projected, entity) > entityPriority(projected, prior)
                    || entityPriority(projected, entity) == entityPriority(projected, prior)
                            && Integer.compareUnsigned(entity.generation(), prior.generation()) > 0) {
                selected.put(id, entity);
            }
        }
        final var actual = LegacyCheckpointStateInventory.baselineView(projected.view());
        final List<TargetMigrationCustodyGuard> guards = new ArrayList<>();
        for (var entity : selected.values()) {
            final var raw = projected.view().entries().get(Math.toIntExact(entity.originalOrdinal()));
            final var ref = reference(sealed.originalRow(entity.originalOrdinal()));
            final var digest = Bytes.sha256(
                    sealed.custody().manifest().canonicalBytes(), entity.canonicalBytes(), raw.canonicalBytes());
            final var charge = held.retainedLegacyCharge(entity.messageId(), actual);
            final boolean identity = entity.status() == null;
            guards.add(
                    identity
                            ? TargetMigrationCustodyGuard.heldIdentity(
                                    plan.migrationId(), plan.digest(), entity.messageId(), ref, digest, root, charge)
                            : TargetMigrationCustodyGuard.held(
                                    plan.migrationId(),
                                    plan.digest(),
                                    entity.messageId(),
                                    entity.generation(),
                                    ref,
                                    digest,
                                    root,
                                    charge));
        }
        held.requireCurrent();
        return List.copyOf(guards);
    }

    private static int entityPriority(Material material, Entity entity) {
        final var row = material.view().entries().get(Math.toIntExact(entity.originalOrdinal()));
        return row.family() == ColumnFamily.ID && kind(row.key(), 1) ? 3 : row.family() == ColumnFamily.ID ? 2 : 1;
    }

    /** A physical duplicate or command-ID conflict advances only the retained original first/POS audit. */
    public static TailProjection projectRetainedResult(
            Sealed sealed,
            Held held,
            SourceReplayEntry original,
            SourceReplayOutcome outcome,
            ShardStore legacyAfter,
            TargetMigrationImporter.PreparedCandidate candidate,
            ImmutableObjects objects,
            LegacyCheckpointStateInventory.ReadLimits readLimits,
            Limits limits,
            BoundedReadBudget targetBudget) {
        held.requireCurrent();
        candidate.requireStore(candidate.store());
        if ((original instanceof SourceReplayRecord) != outcome.isCommand()
                || !Arrays.equals(
                        original.position().canonicalBytes(), outcome.position().canonicalBytes())) {
            throw new IllegalArgumentException("retained result is not this exact physical Source record");
        }
        final var after = LegacyCheckpointStateInventory.inspectLive(
                legacyAfter,
                sealed.material().view(),
                original.position(),
                readLimits,
                held::requireCurrent,
                candidate.plan());
        final Map<String, LegacyCheckpointStateInventory.RawEntry> rows = new HashMap<>();
        for (var row : after.entries()) {
            rows.put(rowId(row.family(), row.key()), row);
        }
        final byte[] id = original instanceof SourceReplayRecord command
                ? command.command().commandId().bytes()
                : ((SourceReplayMutation) original).mutation().systemMutationId();
        final var position = Objects.requireNonNull(
                rows.get(rowId(
                        ColumnFamily.DEDUPE,
                        KeyCodec.dedupePosition(original.position().canonicalBytes()))),
                "actual retained POSITION audit");
        if (!Arrays.equals(ValueEnvelope.decode(position.value(), 3).payload(), id)) {
            throw new IllegalArgumentException("retained POSITION changed original Command/System identity");
        }
        if (original instanceof SourceReplayRecord command) {
            requireRetainedCommandFirst(rows, command, outcome.commandResult());
        } else {
            final var mutation = ((SourceReplayMutation) original).mutation();
            final var row = Objects.requireNonNull(
                    rows.get(rowId(ColumnFamily.DEDUPE, KeyCodec.dedupeSystemMutation(mutation.systemMutationId()))),
                    "original System first");
            final var first =
                    SystemMutationResult.decode(ValueEnvelope.decode(row.value(), SystemMutationResult.VALUE_TYPE)
                            .payload());
            if (!Arrays.equals(first.mutationId(), mutation.systemMutationId())
                    || SourcePositionCodec.decode(first.appliedSourcePosition()).compareTo(original.position()) >= 0
                    || !Arrays.equals(first.mutationHash(), mutation.mutationHash())
                            && outcome.systemMutationResult().applyStatus() != ApplyStatus.REJECTED) {
                throw new IllegalArgumentException(
                        "retained System first/hash/result is not an original duplicate or conflict");
            }
        }
        held.requirePrefixEvidence(original, outcome, after);
        final var target = candidate.store().readWithBudget(targetBudget, () -> {
            held.requireCurrent();
            return candidate.store().appliedShardLogPosition();
        });
        if (target.value() == null
                || !target.value().sameSourceIdentity(original.position())
                || target.value().compareTo(original.position()) >= 0) {
            throw new IllegalStateException("retained physical audit does not follow actual Target Source frontier");
        }
        final byte[] frame = original instanceof SourceReplayRecord command
                ? CommandCodec.encodeFrame(command.command())
                : ((SourceReplayMutation) original).mutation().canonicalEnvelope();
        final byte[] result = outcome.isCommand()
                ? outcome.commandResult().encode()
                : outcome.systemMutationResult().encode();
        final List<byte[]> records = new ArrayList<>();
        records.add(Bytes.concat(
                Bytes.u8(1),
                Bytes.lp32(target.value().canonicalBytes()),
                Bytes.lp32(original.position().canonicalBytes()),
                Bytes.u64beBits(after.sequence()),
                Bytes.lp32(frame),
                Bytes.lp32(result)));
        for (var row : after.entries()) {
            records.add(Bytes.concat(Bytes.u8(2), row.canonicalBytes()));
        }
        final var encoded = encodeChunks(Section.CUSTODY, records, limits);
        long bytes = limits.maxChunkBytes();
        for (var chunk : encoded) {
            bytes = Math.addExact(bytes, chunk.bytes().length);
        }
        if (Math.addExact(sealed.material().retainedBytes(), Math.multiplyExact(bytes, 4)) > held.retainedBytes()) {
            throw new IllegalStateException("retained first/POS evidence exceeds original flight's charged capacity");
        }
        objects.requireReserved(Math.addExact(bytes, 2L * limits.maxChunkBytes()), encoded.size() + 3);
        final var raw = publish(sealed.material(), held, objects, limits, Section.CUSTODY, records.size(), encoded);
        final List<byte[]> emptyProjection = List.of(
                Bytes.concat(Bytes.u8(1), original.position().canonicalBytes(), Bytes.sha256(frame), Bytes.u32be(0)));
        final var projected = publish(
                sealed.material(),
                held,
                objects,
                limits,
                Section.CUSTODY,
                1,
                encodeChunks(Section.CUSTODY, emptyProjection, limits));
        held.requireCurrent();
        return new TailProjection(
                original,
                outcome,
                target.value(),
                target.view(),
                List.of(),
                List.of(),
                new TargetMigrationRecordReference(new TargetMigrationObjectReference(raw.manifest()), 0),
                new TargetMigrationRecordReference(new TargetMigrationObjectReference(projected.manifest()), 0),
                List.of());
    }

    private static void requireRetainedCommandFirst(
            Map<String, LegacyCheckpointStateInventory.RawEntry> rows,
            SourceReplayRecord original,
            CommandResult outcome) {
        final var command = original.command();
        final var row = Objects.requireNonNull(
                rows.get(rowId(ColumnFamily.DEDUPE, KeyCodec.dedupeCommand(command.commandId()))),
                "original Command first");
        final var input = ByteBuffer.wrap(ValueEnvelope.decode(row.value(), 1).payload());
        if (input.remaining() < 4) {
            throw new IllegalArgumentException("original Command first is truncated");
        }
        final int version = input.getInt();
        final ProtocolTuple tuple;
        if (version == 1) {
            tuple = ProtocolTuple.managedCommand();
        } else if (version == 2) {
            if (input.remaining() < 4) {
                throw new IllegalArgumentException("original first tuple is truncated");
            }
            final int length = input.getInt();
            if (length <= 0 || length > 4096 || length > input.remaining()) {
                throw new IllegalArgumentException("original first tuple exceeds bound");
            }
            final byte[] encoded = new byte[length];
            input.get(encoded);
            tuple = ProtocolTuple.decode(encoded);
        } else {
            throw new IllegalArgumentException("unknown original Command first version");
        }
        if (input.remaining() < 36) {
            throw new IllegalArgumentException("original Command hash/result is truncated");
        }
        final byte[] hash = new byte[32];
        input.get(hash);
        final int length = input.getInt();
        if (length <= 0 || length != input.remaining()) {
            throw new IllegalArgumentException("original Command first result length differs");
        }
        final byte[] encoded = new byte[length];
        input.get(encoded);
        final var first = CommandResult.decode(encoded);
        final var resultRow = Objects.requireNonNull(
                rows.get(rowId(ColumnFamily.DEDUPE, KeyCodec.dedupeResult(command.commandId()))),
                "original result sidecar");
        if (!Arrays.equals(ValueEnvelope.decode(resultRow.value(), 2).payload(), encoded)
                || SourcePositionCodec.decode(first.appliedSourcePosition()).compareTo(original.position()) >= 0) {
            throw new IllegalArgumentException("original Command first/result/source closure changed");
        }
        final boolean exact = tuple.equals(command.protocolTuple()) && Arrays.equals(hash, command.commandHash());
        if (exact
                ? first.applyStatus() != outcome.applyStatus()
                        || first.stableCode() != outcome.stableCode()
                        || first.generation() != outcome.generation()
                        || first.stateVersion() != outcome.stateVersion()
                        || first.messageStatus() != outcome.messageStatus()
                : outcome.applyStatus() != ApplyStatus.REJECTED
                        || outcome.stableCode() != com.nereusstream.delay.protocol.StableCode.COMMAND_ID_CONFLICT) {
            throw new IllegalArgumentException(
                    "physical retained result changed the original first or command-ID conflict");
        }
    }

    /** Exact full desired data and physical deletions; the store worker computes Native fees and actual S atomically. */
    public record TailProjection(
            SourceReplayEntry original,
            SourceReplayOutcome outcome,
            SourcePosition prior,
            ShardStore.ReadView targetView,
            List<TargetRow> desired,
            List<RowKey> deletions,
            TargetMigrationRecordReference tailEvidence,
            TargetMigrationRecordReference projectionEvidence,
            List<RetainedEntity> retained) {
        public TailProjection {
            Objects.requireNonNull(original);
            Objects.requireNonNull(outcome);
            Objects.requireNonNull(prior);
            Objects.requireNonNull(targetView);
            desired = List.copyOf(desired);
            deletions = List.copyOf(deletions);
            retained = List.copyOf(retained);
            Objects.requireNonNull(tailEvidence);
            Objects.requireNonNull(projectionEvidence);
        }
    }

    /**
     * Rebuilds from the real old applier's complete after-view, rather than guessing effects from command types.
     * Cancel/expiry/outcome/control/reservation changes retain their original domain and durable no-send custody;
     * safely scheduled work is rebuilt with explicit imported-prefix origin. No old wire bytes are rewritten.
     */
    public static TailProjection projectPrefix(
            Sealed sealed,
            Held held,
            SourceReplayEntry original,
            SourceReplayOutcome outcome,
            ShardStore legacyAfter,
            TargetMigrationImporter.PreparedCandidate candidate,
            ImmutableObjects objects,
            LegacyCheckpointStateInventory.ReadLimits readLimits,
            Limits limits,
            BoundedReadBudget targetBudget) {
        Objects.requireNonNull(sealed);
        Objects.requireNonNull(held);
        Objects.requireNonNull(original);
        Objects.requireNonNull(outcome);
        Objects.requireNonNull(candidate);
        Objects.requireNonNull(objects);
        held.requireCurrent();
        candidate.requireStore(candidate.store());
        final var plan = candidate.plan();
        final var material = sealed.material();
        if (Math.addExact(material.retainedBytes(), Math.multiplyExact(readLimits.maxBytes(), 4))
                > held.retainedBytes()) {
            throw new IllegalStateException(
                    "original flight has not reserved complete bounded old/Target prefix views");
        }
        if (!Arrays.equals(
                        original.position().canonicalBytes(), outcome.position().canonicalBytes())
                || (original instanceof SourceReplayRecord) != outcome.isCommand()) {
            throw new IllegalArgumentException("old applier outcome is not this original physical Source entry");
        }
        final var after = LegacyCheckpointStateInventory.inspectLive(
                legacyAfter, material.view(), original.position(), readLimits, held::requireCurrent, plan);
        final byte[] identity = original instanceof SourceReplayRecord command
                ? command.command().commandId().bytes()
                : ((SourceReplayMutation) original).mutation().systemMutationId();
        LegacyCheckpointStateInventory.RawEntry positionAudit = null;
        for (var row : after.entries()) {
            if (row.family() == ColumnFamily.DEDUPE
                    && Arrays.equals(
                            row.key(),
                            KeyCodec.dedupePosition(original.position().canonicalBytes()))) {
                positionAudit = row;
            }
        }
        if (positionAudit == null
                || !Arrays.equals(ValueEnvelope.decode(positionAudit.value(), 3).payload(), identity)) {
            throw new IllegalArgumentException("actual old Store has no exact original Command/System POSITION audit");
        }
        held.requirePrefixEvidence(original, outcome, after);
        if (original instanceof SourceReplayMutation mutation
                && mutation.mutation().type() == com.nereusstream.delay.protocol.SystemMutationType.PUBLISH_ADMISSION) {
            final byte[] raw = legacyAfter.get(
                    ColumnFamily.DEDUPE,
                    KeyCodec.dedupeSystemMutation(outcome.systemMutationResult().mutationId()));
            final var first = SystemMutationResult.decode(
                    ValueEnvelope.decode(raw, SystemMutationResult.VALUE_TYPE).payload());
            if (first.applyStatus() == ApplyStatus.APPLIED
                    && SourcePositionCodec.decode(first.appliedSourcePosition())
                                    .compareTo(plan.baseline().source())
                            > 0) {
                throw new IllegalStateException(
                        "late old Admission crossed the frozen migration baseline; preserve custody and stop admission");
            }
        }
        final var actual = candidate.store().readWithBudget(targetBudget, () -> {
            held.requireCurrent();
            final Map<String, TargetRow> rows = new TreeMap<>();
            for (var family : ColumnFamily.values()) {
                final var visited = candidate
                        .store()
                        .visitResult(family, new byte[0], null, limits.maxEntries(), targetBudget, (row, ignored) -> {
                            if (rows.size() >= limits.maxEntries()) {
                                throw new IllegalStateException("Target prefix view exceeds finite row bound");
                            }
                            final var value = new TargetRow(family, row.key(), row.value());
                            rows.put(rowId(family, row.key()), value);
                            return true;
                        });
                if (visited.stop() != ShardStore.VisitStop.RANGE_END) {
                    throw new IllegalStateException("Target prefix view did not cover every fixed CF");
                }
            }
            return new TargetSnapshot(
                    rows,
                    candidate.store().appliedShardLogPosition(),
                    candidate.store().shardMutationSequence());
        });
        final SourcePosition prior = actual.value().source();
        if (prior == null
                || !prior.sameSourceIdentity(original.position())
                || prior.compareTo(original.position()) >= 0) {
            throw new IllegalArgumentException("legacy prefix does not follow its actual Target frontier");
        }
        requireNoUnprojectedNativeActivity(actual.value().rows(), plan);
        final byte[] frame = original instanceof SourceReplayRecord record
                ? CommandCodec.encodeFrame(record.command())
                : ((SourceReplayMutation) original).mutation().canonicalEnvelope();
        final byte[] result = outcome.isCommand()
                ? outcome.commandResult().encode()
                : outcome.systemMutationResult().encode();
        final var prefixMappings = mappingIndex(material.mappings());
        final Map<String, ScheduleBinding> reservationBindings = new HashMap<>();
        for (var row : after.entries()) {
            if (row.family() == ColumnFamily.ID && kind(row.key(), 4)) {
                final var binding = ScheduleBinding.decode(
                        ValueEnvelope.decodeAny(row.value()).payload());
                reservationBindings.put(Bytes.hex(binding.delayMessageId().bytes()), binding);
            }
        }
        final List<ExternalRecord> prefixExternal = new ArrayList<>(material.externalRecords());
        prefixExternal.addAll(held.externalRecords());
        if (original instanceof SourceReplayRecord command) {
            prefixExternal.add(
                    new ExternalRecord("SOURCE", command.command().commandId().bytes(), original.position(), frame));
        }
        final Map<String, ReservationConversion> prefixReservations = new HashMap<>();
        for (var row : after.entries()) {
            if (row.family() == ColumnFamily.ID && kind(row.key(), 2)) {
                final var reservation = PayloadReservation.decode(
                        ValueEnvelope.decodeAny(row.value()).payload());
                final var binding = reservationBindings.get(
                        Bytes.hex(reservation.delayMessageId().bytes()));
                if (binding == null) {
                    if (reservation.status() == PayloadReservationStatus.RESERVED) {
                        throw new IllegalArgumentException("actual old reservation lost original Prepare binding");
                    }
                    continue;
                }
                ReservationConversion acquired;
                try {
                    acquired = held.reservation(
                            reservation,
                            binding,
                            requireMapping(
                                    prefixMappings, reservation.intent().laneId(), binding.canonicalLaneTuple()));
                } catch (AuditUnavailable unavailable) {
                    acquired = material.reservationConversions.get(Bytes.hex(reservation.reservationId()));
                }
                final var proof = acquired;
                if (proof == null) {
                    if (reservation.status() == PayloadReservationStatus.RESERVED) {
                        throw new AuditUnavailable("actual RESERVED prefix has no original receipt/object/quota proof");
                    }
                    continue;
                }
                held.requireReservationCustody(
                        reservation,
                        PayloadReservationReceipt.decodeFrame(proof.receipt().originalBytes()));
                prefixExternal.add(proof.receipt());
                requireOriginalCommand(proof.conversion(), binding, prefixExternal);
                requireReceipt(reservation, binding, proof, prefixExternal);
                prefixReservations.put(Bytes.hex(reservation.reservationId()), proof);
            }
        }
        if ((long) after.entries().size() + prefixExternal.size() > limits.maxEntries()) {
            throw new IllegalStateException("complete prefix raw/external inventory exceeds finite entry bound");
        }
        final List<byte[]> evidence = new ArrayList<>();
        evidence.add(Bytes.concat(
                Bytes.u8(1),
                Bytes.lp32(prior.canonicalBytes()),
                Bytes.lp32(original.position().canonicalBytes()),
                Bytes.u64beBits(after.sequence()),
                Bytes.lp32(frame),
                Bytes.lp32(result)));
        for (var row : after.entries()) {
            evidence.add(Bytes.concat(Bytes.u8(2), row.canonicalBytes()));
        }
        for (var record : prefixExternal) {
            evidence.add(Bytes.concat(Bytes.u8(3), record.canonicalBytes()));
        }
        final var rawChunks = encodeChunks(Section.CUSTODY, evidence, limits);
        long rawBytes = 0;
        for (var chunk : rawChunks) {
            rawBytes = Math.addExact(rawBytes, chunk.bytes().length);
        }
        if (Math.addExact(material.retainedBytes(), Math.multiplyExact(rawBytes, 4)) > held.retainedBytes()) {
            throw new IllegalStateException("original flight has no retained capacity for complete prefix material");
        }
        objects.requireReserved(Math.addExact(rawBytes, limits.maxChunkBytes()), rawChunks.size() + 1);
        final var tail = publish(material, held, objects, limits, Section.CUSTODY, evidence.size(), rawChunks);
        final var byLane = mappingIndex(material.mappings());
        final var unresolvedStrict = unresolvedStrictLanes(after.entries());
        final Map<String, LegacyCheckpointStateInventory.RawEntry> bindings = new HashMap<>();
        final Map<String, AdmissionGate> gates = new HashMap<>();
        final Map<String, LegacyCheckpointStateInventory.RawEntry> current = new TreeMap<>();
        for (var row : after.entries()) {
            if (row.family() == ColumnFamily.ID && kind(row.key(), 4)) {
                final var binding = ScheduleBinding.decode(
                        ValueEnvelope.decode(row.value(), 4).payload());
                requireMapping(byLane, binding.laneId(), binding.canonicalLaneTuple());
                bindings.put(Bytes.hex(binding.delayMessageId().bytes()), row);
            } else if (row.family() == ColumnFamily.ID && kind(row.key(), 1)) {
                current.put(Bytes.hex(messageId(row.key(), 2).bytes()), row);
            } else if (row.family() == ColumnFamily.META && kind(row.key(), 2)) {
                final var lane = new DestinationLaneId(Arrays.copyOfRange(row.key(), 2, row.key().length));
                requireMapping(byLane, lane, null);
                final var envelope = LaneRecordEnvelope.decode(
                        ValueEnvelope.decodeAny(row.value()).payload());
                final AdmissionGate gate = !envelope.isActive()
                        ? AdmissionGate.CLOSED
                        : envelope.typedActiveState().isPresent()
                                ? envelope.activeState().admissionGate()
                                : LaneRecord.decode(envelope.activeStateBytes()).admissionGate();
                gates.put(Bytes.hex(lane.bytes()), gate);
            }
        }
        final var desired = new TreeMap<String, TargetRow>();
        for (var row : actual.value().rows().values()) {
            final var value = TargetValueEnvelope.decodeAny(row.value());
            if (value.valueType() == TargetQuotaIncarnation.VALUE_TYPE
                    && TargetQuotaIncarnation.decode(value.payload()).identity().target() != null) {
                put(desired, row);
            }
        }
        final List<TargetMessageRecord> converted = new ArrayList<>();
        final List<RetainedEntity> retained = new ArrayList<>();
        // Native includes A while the protected old applier does not. Preserve both actual timelines, never
        // invent an old Source position or force the legacy sequence to count the Target-only activation.
        final var actualS = new TargetQuotaMutation(
                TargetQuotaMutation.increment(actual.value().sequence()), original.position(), Bytes.sha256(frame));
        for (var row : current.values()) {
            held.requireCurrent();
            final var id = messageId(row.key(), 2);
            final byte[] payload = ValueEnvelope.decode(row.value(), 1).payload();
            final var location = tail.locate(row.ordinal() + 1);
            final var rawRef = reference(location);
            final boolean retiredIdentity = RetiredMessageIdentityRecord.isEncoded(payload);
            final var old = retiredIdentity ? null : MessageRecord.decode(payload);
            final var bindingRow = bindings.get(Bytes.hex(id.bytes()));
            final var binding = bindingRow == null
                    ? null
                    : ScheduleBinding.decode(
                            ValueEnvelope.decode(bindingRow.value(), 4).payload());
            final byte[] guardRaw =
                    value(actual.value().rows(), ColumnFamily.META, TargetMigrationCustodyGuard.keyFor(id));
            boolean imported = false;
            if (!retiredIdentity
                    && old.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO
                    && (unresolvedStrict.contains(Bytes.hex(old.laneId().bytes()))
                            || gates.get(Bytes.hex(old.laneId().bytes())) != AdmissionGate.OPEN)) {
                throw new IllegalStateException(
                        "actual old strict barrier/control scope cannot be expressed by this prefix mapping");
            }
            if (!retiredIdentity
                    && safeShape(old)
                    && binding != null
                    && guardRaw == null
                    && gates.get(Bytes.hex(old.laneId().bytes())) == AdmissionGate.OPEN
                    && held.noFuture(id, old, binding)) {
                final var mapping = requireMapping(byLane, old.laneId(), binding.canonicalLaneTuple());
                final var spec = held.conversion(id, old, binding, mapping);
                if (spec != null) {
                    requireNoLegacyObligations(after.entries(), after.applied(), id);
                    final List<ExternalRecord> protectedCommands = new ArrayList<>(material.externalRecords());
                    protectedCommands.addAll(held.externalRecords());
                    if (original instanceof SourceReplayRecord command) {
                        protectedCommands.add(new ExternalRecord(
                                "SOURCE", command.command().commandId().bytes(), original.position(), frame));
                    }
                    requireOriginalCommand(spec, binding, protectedCommands);
                    final var source = SourcePositionCodec.decode(old.scheduleSourcePosition());
                    final byte[] ownerKey = Bytes.concat(
                            new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, TargetKeyCodec.KEY_FORMAT}, id.bytes());
                    final byte[] ownerRaw = value(actual.value().rows(), ColumnFamily.META, ownerKey);
                    final TargetScheduleBinding importedBinding;
                    final TargetQuotaPayloadOwner owner;
                    if (ownerRaw == null) {
                        importedBinding = TargetScheduleBinding.importedPrefix(
                                plan,
                                reference(tail.locate(bindingRow.ordinal() + 1)),
                                actualS,
                                id,
                                binding.commandType(),
                                binding.canonicalBody(),
                                spec.originalCommand().source(),
                                mapping.physicalTarget().id(),
                                mapping.domain(),
                                mapping.accountingIncarnation(),
                                spec.requiredDispatchRef(),
                                mapping.dispatchCompatibility(),
                                mapping.controlScope(),
                                null,
                                old.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO
                                        ? requireOrderingDomain(mapping)
                                        : null);
                        owner = binding.commandType()
                                        == com.nereusstream.delay.protocol.CommandType.PREPARE_LARGE_SCHEDULE
                                ? TargetQuotaPayloadOwner.importedPrefixCommitted(
                                        importedBinding,
                                        plan,
                                        originalReservation(
                                                after.entries(),
                                                old.payloadReference().reservationId()),
                                        mapping.legacyProjection().tenantRouteScopeDigest(),
                                        spec.accounting(),
                                        plan.newLineage())
                                : TargetQuotaPayloadOwner.importedPrefixScheduled(
                                        importedBinding,
                                        plan,
                                        mapping.legacyProjection().tenantRouteScopeDigest(),
                                        spec.accounting(),
                                        plan.newLineage());
                    } else {
                        final var originalOwner = TargetQuotaPayloadOwner.decode(
                                TargetValueEnvelope.decode(ownerRaw, TargetQuotaPayloadOwner.VALUE_TYPE)
                                        .payload());
                        final byte[] bindingRaw = Objects.requireNonNull(
                                value(
                                        actual.value().rows(),
                                        ColumnFamily.ID,
                                        TargetKeyCodec.scheduleBinding(originalOwner.initialBindingDigest())),
                                "original retained Target payload binding");
                        importedBinding = TargetScheduleBinding.decodeReferenced(
                                originalOwner.initialBindingDigest(),
                                TargetValueEnvelope.decode(bindingRaw, TargetScheduleBinding.VALUE_TYPE)
                                        .payload());
                        originalOwner.requireInitialBinding(importedBinding);
                        if (originalOwner.phase() == TargetQuotaPayloadOwner.Phase.RESERVED
                                && old.payloadReference() != null) {
                            final var committed = originalReservation(
                                    after.entries(), old.payloadReference().reservationId());
                            final var proof = Objects.requireNonNull(
                                    prefixReservations.get(Bytes.hex(committed.reservationId())),
                                    "actual Commit original receipt proof");
                            owner = originalOwner.commit(old.payloadReference(), actualS, (before, next, floor) -> {
                                held.requireCurrent();
                                held.requireReservationCustody(
                                        committed,
                                        PayloadReservationReceipt.decodeFrame(
                                                proof.receipt().originalBytes()));
                            });
                        } else {
                            owner = originalOwner;
                        }
                    }
                    final var message = convert(id, old, binding, mapping, importedBinding, owner);
                    requireReferences(importedBinding, mapping, spec);
                    converted.add(message);
                    addConverted(desired, message, importedBinding, owner, spec.supportingRows());
                    imported = true;
                }
            }
            if (!imported
                    && !retiredIdentity
                    && old.status() == MessageStatus.SCHEDULED
                    && old.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO) {
                throw new IllegalStateException(
                        "safe strict Source work lacks its exact noFuture/mapping authority; cannot strand it in custody");
            }
            if (!imported) {
                retained.add(new RetainedEntity(
                        id,
                        old == null ? 0 : old.generation(),
                        retiredIdentity,
                        rawRef,
                        Bytes.sha256(row.canonicalBytes()),
                        held.retainedLegacyCharge(id, after)));
            }
        }
        // Uncommitted reservations may have no MESSAGE at all; they still own payload/quota/receipt protection.
        for (var row : after.entries()) {
            if (row.family() == ColumnFamily.ID && kind(row.key(), 2)) {
                final var reservation = PayloadReservation.decode(
                        ValueEnvelope.decodeAny(row.value()).payload());
                final var mapping = requireMapping(byLane, reservation.intent().laneId(), null);
                final byte[] nativeKey = Bytes.concat(
                        new byte[] {TargetKeyCodec.RESERVATION_TAG, TargetKeyCodec.KEY_FORMAT},
                        reservation.delayMessageId().bytes());
                final byte[] nativeRaw = value(actual.value().rows(), ColumnFamily.ID, nativeKey);
                final var previousReservation = nativeRaw == null
                        ? null
                        : TargetReservationRecord.decode(
                                TargetValueEnvelope.decode(nativeRaw, TargetReservationRecord.VALUE_TYPE)
                                        .payload());
                final var proof = prefixReservations.get(Bytes.hex(reservation.reservationId()));
                if (previousReservation != null
                        && previousReservation.status() == PayloadReservationStatus.RESERVED
                        && (reservation.status() == PayloadReservationStatus.ABANDONED
                                || reservation.status() == PayloadReservationStatus.EXPIRED)) {
                    if (proof == null) {
                        throw new AuditUnavailable(
                                "reservation terminal prefix lost its original receipt/object custody");
                    }
                    final var spec = proof.conversion();
                    final byte[] ownerKey = Bytes.concat(
                            new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, TargetKeyCodec.KEY_FORMAT},
                            reservation.delayMessageId().bytes());
                    final var owner = TargetQuotaPayloadOwner.decode(TargetValueEnvelope.decode(
                                    Objects.requireNonNull(value(actual.value().rows(), ColumnFamily.META, ownerKey)),
                                    TargetQuotaPayloadOwner.VALUE_TYPE)
                            .payload());
                    final var binding = TargetScheduleBinding.decodeReferenced(
                            owner.initialBindingDigest(),
                            TargetValueEnvelope.decode(
                                            Objects.requireNonNull(value(
                                                    actual.value().rows(),
                                                    ColumnFamily.ID,
                                                    TargetKeyCodec.scheduleBinding(owner.initialBindingDigest()))),
                                            TargetScheduleBinding.VALUE_TYPE)
                                    .payload());
                    final var retainedOwner = owner.retain(actualS, (before, next, floor) -> {
                        held.requireCurrent();
                        held.requireReservationCustody(
                                reservation,
                                PayloadReservationReceipt.decodeFrame(
                                        proof.receipt().originalBytes()));
                    });
                    addReservation(
                            desired,
                            previousReservation.finish(reservation.status(), actualS, null),
                            binding,
                            retainedOwner,
                            spec.supportingRows());
                    continue;
                }
                if (proof != null
                        && (reservation.status() == PayloadReservationStatus.RESERVED
                                || reservation.status() == PayloadReservationStatus.COMMITTED)) {
                    requireNoLegacyObligations(after.entries(), after.applied(), reservation.delayMessageId());
                    final var oldBindingRow = Objects.requireNonNull(
                            bindings.get(Bytes.hex(reservation.delayMessageId().bytes())), "original Prepare binding");
                    final var oldBinding = ScheduleBinding.decode(
                            ValueEnvelope.decodeAny(oldBindingRow.value()).payload());
                    final var spec = proof.conversion();
                    final byte[] ownerKey = Bytes.concat(
                            new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, TargetKeyCodec.KEY_FORMAT},
                            reservation.delayMessageId().bytes());
                    final byte[] projectedOwner = value(desired, ColumnFamily.META, ownerKey);
                    final byte[] priorOwner = value(actual.value().rows(), ColumnFamily.META, ownerKey);
                    final TargetScheduleBinding binding;
                    final TargetQuotaPayloadOwner owner;
                    if (projectedOwner != null || priorOwner != null) {
                        owner = TargetQuotaPayloadOwner.decode(TargetValueEnvelope.decode(
                                        projectedOwner != null ? projectedOwner : priorOwner,
                                        TargetQuotaPayloadOwner.VALUE_TYPE)
                                .payload());
                        final byte[] bindingKey = TargetKeyCodec.scheduleBinding(owner.initialBindingDigest());
                        final byte[] projectedBinding = value(desired, ColumnFamily.ID, bindingKey);
                        final byte[] priorBinding = value(actual.value().rows(), ColumnFamily.ID, bindingKey);
                        binding = TargetScheduleBinding.decodeReferenced(
                                owner.initialBindingDigest(),
                                TargetValueEnvelope.decode(
                                                Objects.requireNonNull(
                                                        projectedBinding != null ? projectedBinding : priorBinding),
                                                TargetScheduleBinding.VALUE_TYPE)
                                        .payload());
                    } else {
                        binding = TargetScheduleBinding.importedPrefix(
                                plan,
                                reference(tail.locate(oldBindingRow.ordinal() + 1)),
                                actualS,
                                reservation.delayMessageId(),
                                oldBinding.commandType(),
                                oldBinding.canonicalBody(),
                                spec.originalCommand().source(),
                                mapping.physicalTarget().id(),
                                mapping.domain(),
                                mapping.accountingIncarnation(),
                                spec.requiredDispatchRef(),
                                mapping.dispatchCompatibility(),
                                mapping.controlScope(),
                                null,
                                reservation.intent().orderingMode() == OrderingMode.DELIVERY_TIME_FIFO
                                        ? requireOrderingDomain(mapping)
                                        : null);
                        owner = reservation.status() == PayloadReservationStatus.RESERVED
                                ? TargetQuotaPayloadOwner.importedPrefixReserved(
                                        binding,
                                        plan,
                                        reservation.reservationId(),
                                        mapping.legacyProjection().tenantRouteScopeDigest(),
                                        spec.accounting(),
                                        plan.newLineage())
                                : TargetQuotaPayloadOwner.importedPrefixCommitted(
                                        binding,
                                        plan,
                                        reservation,
                                        mapping.legacyProjection().tenantRouteScopeDigest(),
                                        spec.accounting(),
                                        plan.newLineage());
                    }
                    requireReferences(binding, mapping, spec);
                    final TargetReservationRecord convertedReservation;
                    if (previousReservation != null) {
                        if (!Arrays.equals(previousReservation.reservationId(), reservation.reservationId())) {
                            throw new IllegalStateException(
                                    "old Source reservation conflicts with actual Native ownership");
                        }
                        convertedReservation = previousReservation.status() == reservation.status()
                                ? previousReservation
                                : previousReservation.finish(
                                        reservation.status(), actualS, reservation.committedPayload());
                    } else {
                        convertedReservation = TargetReservationRecord.imported(
                                binding,
                                plan,
                                reference(tail.locate(row.ordinal() + 1)),
                                reservation,
                                CommandCodec.decodeFrame(spec.originalCommand().originalBytes()),
                                externalReference(tail, after.entries().size(), prefixExternal, proof.receipt()),
                                PayloadReservationReceipt.decodeFrame(
                                        proof.receipt().originalBytes()),
                                TargetOrderState.OrderingContract.LEGACY_DELIVERY_TIME_FIFO);
                    }
                    addReservation(desired, convertedReservation, binding, owner, spec.supportingRows());
                    continue;
                }
                if (!current.containsKey(Bytes.hex(reservation.delayMessageId().bytes()))) {
                    retained.add(new RetainedEntity(
                            reservation.delayMessageId(),
                            0,
                            true,
                            reference(tail.locate(row.ordinal() + 1)),
                            Bytes.sha256(row.canonicalBytes()),
                            held.retainedLegacyCharge(reservation.delayMessageId(), after)));
                }
            } else if (row.family() == ColumnFamily.TERMINAL && kind(row.key(), 1)) {
                final var terminal = TerminalGenerationRecord.decode(
                        ValueEnvelope.decodeAny(row.value()).payload());
                if (!current.containsKey(Bytes.hex(terminal.messageId().bytes()))) {
                    retained.add(new RetainedEntity(
                            terminal.messageId(),
                            terminal.generation(),
                            false,
                            reference(tail.locate(row.ordinal() + 1)),
                            Bytes.sha256(row.canonicalBytes()),
                            held.retainedLegacyCharge(terminal.messageId(), after)));
                }
            }
        }
        final var completeRetained = new TreeMap<String, RetainedEntity>();
        for (var entity : retained) {
            final String id = Bytes.hex(entity.messageId().bytes());
            final var priorEntity = completeRetained.putIfAbsent(id, entity);
            if (priorEntity != null) {
                if (!Arrays.equals(
                        priorEntity.retainedCharge().canonicalBytes(),
                        entity.retainedCharge().canonicalBytes())) {
                    throw new IllegalStateException(
                            "original per-ID legacy fee custody changed within one complete prefix");
                }
                final int generation = Integer.compareUnsigned(priorEntity.generation(), entity.generation()) >= 0
                        ? priorEntity.generation()
                        : entity.generation();
                completeRetained.put(
                        id,
                        new RetainedEntity(
                                entity.messageId(),
                                generation,
                                priorEntity.identityOnly() || entity.identityOnly(),
                                new TargetMigrationRecordReference(
                                        new TargetMigrationObjectReference(tail.manifest()), 0),
                                Bytes.sha256(
                                        tail.manifest().canonicalBytes(),
                                        entity.messageId().bytes()),
                                entity.retainedCharge()));
            }
        }
        qualifyImportedSummaries(after, desired, actual.value().rows(), plan, held);
        buildOrderStates(
                material.mappings(),
                converted,
                after.entries(),
                desired,
                actual.value().rows());
        buildQueues(material.mappings(), converted, desired, actual.value().rows());
        final List<RowKey> deletions = new ArrayList<>();
        for (var row : actual.value().rows().values()) {
            final int type = TargetValueEnvelope.decodeAny(row.value()).valueType();
            if ((type == TargetMessageRecord.VALUE_TYPE
                            || type == TargetTimelineWorkRef.VALUE_TYPE
                            || type == TargetExpiryRef.VALUE_TYPE
                            || type == TargetReservationRecord.VALUE_TYPE)
                    && !desired.containsKey(rowId(row.family(), row.key()))) {
                deletions.add(new RowKey(row.family(), row.key()));
            }
        }
        final List<byte[]> projection = new ArrayList<>();
        projection.add(Bytes.concat(Bytes.u8(1), original.position().canonicalBytes(), Bytes.sha256(frame)));
        for (var row : desired.values()) {
            projection.add(Bytes.concat(Bytes.u8(2), row.canonicalBytes()));
        }
        for (var row : deletions) {
            projection.add(Bytes.concat(Bytes.u8(3), Bytes.u8(row.family().ordinal()), Bytes.lp32(row.key())));
        }
        for (var entity : completeRetained.values()) {
            projection.add(Bytes.concat(
                    Bytes.u8(4),
                    entity.messageId().bytes(),
                    Bytes.u32beBits(entity.generation()),
                    Bytes.u8(entity.identityOnly() ? 1 : 0),
                    Bytes.lp32(entity.original().canonicalBytes()),
                    entity.resultDigest(),
                    Bytes.lp32(entity.retainedCharge().canonicalBytes())));
        }
        final var projectionChunks = encodeChunks(Section.CUSTODY, projection, limits);
        long projectionBytes = 0;
        for (var chunk : projectionChunks) {
            projectionBytes = Math.addExact(projectionBytes, chunk.bytes().length);
        }
        objects.requireReserved(Math.addExact(projectionBytes, limits.maxChunkBytes()), projectionChunks.size() + 1);
        final var projected =
                publish(material, held, objects, limits, Section.CUSTODY, projection.size(), projectionChunks);
        held.requirePrefixEvidence(original, outcome, after);
        held.requireCurrent();
        return new TailProjection(
                original,
                outcome,
                prior,
                actual.view(),
                List.copyOf(desired.values()),
                deletions,
                new TargetMigrationRecordReference(new TargetMigrationObjectReference(tail.manifest()), 0),
                new TargetMigrationRecordReference(new TargetMigrationObjectReference(projected.manifest()), 0),
                List.copyOf(completeRetained.values()));
    }

    private static TargetMigrationRecordReference reference(RecordLocation location) {
        return new TargetMigrationRecordReference(
                new TargetMigrationObjectReference(location.object()), location.ordinal());
    }

    private static TargetMigrationRecordReference externalReference(
            Companion tail, int rawRows, List<ExternalRecord> records, ExternalRecord exact) {
        for (int index = 0; index < records.size(); index++) {
            if (Arrays.equals(records.get(index).canonicalBytes(), exact.canonicalBytes())) {
                return reference(tail.locate(1L + rawRows + index));
            }
        }
        throw new IllegalArgumentException("original prefix receipt is outside the fixed captured inventory");
    }

    private record TargetSnapshot(Map<String, TargetRow> rows, SourcePosition source, long sequence) {}

    private static void requireNoUnprojectedNativeActivity(Map<String, TargetRow> rows, TargetMigrationPlan plan) {
        final java.util.Set<String> prefixSources = new java.util.HashSet<>();
        for (var row : rows.values()) {
            final var value = TargetValueEnvelope.decodeAny(row.value());
            if (value.valueType() == TargetMigrationPrefixRecord.VALUE_TYPE) {
                final var prefix = TargetMigrationPrefixRecord.decode(value.payload());
                prefix.requirePlan(plan);
                prefixSources.add(Bytes.hex(prefix.at().source().canonicalBytes()));
            }
        }
        for (var row : rows.values()) {
            final var value = TargetValueEnvelope.decodeAny(row.value());
            if (value.valueType() == TargetMessageRecord.VALUE_TYPE) {
                final var message = TargetMessageRecord.decode(value.payload());
                if (message.runtime().currentWorkKind() != CurrentSendWorkKind.TIMELINE
                        || !message.runtime().attemptObligations().isEmpty()
                        || message.scheduleSource().compareTo(plan.baseline().source()) > 0
                                && !prefixSources.contains(
                                        Bytes.hex(message.scheduleSource().canonicalBytes()))) {
                    throw new IllegalStateException(
                            "actual new Native business is not represented by the old shadow; original owner interpretation is required");
                }
            } else if (value.valueType() == TargetScheduleBinding.VALUE_TYPE
                    && !TargetScheduleBinding.decode(value.payload()).imported()) {
                throw new IllegalStateException(
                        "ordinary Native-owned work cannot be overwritten by an old-domain whole-view projection");
            }
        }
    }

    private static String rowId(ColumnFamily family, byte[] key) {
        return family.ordinal() + ":" + Bytes.hex(key);
    }

    private static byte[] value(Map<String, TargetRow> rows, ColumnFamily family, byte[] key) {
        final var row = rows.get(rowId(family, key));
        return row == null ? null : row.value();
    }

    /** Publication creates protected immutable companion objects only; it cannot adopt or activate a candidate. */
    public static Sealed seal(Material material, Held held, Limits limits, ImmutableObjects objects) {
        Objects.requireNonNull(material, "material");
        Objects.requireNonNull(held, "held");
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(objects, "objects");
        held.requireCurrent();
        if (!material.summary().equals("READY")) {
            throw new IllegalStateException("blocked material cannot be sealed for migration");
        }
        if (Math.addExact(Math.multiplyExact(material.retainedBytes(), 4), 2L * limits.maxChunkBytes())
                > held.retainedBytes()) {
            throw new IllegalStateException("original migration flight has no charged bounded chunk buffers");
        }
        final List<byte[]> mapping = new ArrayList<>();
        for (var entry : material.mappings()) {
            mapping.add(entry.canonicalBytes());
        }
        final List<byte[]> custody = new ArrayList<>();
        // All original raw rows lead this fixed inventory, so oldRecordRef is stable and direct.
        for (var entry : material.view().entries()) {
            custody.add(Bytes.concat(Bytes.u8(1), entry.canonicalBytes()));
        }
        for (var entry : material.dispositions()) {
            custody.add(Bytes.concat(Bytes.u8(2), entry.canonicalBytes()));
        }
        for (var entry : material.entities()) {
            custody.add(Bytes.concat(Bytes.u8(3), entry.canonicalBytes()));
        }
        for (var entry : material.externalRecords()) {
            custody.add(Bytes.concat(Bytes.u8(4), entry.canonicalBytes()));
        }
        final List<byte[]> conservation = List.of(Bytes.concat(
                material.baseline().canonicalBytes(),
                material.view().manifest().manifestSha256(),
                material.conservation().canonicalBytes()));
        final var mappingChunks = encodeChunks(Section.MAPPING, mapping, limits);
        final var custodyChunks = encodeChunks(Section.CUSTODY, custody, limits);
        final var conservationChunks = encodeChunks(Section.CONSERVATION, conservation, limits);
        final int count = Math.addExact(
                3, Math.addExact(mappingChunks.size(), Math.addExact(custodyChunks.size(), conservationChunks.size())));
        if (count > limits.maxChunks()) {
            throw new IllegalStateException("migration companion exceeds its fixed object bound");
        }
        long encodedBytes = 3L * limits.maxChunkBytes();
        for (var encoded : mappingChunks) {
            encodedBytes = Math.addExact(encodedBytes, encoded.bytes().length);
        }
        for (var encoded : custodyChunks) {
            encodedBytes = Math.addExact(encodedBytes, encoded.bytes().length);
        }
        for (var encoded : conservationChunks) {
            encodedBytes = Math.addExact(encodedBytes, encoded.bytes().length);
        }
        objects.requireReserved(encodedBytes, count);
        final var sealedMapping =
                publish(material, held, objects, limits, Section.MAPPING, mapping.size(), mappingChunks);
        final var sealedCustody =
                publish(material, held, objects, limits, Section.CUSTODY, custody.size(), custodyChunks);
        final var sealedConservation =
                publish(material, held, objects, limits, Section.CONSERVATION, conservation.size(), conservationChunks);
        held.requireCurrent();
        return new Sealed(material, sealedMapping, sealedCustody, sealedConservation);
    }

    private record EncodedChunk(long first, int records, byte[] bytes) {}

    /** Complete bytes obtained solely through the manifest's fixed exact object inventory, never a provider list. */
    public record LoadedCompanion(Companion companion, List<byte[]> records) {
        public LoadedCompanion {
            Objects.requireNonNull(companion);
            records = records.stream().map(Bytes::copy).toList();
            if (records.size() != companion.records()) {
                throw new IllegalArgumentException("loaded companion is incomplete");
            }
        }

        @Override
        public List<byte[]> records() {
            return records.stream().map(Bytes::copy).toList();
        }
    }

    public static Companion readCompanionIndex(
            CheckpointResource manifest,
            Section expected,
            Material material,
            Held held,
            Limits limits,
            ImmutableObjects objects) {
        Objects.requireNonNull(manifest);
        Objects.requireNonNull(expected);
        Objects.requireNonNull(material);
        held.requireCurrent();
        if (manifest.manifestLength() > limits.maxChunkBytes()) {
            throw new IllegalArgumentException("companion manifest exceeds bound");
        }
        final byte[] encoded = readExact(objects, manifest, limits.maxChunkBytes());
        final var fields = QueryCodecSupport.read(encoded, "migration companion manifest");
        if (fields.size() < 5
                || (long) fields.size() > (long) limits.maxChunks() + 5
                || QueryCodecSupport.uint32(fields.get(0), 1) != 1
                || QueryCodecSupport.uint32(fields.get(1), 2) != expected.ordinal() + 1
                || !Arrays.equals(
                        QueryCodecSupport.fixed(fields.get(2), 3, 32),
                        material.view().manifest().manifestSha256())
                || !Arrays.equals(
                        QueryCodecSupport.bytes(fields.get(3), 4),
                        material.baseline().canonicalBytes())) {
            throw new IllegalArgumentException("companion manifest changed exact baseline or section");
        }
        final long count = QueryCodecSupport.uint(fields.get(4), 5);
        if (count < 0 || count > Math.min(Integer.MAX_VALUE - 1L, 6L * limits.maxEntries())) {
            throw new IllegalArgumentException("companion record count exceeds finite bound");
        }
        final List<Chunk> chunks = new ArrayList<>();
        final java.util.Set<String> identities = new java.util.HashSet<>();
        long maximumBytes = 0;
        for (int index = 5; index < fields.size(); index++) {
            final byte[] raw = QueryCodecSupport.bytes(fields.get(index), 6);
            if (raw.length < 16 || raw.length > TargetMigrationObjectReference.MAX_CANONICAL_BYTES + 16) {
                throw new IllegalArgumentException("fixed chunk entry exceeds resource reference bound");
            }
            final var input = ByteBuffer.wrap(raw);
            final long first = input.getLong();
            final int records = input.getInt();
            final int length = input.getInt();
            if (length <= 0 || length != input.remaining()) {
                throw new IllegalArgumentException("invalid fixed chunk resource length");
            }
            final byte[] resourceBytes = new byte[length];
            input.get(resourceBytes);
            final var resource =
                    new TargetMigrationObjectReference(CheckpointResource.decode(resourceBytes)).resource();
            if (resource.manifestLength() > limits.maxChunkBytes()
                    || !Arrays.equals(resource.recoveryLineageId(), manifest.recoveryLineageId())
                    || !Arrays.equals(resource.checkpointId(), manifest.checkpointId())
                    || !identities.add(Bytes.hex(resource.exactResourceCanonicalBytes()))) {
                throw new IllegalArgumentException(
                        "fixed companion inventory changed exact object scope or repeated an object");
            }
            maximumBytes = Math.addExact(maximumBytes, resource.manifestLength());
            chunks.add(new Chunk(first, records, resource));
        }
        if (maximumBytes > limits.maxRetainedBytes()
                || Math.addExact(material.retainedBytes(), Math.multiplyExact(maximumBytes, 3))
                        > held.retainedBytes()) {
            throw new IllegalStateException("original flight has no charged complete companion read buffers");
        }
        final var companion = new Companion(expected, count, manifest, chunks);
        objects.requireReserved(maximumBytes, chunks.size());
        held.requireCurrent();
        return companion;
    }

    public static LoadedCompanion readCompanion(
            CheckpointResource manifest,
            Section expected,
            Material material,
            Held held,
            Limits limits,
            ImmutableObjects objects) {
        final var companion = readCompanionIndex(manifest, expected, material, held, limits, objects);
        final List<byte[]> records = new ArrayList<>();
        for (int chunk = 0; chunk < companion.chunks().size(); chunk++) {
            records.addAll(readCompanionChunk(companion, chunk, held, limits, objects));
        }
        held.requireCurrent();
        return new LoadedCompanion(companion, records);
    }

    /** One exact fixed chunk per owned-I/O turn; no object listing or reconstructed inventory. */
    public static List<byte[]> readCompanionChunk(
            Companion companion, int chunkIndex, Held held, Limits limits, ImmutableObjects objects) {
        held.requireCurrent();
        if (chunkIndex < 0 || chunkIndex >= companion.chunks().size()) {
            throw new IllegalArgumentException("fixed companion chunk index is outside its authenticated inventory");
        }
        final var chunk = companion.chunks().get(chunkIndex);
        final byte[] bytes = readExact(objects, chunk.object(), limits.maxChunkBytes());
        final var input = ByteBuffer.wrap(bytes);
        if (input.remaining() < 13
                || input.getInt() != 1
                || Byte.toUnsignedInt(input.get()) != companion.section().ordinal() + 1
                || input.getLong() != chunk.firstOrdinal()) {
            throw new IllegalArgumentException("fixed companion chunk header changed");
        }
        final List<byte[]> records = new ArrayList<>();
        for (int index = 0; index < chunk.records(); index++) {
            if (input.remaining() < 4) {
                throw new IllegalArgumentException("fixed companion chunk is truncated");
            }
            final int length = input.getInt();
            if (length <= 0 || length > input.remaining()) {
                throw new IllegalArgumentException("fixed companion record is truncated");
            }
            final byte[] record = new byte[length];
            input.get(record);
            records.add(record);
        }
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("fixed companion chunk has unaccounted records");
        }
        held.requireCurrent();
        return List.copyOf(records);
    }

    private static byte[] readExact(ImmutableObjects objects, CheckpointResource resource, int maximumBytes) {
        if (resource.manifestLength() < 0 || resource.manifestLength() > maximumBytes) {
            throw new IllegalArgumentException("immutable companion object exceeds byte bound");
        }
        final byte[] bytes =
                Objects.requireNonNull(objects.readExact(resource, maximumBytes), "immutable companion exact bytes");
        if (bytes.length != resource.manifestLength()
                || !Bytes.constantTimeEquals(Bytes.sha256(bytes), resource.manifestSha256())) {
            throw new IllegalArgumentException("immutable companion exact version/hash/length mismatch");
        }
        return bytes;
    }

    private static List<EncodedChunk> encodeChunks(Section section, List<byte[]> records, Limits limits) {
        final List<EncodedChunk> result = new ArrayList<>();
        int index = 0;
        while (index < records.size()) {
            final int first = index;
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream(limits.maxChunkBytes());
            bytes.writeBytes(Bytes.u32be(1));
            bytes.writeBytes(Bytes.u8(section.ordinal() + 1));
            bytes.writeBytes(Bytes.u64be(first));
            while (index < records.size()) {
                final byte[] record = records.get(index);
                if ((long) bytes.size() + 4 + record.length > limits.maxChunkBytes()) {
                    if (index == first) {
                        throw new IllegalStateException("one migration record exceeds the fixed chunk byte bound");
                    }
                    break;
                }
                bytes.writeBytes(Bytes.lp32(record));
                index++;
            }
            result.add(new EncodedChunk(first, index - first, bytes.toByteArray()));
            if (result.size() > limits.maxChunks()) {
                throw new IllegalStateException("migration companion chunk count exceeds bound");
            }
        }
        return List.copyOf(result);
    }

    private static Companion publish(
            Material material,
            Held held,
            ImmutableObjects objects,
            Limits limits,
            Section section,
            int records,
            List<EncodedChunk> encoded) {
        final List<Chunk> chunks = new ArrayList<>();
        for (int index = 0; index < encoded.size(); index++) {
            final var chunk = encoded.get(index);
            final var object = putExact(material, held, objects, section, index, false, chunk.bytes());
            chunks.add(new Chunk(chunk.first(), chunk.records(), object));
        }
        final byte[] manifest = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.uint32(out, 2, section.ordinal() + 1);
            CanonicalProtobuf.bytes(out, 3, material.view().manifest().manifestSha256());
            CanonicalProtobuf.bytes(out, 4, material.baseline().canonicalBytes());
            CanonicalProtobuf.uint64(out, 5, records);
            for (var chunk : chunks) {
                CanonicalProtobuf.bytes(out, 6, chunk.canonicalBytes());
            }
        });
        if (manifest.length > limits.maxChunkBytes()) {
            throw new IllegalStateException("fixed companion manifest exceeds byte bound");
        }
        return new Companion(
                section, records, putExact(material, held, objects, section, chunks.size(), true, manifest), chunks);
    }

    private static CheckpointResource putExact(
            Material material,
            Held held,
            ImmutableObjects objects,
            Section section,
            int chunk,
            boolean manifest,
            byte[] bytes) {
        held.requireCurrent();
        final var ref =
                Objects.requireNonNull(objects.put(section, chunk, manifest, bytes), "immutable companion resource");
        if (!Arrays.equals(ref.recoveryLineageId(), material.view().manifest().recoveryLineageId())
                || !Arrays.equals(ref.checkpointId(), material.view().manifest().checkpointId())
                || ref.manifestLength() != bytes.length
                || !Bytes.constantTimeEquals(ref.manifestSha256(), Bytes.sha256(bytes))) {
            throw new IllegalStateException(
                    "companion provider returned a different immutable object identity or content");
        }
        final byte[] actual =
                Objects.requireNonNull(objects.readExact(ref, bytes.length), "exact immutable companion bytes");
        if (!Arrays.equals(actual, bytes)) {
            throw new IllegalStateException("companion exact version read differs from the proposed full content");
        }
        held.requireCurrent();
        return ref;
    }

    public static Material prepare(LegacyCheckpointStateInventory.CompleteView view, Held held, Limits limits) {
        Objects.requireNonNull(view, "view");
        Objects.requireNonNull(held, "held");
        Objects.requireNonNull(limits, "limits");
        held.requireCurrent();
        if (view.entries().size() > limits.maxEntries()
                || held.retainedBytes() < view.inventory().scannedBytes()) {
            throw new IllegalStateException("original migration flight has not reserved its complete legacy view");
        }
        final TargetQuotaMutation baseline = Objects.requireNonNull(held.baseline(view), "actual baseline");
        if (baseline.isLocalMutation()
                || baseline.sequence() != view.manifest().shardMutationSequence()
                || !Arrays.equals(
                        baseline.source().canonicalBytes(),
                        view.manifest().appliedShardLogPosition().canonicalBytes())) {
            throw new IllegalArgumentException("migration baseline differs from the original applied Source fact");
        }
        final var mappings = List.copyOf(held.mappings());
        final var external = List.copyOf(held.externalRecords());
        if ((long) view.entries().size() + mappings.size() + external.size() > limits.maxEntries()) {
            throw new IllegalStateException("migration input entry count exceeds the original flight bound");
        }
        final EnumSet<Audit> blockers = EnumSet.allOf(Audit.class);
        audit(
                () -> held.requireClosedWriterDomain(view, mappings),
                blockers,
                Audit.FENCED_MAINTENANCE_AUTHORITY_NOT_AUDITED);
        final Map<String, LaneMapping> byLane = mappingIndex(mappings);
        final var oldControls = laneControls(view.entries());
        final var strictBarriers = unresolvedStrictLanes(view.entries());
        final Map<String, LegacyCheckpointStateInventory.RawEntry> bindings = new HashMap<>();
        final Map<String, PayloadReservation> originalReservations = new HashMap<>();
        final List<String> conflicts = new ArrayList<>();
        for (var entry : view.entries()) {
            if (entry.family() == ColumnFamily.ID && kind(entry.key(), 2)) {
                final var reservation = PayloadReservation.decode(
                        ValueEnvelope.decodeAny(entry.value()).payload());
                originalReservations.put(Bytes.hex(reservation.reservationId()), reservation);
            }
            if (entry.family() == ColumnFamily.META && kind(entry.key(), 2)) {
                if (entry.key().length != 2 + DestinationLaneId.LENGTH) {
                    throw new IllegalArgumentException("invalid legacy Lane locator");
                }
                requireMapping(
                        byLane, new DestinationLaneId(Arrays.copyOfRange(entry.key(), 2, entry.key().length)), null);
            }
            if (entry.family() == ColumnFamily.ID && kind(entry.key(), 4)) {
                final ScheduleBinding binding = ScheduleBinding.decode(
                        ValueEnvelope.decodeAny(entry.value()).payload());
                if (!Arrays.equals(entry.key(), KeyCodec.idScheduleBinding(binding.delayMessageId()))) {
                    throw new IllegalArgumentException("legacy binding key and value disagree");
                }
                requireMapping(byLane, binding.laneId(), binding.canonicalLaneTuple());
                bindings.put(Bytes.hex(binding.delayMessageId().bytes()), entry);
            }
        }
        // A structural conflict is never discharged merely by retaining its bytes.
        for (var conflict : view.inventory().conflicts()) {
            if (!custodyDecisionReason(conflict.reason())) {
                conflicts.add(conflict.reason().name() + ":" + Bytes.hex(conflict.oldKeyDigest()));
            }
        }
        final List<Entity> entities = new ArrayList<>();
        final List<RowDisposition> dispositions = new ArrayList<>();
        final Map<String, TargetRow> proposed = new TreeMap<>();
        final List<TargetMessageRecord> converted = new ArrayList<>();
        final Map<String, Conversion> conversions = new TreeMap<>();
        final Map<String, ReservationConversion> reservationConversions = new TreeMap<>();
        long convertedRows = 0, messageCount = 0, convertedCount = 0, payloadBytes = 0, convertedBytes = 0;
        long reservationCount = 0, convertedReservationCount = 0, reservedBytes = 0, convertedReservedBytes = 0;
        for (var entry : view.entries()) {
            held.requireCurrent();
            Disposition disposition = Disposition.RETAINED_LEGACY;
            String reason = "EXACT_ORIGINAL_CUSTODY";
            if (entry.family() == ColumnFamily.ID && kind(entry.key(), 1)) {
                final DelayMessageId id = messageId(entry.key(), 2);
                final byte[] payload = ValueEnvelope.decodeAny(entry.value()).payload();
                if (RetiredMessageIdentityRecord.isEncoded(payload)) {
                    final var retired = RetiredMessageIdentityRecord.decode(payload);
                    if (!id.equals(retired.messageId())) {
                        throw new IllegalArgumentException("retired identity key mismatch");
                    }
                    entities.add(new Entity(
                            id,
                            0,
                            null,
                            Disposition.RETAINED_LEGACY,
                            null,
                            entry.ordinal(),
                            0,
                            "RETIRED_IDENTITY_REUSE_FLOOR_REFERENCE_PROTECTION"));
                    dispositions.add(
                            new RowDisposition(entry.ordinal(), disposition, "RETIRED_IDENTITY_EXACT_CUSTODY"));
                    continue;
                }
                final MessageRecord old = MessageRecord.decode(payload);
                final var bindingEntry = bindings.get(Bytes.hex(id.bytes()));
                final ScheduleBinding binding = bindingEntry == null
                        ? null
                        : ScheduleBinding.decode(
                                ValueEnvelope.decodeAny(bindingEntry.value()).payload());
                final LaneMapping mapping = old.laneId() == null ? null : requireMapping(byLane, old.laneId(), null);
                final boolean strict = old.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO;
                final var laneControl = oldControls.get(Bytes.hex(old.laneId().bytes()));
                final boolean strictMappable = !strict
                        || mapping.orderingDomain() != null
                                && laneControl != null
                                && laneControl.gate() == AdmissionGate.OPEN
                                && !strictBarriers.contains(
                                        Bytes.hex(old.laneId().bytes()));
                final long length = old.payloadReference() == null
                        ? old.payload().length
                        : old.payloadReference().length();
                messageCount++;
                payloadBytes = Math.addExact(payloadBytes, length);
                if (!blockers.contains(Audit.FENCED_MAINTENANCE_AUTHORITY_NOT_AUDITED)
                        && strictMappable
                        && safeShape(old)
                        && binding != null
                        && held.noFuture(id, old, binding)) {
                    Conversion conversion;
                    if (binding.commandType() == com.nereusstream.delay.protocol.CommandType.PREPARE_LARGE_SCHEDULE) {
                        final var committed = old.payloadReference() == null
                                ? null
                                : originalReservations.get(
                                        Bytes.hex(old.payloadReference().reservationId()));
                        final var proof = committed == null ? null : held.reservation(committed, binding, mapping);
                        if (proof != null && committed.status() == PayloadReservationStatus.COMMITTED) {
                            requireReceipt(committed, binding, proof, external);
                            held.requireReservationCustody(
                                    committed,
                                    PayloadReservationReceipt.decodeFrame(
                                            proof.receipt().originalBytes()));
                            reservationConversions.put(Bytes.hex(committed.reservationId()), proof);
                            conversion = proof.conversion();
                        } else {
                            conversion = null;
                        }
                    } else {
                        conversion = held.conversion(id, old, binding, mapping);
                    }
                    if (conversion != null) {
                        try {
                            requireNoLegacyObligations(view.entries(), baseline.source(), id);
                        } catch (AuditUnavailable outstanding) {
                            conflicts.add("ORIGINAL_ALL_GENERATION_ATTEMPTS_REQUIRE_LEGACY_CUSTODY:"
                                    + Bytes.hex(entry.key()));
                            if (old.payloadReference() != null) {
                                reservationConversions.remove(
                                        Bytes.hex(old.payloadReference().reservationId()));
                            }
                            conversion = null;
                        }
                    }
                    if (conversion != null) {
                        requireOriginalCommand(conversion, binding, external);
                        conversions.put(Bytes.hex(id.bytes()), conversion);
                        disposition = Disposition.CONVERTED;
                        reason = "FROZEN_NO_FUTURE_ORIGINAL_SCHEDULE";
                        convertedCount++;
                        convertedBytes = Math.addExact(convertedBytes, length);
                    } else {
                        reason = "NO_APPROVED_TARGET_BINDING_RETAIN_ORIGINAL";
                        conflicts.add(
                                "SCHEDULED_ORIGINAL_BINDING_PAYLOAD_AUTHORITY_REQUIRED:" + Bytes.hex(entry.key()));
                    }
                } else {
                    reason = "ORIGINAL_STATE_OR_NO_FUTURE_REQUIRES_LEGACY_CUSTODY";
                }
                if (old.status() == MessageStatus.SCHEDULED && binding == null) {
                    conflicts.add("SCHEDULED_ORIGINAL_BINDING_REQUIRED:" + Bytes.hex(entry.key()));
                }
                if (strict && old.status() == MessageStatus.SCHEDULED && disposition != Disposition.CONVERTED) {
                    conflicts.add(
                            "STRICT_SCHEDULED_MAPPING_OR_BARRIER_REQUIRES_RECONCILIATION:" + Bytes.hex(entry.key()));
                }
                entities.add(new Entity(
                        id,
                        old.generation(),
                        old.status(),
                        disposition,
                        old.laneId(),
                        entry.ordinal(),
                        length,
                        reason));
            } else if (entry.family() == ColumnFamily.ID && kind(entry.key(), 2)) {
                final var reservation = PayloadReservation.decode(
                        ValueEnvelope.decodeAny(entry.value()).payload());
                if (!Arrays.equals(entry.key(), KeyCodec.idReservation(reservation.reservationId()))) {
                    throw new IllegalArgumentException("legacy reservation key and value disagree");
                }
                reservationCount++;
                final var mapping = requireMapping(byLane, reservation.intent().laneId(), null);
                reason = "RESERVATION_ANCHOR_RECEIPT_PAYLOAD_QUOTA_RETAINED";
                if (reservation.status() == PayloadReservationStatus.COMMITTED
                        && reservationConversions.containsKey(Bytes.hex(reservation.reservationId()))) {
                    disposition = Disposition.CONVERTED;
                    reason = "ORIGINAL_COMMITTED_RECEIPT_AND_MESSAGE_OWNER_CONVERSION";
                    convertedReservationCount++;
                } else if (reservation.status() == PayloadReservationStatus.RESERVED) {
                    reservedBytes =
                            Math.addExact(reservedBytes, reservation.intent().expectedPayloadLength());
                    final var bindingRow =
                            bindings.get(Bytes.hex(reservation.delayMessageId().bytes()));
                    final var binding = bindingRow == null
                            ? null
                            : ScheduleBinding.decode(
                                    ValueEnvelope.decodeAny(bindingRow.value()).payload());
                    ReservationConversion proof = null;
                    final var reservationControl = oldControls.get(
                            Bytes.hex(reservation.intent().laneId().bytes()));
                    final boolean strictReservationMappable =
                            reservation.intent().orderingMode() != OrderingMode.DELIVERY_TIME_FIFO
                                    || reservationControl != null
                                            && reservationControl.gate() == AdmissionGate.OPEN
                                            && !strictBarriers.contains(Bytes.hex(reservation
                                                    .intent()
                                                    .laneId()
                                                    .bytes()));
                    if (binding != null
                            && strictReservationMappable
                            && !blockers.contains(Audit.FENCED_MAINTENANCE_AUTHORITY_NOT_AUDITED)) {
                        try {
                            proof = held.reservation(reservation, binding, mapping);
                            if (proof != null) {
                                requireOriginalCommand(proof.conversion(), binding, external);
                                requireReceipt(reservation, binding, proof, external);
                                held.requireReservationCustody(
                                        reservation,
                                        PayloadReservationReceipt.decodeFrame(
                                                proof.receipt().originalBytes()));
                                requireReservationIndex(view.entries(), reservation, entry.value());
                            }
                        } catch (AuditUnavailable unavailable) {
                            proof = null;
                        }
                    }
                    if (proof != null) {
                        try {
                            requireNoLegacyObligations(view.entries(), baseline.source(), reservation.delayMessageId());
                        } catch (AuditUnavailable outstanding) {
                            conflicts.add("ORIGINAL_ALL_GENERATION_ATTEMPTS_REQUIRE_LEGACY_CUSTODY:"
                                    + Bytes.hex(entry.key()));
                            proof = null;
                        }
                    }
                    if (proof != null) {
                        reservationConversions.put(Bytes.hex(reservation.reservationId()), proof);
                        disposition = Disposition.CONVERTED;
                        reason = "ORIGINAL_PREPARE_RECEIPT_OBJECT_OWNER_RESERVED_CONVERSION";
                        convertedReservationCount++;
                        convertedReservedBytes = Math.addExact(
                                convertedReservedBytes, reservation.intent().expectedPayloadLength());
                    } else {
                        conflicts.add(
                                "RESERVED_ORIGINAL_RECEIPT_OBJECT_QUOTA_PROOFS_REQUIRED:" + Bytes.hex(entry.key()));
                    }
                }
                entities.add(new Entity(
                        reservation.delayMessageId(),
                        0,
                        null,
                        disposition,
                        reservation.intent().laneId(),
                        entry.ordinal(),
                        reservation.intent().expectedPayloadLength(),
                        "RESERVATION_" + reservation.status().name()));
            } else if (entry.family() == ColumnFamily.TERMINAL && kind(entry.key(), 1)) {
                final var terminal = TerminalGenerationRecord.decode(
                        ValueEnvelope.decodeAny(entry.value()).payload());
                entities.add(new Entity(
                        messageId(entry.key(), 2),
                        terminal.generation(),
                        terminal.status(),
                        Disposition.RETAINED_LEGACY,
                        null,
                        entry.ordinal(),
                        0,
                        "TERMINAL_RESULT_FLOOR_REFERENCE_PROTECTION"));
            }
            if (disposition == Disposition.CONVERTED) {
                convertedRows++;
            }
            dispositions.add(new RowDisposition(entry.ordinal(), disposition, reason));
        }
        for (var lane : strictBarriers) {
            conflicts.add("STRICT_ORIGINAL_BARRIER_REQUIRES_LEGACY_DRAIN:" + lane);
        }
        if (conflicts.isEmpty()) {
            blockers.remove(Audit.COMPLETE_STATE_MAPPING_NOT_AUDITED);
        }
        audit(() -> held.requireLegacyCustody(view, external), blockers, Audit.EXTERNAL_LEGACY_OBLIGATIONS_NOT_AUDITED);
        final Conservation conservation = new Conservation(
                view.entries().size(),
                view.inventory().scannedBytes(),
                convertedRows,
                view.entries().size() - convertedRows,
                messageCount,
                convertedCount,
                messageCount - convertedCount,
                payloadBytes,
                convertedBytes,
                payloadBytes - convertedBytes,
                reservationCount,
                convertedReservationCount,
                reservationCount - convertedReservationCount,
                reservedBytes,
                convertedReservedBytes,
                reservedBytes - convertedReservedBytes);
        long retained = view.inventory().scannedBytes();
        for (var row : proposed.values()) {
            retained = Math.addExact(retained, row.canonicalBytes().length);
        }
        for (var mapping : mappings) {
            retained = Math.addExact(retained, mapping.canonicalBytes().length);
        }
        for (var record : external) {
            retained = Math.addExact(retained, record.canonicalBytes().length);
        }
        for (var entity : entities) {
            retained = Math.addExact(retained, entity.canonicalBytes().length);
        }
        for (var disposition : dispositions) {
            retained = Math.addExact(retained, disposition.canonicalBytes().length);
        }
        if (retained > limits.maxRetainedBytes() || retained > held.retainedBytes()) {
            throw new IllegalStateException("migration material exceeds the original flight's reserved retained bytes");
        }
        final var provisional = new Material(
                view,
                baseline,
                mappings,
                external,
                entities,
                dispositions,
                List.copyOf(proposed.values()),
                conservation,
                blockers,
                conflicts,
                retained,
                conversions,
                reservationConversions);
        audit(() -> held.requireConservation(provisional), blockers, Audit.CONSERVATION_NOT_AUDITED);
        held.requireCurrent();
        return new Material(
                view,
                baseline,
                mappings,
                external,
                entities,
                dispositions,
                List.copyOf(proposed.values()),
                conservation,
                blockers,
                conflicts,
                retained,
                conversions,
                reservationConversions);
    }

    /** Second phase: the Plan now binds sealed original content, so imported records can safely bind its digest. */
    public static Material project(Sealed sealed, Held held, TargetMigrationPlan plan, Limits limits) {
        Objects.requireNonNull(sealed, "sealed");
        Objects.requireNonNull(plan, "plan");
        held.requireCurrent();
        final var material = sealed.material();
        if (!material.summary().equals("READY")
                || !Arrays.equals(
                        plan.baseline().canonicalBytes(), material.baseline().canonicalBytes())
                || !plan.mapping().resource().equals(sealed.mapping().manifest())
                || !plan.custody().resource().equals(sealed.custody().manifest())
                || !plan.conservation().resource().equals(sealed.conservation().manifest())
                || !Arrays.equals(
                        plan.oldManifest().resource().manifestSha256(),
                        material.view().manifest().manifestSha256())) {
            throw new IllegalArgumentException("migration Plan does not bind this exact sealed legacy material");
        }
        final var mappings = mappingIndex(material.mappings());
        final Map<String, LegacyCheckpointStateInventory.RawEntry> bindings = new HashMap<>();
        for (var row : material.view().entries()) {
            if (row.family() == ColumnFamily.ID && kind(row.key(), 4)) {
                final var binding = ScheduleBinding.decode(
                        ValueEnvelope.decodeAny(row.value()).payload());
                bindings.put(Bytes.hex(binding.delayMessageId().bytes()), row);
            }
        }
        final Map<String, TargetRow> rows = new TreeMap<>();
        final List<TargetMessageRecord> messages = new ArrayList<>();
        for (var entity : material.entities()) {
            if (entity.disposition() != Disposition.CONVERTED) {
                continue;
            }
            held.requireCurrent();
            final var original = material.view().entries().get(Math.toIntExact(entity.originalOrdinal()));
            if (original.family() == ColumnFamily.ID && kind(original.key(), 2)) {
                final var reservation = PayloadReservation.decode(
                        ValueEnvelope.decodeAny(original.value()).payload());
                final var proof = Objects.requireNonNull(
                        material.reservationConversions.get(Bytes.hex(reservation.reservationId())),
                        "original reservation conversion");
                final var bindingRow = Objects.requireNonNull(
                        bindings.get(Bytes.hex(reservation.delayMessageId().bytes())), "original Prepare binding");
                final var oldBinding = ScheduleBinding.decode(
                        ValueEnvelope.decodeAny(bindingRow.value()).payload());
                final var mapping =
                        requireMapping(mappings, reservation.intent().laneId(), oldBinding.canonicalLaneTuple());
                final var spec = proof.conversion();
                final var binding = TargetScheduleBinding.imported(
                        plan,
                        reference(sealed.originalRow(bindingRow.ordinal())),
                        reservation.delayMessageId(),
                        oldBinding.commandType(),
                        oldBinding.canonicalBody(),
                        spec.originalCommand().source(),
                        mapping.physicalTarget().id(),
                        mapping.domain(),
                        mapping.accountingIncarnation(),
                        spec.requiredDispatchRef(),
                        mapping.dispatchCompatibility(),
                        mapping.controlScope(),
                        null,
                        reservation.intent().orderingMode() == OrderingMode.DELIVERY_TIME_FIFO
                                ? requireOrderingDomain(mapping)
                                : null);
                requireReferences(binding, mapping, spec);
                held.requireReservationCustody(
                        reservation,
                        PayloadReservationReceipt.decodeFrame(proof.receipt().originalBytes()));
                final var owner = reservation.status() == PayloadReservationStatus.RESERVED
                        ? TargetQuotaPayloadOwner.importedReserved(
                                binding,
                                plan,
                                reservation.reservationId(),
                                mapping.legacyProjection().tenantRouteScopeDigest(),
                                spec.accounting(),
                                plan.newLineage())
                        : TargetQuotaPayloadOwner.importedCommitted(
                                binding,
                                plan,
                                reservation,
                                mapping.legacyProjection().tenantRouteScopeDigest(),
                                spec.accounting(),
                                plan.newLineage());
                final var projected = TargetReservationRecord.imported(
                        binding,
                        plan,
                        reference(sealed.originalRow(original.ordinal())),
                        reservation,
                        CommandCodec.decodeFrame(spec.originalCommand().originalBytes()),
                        reference(sealed.externalRecord(proof.receipt())),
                        PayloadReservationReceipt.decodeFrame(proof.receipt().originalBytes()),
                        TargetOrderState.OrderingContract.LEGACY_DELIVERY_TIME_FIFO);
                addReservation(rows, projected, binding, owner, spec.supportingRows());
                continue;
            }
            final var old = MessageRecord.decode(
                    ValueEnvelope.decodeAny(original.value()).payload());
            final var bindingRow = Objects.requireNonNull(
                    bindings.get(Bytes.hex(entity.messageId().bytes())), "original binding row");
            final var binding = ScheduleBinding.decode(
                    ValueEnvelope.decodeAny(bindingRow.value()).payload());
            if (!held.noFuture(entity.messageId(), old, binding)) {
                throw new IllegalStateException("original noFuture protection changed after material sealing");
            }
            final var location = sealed.originalRow(bindingRow.ordinal());
            final var ref = new TargetMigrationRecordReference(
                    new TargetMigrationObjectReference(location.object()), location.ordinal());
            final var spec = Objects.requireNonNull(
                    material.conversions.get(Bytes.hex(entity.messageId().bytes())), "frozen conversion");
            final var mapping = requireMapping(mappings, old.laneId(), binding.canonicalLaneTuple());
            final var imported = TargetScheduleBinding.imported(
                    plan,
                    ref,
                    entity.messageId(),
                    binding.commandType(),
                    binding.canonicalBody(),
                    spec.originalCommand().source(),
                    mapping.physicalTarget().id(),
                    mapping.domain(),
                    mapping.accountingIncarnation(),
                    spec.requiredDispatchRef(),
                    mapping.dispatchCompatibility(),
                    mapping.controlScope(),
                    null,
                    old.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO ? requireOrderingDomain(mapping) : null);
            final var owner =
                    binding.commandType() == com.nereusstream.delay.protocol.CommandType.PREPARE_LARGE_SCHEDULE
                            ? TargetQuotaPayloadOwner.importedCommitted(
                                    imported,
                                    plan,
                                    originalReservation(
                                            material.view().entries(),
                                            old.payloadReference().reservationId()),
                                    mapping.legacyProjection().tenantRouteScopeDigest(),
                                    spec.accounting(),
                                    plan.newLineage())
                            : TargetQuotaPayloadOwner.importedScheduled(
                                    imported,
                                    plan,
                                    mapping.legacyProjection().tenantRouteScopeDigest(),
                                    spec.accounting(),
                                    plan.newLineage());
            final var message = convert(entity.messageId(), old, binding, mapping, imported, owner);
            requireReferences(imported, mapping, spec);
            messages.add(message);
            addConverted(rows, message, imported, owner, spec.supportingRows());
        }
        qualifyImportedSummaries(
                LegacyCheckpointStateInventory.baselineView(material.view()), rows, Map.of(), plan, held);
        buildOrderStates(material.mappings(), messages, material.view().entries(), rows);
        buildQueues(material.mappings(), messages, rows);
        long retained = material.retainedBytes();
        for (var row : rows.values()) {
            retained = Math.addExact(retained, row.canonicalBytes().length);
        }
        if (retained > limits.maxRetainedBytes() || retained > held.retainedBytes()) {
            throw new IllegalStateException("Target migration projection exceeds original retained-byte reservation");
        }
        final var result = new Material(
                material.view,
                material.baseline,
                material.mappings,
                material.external,
                material.entities,
                material.dispositions,
                List.copyOf(rows.values()),
                material.conservation,
                material.blockers,
                material.conflicts,
                retained,
                material.conversions,
                material.reservationConversions);
        held.requireConservation(result);
        held.requireCurrent();
        return result;
    }

    private static boolean safeShape(MessageRecord message) {
        final var runtime = message.runtimeIndex();
        return message.status() == MessageStatus.SCHEDULED
                && runtime.currentWorkKind() == CurrentSendWorkKind.TIMELINE
                && runtime.admissionsUsed() == 0
                && runtime.attemptObligations().isEmpty()
                && !runtime.possibleDestinationDuplicate()
                && runtime.claimId().length == 0
                && runtime.publishAttemptId().length == 0
                && message.retryEligibilityAtEpochMs() == message.deliverAtEpochMs();
    }

    private static TargetMessageRecord convert(
            DelayMessageId id,
            MessageRecord old,
            ScheduleBinding original,
            LaneMapping mapping,
            TargetScheduleBinding binding,
            TargetQuotaPayloadOwner owner) {
        final var source = SourcePositionCodec.decode(old.scheduleSourcePosition());
        if (!binding.messageId().equals(id)
                || binding.commandType() != original.commandType()
                || !Arrays.equals(binding.canonicalBody(), original.canonicalBody())
                || !binding.bindingSource().sameSourceIdentity(source)
                || binding.bindingSource().compareTo(source) > 0
                || !binding.target().equals(mapping.physicalTarget().id())
                || !binding.domain().equals(mapping.domain())
                || !Arrays.equals(binding.accountingIncarnation(), mapping.accountingIncarnation())
                || !Arrays.equals(binding.controlScopeRef(), mapping.controlScope())
                || !Arrays.equals(binding.offeredDispatchRef(), mapping.dispatchCompatibility())
                || !binding.intent().profile().equals(mapping.legacyProjection().destinationProfile())) {
            throw new IllegalArgumentException(
                    "migration conversion altered original Schedule or frozen execution mapping");
        }
        if (CanonicalLaneTuple.orderingProjection(mapping.canonicalLaneTuple()).orderingMode() != old.orderingMode()) {
            throw new IllegalArgumentException("original Lane ordering mode differs from Message and original body");
        }
        final var locator = new TargetMessageLocator(
                id,
                old.generation(),
                binding.target(),
                binding.domain(),
                binding.accountingIncarnation(),
                old.orderingMode(),
                old.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO ? requireOrderingDomain(mapping) : null,
                binding.digest());
        binding.requireLocator(locator);
        binding.requireMessageSource(source);
        // No old Profile-scoped Native signature is promoted into a common Target scope.
        final var work = new TargetTimelineWorkRef(
                locator,
                TimelineWorkKind.INITIAL_SCHEDULE,
                old.deliverAtEpochMs(),
                old.deliverAtEpochMs(),
                source.sourceOrderToken(),
                1,
                old.runtimeIndex().runtimeRevision(),
                UncertainRetryAuthority.NONE,
                null,
                null,
                false);
        final var runtime = new TargetGenerationRuntimeIndex(
                old.generation(),
                GenerationAggregateState.SCHEDULED,
                CurrentSendWorkKind.TIMELINE,
                work,
                null,
                null,
                List.of(),
                0,
                0,
                false,
                old.runtimeIndex().runtimeRevision());
        final var next = new TargetMessageRecord(
                locator,
                old.stateVersion(),
                old.deliverAtEpochMs(),
                old.expireAtEpochMs(),
                old.retryEligibilityAtEpochMs(),
                old.nativeDeliveryPolicy(),
                source,
                old.payloadReference() == null ? old.payload() : null,
                old.payloadReference(),
                runtime);
        owner.requireMessagePayload(next);
        return next;
    }

    private static void addConverted(
            Map<String, TargetRow> rows,
            TargetMessageRecord message,
            TargetScheduleBinding binding,
            TargetQuotaPayloadOwner owner,
            List<TargetRow> supporting) {
        put(
                rows,
                typed(ColumnFamily.ID, message.encodedKey(), TargetMessageRecord.VALUE_TYPE, message.canonicalBytes()));
        final var work = message.runtime().timeline();
        put(
                rows,
                typed(
                        ColumnFamily.TIMELINE,
                        work.ordinaryKey(),
                        TargetTimelineWorkRef.VALUE_TYPE,
                        work.canonicalBytes()));
        final var expiry = new TargetExpiryRef(message.locator(), message.expireAtEpochMs());
        put(
                rows,
                typed(ColumnFamily.TIMELINE, expiry.encodedKey(), TargetExpiryRef.VALUE_TYPE, expiry.canonicalBytes()));
        put(
                rows,
                typed(
                        ColumnFamily.ID,
                        binding.encodedKey(),
                        TargetScheduleBinding.VALUE_TYPE,
                        binding.canonicalBytes()));
        put(rows, typed(ColumnFamily.META, owner.key(), TargetQuotaPayloadOwner.VALUE_TYPE, owner.canonicalBytes()));
        for (var row : supporting) {
            put(rows, row);
        }
    }

    private static void addReservation(
            Map<String, TargetRow> rows,
            TargetReservationRecord reservation,
            TargetScheduleBinding binding,
            TargetQuotaPayloadOwner owner,
            List<TargetRow> supporting) {
        reservation.requireBinding(binding);
        reservation.requireOwner(owner);
        final byte[] payload = reservation.canonicalBytes();
        put(rows, typed(ColumnFamily.ID, reservation.key(), TargetReservationRecord.VALUE_TYPE, payload));
        put(rows, typed(ColumnFamily.ID, reservation.lookupKey(), TargetReservationRecord.VALUE_TYPE, payload));
        if (reservation.status() == PayloadReservationStatus.RESERVED) {
            put(
                    rows,
                    typed(ColumnFamily.ID, reservation.targetIndexKey(), TargetReservationRecord.VALUE_TYPE, payload));
            put(
                    rows,
                    typed(ColumnFamily.TIMELINE, reservation.expiryKey(), TargetReservationRecord.VALUE_TYPE, payload));
        }
        put(
                rows,
                typed(
                        ColumnFamily.ID,
                        binding.encodedKey(),
                        TargetScheduleBinding.VALUE_TYPE,
                        binding.canonicalBytes()));
        put(rows, typed(ColumnFamily.META, owner.key(), TargetQuotaPayloadOwner.VALUE_TYPE, owner.canonicalBytes()));
        for (var row : supporting) {
            put(rows, row);
        }
    }

    private static void buildQueues(
            List<LaneMapping> mappings, List<TargetMessageRecord> converted, Map<String, TargetRow> rows) {
        buildQueues(mappings, converted, rows, Map.of());
    }

    private static void buildQueues(
            List<LaneMapping> mappings,
            List<TargetMessageRecord> converted,
            Map<String, TargetRow> rows,
            Map<String, TargetRow> previous) {
        final var byTarget = new TreeMap<String, List<LaneMapping>>();
        for (var mapping : mappings) {
            byTarget.computeIfAbsent(Bytes.hex(mapping.physicalTarget().id().bytes()), ignored -> new ArrayList<>())
                    .add(mapping);
        }
        for (var group : byTarget.values()) {
            final var first = group.getFirst();
            final boolean funded = rows.values().stream().anyMatch(row -> {
                final var value = TargetValueEnvelope.decodeAny(row.value());
                return value.valueType() == TargetQuotaIncarnation.VALUE_TYPE
                        && Objects.equals(
                                TargetQuotaIncarnation.decode(value.payload())
                                        .identity()
                                        .target(),
                                first.physicalTarget().id());
            });
            if (!funded) {
                if (converted.stream().anyMatch(message -> message.locator()
                        .target()
                        .equals(first.physicalTarget().id()))) {
                    throw new IllegalStateException("converted Target has no actual original C allocated descriptor");
                }
                continue;
            }
            for (var mapping : group) {
                if (!Arrays.equals(first.accountingIncarnation(), mapping.accountingIncarnation())) {
                    throw new IllegalArgumentException("same Target cannot merge different accounting incarnations");
                }
            }
            final var slots = new TreeMap<Integer, LaneMapping>();
            for (var mapping : group) {
                slots.putIfAbsent(mapping.domain().slot(), mapping);
            }
            final List<TargetDomainState> domains = new ArrayList<>();
            for (var entry : slots.entrySet()) {
                if (entry.getKey() != domains.size()) {
                    throw new IllegalArgumentException("migration domain slots must retain contiguous history");
                }
                final var mapping = entry.getValue();
                TargetHeadRef head = null;
                for (var message : converted) {
                    if (message.locator().target().equals(first.physicalTarget().id())
                            && message.locator().domain().equals(mapping.domain())) {
                        final var work = message.runtime().timeline();
                        final TargetHeadRef candidate;
                        if (message.locator().orderingMode() == OrderingMode.DELIVERY_TIME_FIFO) {
                            final var orderRow = Objects.requireNonNull(
                                    rows.get(rowId(
                                            ColumnFamily.META,
                                            TargetKeyCodec.orderState(
                                                    message.locator().target(),
                                                    message.locator().orderingDomain()))),
                                    "complete imported order state");
                            candidate = TargetOrderState.decode(
                                            TargetValueEnvelope.decode(orderRow.value(), TargetOrderState.VALUE_TYPE)
                                                    .payload())
                                    .serviceableHead();
                        } else {
                            candidate = new TargetHeadRef(
                                    work.ordinaryKey(),
                                    message.locator().messageId(),
                                    message.locator().generation(),
                                    work.ordinaryEligibilityAtEpochMs());
                        }
                        if (candidate != null
                                && (head == null
                                        || candidate.timeEpochMs() < head.timeEpochMs()
                                        || candidate.timeEpochMs() == head.timeEpochMs()
                                                && Arrays.compareUnsigned(candidate.key(), head.key()) < 0)) {
                            head = candidate;
                        }
                    }
                }
                domains.add(new TargetDomainState(
                        mapping.domain(),
                        TargetDomainState.Lifecycle.ACTIVE,
                        mapping.dispatchCompatibility(),
                        mapping.controlScope(),
                        null,
                        head,
                        null));
            }
            final byte[] key = TargetKeyCodec.state(first.physicalTarget().id());
            final byte[] before = value(previous, ColumnFamily.META, key);
            final var prior = before == null
                    ? null
                    : TargetQueueState.decode(TargetValueEnvelope.decode(before, TargetQueueState.VALUE_TYPE)
                            .payload());
            if (prior != null
                    && (prior.controlVersion() != 1
                            || prior.admissionState() != TargetQueueState.AdmissionState.OPEN
                            || prior.nativeIndexLeadCapMs() != 0
                            || prior.domains().size() != domains.size())) {
                throw new IllegalStateException(
                        "actual Native queue controls/domains cannot be overwritten by old shadow state");
            }
            var queue = new TargetQueueState(
                    first.physicalTarget().id(),
                    prior == null ? 1 : prior.headRevision(),
                    1,
                    TargetQueueState.AdmissionState.OPEN,
                    first.accountingIncarnation(),
                    0,
                    domains);
            if (prior != null && !Arrays.equals(prior.canonicalBytes(), queue.canonicalBytes())) {
                queue = new TargetQueueState(
                        queue.targetId(),
                        TargetQueueState.nextRevision(prior.headRevision()),
                        queue.controlVersion(),
                        queue.admissionState(),
                        queue.accountingIncarnation(),
                        queue.nativeIndexLeadCapMs(),
                        queue.domains());
            }
            put(
                    rows,
                    typed(
                            ColumnFamily.META,
                            TargetKeyCodec.identity(first.physicalTarget().id()),
                            CanonicalTargetPartition.VALUE_TYPE,
                            first.physicalTarget().canonicalBytes()));
            put(rows, typed(ColumnFamily.META, key, TargetQueueState.VALUE_TYPE, queue.canonicalBytes()));
        }
    }

    private static Map<String, LaneMapping> mappingIndex(List<LaneMapping> mappings) {
        final Map<String, LaneMapping> byLane = new TreeMap<>();
        final Map<String, LaneMapping> domains = new TreeMap<>();
        final Map<String, LaneMapping> orders = new TreeMap<>();
        for (var mapping : mappings) {
            if (byLane.put(Bytes.hex(mapping.lane().bytes()), mapping) != null) {
                throw new IllegalArgumentException("duplicate migration Lane mapping");
            }
            if (mapping.orderingDomain() != null) {
                final var previous = orders.putIfAbsent(
                        Bytes.hex(mapping.physicalTarget().id().bytes()) + ":" + Bytes.hex(mapping.orderingDomain()),
                        mapping);
                if (previous != null && !previous.lane().equals(mapping.lane())) {
                    throw new IllegalArgumentException(
                            "distinct old strict Lanes cannot silently merge their ordering contract/barrier");
                }
            }
            final String key = Bytes.hex(mapping.physicalTarget().id().bytes()) + ":"
                    + mapping.domain().slot();
            final var prior = domains.putIfAbsent(key, mapping);
            if (prior != null
                    && (!prior.domain().equals(mapping.domain())
                            || !Arrays.equals(prior.credentialScope(), mapping.credentialScope())
                            || !Arrays.equals(prior.controlScope(), mapping.controlScope())
                            || !Arrays.equals(prior.dispatchCompatibility(), mapping.dispatchCompatibility())
                            || !Arrays.equals(prior.accountingIncarnation(), mapping.accountingIncarnation())
                            || !Arrays.equals(
                                    prior.legacyProjection().tenantRouteScopeDigest(),
                                    mapping.legacyProjection().tenantRouteScopeDigest()))) {
                throw new IllegalArgumentException(
                        "migration would merge incompatible credential/control/accounting execution domains");
            }
        }
        return byLane;
    }

    private record LaneControl(AdmissionGate gate, long controlVersion, long runtimeVersion) {}

    private static Map<String, LaneControl> laneControls(List<LegacyCheckpointStateInventory.RawEntry> entries) {
        final Map<String, LaneControl> result = new HashMap<>();
        for (var row : entries) {
            if (row.family() == ColumnFamily.META && kind(row.key(), 2)) {
                final var lane = new DestinationLaneId(Arrays.copyOfRange(row.key(), 2, row.key().length));
                final var envelope = LaneRecordEnvelope.decode(
                        ValueEnvelope.decodeAny(row.value()).payload());
                final LaneControl control;
                if (!envelope.isActive()) {
                    control = new LaneControl(
                            AdmissionGate.CLOSED, envelope.terminalGuard().laneControlVersion(), 0);
                } else if (envelope.typedActiveState().isPresent()) {
                    final var active = envelope.activeState();
                    if (!active.laneId().equals(lane)) {
                        throw new IllegalArgumentException("typed Lane control key/value differs");
                    }
                    control =
                            new LaneControl(active.admissionGate(), active.laneControlVersion(), active.laneVersion());
                } else {
                    final var active = LaneRecord.decode(envelope.activeStateBytes());
                    if (!active.laneId().equals(lane)) {
                        throw new IllegalArgumentException("legacy Lane control key/value differs");
                    }
                    control =
                            new LaneControl(active.admissionGate(), active.laneControlVersion(), active.laneVersion());
                }
                result.put(Bytes.hex(lane.bytes()), control);
            }
        }
        return result;
    }

    private static java.util.Set<String> unresolvedStrictLanes(List<LegacyCheckpointStateInventory.RawEntry> entries) {
        final var strict = new java.util.HashSet<String>();
        final var byMessage = new HashMap<String, String>();
        final var messages = new ArrayList<MessageRecord>();
        for (var row : entries) {
            if (row.family() == ColumnFamily.ID && kind(row.key(), 1)) {
                final byte[] payload = ValueEnvelope.decodeAny(row.value()).payload();
                if (RetiredMessageIdentityRecord.isEncoded(payload)) {
                    continue;
                }
                final var message = MessageRecord.decode(payload);
                if (message.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO) {
                    final String lane = Bytes.hex(message.laneId().bytes());
                    strict.add(lane);
                    byMessage.put(Bytes.hex(messageId(row.key(), 2).bytes()), lane);
                    messages.add(message);
                }
            } else if (row.family() == ColumnFamily.ID && kind(row.key(), 4)) {
                final var binding = ScheduleBinding.decode(
                        ValueEnvelope.decodeAny(row.value()).payload());
                if (binding.intent().orderingMode() == OrderingMode.DELIVERY_TIME_FIFO) {
                    final String lane = Bytes.hex(binding.laneId().bytes());
                    strict.add(lane);
                    byMessage.put(Bytes.hex(binding.delayMessageId().bytes()), lane);
                }
            }
        }
        final var unresolved = new java.util.HashSet<String>();
        for (var message : messages) {
            if (message.status() == MessageStatus.CLAIMED
                    || message.status() == MessageStatus.PUBLISHING
                    || message.status() == MessageStatus.UNCERTAIN
                    || message.status() == MessageStatus.HANDED_OFF
                    || !message.runtimeIndex().attemptObligations().isEmpty()) {
                unresolved.add(Bytes.hex(message.laneId().bytes()));
            }
        }
        for (var row : entries) {
            if (row.family() == ColumnFamily.INFLIGHT) {
                final var value = ValueEnvelope.decodeAny(row.value());
                final DestinationLaneId lane = value.valueType() == PublishAttemptLedger.VALUE_TYPE
                        ? PublishAttemptLedger.decode(value.payload()).laneId()
                        : value.valueType() == ClaimRecord.VALUE_TYPE
                                ? ClaimRecord.decode(value.payload()).laneId()
                                : null;
                if (lane != null && strict.contains(Bytes.hex(lane.bytes()))) {
                    unresolved.add(Bytes.hex(lane.bytes()));
                }
            } else if (row.family() == ColumnFamily.TERMINAL && kind(row.key(), 1)) {
                final var terminal = TerminalGenerationRecord.decode(
                        ValueEnvelope.decodeAny(row.value()).payload());
                if (!terminal.openObligations().isEmpty()) {
                    final String lane =
                            byMessage.get(Bytes.hex(terminal.messageId().bytes()));
                    if (lane != null) {
                        unresolved.add(lane);
                    }
                }
            }
        }
        return unresolved;
    }

    private static byte[] requireOrderingDomain(LaneMapping mapping) {
        if (mapping.orderingDomain() == null) {
            throw new IllegalArgumentException("strict mapping lacks frozen original ordering domain");
        }
        return mapping.orderingDomain();
    }

    private static void buildOrderStates(
            List<LaneMapping> mappings,
            List<TargetMessageRecord> messages,
            List<LegacyCheckpointStateInventory.RawEntry> legacy,
            Map<String, TargetRow> rows) {
        buildOrderStates(mappings, messages, legacy, rows, Map.of());
    }

    private static void buildOrderStates(
            List<LaneMapping> mappings,
            List<TargetMessageRecord> messages,
            List<LegacyCheckpointStateInventory.RawEntry> legacy,
            Map<String, TargetRow> rows,
            Map<String, TargetRow> previous) {
        final var controls = laneControls(legacy);
        for (var mapping : mappings) {
            if (mapping.orderingDomain() == null) {
                continue;
            }
            final var control = controls.get(Bytes.hex(mapping.lane().bytes()));
            final List<TargetMessageRecord> ordered = messages.stream()
                    .filter(message -> message.locator().orderingMode() == OrderingMode.DELIVERY_TIME_FIFO
                            && message.locator()
                                    .target()
                                    .equals(mapping.physicalTarget().id())
                            && Arrays.equals(message.locator().orderingDomain(), mapping.orderingDomain()))
                    .toList();
            final byte[] key =
                    TargetKeyCodec.orderState(mapping.physicalTarget().id(), mapping.orderingDomain());
            final byte[] before = value(previous, ColumnFamily.META, key);
            TargetReservationRecord reserved = null;
            for (var row : rows.values()) {
                final var value = TargetValueEnvelope.decodeAny(row.value());
                if (value.valueType() == TargetReservationRecord.VALUE_TYPE) {
                    final var candidate = TargetReservationRecord.decode(value.payload());
                    if (candidate
                                    .locator()
                                    .target()
                                    .equals(mapping.physicalTarget().id())
                            && Arrays.equals(candidate.locator().orderingDomain(), mapping.orderingDomain())) {
                        reserved = candidate;
                        break;
                    }
                }
            }
            if (ordered.isEmpty() && before == null && reserved == null) {
                continue;
            }
            if (control == null || control.gate() != AdmissionGate.OPEN) {
                throw new IllegalStateException("strict original control gate has no exact executable Target mapping");
            }
            TargetMessageRecord first = null;
            for (var message : ordered) {
                if (first == null
                        || Arrays.compareUnsigned(
                                        message.runtime().timeline().ordinaryKey(),
                                        first.runtime().timeline().ordinaryKey())
                                < 0) {
                    first = message;
                }
            }
            TargetHeadRef head = null;
            if (first != null) {
                final var work = first.runtime().timeline();
                final byte[] headKey = TargetKeyCodec.orderedHead(
                        mapping.physicalTarget().id(),
                        mapping.domain(),
                        work.ordinaryEligibilityAtEpochMs(),
                        mapping.orderingDomain());
                head = new TargetHeadRef(
                        headKey,
                        first.locator().messageId(),
                        first.locator().generation(),
                        work.ordinaryEligibilityAtEpochMs());
                put(
                        rows,
                        typed(ColumnFamily.TIMELINE, headKey, TargetTimelineWorkRef.VALUE_TYPE, work.canonicalBytes()));
            }
            final var prior = before == null
                    ? null
                    : TargetOrderState.decode(TargetValueEnvelope.decode(before, TargetOrderState.VALUE_TYPE)
                            .payload());
            if (prior != null
                    && (prior.barrier() != null
                            || prior.orderingContract()
                                    != TargetOrderState.OrderingContract.LEGACY_DELIVERY_TIME_FIFO)) {
                throw new IllegalStateException(
                        "new Native ordering activity cannot be overwritten by an old-domain projection");
            }
            var order = new TargetOrderState(
                    mapping.physicalTarget().id(),
                    mapping.orderingDomain(),
                    first != null
                            ? first.locator().messageId().routingId().shardId()
                            : prior != null
                                    ? prior.sourceShard()
                                    : Objects.requireNonNull(reserved)
                                            .locator()
                                            .messageId()
                                            .routingId()
                                            .shardId(),
                    mapping.domain(),
                    mapping.accountingIncarnation(),
                    TargetOrderState.OrderingContract.LEGACY_DELIVERY_TIME_FIFO,
                    prior == null ? 1 : prior.stateRevision(),
                    control.controlVersion(),
                    TargetOrderState.Gate.OPEN,
                    null,
                    head,
                    null);
            if (prior != null && !Arrays.equals(prior.canonicalBytes(), order.canonicalBytes())) {
                order = new TargetOrderState(
                        order.target(),
                        order.orderingDomain(),
                        order.sourceShard(),
                        order.executionDomain(),
                        order.accountingIncarnation(),
                        order.orderingContract(),
                        TargetQueueState.nextRevision(prior.stateRevision()),
                        order.controlVersion(),
                        order.gate(),
                        null,
                        head,
                        null);
            }
            put(rows, typed(ColumnFamily.META, key, TargetOrderState.VALUE_TYPE, order.canonicalBytes()));
        }
    }

    private static LaneMapping requireMapping(Map<String, LaneMapping> mappings, DestinationLaneId lane, byte[] tuple) {
        final var mapping = mappings.get(Bytes.hex(lane.bytes()));
        if (mapping == null || (tuple != null && !Arrays.equals(tuple, mapping.canonicalLaneTuple()))) {
            throw new IllegalArgumentException("old Lane is outside the frozen complete writer mapping");
        }
        return mapping;
    }

    private static boolean custodyDecisionReason(LegacyCheckpointStateInventory.ConflictReason reason) {
        return switch (reason) {
            case LEGACY_NAMESPACE_UNAUDITED,
                    LEGACY_CONTROL_METADATA_UNAUDITED,
                    CLAIM_REQUIRES_SOURCE_CUT_RECONCILIATION,
                    SCHEDULED_REQUIRES_OLD_SEND_RECOVERY,
                    UNRESOLVED_SEND_REQUIRES_OLD_RECOVERY,
                    HANDOFF_REQUIRES_OLD_BROKER_RESPONSIBILITY,
                    TERMINAL_REQUIRES_FLOOR_AND_REFERENCE_CLOSURE -> true;
            default -> false;
        };
    }

    private static void requireOriginalCommand(
            Conversion conversion, ScheduleBinding binding, List<ExternalRecord> protectedRecords) {
        final var original = conversion.originalCommand();
        final var command = CommandCodec.decodeFrame(original.originalBytes());
        if (!command.delayMessageId().equals(binding.delayMessageId())
                || command.type() != binding.commandType()
                || !Arrays.equals(command.canonicalBody(), binding.canonicalBody())
                || protectedRecords.stream()
                        .noneMatch(record -> record.kind().equals("SOURCE")
                                && Arrays.equals(
                                        record.source().canonicalBytes(),
                                        original.source().canonicalBytes())
                                && Arrays.equals(record.originalBytes(), original.originalBytes()))) {
            throw new IllegalArgumentException(
                    "conversion original Command/frame/hash/Source is outside complete protected custody");
        }
        for (var profile : List.of(conversion.destination(), conversion.capability())) {
            if (protectedRecords.stream()
                    .noneMatch(record -> record.kind().equals("PROFILE")
                            && Arrays.equals(record.originalBytes(), profile.canonicalBytes()))) {
                throw new IllegalArgumentException(
                        "conversion Profile version/body is outside original protected custody");
            }
        }
    }

    private static void requireReceipt(
            PayloadReservation original,
            ScheduleBinding binding,
            ReservationConversion conversion,
            List<ExternalRecord> protectedRecords) {
        final var receipt =
                PayloadReservationReceipt.decodeFrame(conversion.receipt().originalBytes());
        final var command = CommandCodec.decodeFrame(
                conversion.conversion().originalCommand().originalBytes());
        final var body = PrepareLargeScheduleBody.decode(binding.canonicalBody());
        if (!command.commandId().equals(original.commandId())
                || !Arrays.equals(command.commandHash(), original.commandHash())
                || !Arrays.equals(receipt.reservationId(), original.reservationId())
                || !receipt.delayMessageId().equals(original.delayMessageId())
                || !receipt.shardId().equals(original.shardId())
                || !Arrays.equals(
                        receipt.appliedSourcePosition().canonicalBytes(), original.receiptAnchorSourcePosition())
                || !Arrays.equals(
                        conversion.conversion().originalCommand().source().canonicalBytes(),
                        original.receiptAnchorSourcePosition())
                || receipt.stateVersion() != original.receiptAnchorStateVersion()
                || receipt.expectedLength() != original.intent().expectedPayloadLength()
                || !Arrays.equals(receipt.payloadSha256(), original.intent().payloadSha256())
                || receipt.reservationExpiryEpochMs() != original.reservationExpiryEpochMs()
                || !receipt.objectStoreProfile().equals(body.objectStoreProfile())
                || !receipt.trustSet().equals(body.trustSet())
                || protectedRecords.stream()
                        .noneMatch(record -> record.kind().equals("RECEIPT")
                                && Arrays.equals(
                                        record.canonicalBytes(),
                                        conversion.receipt().canonicalBytes()))) {
            throw new IllegalArgumentException(
                    "reservation conversion changed original Prepare/receipt/object authority");
        }
    }

    private static void requireReservationIndex(
            List<LegacyCheckpointStateInventory.RawEntry> rows, PayloadReservation reservation, byte[] primary) {
        final byte[] expected =
                KeyCodec.reservationExpiry(reservation.reservationExpiryEpochMs(), reservation.reservationId());
        for (var row : rows) {
            if (row.family() == ColumnFamily.TIMELINE && Arrays.equals(row.key(), expected)) {
                if (!Arrays.equals(
                        ValueEnvelope.decode(row.value(), 5).payload(),
                        ValueEnvelope.decode(primary, 2).payload())) {
                    throw new IllegalArgumentException("old reservation expiry and primary projections differ");
                }
                return;
            }
        }
        throw new IllegalArgumentException("old RESERVED projection has no exact expiry index");
    }

    private static PayloadReservation originalReservation(
            List<LegacyCheckpointStateInventory.RawEntry> rows, byte[] reservationId) {
        for (var row : rows) {
            if (row.family() == ColumnFamily.ID && Arrays.equals(row.key(), KeyCodec.idReservation(reservationId))) {
                return PayloadReservation.decode(
                        ValueEnvelope.decode(row.value(), 2).payload());
            }
        }
        throw new IllegalArgumentException("Message lost its complete original committed reservation");
    }

    private static void requireReferences(TargetScheduleBinding binding, LaneMapping mapping, Conversion conversion) {
        final var original = mapping.legacyProjection();
        if (!original.destinationProfile().equals(conversion.destination().ref())
                || !original.capabilityProfile().equals(conversion.capability().ref())) {
            throw new IllegalArgumentException("conversion altered frozen original ProfileRef/version");
        }
        final var required = TargetDispatchCompatibility.fromProfiles(
                mapping.physicalTarget(), conversion.destination(), conversion.capability());
        TargetDispatchCompatibility offered = null;
        TargetControlScope controls = null;
        boolean requiredPresent = false;
        for (var row : conversion.supportingRows()) {
            final var value = TargetValueEnvelope.decodeAny(row.value());
            if (value.valueType() == TargetDispatchCompatibility.VALUE_TYPE) {
                final var dispatch = TargetDispatchCompatibility.decode(value.payload());
                if (row.family() != ColumnFamily.META
                        || !Arrays.equals(row.key(), TargetKeyCodec.dispatchCompatibility(dispatch.digest()))) {
                    throw new IllegalArgumentException("frozen dispatch support changed its exact native key");
                }
                if (dispatch.equals(required)) {
                    requiredPresent = true;
                }
                if (Arrays.equals(dispatch.digest(), mapping.dispatchCompatibility())) {
                    offered = dispatch;
                }
            } else if (value.valueType() == TargetControlScope.VALUE_TYPE) {
                final var scope = TargetControlScope.decode(value.payload());
                if (row.family() != ColumnFamily.META
                        || !Arrays.equals(row.key(), TargetKeyCodec.controlScope(scope.digest()))) {
                    throw new IllegalArgumentException("frozen control support changed its exact native key");
                }
                if (Arrays.equals(scope.digest(), mapping.controlScope())) {
                    controls = scope;
                }
            }
        }
        if (!requiredPresent || !Arrays.equals(required.authorizationScope(), mapping.credentialScope())) {
            throw new IllegalArgumentException("conversion changed frozen credential authorization scope");
        }
        binding.requireReferences(
                mapping.physicalTarget(),
                conversion.destination(),
                conversion.capability(),
                required,
                Objects.requireNonNull(offered, "complete frozen offered dispatch"),
                Objects.requireNonNull(controls, "complete frozen control scope"));
    }

    private static void audit(Runnable action, EnumSet<Audit> blockers, Audit completed) {
        try {
            action.run();
            blockers.remove(completed);
        } catch (AuditUnavailable unavailable) {
            /* explicit remaining blocker */
        }
    }

    private static TargetRow typed(ColumnFamily family, byte[] key, int type, byte[] payload) {
        return new TargetRow(family, key, TargetValueEnvelope.encode(type, payload));
    }

    private static void put(Map<String, TargetRow> rows, TargetRow row) {
        final String key = row.family().ordinal() + ":" + Bytes.hex(row.key());
        final var prior = rows.putIfAbsent(key, row);
        if (prior != null && !Arrays.equals(prior.value(), row.value())) {
            throw new IllegalArgumentException("conflicting imported Target rows");
        }
    }

    private static boolean kind(byte[] key, int tag) {
        return key.length >= 2 && Byte.toUnsignedInt(key[0]) == tag && key[1] == 1;
    }

    private static DelayMessageId messageId(byte[] key, int offset) {
        if (key.length < offset + DelayMessageId.LENGTH) {
            throw new IllegalArgumentException("truncated legacy message key");
        }
        return new DelayMessageId(Arrays.copyOfRange(key, offset, offset + DelayMessageId.LENGTH));
    }

    private static byte[] fixed(byte[] bytes, int length, String name) {
        Bytes.requireLength(bytes, length, name);
        return Bytes.copy(bytes);
    }

    private static byte[] copyBounded(byte[] bytes, int maximum, String name) {
        if (bytes == null || bytes.length == 0 || bytes.length > maximum) {
            throw new IllegalArgumentException(name + " exceeds bound");
        }
        return Bytes.copy(bytes);
    }
}
