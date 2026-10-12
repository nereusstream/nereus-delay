package com.nereusstream.delay.assessment;

import com.nereusstream.delay.ownership.SourceRecordConsumer;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.store.CheckpointManifest;
import com.nereusstream.delay.store.CheckpointManifestLimits;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.LegacyCheckpointStateInventory;
import com.nereusstream.delay.store.StoreMetadata;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Read-only, cut-bound migration diagnosis. It creates no destination Store or activation authority. */
public final class LegacyCheckpointMigrationDryRun {
    // Registered legacy META fixed-kind 2 holds StoreMetadata; ShardStore keeps its constants package-private.
    private static final int LEGACY_STORE_IDENTITY_METADATA_KIND = 2;

    private LegacyCheckpointMigrationDryRun() {}

    /**
     * The caller retains the authenticated legacy Manifest and protected Source cut. A replayed copy must first
     * be checkpointed at its resulting cut and authenticated; the original Manifest cannot certify modified bytes.
     * Missing mapping/conservation proofs remain explicit blockers even when the bounded inventory has no errors.
     */
    public static Report inspect(
            final Path image,
            final CheckpointManifest authenticatedManifest,
            final CheckpointManifestLimits physicalLimits,
            final LegacyCheckpointStateInventory.ReadLimits readLimits,
            final SourceRecordConsumer.CheckpointCut sourceCut,
            final StoreMetadata proposedTargetStore) {
        Objects.requireNonNull(image, "image");
        Objects.requireNonNull(authenticatedManifest, "authenticatedManifest");
        Objects.requireNonNull(physicalLimits, "physicalLimits");
        Objects.requireNonNull(readLimits, "readLimits");
        Objects.requireNonNull(sourceCut, "sourceCut");
        Objects.requireNonNull(proposedTargetStore, "proposedTargetStore");
        if (proposedTargetStore.storeFormatVersion() != 2
                || !proposedTargetStore.shardId().equals(authenticatedManifest.shardId())
                || proposedTargetStore.storeIncarnationUuid().equals(authenticatedManifest.sourceStoreIncarnation())
                || Arrays.equals(proposedTargetStore.dbIdentity(), authenticatedManifest.dbIdentity())) {
            throw new IllegalArgumentException("migration requires a distinct format-2 Store identity in the same Shard");
        }
        if (readLimits.maxRecords() == Integer.MAX_VALUE
                || readLimits.maxBytes() == Long.MAX_VALUE
                || readLimits.maxElapsedNanos() == Long.MAX_VALUE) {
            throw new IllegalArgumentException("migration dry-run requires finite record, byte and elapsed limits");
        }
        sourceCut.requireCurrent();
        final SourcePosition captured = Objects.requireNonNull(sourceCut.position(), "protected Source cut position");
        if (!captured.shardId().equals(authenticatedManifest.shardId())) {
            throw new IllegalArgumentException("migration Source cut belongs to another Shard");
        }
        final byte[] exactCut = captured.canonicalBytes();
        final var inventory = LegacyCheckpointStateInventory.inspect(
                image, authenticatedManifest.shardId(), authenticatedManifest, physicalLimits, readLimits);
        requireCurrentCut(sourceCut, exactCut);
        final var blockers = new ArrayList<BatchBlocker>();
        final SourcePosition applied = inventory.physicalProof().appliedSourcePosition();
        if (!captured.sameSourceIdentity(applied)) {
            blockers.add(BatchBlocker.SOURCE_IDENTITY_MISMATCH);
        } else if (!Arrays.equals(exactCut, applied.canonicalBytes())) {
            blockers.add(applied.compareTo(captured) < 0
                    ? BatchBlocker.SOURCE_TAIL_NOT_APPLIED
                    : BatchBlocker.APPLIED_SOURCE_DIFFERS_FROM_CAPTURED_CUT);
        }
        blockers.add(BatchBlocker.FENCED_MAINTENANCE_AUTHORITY_NOT_AUDITED);
        blockers.add(BatchBlocker.COMPLETE_STATE_MAPPING_NOT_AUDITED);
        blockers.add(BatchBlocker.EXTERNAL_LEGACY_OBLIGATIONS_NOT_AUDITED);
        blockers.add(BatchBlocker.CONSERVATION_NOT_AUDITED);
        final var report = new Report(
                inventory, authenticatedManifest.manifestSha256(), captured, proposedTargetStore, blockers);
        requireCurrentCut(sourceCut, exactCut);
        return report;
    }

    private static void requireCurrentCut(final SourceRecordConsumer.CheckpointCut cut, final byte[] expected) {
        cut.requireCurrent();
        if (!Arrays.equals(expected, Objects.requireNonNull(cut.position(), "Source cut position").canonicalBytes())) {
            throw new IllegalStateException("protected migration Source cut changed during read-only inspection");
        }
    }

    public enum BatchBlocker {
        SOURCE_IDENTITY_MISMATCH,
        SOURCE_TAIL_NOT_APPLIED,
        APPLIED_SOURCE_DIFFERS_FROM_CAPTURED_CUT,
        FENCED_MAINTENANCE_AUTHORITY_NOT_AUDITED,
        COMPLETE_STATE_MAPPING_NOT_AUDITED,
        EXTERNAL_LEGACY_OBLIGATIONS_NOT_AUDITED,
        CONSERVATION_NOT_AUDITED
    }

    /** Obtained only from the complete bounded inspection; a report is diagnostic data, not an execution permit. */
    public static final class Report {
        private final LegacyCheckpointStateInventory.Inventory inventory;
        private final byte[] manifestSha256;
        private final SourcePosition sourceCut;
        private final StoreMetadata proposedTargetStore;
        private final List<BatchBlocker> blockers;

        private Report(
                final LegacyCheckpointStateInventory.Inventory inventory,
                final byte[] manifestSha256,
                final SourcePosition sourceCut,
                final StoreMetadata proposedTargetStore,
                final List<BatchBlocker> blockers) {
            this.inventory = inventory;
            this.manifestSha256 = Bytes.copy(manifestSha256);
            this.sourceCut = sourceCut;
            this.proposedTargetStore = proposedTargetStore;
            this.blockers = blockers.stream().sorted().toList();
        }

        public LegacyCheckpointStateInventory.Inventory inventory() {
            return inventory;
        }

        public List<BatchBlocker> blockers() {
            return blockers;
        }

        /** READY remains unavailable until actual mapping and independent conservation are implemented. */
        public String summary() {
            return "BLOCKED";
        }

        /** Stable diagnostic JSON; binary Source positions are their complete canonical bytes in hexadecimal. */
        public String canonicalJson(final int maxReportBytes) {
            if (maxReportBytes <= 0 || maxReportBytes == Integer.MAX_VALUE) {
                throw new IllegalArgumentException("migration report requires a finite positive output byte bound");
            }
            final var proof = inventory.physicalProof();
            long maximumBytes = 4096L
                    + 2L * proof.appliedSourcePosition().canonicalBytes().length
                    + 2L * sourceCut.canonicalBytes().length;
            for (var conflict : inventory.conflicts()) {
                maximumBytes = Math.addExact(maximumBytes, 512L + 2L * conflict.sourcePosition().length);
            }
            maximumBytes = Math.addExact(maximumBytes,
                    blockers.size() * (512L + 2L * proof.appliedSourcePosition().canonicalBytes().length));
            // All emitted strings are generated ASCII names, numeric text, UUIDs or hex. Bound before hex copies
            // and the sorted output list; a large legacy Source identity must not be repeated without accounting.
            if (maximumBytes > maxReportBytes) {
                throw new IllegalStateException("migration report exceeds its configured output byte bound");
            }
            final StringBuilder json = new StringBuilder(4096);
            json.append("{\"appliedSourcePosition\":").append(hex(proof.appliedSourcePosition().canonicalBytes()));
            json.append(",\"chargedReadBytes\":")
                    .append(AssessmentCanonical.quote(Long.toString(inventory.chargedBytes())));
            json.append(",\"checkpointId\":").append(hex(proof.checkpointId()));
            json.append(",\"conflicts\":[");
            final List<LegacyCheckpointStateInventory.Conflict> conflicts = inventory.conflicts().stream()
                    .sorted(Comparator.comparing(
                                    (LegacyCheckpointStateInventory.Conflict c) -> Bytes.hex(c.oldKeyDigest()))
                            .thenComparing(LegacyCheckpointStateInventory.Conflict::recordKind)
                            .thenComparing(c -> c.reason().name())
                            .thenComparing(c -> Bytes.hex(c.sourcePosition())))
                    .toList();
            boolean separator = false;
            for (var conflict : conflicts) {
                if (separator) {
                    json.append(',');
                }
                json.append("{\"oldKeyDigest\":").append(hex(conflict.oldKeyDigest()));
                json.append(",\"reason\":").append(AssessmentCanonical.quote(conflict.reason().name()));
                json.append(",\"recordKind\":").append(AssessmentCanonical.quote(conflict.recordKind()));
                json.append(",\"sourcePosition\":").append(hex(conflict.sourcePosition())).append('}');
                separator = true;
            }
            final byte[] metadataDigest = Bytes.sha256(KeyCodec.metaFixed(LEGACY_STORE_IDENTITY_METADATA_KIND));
            for (var blocker : blockers) {
                if (separator) {
                    json.append(',');
                }
                json.append("{\"oldKeyDigest\":").append(hex(metadataDigest));
                json.append(",\"reason\":").append(AssessmentCanonical.quote(blocker.name()));
                json.append(",\"recordKind\":\"STORE\",\"sourcePosition\":")
                        .append(hex(proof.appliedSourcePosition().canonicalBytes())).append('}');
                separator = true;
            }
            json.append("],\"manifestSha256\":").append(hex(manifestSha256));
            json.append(",\"messageDispositions\":").append(counts(inventory.messageDispositions()));
            json.append(",\"messageStatuses\":").append(counts(inventory.messageStatuses()));
            json.append(",\"newDbIdentity\":").append(hex(proposedTargetStore.dbIdentity()));
            json.append(",\"newStoreFormat\":2,\"newStoreIncarnation\":")
                    .append(AssessmentCanonical.quote(proposedTargetStore.storeIncarnationUuid().toString()));
            json.append(",\"oldDbIdentity\":").append(hex(proof.metadata().dbIdentity()));
            json.append(",\"oldStoreFormat\":1,\"oldStoreIncarnation\":")
                    .append(AssessmentCanonical.quote(proof.metadata().storeIncarnationUuid().toString()));
            json.append(",\"physicalBytes\":")
                    .append(AssessmentCanonical.quote(Long.toString(proof.physicalBytes())));
            json.append(",\"recordsByFamily\":").append(counts(inventory.recordsByFamily()));
            json.append(",\"retiredMessageIdentities\":")
                    .append(AssessmentCanonical.quote(Long.toString(inventory.retiredMessageIdentities())));
            json.append(",\"scannedBytes\":")
                    .append(AssessmentCanonical.quote(Long.toString(inventory.scannedBytes())));
            json.append(",\"scannedRecords\":")
                    .append(AssessmentCanonical.quote(Long.toString(inventory.scannedRecords())));
            json.append(",\"schemaVersion\":1,\"shard\":{\"partition\":")
                    .append(proof.metadata().shardId().partition());
            json.append(",\"routeIncarnation\":").append(AssessmentCanonical.quote(
                    proof.metadata().shardId().routeIncarnation().uuid().toString())).append('}');
            json.append(",\"shardMutationSequence\":")
                    .append(AssessmentCanonical.quote(Long.toUnsignedString(proof.mutationSequence())));
            json.append(",\"sourceCut\":").append(hex(sourceCut.canonicalBytes()));
            json.append(",\"summary\":\"BLOCKED\"}");
            if (json.length() > maxReportBytes) {
                throw new IllegalStateException("migration report exceeds its configured output byte bound");
            }
            return json.toString();
        }

        private static String hex(final byte[] value) {
            return AssessmentCanonical.quote(Bytes.hex(value));
        }

        private static String counts(final Map<? extends Enum<?>, Long> values) {
            final StringBuilder json = new StringBuilder("{");
            final var entries = values.entrySet().stream()
                    .sorted(Comparator.comparing(entry -> entry.getKey().name())).toList();
            for (int index = 0; index < entries.size(); index++) {
                if (index != 0) {
                    json.append(',');
                }
                final var entry = entries.get(index);
                json.append(AssessmentCanonical.quote(entry.getKey().name())).append(':')
                        .append(AssessmentCanonical.quote(Long.toString(entry.getValue())));
            }
            return json.append('}').toString();
        }
    }
}
