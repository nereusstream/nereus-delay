package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.EvidenceKindV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Closed V1 checkpoint manifest projection. The object-store publication
 * fields are supplied by the adapter; this class only emits the canonical JCS
 * JSON and never treats a local draft as catalog-visible.
 */
public record CheckpointManifest(
        byte[] checkpointId,
        byte[] recoveryLineageId,
        long lineageGeneration,
        ParentCheckpoint parentCheckpoint,
        byte[] restoredFromCheckpointId,
        CreatedBy createdBy,
        CreatedAt createdAt,
        ShardId shardId,
        byte[] dbIdentity,
        UUID sourceStoreIncarnation,
        int storeFormatVersion,
        long shardMutationSequence,
        SourcePosition appliedShardLogPosition,
        byte[] controlStateDigest,
        byte[] referencedSemanticVersionsDigest,
        List<EvidenceCursorV1> evidenceCursors,
        List<FileEntry> files) {
    private static final int ID_LENGTH = 16;

    public CheckpointManifest {
        requireNonZeroLength(checkpointId, ID_LENGTH, "checkpointId");
        requireNonZeroLength(recoveryLineageId, ID_LENGTH, "recoveryLineageId");
        if (lineageGeneration < 0 || shardMutationSequence < 0) {
            throw new IllegalArgumentException("manifest counters must be non-negative");
        }
        if (restoredFromCheckpointId != null) {
            requireNonZeroLength(restoredFromCheckpointId, ID_LENGTH, "restoredFromCheckpointId");
        }
        Objects.requireNonNull(createdBy, "createdBy");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(shardId, "shardId");
        requireNonZero(dbIdentity, "dbIdentity");
        Objects.requireNonNull(sourceStoreIncarnation, "sourceStoreIncarnation");
        if (storeFormatVersion != 1) {
            throw new IllegalArgumentException("unsupported store format version");
        }
        Objects.requireNonNull(appliedShardLogPosition, "appliedShardLogPosition");
        if (!shardId.equals(appliedShardLogPosition.shardId())) {
            throw new IllegalArgumentException("source position does not belong to manifest shard");
        }
        requireLength(controlStateDigest, 32, "controlStateDigest");
        requireLength(referencedSemanticVersionsDigest, 32, "referencedSemanticVersionsDigest");
        if (evidenceCursors == null) {
            throw new IllegalArgumentException("evidenceCursors must not be null");
        }
        evidenceCursors = evidenceCursors.stream().sorted().toList();
        for (int index = 1; index < evidenceCursors.size(); index++) {
            if (evidenceCursors.get(index - 1).compareTo(evidenceCursors.get(index)) == 0) {
                throw new IllegalArgumentException("duplicate evidence cursor identity");
            }
        }
        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("checkpoint manifest requires at least one file");
        }
        files = files.stream().sorted((left, right) -> CheckpointFileInventory.compareCanonicalNames(
                left.name(), right.name())).toList();
        for (int index = 1; index < files.size(); index++) {
            if (files.get(index - 1).name().equals(files.get(index).name())) {
                throw new IllegalArgumentException("duplicate checkpoint file name");
            }
        }
        checkpointId = Bytes.copy(checkpointId);
        recoveryLineageId = Bytes.copy(recoveryLineageId);
        restoredFromCheckpointId = copyNullable(restoredFromCheckpointId);
        dbIdentity = Bytes.copy(dbIdentity);
        controlStateDigest = Bytes.copy(controlStateDigest);
        referencedSemanticVersionsDigest = Bytes.copy(referencedSemanticVersionsDigest);
        evidenceCursors = List.copyOf(evidenceCursors);
        files = List.copyOf(files);
    }

    /** Backwards-compatible empty-cursor constructor for local callers. */
    public CheckpointManifest(
            byte[] checkpointId,
            byte[] recoveryLineageId,
            long lineageGeneration,
            ParentCheckpoint parentCheckpoint,
            byte[] restoredFromCheckpointId,
            CreatedBy createdBy,
            CreatedAt createdAt,
            ShardId shardId,
            byte[] dbIdentity,
            UUID sourceStoreIncarnation,
            int storeFormatVersion,
            long shardMutationSequence,
            SourcePosition appliedShardLogPosition,
            byte[] controlStateDigest,
            byte[] referencedSemanticVersionsDigest,
            List<FileEntry> files) {
        this(checkpointId, recoveryLineageId, lineageGeneration, parentCheckpoint, restoredFromCheckpointId,
                createdBy, createdAt, shardId, dbIdentity, sourceStoreIncarnation, storeFormatVersion,
                shardMutationSequence, appliedShardLogPosition, controlStateDigest,
                referencedSemanticVersionsDigest, List.of(), files);
    }

    @Override
    public byte[] checkpointId() {
        return Bytes.copy(checkpointId);
    }

    @Override
    public byte[] recoveryLineageId() {
        return Bytes.copy(recoveryLineageId);
    }

    @Override
    public byte[] restoredFromCheckpointId() {
        return copyNullable(restoredFromCheckpointId);
    }

    @Override
    public byte[] dbIdentity() {
        return Bytes.copy(dbIdentity);
    }

    @Override
    public byte[] controlStateDigest() {
        return Bytes.copy(controlStateDigest);
    }

    @Override
    public byte[] referencedSemanticVersionsDigest() {
        return Bytes.copy(referencedSemanticVersionsDigest);
    }

    /** Returns the exact UTF-8 bytes to upload as the final manifest object. */
    public byte[] canonicalJsonBytes() {
        return canonicalJson().getBytes(StandardCharsets.UTF_8);
    }

    public String canonicalJson() {
        final StringBuilder json = new StringBuilder(4096);
        json.append('{');
        field(json, "appliedShardLogPosition", sourcePositionJson(appliedShardLogPosition));
        field(json, "checkpointId", quote(b64(checkpointId)));
        field(json, "controlStateDigest", quote(hex(controlStateDigest)));
        field(json, "createdAt", createdAt.toJson());
        field(json, "createdBy", createdBy.toJson());
        field(json, "dbIdentity", quote(b64(dbIdentity)));
        json.append(",\"evidenceCursors\":[");
        for (int index = 0; index < evidenceCursors.size(); index++) {
            if (index != 0) {
                json.append(',');
            }
            json.append(evidenceCursorJson(evidenceCursors.get(index)));
        }
        json.append(']');
        json.append(",\"files\":[");
        for (int index = 0; index < files.size(); index++) {
            if (index != 0) {
                json.append(',');
            }
            json.append(files.get(index).toJson());
        }
        json.append(']');
        field(json, "lineageGeneration", quote(u64(lineageGeneration)));
        field(json, "manifestVersion", "1");
        field(json, "parentCheckpoint", parentCheckpoint == null ? "null" : parentCheckpoint.toJson());
        field(json, "recoveryLineageId", quote(b64(recoveryLineageId)));
        field(json, "referencedSemanticVersionsDigest", quote(hex(referencedSemanticVersionsDigest)));
        field(json, "restoredFromCheckpointId", restoredFromCheckpointId == null
                ? "null" : quote(b64(restoredFromCheckpointId)));
        field(json, "shardId", shardIdJson());
        field(json, "shardMutationSequence", quote(u64(shardMutationSequence)));
        field(json, "sourceStoreIncarnation", quote(sourceStoreIncarnation.toString()));
        field(json, "storeFormatVersion", "1");
        json.append('}');
        return json.toString();
    }

    public byte[] manifestSha256() {
        return Bytes.sha256(canonicalJsonBytes());
    }

    /** Validates this immutable projection against an activated manifest limit set. */
    public void validateLimits(final CheckpointManifestLimits limits) {
        Objects.requireNonNull(limits, "limits").validateManifest(this);
    }

    /**
     * Decodes only the exact V1 canonical JSON projection emitted by this
     * class.  A downloaded manifest is recovery authority only after this
     * byte-for-byte canonicality check succeeds.
     */
    public static CheckpointManifest decodeCanonicalJson(final byte[] encoded) {
        return decodeCanonicalJson(encoded, CheckpointManifestLimits.unbounded());
    }

    /** Decodes canonical JSON only when it fits the activated manifest limits. */
    public static CheckpointManifest decodeCanonicalJson(final byte[] encoded,
                                                         final CheckpointManifestLimits limits) {
        return CheckpointManifestJson.decode(encoded, Objects.requireNonNull(limits, "limits"));
    }

    private String shardIdJson() {
        return "{\"partition\":" + shardId.partition() + ",\"routeIncarnation\":"
                + quote(shardId.routeIncarnation().uuid().toString()) + "}";
    }

    private static String sourcePositionJson(final SourcePosition position) {
        if (position instanceof KafkaSourcePosition kafka) {
            return "{\"brokerLogAppendTime\":" + quote(u64(kafka.brokerLogAppendTimeEpochMs()))
                    + ",\"clusterId\":" + quote(b64(kafka.authenticatedClusterId().getBytes(StandardCharsets.UTF_8)))
                    + ",\"kind\":\"KAFKA\",\"leaderEpoch\":"
                    + (kafka.leaderEpoch() == null ? "null" : Integer.toString(kafka.leaderEpoch()))
                    + ",\"offset\":" + quote(u64Bits(kafka.offset())) + ",\"partition\":"
                    + kafka.shardId().partition() + ",\"routeIncarnation\":"
                    + quote(kafka.shardId().routeIncarnation().uuid().toString()) + ",\"topicUuid\":"
                    + quote(kafka.nativeTopicUuid().toString()) + "}";
        }
        final PulsarSourcePosition pulsar = (PulsarSourcePosition) position;
        return "{\"batchIndex\":" + pulsar.normalizedBatchIndex() + ",\"batchSize\":" + pulsar.batchSize()
                + ",\"brokerEntryTimestamp\":" + quote(u64(pulsar.brokerEntryTimestampEpochMs()))
                + ",\"entryId\":" + quote(u64Bits(pulsar.entryId())) + ",\"entryKind\":\""
                + pulsar.entryKind().name() + "\",\"kind\":\"PULSAR\",\"ledgerId\":"
                + quote(u64Bits(pulsar.ledgerId())) + ",\"partition\":" + pulsar.shardId().partition()
                + ",\"physicalTopic\":" + quote(pulsar.physicalTopic()) + ",\"resourceIncarnation\":"
                + quote(b64(pulsar.brokerResourceIncarnation())) + ",\"routeIncarnation\":"
                + quote(pulsar.shardId().routeIncarnation().uuid().toString()) + "}";
    }

    private static String evidenceCursorJson(final EvidenceCursorV1 cursor) {
        final StringBuilder json = new StringBuilder(512).append('{');
        if (cursor.evidenceKind() == EvidenceKindV1.KAFKA_RECEIPT_CONTIGUOUS) {
            field(json, "destinationLaneId", quote(b64(cursor.destinationLaneId())));
            field(json, "evidenceGeneration", quote(u64(cursor.evidenceGeneration())));
            field(json, "evidenceKind", quote(cursor.evidenceKind().name()));
            field(json, "evidenceResourceIncarnation", quote(b64(cursor.evidenceResourceIncarnation())));
            field(json, "laneIncarnation", quote(b64(cursor.laneIncarnation())));
            field(json, "lastObservedLsoExclusive", quote(u64Bits(cursor.lastObservedLsoExclusive())));
            field(json, "maxBrokerPersistedAtThroughCursor",
                    quote(u64(cursor.maxBrokerPersistedAtThroughCursor())));
            field(json, "nextOffsetExclusive", quote(u64Bits(cursor.nextOffsetExclusive())));
            field(json, "physicalPartition", Integer.toString(cursor.physicalPartition()));
            field(json, "topicUuid", quote(uuidText(cursor.topicUuid())));
        } else {
            field(json, "batchIndex", Integer.toString(cursor.normalizedBatchIndex()));
            field(json, "batchSize", Integer.toString(cursor.batchSize()));
            field(json, "destinationLaneId", quote(b64(cursor.destinationLaneId())));
            field(json, "entryId", quote(u64Bits(cursor.entryId())));
            field(json, "evidenceGeneration", quote(u64(cursor.evidenceGeneration())));
            field(json, "evidenceKind", quote(cursor.evidenceKind().name()));
            field(json, "evidenceResourceIncarnation", quote(b64(cursor.evidenceResourceIncarnation())));
            field(json, "laneIncarnation", quote(b64(cursor.laneIncarnation())));
            field(json, "ledgerId", quote(u64Bits(cursor.ledgerId())));
            field(json, "maxBrokerPersistedAtThroughCursor",
                    quote(u64(cursor.maxBrokerPersistedAtThroughCursor())));
            field(json, "physicalPartition", Integer.toString(cursor.physicalPartition()));
            field(json, "physicalTopic", quote(cursor.physicalTopic()));
            field(json, "physicalTopicCreationTimestamp", quote(u64(cursor.physicalTopicCreationTimestamp())));
            field(json, "resourceToken", quote(b64(cursor.resourceToken())));
        }
        return json.append('}').toString();
    }

    private static String uuidText(final byte[] value) {
        if (value == null || value.length != 16) {
            throw new IllegalArgumentException("UUID bytes must be 16 bytes");
        }
        final ByteBuffer buffer = ByteBuffer.wrap(value);
        return new UUID(buffer.getLong(), buffer.getLong()).toString();
    }

    private static void field(final StringBuilder json, final String name, final String value) {
        if (json.length() > 1) {
            json.append(',');
        }
        json.append(quote(name)).append(':').append(value);
    }

    private static String u64(final long value) {
        if (value < 0) {
            throw new IllegalArgumentException("unsigned manifest value is negative");
        }
        return Long.toUnsignedString(value);
    }

    private static String u64Bits(final long value) {
        return Long.toUnsignedString(value);
    }

    private static String b64(final byte[] value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value);
    }

    private static String hex(final byte[] value) {
        return Bytes.hex(value);
    }

    private static String quote(final String value) {
        final StringBuilder result = new StringBuilder(value.length() + 2).append('"');
        for (int index = 0; index < value.length(); index++) {
            final char character = value.charAt(index);
            switch (character) {
                case '"' -> result.append("\\\"");
                case '\\' -> result.append("\\\\");
                case '\b' -> result.append("\\b");
                case '\f' -> result.append("\\f");
                case '\n' -> result.append("\\n");
                case '\r' -> result.append("\\r");
                case '\t' -> result.append("\\t");
                default -> {
                    if (character < 0x20) {
                        result.append(String.format("\\u%04x", (int) character));
                    } else {
                        result.append(character);
                    }
                }
            }
        }
        return result.append('"').toString();
    }

    private static void requireLength(final byte[] value, final int length, final String name) {
        if (value == null || value.length != length) {
            throw new IllegalArgumentException(name + " must be " + length + " bytes");
        }
    }

    private static void requireNonZeroLength(final byte[] value, final int length, final String name) {
        requireLength(value, length, name);
        requireNonZero(value, name);
    }

    private static void requireNonZero(final byte[] value, final String name) {
        if (value == null || value.length == 0 || java.util.Arrays.stream(toIntArray(value)).allMatch(item -> item == 0)) {
            throw new IllegalArgumentException(name + " must be non-empty and non-zero");
        }
    }

    private static byte[] copyNullable(final byte[] value) {
        return value == null ? null : Bytes.copy(value);
    }

    private static int[] toIntArray(final byte[] value) {
        final int[] result = new int[value.length];
        for (int index = 0; index < value.length; index++) {
            result[index] = value[index];
        }
        return result;
    }

    public record ParentCheckpoint(byte[] checkpointId, String manifestSha256) {
        public ParentCheckpoint {
            requireNonZeroLength(checkpointId, ID_LENGTH, "parent checkpointId");
            if (manifestSha256 == null || !manifestSha256.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("parent manifest hash must be lowercase SHA-256 hex");
            }
            checkpointId = Bytes.copy(checkpointId);
        }

        @Override
        public byte[] checkpointId() {
            return Bytes.copy(checkpointId);
        }

        private String toJson() {
            return "{\"checkpointId\":" + quote(b64(checkpointId)) + ",\"manifestSha256\":"
                    + quote(manifestSha256) + "}";
        }
    }

    public record CreatedBy(byte[] deploymentId, byte[] workerRunId, long ownerEpoch) {
        public CreatedBy {
            requireNonZero(deploymentId, "deploymentId");
            requireNonZero(workerRunId, "workerRunId");
            if (ownerEpoch < 0) {
                throw new IllegalArgumentException("ownerEpoch must be non-negative");
            }
            deploymentId = Bytes.copy(deploymentId);
            workerRunId = Bytes.copy(workerRunId);
        }

        @Override
        public byte[] deploymentId() {
            return Bytes.copy(deploymentId);
        }

        @Override
        public byte[] workerRunId() {
            return Bytes.copy(workerRunId);
        }

        private String toJson() {
            return "{\"deploymentId\":" + quote(b64(deploymentId)) + ",\"ownerEpoch\":"
                    + quote(u64(ownerEpoch)) + ",\"workerRunId\":" + quote(b64(workerRunId)) + "}";
        }
    }

    public record CreatedAt(long earliestEpochMs, long latestEpochMs, String source, byte[] sourceId,
                            long sourceConfigGeneration, long sampleSequence, long monotonicAnchorNs,
                            byte[] sourceEvidenceSha256, int sourceKeyVersion, byte[] sourceSignature) {
        public CreatedAt {
            if (earliestEpochMs < 0 || latestEpochMs < earliestEpochMs || source == null || source.isBlank()
                    || sourceConfigGeneration < 0 || sampleSequence < 0 || monotonicAnchorNs < 0
                    || sourceKeyVersion < 0) {
                throw new IllegalArgumentException("invalid checkpoint time evidence");
            }
            final TrustedUtcIntervalEvidence.Source sourceKind = parseSource(source);
            requireNonZero(sourceId, "sourceId");
            requireLength(sourceEvidenceSha256, 32, "sourceEvidenceSha256");
            if (sourceSignature != null && sourceSignature.length != 64) {
                throw new IllegalArgumentException("source signature must be 64 bytes");
            }
            if (sourceKind == TrustedUtcIntervalEvidence.Source.SIGNED_TIME_SERVICE) {
                if (sourceKeyVersion <= 0 || sourceSignature == null) {
                    throw new IllegalArgumentException("signed time evidence requires key and signature");
                }
            } else if (sourceKeyVersion != 0 || sourceSignature != null) {
                throw new IllegalArgumentException("unsigned time evidence cannot carry a key or signature");
            }
            sourceId = Bytes.copy(sourceId);
            sourceEvidenceSha256 = Bytes.copy(sourceEvidenceSha256);
            sourceSignature = copyNullable(sourceSignature);
        }

        @Override
        public byte[] sourceId() {
            return Bytes.copy(sourceId);
        }

        @Override
        public byte[] sourceEvidenceSha256() {
            return Bytes.copy(sourceEvidenceSha256);
        }

        @Override
        public byte[] sourceSignature() {
            return copyNullable(sourceSignature);
        }

        private String toJson() {
            return "{\"earliestEpochMs\":" + quote(Long.toString(earliestEpochMs))
                    + ",\"latestEpochMs\":" + quote(Long.toString(latestEpochMs))
                    + ",\"monotonicAnchorNs\":" + quote(u64(monotonicAnchorNs))
                    + ",\"sampleSequence\":" + quote(u64(sampleSequence)) + ",\"source\":" + quote(source)
                    + ",\"sourceConfigGeneration\":" + quote(u64(sourceConfigGeneration))
                    + ",\"sourceEvidenceSha256\":" + quote(hex(sourceEvidenceSha256))
                    + ",\"sourceId\":" + quote(b64(sourceId)) + ",\"sourceKeyVersion\":" + sourceKeyVersion
                    + ",\"sourceSignature\":" + (sourceSignature == null ? "null" : quote(b64(sourceSignature)))
                    + "}";
        }

        private static TrustedUtcIntervalEvidence.Source parseSource(final String value) {
            try {
                return TrustedUtcIntervalEvidence.Source.valueOf(value);
            } catch (IllegalArgumentException exception) {
                throw new IllegalArgumentException("unknown checkpoint time evidence source", exception);
            }
        }
    }

    public record FileEntry(String name, long length, byte[] checksum, byte[] objectKey, byte[] objectVersion,
                            byte[] etag) {
        public FileEntry {
            name = new CheckpointFileInventory(name, length, checksum).name();
            if (length < 0) {
                throw new IllegalArgumentException("file length must be non-negative");
            }
            requireLength(checksum, 32, "file checksum");
            requireNonZero(objectKey, "objectKey");
            requireNonZero(objectVersion, "objectVersion");
            if (etag != null && etag.length == 0) {
                throw new IllegalArgumentException("etag must be non-empty when present");
            }
            checksum = Bytes.copy(checksum);
            objectKey = Bytes.copy(objectKey);
            objectVersion = Bytes.copy(objectVersion);
            etag = copyNullable(etag);
        }

        @Override
        public byte[] checksum() {
            return Bytes.copy(checksum);
        }

        @Override
        public byte[] objectKey() {
            return Bytes.copy(objectKey);
        }

        @Override
        public byte[] objectVersion() {
            return Bytes.copy(objectVersion);
        }

        @Override
        public byte[] etag() {
            return copyNullable(etag);
        }

        private String toJson() {
            return "{\"checksum\":" + quote(hex(checksum)) + ",\"etag\":"
                    + (etag == null ? "null" : quote(b64(etag))) + ",\"length\":" + quote(u64(length))
                    + ",\"name\":" + quote(name) + ",\"objectKey\":" + quote(b64(objectKey))
                    + ",\"objectVersion\":" + quote(b64(objectVersion)) + "}";
        }
    }
}
