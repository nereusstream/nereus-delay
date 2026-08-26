package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Closed outer SLO event-identity union.
 *
 * <p>The branch payload is kept as canonical bytes here because the measured
 * component owns the semantic source object (for example a Shard Subject).
 * The outer branch number, common field shape and fixed identity widths are
 * nevertheless enforced at this protocol boundary, so a payload cannot be
 * relabelled as another objective.</p>
 */
public final class SloSampleEventIdentity {
    private final SloObjectiveName objective;
    private final byte[] canonicalBytes;

    public SloSampleEventIdentity(final SloObjectiveName objective, final byte[] canonicalBranchPayload) {
        this.objective = Objects.requireNonNull(objective, "objective");
        Objects.requireNonNull(canonicalBranchPayload, "canonicalBranchPayload");
        if (canonicalBranchPayload.length == 0) {
            throw new IllegalArgumentException("SLO event identity branch payload must not be empty");
        }
        validateBranchPayload(objective, canonicalBranchPayload);
        final byte[] outer = CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, objective.wireValue(), canonicalBranchPayload));
        this.canonicalBytes = outer;
        // Reading the constructed value also verifies that the branch is the
        // exact oneof selected by the objective and that no unknown outer field
        // can be smuggled into the identity.
        decode(outer);
    }

    private SloSampleEventIdentity(
            final SloObjectiveName objective, final byte[] canonicalBytes, final boolean alreadyCanonical) {
        this.objective = Objects.requireNonNull(objective, "objective");
        this.canonicalBytes = Bytes.copy(canonicalBytes);
        if (!alreadyCanonical) {
            throw new IllegalArgumentException("internal SLO identity constructor misuse");
        }
    }

    public SloObjectiveName objective() {
        return objective;
    }

    public byte[] canonicalBytes() {
        return Bytes.copy(canonicalBytes);
    }

    public byte[] branchPayload() {
        final var fields = QueryCodecSupport.read(canonicalBytes, "SloSampleEventIdentity");
        return QueryCodecSupport.nested(fields.get(0), objective.wireValue());
    }

    /** Returns the exact non-negative path start from a due-admission identity. */
    public long dueAdmissionPathStartEpochMs() {
        requireDueAdmissionIdentity();
        final var fields = QueryCodecSupport.read(branchPayload(), "SloDueAdmissionIdentity");
        return nonNegativeInt64(QueryCodecSupport.uint64Bits(fields.get(2), 3), "path_start_epoch_ms");
    }

    /** Returns the managed path encoded in a due-admission identity. */
    public SloPath dueAdmissionPath() {
        requireDueAdmissionIdentity();
        final var fields = QueryCodecSupport.read(branchPayload(), "SloDueAdmissionIdentity");
        return SloPath.fromWire(QueryCodecSupport.uint(fields.get(3), 4));
    }

    /** Returns the exact Source Position embedded by a command-applied identity. */
    public SourcePosition commandAppliedSourcePosition() {
        if (objective != SloObjectiveName.COMMAND_APPLIED_LATENCY) {
            throw new IllegalStateException("identity is not a command-applied branch");
        }
        final var fields = QueryCodecSupport.read(branchPayload(), "SloCommandAppliedIdentity");
        return QueryCodecSupport.decodeSourcePosition(QueryCodecSupport.nested(fields.get(0), 1));
    }

    /** Returns the self-routing Message ID embedded by a due-admission identity. */
    public DelayMessageId dueAdmissionMessageId() {
        requireDueAdmissionIdentity();
        final var fields = QueryCodecSupport.read(branchPayload(), "SloDueAdmissionIdentity");
        return new DelayMessageId(QueryCodecSupport.fixed(fields.get(0), 1, DelayMessageId.LENGTH));
    }

    public static SloSampleEventIdentity decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "SloSampleEventIdentity");
        if (fields.size() != 1) {
            throw new IllegalArgumentException("SloSampleEventIdentity must select one branch");
        }
        final var field = fields.get(0);
        final SloObjectiveName objective = SloObjectiveName.fromWire(field.number());
        final byte[] branch = QueryCodecSupport.nested(field, field.number());
        validateBranchPayload(objective, branch);
        final SloSampleEventIdentity result = new SloSampleEventIdentity(objective, Bytes.copy(encoded), true);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "SloSampleEventIdentity");
        return result;
    }

    private static void validateBranchPayload(final SloObjectiveName objective, final byte[] payload) {
        final var fields = QueryCodecSupport.read(payload, "SloSampleEventIdentity branch");
        switch (objective) {
            case COMMAND_QUEUED_LATENCY -> {
                QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "SloCommandQueuedIdentity");
                fixed(fields.get(0), 1, CommandId.LENGTH, "commandId");
                fixed(fields.get(1), 2, 32, "commandHash");
                fixed(fields.get(2), 3, 16, "physicalEnqueueAttemptId");
            }
            case COMMAND_APPLIED_LATENCY -> {
                QueryCodecSupport.requireNumbers(fields, new int[] {1}, "SloCommandAppliedIdentity");
                QueryCodecSupport.decodeSourcePosition(QueryCodecSupport.nested(fields.get(0), 1));
            }
            case DUE_ADMISSION_LAG -> {
                QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4}, "SloDueAdmissionIdentity");
                fixed(fields.get(0), 1, DelayMessageId.LENGTH, "delayMessageId");
                QueryCodecSupport.uint32Bits(fields.get(1), 2);
                nonNegativeInt64(QueryCodecSupport.uint64Bits(fields.get(2), 3), "path_start_epoch_ms");
                final SloPath path = SloPath.fromWire(QueryCodecSupport.uint(fields.get(3), 4));
                if (path == SloPath.NOT_APPLICABLE || path == SloPath.AUTO_FAST_NATIVE) {
                    throw new IllegalArgumentException("due-admission identity requires a managed path");
                }
            }
            case NATIVE_HANDOFF_ACK_LAG -> {
                QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "SloNativeHandoffIdentity");
                fixed(fields.get(0), 1, 32, "nativeDeliveryId");
                fixed(fields.get(1), 2, 32, "submissionHash");
            }
            case QUERY_LATENCY -> {
                QueryCodecSupport.requireNumbers(fields, new int[] {1}, "SloQueryIdentity");
                nonZeroFixed(fields.get(0), 1, 16, "queryRequestId");
            }
            case OWNERSHIP_FAILOVER_RTO -> {
                QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "SloOwnershipLossIdentity");
                nestedNonEmpty(fields.get(0), 1, "shardSubject");
                QueryCodecSupport.uint(fields.get(1), 2);
            }
            case LOCAL_DISK_LOSS_RTO -> {
                QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "SloLocalDiskLossIdentity");
                nestedNonEmpty(fields.get(0), 1, "shardSubject");
                fixed(fields.get(1), 2, 16, "storeIncarnation");
            }
            case CHECKPOINT_AGE -> {
                QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "SloCheckpointAgeIdentity");
                nestedNonEmpty(fields.get(0), 1, "shardSubject");
                QueryCodecSupport.uint(fields.get(1), 2);
                QueryCodecSupport.uint(fields.get(2), 3);
            }
            case SOURCE_RETENTION_TIME_MARGIN, SOURCE_RETENTION_BYTE_MARGIN -> {
                QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4}, "SloSourceMarginIdentity");
                nestedNonEmpty(fields.get(0), 1, "shardSubject");
                QueryCodecSupport.uint(fields.get(1), 2);
                QueryCodecSupport.uint(fields.get(2), 3);
                QueryCodecSupport.uint(fields.get(3), 4);
            }
            case POSSIBLE_DUPLICATE_WINDOW -> {
                QueryCodecSupport.requireNumbers(fields, new int[] {1}, "SloPossibleDuplicateIdentity");
                fixed(fields.get(0), 1, 32, "publishAttemptId");
            }
            case HEALTHY_LANE_DISCOVERY_AGE -> validateLane(fields, "SloLaneDiscoveryIdentity", "readyGeneration");
            case HEALTHY_LANE_SERVICE_GAP -> validateLane(fields, "SloLaneServiceGapIdentity", "serviceGapGeneration");
            case LANE_RECOVERY_READY_RTO -> {
                QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5}, "SloLaneRecoveryReadyIdentity");
                nestedNonEmpty(fields.get(0), 1, "shardSubject");
                fixed(fields.get(1), 2, 32, "destinationLaneId");
                fixed(fields.get(2), 3, 16, "laneIncarnation");
                QueryCodecSupport.uint(fields.get(3), 4);
                QueryCodecSupport.uint(fields.get(4), 5);
            }
        }
    }

    private void requireDueAdmissionIdentity() {
        if (objective != SloObjectiveName.DUE_ADMISSION_LAG) {
            throw new IllegalStateException("identity is not a due-admission branch");
        }
    }

    private static long nonNegativeInt64(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " exceeds the non-negative int64 range");
        }
        return value;
    }

    private static void validateLane(
            final java.util.List<CanonicalProtobuf.Reader.Field> fields,
            final String name,
            final String generationName) {
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, name);
        fixed(fields.get(0), 1, 32, "destinationLaneId");
        fixed(fields.get(1), 2, 16, "laneIncarnation");
        QueryCodecSupport.uint(fields.get(2), 3);
        if (generationName.isEmpty()) {
            throw new IllegalArgumentException("identity generation name must not be empty");
        }
    }

    private static byte[] fixed(
            final CanonicalProtobuf.Reader.Field field, final int number, final int length, final String name) {
        return QueryCodecSupport.fixed(field, number, length);
    }

    private static byte[] nonZeroFixed(
            final CanonicalProtobuf.Reader.Field field, final int number, final int length, final String name) {
        final byte[] value = fixed(field, number, length, name);
        boolean nonZero = false;
        for (byte current : value) {
            nonZero |= current != 0;
        }
        if (!nonZero) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return value;
    }

    private static void nestedNonEmpty(
            final CanonicalProtobuf.Reader.Field field, final int number, final String name) {
        final byte[] value = QueryCodecSupport.nested(field, number);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof SloSampleEventIdentity that
                && objective == that.objective
                && Arrays.equals(canonicalBytes, that.canonicalBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objective, Arrays.hashCode(canonicalBytes));
    }
}
