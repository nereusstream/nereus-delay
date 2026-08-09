package io.nereusstream.delay.protocol;

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
public final class SloSampleEventIdentityV1 {
    private final SloObjectiveNameV1 objective;
    private final byte[] canonicalBytes;

    public SloSampleEventIdentityV1(final SloObjectiveNameV1 objective, final byte[] canonicalBranchPayload) {
        this.objective = Objects.requireNonNull(objective, "objective");
        Objects.requireNonNull(canonicalBranchPayload, "canonicalBranchPayload");
        if (canonicalBranchPayload.length == 0) {
            throw new IllegalArgumentException("SLO event identity branch payload must not be empty");
        }
        validateBranchPayload(objective, canonicalBranchPayload);
        final byte[] outer = CanonicalProtobuf.message(output ->
                CanonicalProtobuf.bytes(output, objective.wireValue(), canonicalBranchPayload));
        this.canonicalBytes = outer;
        // Reading the constructed value also verifies that the branch is the
        // exact oneof selected by the objective and that no unknown outer field
        // can be smuggled into the identity.
        decode(outer);
    }

    private SloSampleEventIdentityV1(final SloObjectiveNameV1 objective, final byte[] canonicalBytes,
                                     final boolean alreadyCanonical) {
        this.objective = Objects.requireNonNull(objective, "objective");
        this.canonicalBytes = Bytes.copy(canonicalBytes);
        if (!alreadyCanonical) {
            throw new IllegalArgumentException("internal SLO identity constructor misuse");
        }
    }

    public SloObjectiveNameV1 objective() {
        return objective;
    }

    public byte[] canonicalBytes() {
        return Bytes.copy(canonicalBytes);
    }

    public byte[] branchPayload() {
        final var fields = QueryCodecSupport.read(canonicalBytes, "SloSampleEventIdentityV1");
        return QueryCodecSupport.nested(fields.get(0), objective.wireValue());
    }

    /** Returns the exact non-negative path start from a due-admission identity. */
    public long dueAdmissionPathStartEpochMs() {
        requireDueAdmissionIdentity();
        final var fields = QueryCodecSupport.read(branchPayload(), "SloDueAdmissionIdentityV1");
        return nonNegativeInt64(QueryCodecSupport.uint64Bits(fields.get(2), 3),
                "path_start_epoch_ms");
    }

    /** Returns the managed path encoded in a due-admission identity. */
    public SloPathV1 dueAdmissionPath() {
        requireDueAdmissionIdentity();
        final var fields = QueryCodecSupport.read(branchPayload(), "SloDueAdmissionIdentityV1");
        return SloPathV1.fromWire(QueryCodecSupport.uint(fields.get(3), 4));
    }

    public static SloSampleEventIdentityV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "SloSampleEventIdentityV1");
        if (fields.size() != 1) {
            throw new IllegalArgumentException("SloSampleEventIdentityV1 must select one branch");
        }
        final var field = fields.get(0);
        final SloObjectiveNameV1 objective = SloObjectiveNameV1.fromWire(field.number());
        final byte[] branch = QueryCodecSupport.nested(field, field.number());
        validateBranchPayload(objective, branch);
        final SloSampleEventIdentityV1 result = new SloSampleEventIdentityV1(objective,
                Bytes.copy(encoded), true);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "SloSampleEventIdentityV1");
        return result;
    }

    private static void validateBranchPayload(final SloObjectiveNameV1 objective, final byte[] payload) {
        final var fields = QueryCodecSupport.read(payload, "SloSampleEventIdentityV1 branch");
        switch (objective) {
            case COMMAND_QUEUED_LATENCY -> {
                QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3}, "SloCommandQueuedIdentityV1");
                fixed(fields.get(0), 1, CommandId.LENGTH, "commandId");
                fixed(fields.get(1), 2, 32, "commandHash");
                fixed(fields.get(2), 3, 16, "physicalEnqueueAttemptId");
            }
            case COMMAND_APPLIED_LATENCY -> {
                QueryCodecSupport.requireNumbers(fields, new int[]{1}, "SloCommandAppliedIdentityV1");
                QueryCodecSupport.decodeSourcePosition(QueryCodecSupport.nested(fields.get(0), 1));
            }
            case DUE_ADMISSION_LAG -> {
                QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4}, "SloDueAdmissionIdentityV1");
                fixed(fields.get(0), 1, DelayMessageId.LENGTH, "delayMessageId");
                QueryCodecSupport.uint32Bits(fields.get(1), 2);
                nonNegativeInt64(QueryCodecSupport.uint64Bits(fields.get(2), 3), "path_start_epoch_ms");
                final SloPathV1 path = SloPathV1.fromWire(QueryCodecSupport.uint(fields.get(3), 4));
                if (path == SloPathV1.NOT_APPLICABLE || path == SloPathV1.AUTO_FAST_NATIVE) {
                    throw new IllegalArgumentException("due-admission identity requires a managed path");
                }
            }
            case NATIVE_HANDOFF_ACK_LAG -> {
                QueryCodecSupport.requireNumbers(fields, new int[]{1, 2}, "SloNativeHandoffIdentityV1");
                fixed(fields.get(0), 1, 32, "nativeDeliveryId");
                fixed(fields.get(1), 2, 32, "submissionHash");
            }
            case QUERY_LATENCY -> {
                QueryCodecSupport.requireNumbers(fields, new int[]{1}, "SloQueryIdentityV1");
                nonZeroFixed(fields.get(0), 1, 16, "queryRequestId");
            }
            case OWNERSHIP_FAILOVER_RTO -> {
                QueryCodecSupport.requireNumbers(fields, new int[]{1, 2}, "SloOwnershipLossIdentityV1");
                nestedNonEmpty(fields.get(0), 1, "shardSubject");
                QueryCodecSupport.uint(fields.get(1), 2);
            }
            case LOCAL_DISK_LOSS_RTO -> {
                QueryCodecSupport.requireNumbers(fields, new int[]{1, 2}, "SloLocalDiskLossIdentityV1");
                nestedNonEmpty(fields.get(0), 1, "shardSubject");
                fixed(fields.get(1), 2, 16, "storeIncarnation");
            }
            case CHECKPOINT_AGE -> {
                QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3}, "SloCheckpointAgeIdentityV1");
                nestedNonEmpty(fields.get(0), 1, "shardSubject");
                QueryCodecSupport.uint(fields.get(1), 2);
                QueryCodecSupport.uint(fields.get(2), 3);
            }
            case SOURCE_RETENTION_TIME_MARGIN, SOURCE_RETENTION_BYTE_MARGIN -> {
                QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4},
                        "SloSourceMarginIdentityV1");
                nestedNonEmpty(fields.get(0), 1, "shardSubject");
                QueryCodecSupport.uint(fields.get(1), 2);
                QueryCodecSupport.uint(fields.get(2), 3);
                QueryCodecSupport.uint(fields.get(3), 4);
            }
            case POSSIBLE_DUPLICATE_WINDOW -> {
                QueryCodecSupport.requireNumbers(fields, new int[]{1}, "SloPossibleDuplicateIdentityV1");
                fixed(fields.get(0), 1, 32, "publishAttemptId");
            }
            case HEALTHY_LANE_DISCOVERY_AGE -> validateLane(fields, "SloLaneDiscoveryIdentityV1", "readyGeneration");
            case HEALTHY_LANE_SERVICE_GAP -> validateLane(fields, "SloLaneServiceGapIdentityV1",
                    "serviceGapGeneration");
            case LANE_RECOVERY_READY_RTO -> {
                QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5},
                        "SloLaneRecoveryReadyIdentityV1");
                nestedNonEmpty(fields.get(0), 1, "shardSubject");
                fixed(fields.get(1), 2, 32, "destinationLaneId");
                fixed(fields.get(2), 3, 16, "laneIncarnation");
                QueryCodecSupport.uint(fields.get(3), 4);
                QueryCodecSupport.uint(fields.get(4), 5);
            }
        }
    }

    private void requireDueAdmissionIdentity() {
        if (objective != SloObjectiveNameV1.DUE_ADMISSION_LAG) {
            throw new IllegalStateException("identity is not a due-admission branch");
        }
    }

    private static long nonNegativeInt64(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " exceeds the non-negative int64 range");
        }
        return value;
    }

    private static void validateLane(final java.util.List<CanonicalProtobuf.Reader.Field> fields,
                                     final String name, final String generationName) {
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3}, name);
        fixed(fields.get(0), 1, 32, "destinationLaneId");
        fixed(fields.get(1), 2, 16, "laneIncarnation");
        QueryCodecSupport.uint(fields.get(2), 3);
        if (generationName.isEmpty()) {
            throw new IllegalArgumentException("identity generation name must not be empty");
        }
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length,
                                final String name) {
        return QueryCodecSupport.fixed(field, number, length);
    }

    private static byte[] nonZeroFixed(final CanonicalProtobuf.Reader.Field field, final int number,
                                       final int length, final String name) {
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

    private static void nestedNonEmpty(final CanonicalProtobuf.Reader.Field field, final int number,
                                       final String name) {
        final byte[] value = QueryCodecSupport.nested(field, number);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof SloSampleEventIdentityV1 that
                && objective == that.objective
                && Arrays.equals(canonicalBytes, that.canonicalBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objective, Arrays.hashCode(canonicalBytes));
    }
}
