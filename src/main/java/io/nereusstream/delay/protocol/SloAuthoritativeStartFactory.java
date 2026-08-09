package io.nereusstream.delay.protocol;

import java.util.Objects;

/**
 * Reconstructs the two Shard-derived V1 SLO Start branches from typed
 * authority fields.
 *
 * <p>This class intentionally does not inspect an arbitrary business message
 * or invent a timestamp/evidence digest. The caller must supply the exact
 * Source Position, or the exact Message/eligibility path fields and semantic
 * evidence digest that were durably established by the relevant authority.
 * The returned value is therefore safe to pass to the shard SLO outbox for
 * idempotent recovery materialization, while production authority and source
 * ordering remain outside this pure protocol helper.</p>
 */
public final class SloAuthoritativeStartFactory {
    private SloAuthoritativeStartFactory() {
    }

    /**
     * Reconstructs a {@code COMMAND_APPLIED_LATENCY} Start from the exact
     * Broker-persisted Source Position.
     *
     * <p>The Broker endpoint uses the Source Position's authenticated
     * persistence timestamp and the Registry's exact Source Position digest.
     * The Source Position itself is the event identity, so a replay of the
     * same record produces byte-identical sample and Start identities.</p>
     */
    public static SloSampleStartV1 commandApplied(final SloObjectiveV1 objective,
                                                   final SourcePosition sourcePosition) {
        requireObjective(objective, SloObjectiveNameV1.COMMAND_APPLIED_LATENCY);
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        final byte[] sourcePositionBytes = QueryCodecSupport.encodeSourcePosition(sourcePosition);
        final SloSampleEventIdentityV1 identity = new SloSampleEventIdentityV1(
                SloObjectiveNameV1.COMMAND_APPLIED_LATENCY,
                CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, sourcePositionBytes)));
        final long brokerPersistenceAt = sourcePosition.brokerPersistenceTimeEpochMs();
        final SloTimeEndpointV1 start = new SloTimeEndpointV1(
                SloTimeEndpointKindV1.BROKER_PERSISTENCE, brokerPersistenceAt, brokerPersistenceAt,
                Bytes.sha256(sourcePositionBytes));
        return new SloSampleStartV1(objective, SloPathV1.NOT_APPLICABLE, identity, start,
                timeoutAt(objective, brokerPersistenceAt));
    }

    /**
     * Reconstructs a managed {@code DUE_ADMISSION_LAG} Start from the exact
     * Message generation and path authority projection.
     *
     * @param generation the complete unsigned-32 generation value
     * @param pathStartEpochMs the authoritative {@code deliverAt} or managed
     *        handoff {@code actionAt}, depending on {@code path}
     * @param semanticEvidenceSha256 the exact digest of the durable semantic
     *        evidence that established this path start
     */
    public static SloSampleStartV1 dueAdmission(final SloObjectiveV1 objective,
                                                final DelayMessageId delayMessageId,
                                                final long generation,
                                                final SloPathV1 path,
                                                final long pathStartEpochMs,
                                                final byte[] semanticEvidenceSha256) {
        requireObjective(objective, SloObjectiveNameV1.DUE_ADMISSION_LAG);
        Objects.requireNonNull(delayMessageId, "delayMessageId");
        Objects.requireNonNull(path, "path");
        if (generation < 0 || generation > 0xffff_ffffL) {
            throw new IllegalArgumentException("generation must be an unsigned 32-bit value");
        }
        if (path == SloPathV1.NOT_APPLICABLE || path == SloPathV1.AUTO_FAST_NATIVE) {
            throw new IllegalArgumentException("due-admission Start requires a managed path");
        }
        if (pathStartEpochMs < 0) {
            throw new IllegalArgumentException("pathStartEpochMs must be non-negative");
        }
        Bytes.requireLength(semanticEvidenceSha256, SloTimeEndpointV1.HASH_LENGTH,
                "semanticEvidenceSha256");
        final SloSampleEventIdentityV1 identity = new SloSampleEventIdentityV1(
                SloObjectiveNameV1.DUE_ADMISSION_LAG,
                CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.bytes(output, 1, delayMessageId.bytes());
                    CanonicalProtobuf.uint32(output, 2, generation);
                    CanonicalProtobuf.int64(output, 3, pathStartEpochMs);
                    CanonicalProtobuf.uint32(output, 4, path.wireValue());
                }));
        final SloTimeEndpointV1 start = new SloTimeEndpointV1(
                SloTimeEndpointKindV1.SEMANTIC_FIXED_EPOCH, pathStartEpochMs, pathStartEpochMs,
                semanticEvidenceSha256);
        return new SloSampleStartV1(objective, path, identity, start,
                timeoutAt(objective, pathStartEpochMs));
    }

    private static void requireObjective(final SloObjectiveV1 objective,
                                         final SloObjectiveNameV1 expected) {
        Objects.requireNonNull(objective, "objective");
        if (objective.name() != expected) {
            throw new IllegalArgumentException("SLO objective must be " + expected);
        }
    }

    private static Long timeoutAt(final SloObjectiveV1 objective, final long startEpochMs) {
        if (objective.direction() != SloThresholdDirectionV1.AT_MOST) {
            return null;
        }
        if (objective.threshold() < 0) {
            throw new IllegalArgumentException("SLO threshold exceeds the local signed epoch range");
        }
        try {
            return Math.addExact(startEpochMs, objective.threshold());
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException("SLO timeout overflows epoch range", exception);
        }
    }
}
