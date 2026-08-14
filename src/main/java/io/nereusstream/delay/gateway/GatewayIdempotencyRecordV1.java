package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.transport.Digest32;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Immutable in-memory form of the Gateway single-value idempotency record. */
public final class GatewayIdempotencyRecordV1 {
    public static final int VERSION = 1;

    private final Digest32 gatewayKeyHash;
    private final GatewayOperationKindV1 operation;
    private final Digest32 requestBodyHash;
    private final byte[] preparedSubmissionBytes;
    private final Digest32 preparedSubmissionHash;
    private final GatewayIdempotencyPhaseV1 phase;
    private final List<GatewayPhysicalAttemptV1> attempts;
    private final byte[] aggregateOutcomeBytes;
    private final long createdAtEpochMs;
    private final long retainUntilEpochMs;
    private final long revision;

    public GatewayIdempotencyRecordV1(final Digest32 gatewayKeyHash, final GatewayOperationKindV1 operation,
                                      final Digest32 requestBodyHash, final byte[] preparedSubmissionBytes,
                                      final GatewayIdempotencyPhaseV1 phase,
                                      final List<GatewayPhysicalAttemptV1> attempts,
                                      final byte[] aggregateOutcomeBytes, final long createdAtEpochMs,
                                      final long retainUntilEpochMs, final long revision) {
        this.gatewayKeyHash = Objects.requireNonNull(gatewayKeyHash, "gatewayKeyHash");
        this.operation = Objects.requireNonNull(operation, "operation");
        this.requestBodyHash = Objects.requireNonNull(requestBodyHash, "requestBodyHash");
        this.preparedSubmissionBytes = Bytes.copy(Objects.requireNonNull(preparedSubmissionBytes,
                "preparedSubmissionBytes"));
        this.preparedSubmissionHash = new Digest32(Bytes.sha256(this.preparedSubmissionBytes));
        if (phase == null || createdAtEpochMs < 0 || retainUntilEpochMs < createdAtEpochMs || revision <= 0) {
            throw new IllegalArgumentException("invalid Gateway idempotency record bounds");
        }
        this.phase = phase;
        this.attempts = List.copyOf(new ArrayList<>(Objects.requireNonNull(attempts, "attempts")));
        this.aggregateOutcomeBytes = aggregateOutcomeBytes == null ? null : Bytes.copy(aggregateOutcomeBytes);
        this.createdAtEpochMs = createdAtEpochMs;
        this.retainUntilEpochMs = retainUntilEpochMs;
        this.revision = revision;
    }

    public Digest32 gatewayKeyHash() {
        return gatewayKeyHash;
    }

    public GatewayOperationKindV1 operation() {
        return operation;
    }

    public Digest32 requestBodyHash() {
        return requestBodyHash;
    }

    public byte[] preparedSubmissionBytes() {
        return Bytes.copy(preparedSubmissionBytes);
    }

    public Digest32 preparedSubmissionHash() {
        return preparedSubmissionHash;
    }

    public GatewayIdempotencyPhaseV1 phase() {
        return phase;
    }

    public List<GatewayPhysicalAttemptV1> attempts() {
        return attempts;
    }

    public byte[] aggregateOutcomeBytes() {
        return aggregateOutcomeBytes == null ? null : Bytes.copy(aggregateOutcomeBytes);
    }

    public long createdAtEpochMs() {
        return createdAtEpochMs;
    }

    public long retainUntilEpochMs() {
        return retainUntilEpochMs;
    }

    public long revision() {
        return revision;
    }

    public byte[] recordDigest() {
        return Bytes.sha256(Bytes.utf8("nereus-delay-gateway-idempotency-record-v1\0"),
                canonicalWithoutDigest());
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            final byte[] withoutDigest = canonicalWithoutDigest();
            output.write(withoutDigest, 0, withoutDigest.length);
            CanonicalProtobuf.bytes(output, 13, recordDigest());
        });
    }

    GatewayIdempotencyRecordV1 withAttempt(final GatewayPhysicalAttemptV1 attempt) {
        final List<GatewayPhysicalAttemptV1> next = new ArrayList<>(attempts);
        next.add(Objects.requireNonNull(attempt, "attempt"));
        return new GatewayIdempotencyRecordV1(gatewayKeyHash, operation, requestBodyHash,
                preparedSubmissionBytes, GatewayIdempotencyPhaseV1.ACTIVE, next, aggregateOutcomeBytes,
                createdAtEpochMs, retainUntilEpochMs, revision + 1);
    }

    GatewayIdempotencyRecordV1 withOutcome(final PhysicalEnqueueAttemptIdMatch match,
                                           final byte[] outcomeBytes, final GatewayPhysicalAttemptStateV1 state) {
        final List<GatewayPhysicalAttemptV1> next = new ArrayList<>(attempts.size());
        boolean found = false;
        for (GatewayPhysicalAttemptV1 attempt : attempts) {
            if (attempt.physicalAttemptId().equals(match.id())) {
                found = true;
                next.add(new GatewayPhysicalAttemptV1(attempt.attemptNo(), attempt.physicalAttemptId(), state,
                        outcomeBytes, attempt.startedAtEpochMs(), attempt.uncertaintyAtEpochMs(),
                        attempt.revision() + 1, attempt.ownershipNotAfterEpochMs()));
            } else {
                next.add(attempt);
            }
        }
        if (!found) {
            throw new IllegalArgumentException("Gateway attempt is not part of the record");
        }
        return new GatewayIdempotencyRecordV1(gatewayKeyHash, operation, requestBodyHash,
                preparedSubmissionBytes, GatewayIdempotencyPhaseV1.QUIESCENT, next, outcomeBytes, createdAtEpochMs,
                retainUntilEpochMs, revision + 1);
    }

    private byte[] canonicalWithoutDigest() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, gatewayKeyHash.bytes());
            CanonicalProtobuf.uint32(output, 3, operation.wireValue());
            CanonicalProtobuf.bytes(output, 4, requestBodyHash.bytes());
            CanonicalProtobuf.bytes(output, 5, preparedSubmissionBytes);
            CanonicalProtobuf.bytes(output, 6, preparedSubmissionHash.bytes());
            CanonicalProtobuf.uint32(output, 7, phase.ordinal() + 1);
            for (GatewayPhysicalAttemptV1 attempt : attempts) {
                CanonicalProtobuf.bytes(output, 8, attempt.canonicalBytes());
            }
            if (aggregateOutcomeBytes != null) {
                CanonicalProtobuf.bytes(output, 9, aggregateOutcomeBytes);
            }
            CanonicalProtobuf.int64(output, 10, createdAtEpochMs);
            CanonicalProtobuf.int64(output, 11, retainUntilEpochMs);
            CanonicalProtobuf.uint64(output, 12, revision);
        });
    }

    record PhysicalEnqueueAttemptIdMatch(io.nereusstream.delay.transport.PhysicalEnqueueAttemptId id) {
    }
}
