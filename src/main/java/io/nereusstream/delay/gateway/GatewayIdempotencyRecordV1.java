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

    /** Strict decoder for the one-value Gateway idempotency record. */
    public static GatewayIdempotencyRecordV1 decode(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() < 11) {
            throw new IllegalArgumentException("Gateway idempotency record fields are incomplete");
        }
        int index = 0;
        if (uint(field(fields, index++, 1), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported Gateway idempotency record version");
        }
        final Digest32 keyHash = new Digest32(fixed(field(fields, index++, 2), 2, Digest32.LENGTH));
        final GatewayOperationKindV1 operation = GatewayOperationKindV1.fromWire(uint(field(fields, index++, 3), 3));
        final Digest32 bodyHash = new Digest32(fixed(field(fields, index++, 4), 4, Digest32.LENGTH));
        final byte[] preparedBytes = bytes(field(fields, index++, 5), 5);
        final Digest32 preparedHash = new Digest32(fixed(field(fields, index++, 6), 6, Digest32.LENGTH));
        if (!preparedHash.equals(new Digest32(Bytes.sha256(preparedBytes)))) {
            throw new IllegalArgumentException("Gateway prepared submission digest mismatch");
        }
        final GatewayIdempotencyPhaseV1 phase = GatewayIdempotencyPhaseV1.fromWire(
                uint(field(fields, index++, 7), 7));
        final List<GatewayPhysicalAttemptV1> attempts = new ArrayList<>();
        while (index < fields.size() && fields.get(index).number() == 8) {
            attempts.add(GatewayPhysicalAttemptV1.decode(bytes(field(fields, index++, 8), 8)));
        }
        for (int attemptIndex = 0; attemptIndex < attempts.size(); attemptIndex++) {
            if (attempts.get(attemptIndex).attemptNo() != attemptIndex + 1) {
                throw new IllegalArgumentException("Gateway attempt numbers are not source ordered");
            }
        }
        final byte[] aggregateBytes;
        if (index < fields.size() && fields.get(index).number() == 9) {
            aggregateBytes = bytes(field(fields, index++, 9), 9);
        } else {
            aggregateBytes = null;
        }
        final long createdAt = nonNegative(uint(field(fields, index++, 10), 10), "createdAtEpochMs");
        final long retainUntil = nonNegative(uint(field(fields, index++, 11), 11), "retainUntilEpochMs");
        final long revision = positive(uint(field(fields, index++, 12), 12), "revision");
        final byte[] recordDigest = fixed(field(fields, index++, 13), 13, Digest32.LENGTH);
        if (index != fields.size()) {
            throw new IllegalArgumentException("Gateway idempotency record has unknown fields");
        }
        final GatewayIdempotencyRecordV1 result = new GatewayIdempotencyRecordV1(keyHash, operation, bodyHash,
                preparedBytes, phase, attempts, aggregateBytes, createdAt, retainUntil, revision);
        if (!Bytes.constantTimeEquals(recordDigest, result.recordDigest())
                || !Bytes.constantTimeEquals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("Gateway idempotency record digest/canonical bytes mismatch");
        }
        return result;
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

    private static CanonicalProtobuf.Reader.Field field(final List<CanonicalProtobuf.Reader.Field> fields,
                                                         final int index, final int number) {
        if (index >= fields.size() || fields.get(index).number() != number) {
            throw new IllegalArgumentException("unexpected Gateway idempotency field at " + number);
        }
        return fields.get(index);
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.wireType() != 2) {
            throw new IllegalArgumentException("Gateway idempotency field " + number + " is not bytes");
        }
        return field.rawValue();
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        final byte[] value = bytes(field, number);
        if (value.length != length) {
            throw new IllegalArgumentException("Gateway idempotency field " + number + " has invalid length");
        }
        return value;
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("Gateway idempotency field " + number + " is not uint");
        }
        return field.unsignedValue();
    }

    private static long nonNegative(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    private static long positive(final long value, final String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    record PhysicalEnqueueAttemptIdMatch(io.nereusstream.delay.transport.PhysicalEnqueueAttemptId id) {
    }
}
