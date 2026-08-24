package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import com.nereusstream.delay.protocol.EnqueueOutcomeKindV1;
import com.nereusstream.delay.protocol.NativePreparedRefV1;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmissionV1;
import com.nereusstream.delay.protocol.SubmissionOutcomeKindV1;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import com.nereusstream.delay.transport.Digest32;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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

    public GatewayIdempotencyRecordV1(
            final Digest32 gatewayKeyHash,
            final GatewayOperationKindV1 operation,
            final Digest32 requestBodyHash,
            final byte[] preparedSubmissionBytes,
            final GatewayIdempotencyPhaseV1 phase,
            final List<GatewayPhysicalAttemptV1> attempts,
            final byte[] aggregateOutcomeBytes,
            final long createdAtEpochMs,
            final long retainUntilEpochMs,
            final long revision) {
        this.gatewayKeyHash = Objects.requireNonNull(gatewayKeyHash, "gatewayKeyHash");
        this.operation = Objects.requireNonNull(operation, "operation");
        this.requestBodyHash = Objects.requireNonNull(requestBodyHash, "requestBodyHash");
        this.preparedSubmissionBytes =
                Bytes.copy(Objects.requireNonNull(preparedSubmissionBytes, "preparedSubmissionBytes"));
        this.preparedSubmissionHash = new Digest32(Bytes.sha256(this.preparedSubmissionBytes));
        if (phase == null || createdAtEpochMs < 0 || retainUntilEpochMs < createdAtEpochMs || revision <= 0) {
            throw new IllegalArgumentException("invalid Gateway idempotency record bounds");
        }
        final List<GatewayPhysicalAttemptV1> copiedAttempts =
                List.copyOf(new ArrayList<>(Objects.requireNonNull(attempts, "attempts")));
        final byte[] copiedAggregate = aggregateOutcomeBytes == null ? null : Bytes.copy(aggregateOutcomeBytes);
        validateProjection(phase, copiedAttempts, copiedAggregate);
        this.phase = phase;
        this.attempts = copiedAttempts;
        this.aggregateOutcomeBytes = copiedAggregate;
        this.createdAtEpochMs = createdAtEpochMs;
        this.retainUntilEpochMs = retainUntilEpochMs;
        this.revision = revision;
        validateStoredProjection();
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
        return Bytes.sha256(Bytes.utf8("nereus-delay-gateway-idempotency-record-v1\0"), canonicalWithoutDigest());
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
        final GatewayIdempotencyPhaseV1 phase = GatewayIdempotencyPhaseV1.fromWire(uint(field(fields, index++, 7), 7));
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
        final GatewayIdempotencyRecordV1 result = new GatewayIdempotencyRecordV1(
                keyHash,
                operation,
                bodyHash,
                preparedBytes,
                phase,
                attempts,
                aggregateBytes,
                createdAt,
                retainUntil,
                revision);
        if (!Bytes.constantTimeEquals(recordDigest, result.recordDigest())
                || !Bytes.constantTimeEquals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("Gateway idempotency record digest/canonical bytes mismatch");
        }
        return result;
    }

    GatewayIdempotencyRecordV1 withAttempt(final GatewayPhysicalAttemptV1 attempt) {
        final List<GatewayPhysicalAttemptV1> next = new ArrayList<>(attempts);
        next.add(Objects.requireNonNull(attempt, "attempt"));
        return new GatewayIdempotencyRecordV1(
                gatewayKeyHash,
                operation,
                requestBodyHash,
                preparedSubmissionBytes,
                GatewayIdempotencyPhaseV1.ACTIVE,
                next,
                null,
                createdAtEpochMs,
                retainUntilEpochMs,
                checkedIncrement(revision));
    }

    GatewayIdempotencyRecordV1 withOutcome(
            final PhysicalEnqueueAttemptIdMatch match,
            final SubmissionOutcomeMessageV1 outcome,
            final GatewayPhysicalAttemptStateV1 state) {
        Objects.requireNonNull(match, "match");
        final PhysicalEnqueueAttemptIdMatch checkedMatch = match;
        final SubmissionOutcomeMessageV1 checkedOutcome = Objects.requireNonNull(outcome, "outcome");
        final GatewayPhysicalAttemptStateV1 checkedState = Objects.requireNonNull(state, "state");
        validateOutcome(checkedOutcome, checkedMatch.id());
        final byte[] outcomeBytes = checkedOutcome.canonicalBytes();
        final List<GatewayPhysicalAttemptV1> next = new ArrayList<>(attempts.size());
        boolean found = false;
        GatewayPhysicalAttemptV1 currentAttempt = null;
        for (int index = 0; index < attempts.size(); index++) {
            final GatewayPhysicalAttemptV1 attempt = attempts.get(index);
            if (attempt.physicalAttemptId().equals(checkedMatch.id())) {
                found = true;
                currentAttempt = attempt;
                break;
            }
        }
        if (!found) {
            throw new IllegalArgumentException("Gateway attempt is not part of the record");
        }
        final byte[] currentOutcomeBytes = currentAttempt.outcomeBytes();
        if (currentAttempt.state() != GatewayPhysicalAttemptStateV1.STARTED) {
            if (Arrays.equals(currentOutcomeBytes, outcomeBytes)) {
                return this;
            }
            if (currentAttempt.state() != GatewayPhysicalAttemptStateV1.UNCERTAIN
                    || (checkedState != GatewayPhysicalAttemptStateV1.QUEUED
                            && checkedState != GatewayPhysicalAttemptStateV1.DEFINITELY_NOT_QUEUED)) {
                throw new IllegalStateException("Gateway attempt terminal evidence conflict");
            }
        }
        for (GatewayPhysicalAttemptV1 attempt : attempts) {
            if (attempt.physicalAttemptId().equals(checkedMatch.id())) {
                next.add(new GatewayPhysicalAttemptV1(
                        attempt.attemptNo(),
                        attempt.physicalAttemptId(),
                        state,
                        outcomeBytes,
                        attempt.startedAtEpochMs(),
                        attempt.uncertaintyAtEpochMs(),
                        attempt.retryRequestId(),
                        attempt.retryRequestHash(),
                        checkedIncrement(attempt.revision()),
                        attempt.ownershipNotAfterEpochMs()));
            } else {
                next.add(attempt);
            }
        }
        final Aggregate aggregate = aggregate(next, aggregateOutcomeBytes);
        return new GatewayIdempotencyRecordV1(
                gatewayKeyHash,
                operation,
                requestBodyHash,
                preparedSubmissionBytes,
                aggregate.phase(),
                next,
                aggregate.outcomeBytes(),
                createdAtEpochMs,
                retainUntilEpochMs,
                checkedIncrement(revision));
    }

    private void validateOutcome(
            final SubmissionOutcomeMessageV1 outcome,
            final com.nereusstream.delay.transport.PhysicalEnqueueAttemptId attemptId) {
        Objects.requireNonNull(attemptId, "attemptId");
        final PreparedSubmissionV1 prepared;
        try {
            prepared = PreparedSubmissionV1.decode(preparedSubmissionBytes);
        } catch (RuntimeException malformed) {
            throw new IllegalStateException("Gateway prepared submission is malformed", malformed);
        }
        if (prepared.isManaged() != (outcome.kind() == SubmissionOutcomeKindV1.MANAGED)) {
            throw new IllegalStateException("Gateway outcome branch does not match prepared submission");
        }
        if (prepared.isManaged()) {
            final PreparedCommand command = CommandCodec.decodeFrameV1(prepared.managedFrame());
            final CommandQueuedReceiptV1.PreparedCommandRef expected =
                    CommandQueuedReceiptV1.PreparedCommandRef.from(command);
            final var managed = outcome.managed();
            switch (managed.kind()) {
                case QUEUED -> {
                    if (!expected.equals(managed.queued().command())) {
                        throw new IllegalStateException("Gateway queued outcome does not match prepared command");
                    }
                    requireAttemptId(attemptId, managed.queued().physicalEnqueueAttemptId());
                }
                case DEFINITELY_NOT_QUEUED -> {
                    if (!expected.equals(managed.definitelyNotQueued().command())) {
                        throw new IllegalStateException("Gateway definite outcome does not match prepared command");
                    }
                }
                case ENQUEUE_UNCERTAIN -> {
                    if (!expected.equals(managed.uncertain().command())) {
                        throw new IllegalStateException("Gateway uncertain outcome does not match prepared command");
                    }
                    requireAttemptId(attemptId, managed.uncertain().physicalEnqueueAttemptId());
                }
            }
            return;
        }
        final NativePreparedRefV1 expected = prepared.nativePrepared().preparedRef();
        switch (outcome.kind()) {
            case NATIVE_RECEIPT -> {
                if (!expected.equals(outcome.nativeReceipt().prepared())) {
                    throw new IllegalStateException("Gateway native receipt does not match prepared delivery");
                }
                requireAttemptId(attemptId, outcome.nativeReceipt().physicalEnqueueAttemptId());
            }
            case NATIVE_DEFINITELY_NOT_QUEUED -> {
                if (!expected.equals(outcome.nativeDefinitelyNotQueued().nativePrepared())) {
                    throw new IllegalStateException("Gateway native definite outcome does not match prepared delivery");
                }
            }
            case NATIVE_ENQUEUE_UNCERTAIN -> {
                if (!expected.equals(outcome.nativeUncertain().nativePrepared())) {
                    throw new IllegalStateException(
                            "Gateway native uncertain outcome does not match prepared delivery");
                }
                requireAttemptId(attemptId, outcome.nativeUncertain().physicalEnqueueAttemptId());
            }
            case MANAGED -> throw new IllegalStateException("Gateway managed outcome does not match native delivery");
        }
    }

    private static void requireAttemptId(
            final com.nereusstream.delay.transport.PhysicalEnqueueAttemptId expected, final byte[] actual) {
        if (!Arrays.equals(expected.bytes(), actual)) {
            throw new IllegalStateException("Gateway outcome physical attempt identity mismatch");
        }
    }

    private static Aggregate aggregate(
            final List<GatewayPhysicalAttemptV1> attempts, final byte[] previousAggregateBytes) {
        byte[] queuedAggregate = null;
        if (previousAggregateBytes != null) {
            final SubmissionOutcomeMessageV1 previous = decodeAggregate(previousAggregateBytes);
            if (isQueued(previous)) {
                if (attempts.stream().noneMatch(attempt -> attempt.state() == GatewayPhysicalAttemptStateV1.QUEUED)) {
                    throw new IllegalStateException("Gateway queued aggregate has no queued attempt");
                }
                queuedAggregate = Bytes.copy(previousAggregateBytes);
            }
        }
        GatewayPhysicalAttemptV1 latestUncertain = null;
        GatewayPhysicalAttemptV1 latestDefinite = null;
        boolean hasStarted = false;
        for (GatewayPhysicalAttemptV1 attempt : attempts) {
            if (attempt.state() == GatewayPhysicalAttemptStateV1.QUEUED) {
                if (queuedAggregate == null) {
                    queuedAggregate = requireOutcomeBytes(attempt);
                }
            } else if (attempt.state() == GatewayPhysicalAttemptStateV1.STARTED) {
                hasStarted = true;
            } else if (attempt.state() == GatewayPhysicalAttemptStateV1.UNCERTAIN) {
                latestUncertain = attempt;
            } else if (attempt.state() == GatewayPhysicalAttemptStateV1.DEFINITELY_NOT_QUEUED) {
                latestDefinite = attempt;
            }
        }
        final GatewayIdempotencyPhaseV1 phase =
                hasStarted ? GatewayIdempotencyPhaseV1.ACTIVE : GatewayIdempotencyPhaseV1.QUIESCENT;
        if (queuedAggregate != null) {
            return new Aggregate(phase, queuedAggregate);
        }
        if (hasStarted) {
            return new Aggregate(phase, null);
        }
        if (latestUncertain != null) {
            return new Aggregate(phase, requireOutcomeBytes(latestUncertain));
        }
        return new Aggregate(phase, latestDefinite == null ? null : requireOutcomeBytes(latestDefinite));
    }

    private static byte[] requireOutcomeBytes(final GatewayPhysicalAttemptV1 attempt) {
        final byte[] bytes = attempt.outcomeBytes();
        if (bytes == null) {
            throw new IllegalStateException("Gateway terminal attempt has no outcome bytes");
        }
        return bytes;
    }

    private static SubmissionOutcomeMessageV1 decodeAggregate(final byte[] bytes) {
        try {
            return SubmissionOutcomeMessageV1.decode(bytes);
        } catch (RuntimeException malformed) {
            throw new IllegalStateException("Gateway aggregate outcome is malformed", malformed);
        }
    }

    private static boolean isQueued(final SubmissionOutcomeMessageV1 outcome) {
        return outcome.kind() == SubmissionOutcomeKindV1.NATIVE_RECEIPT
                || outcome.kind() == SubmissionOutcomeKindV1.MANAGED
                        && outcome.managed().kind() == EnqueueOutcomeKindV1.QUEUED;
    }

    private static void validateProjection(
            final GatewayIdempotencyPhaseV1 phase,
            final List<GatewayPhysicalAttemptV1> attempts,
            final byte[] aggregateBytes) {
        final Set<PhysicalEnqueueAttemptId> physicalIds = new HashSet<>();
        final Set<PhysicalEnqueueAttemptId> retryIds = new HashSet<>();
        boolean hasStarted = false;
        int startedCount = 0;
        boolean queuedSeen = false;
        for (int index = 0; index < attempts.size(); index++) {
            final GatewayPhysicalAttemptV1 attempt = attempts.get(index);
            if (attempt.attemptNo() != index + 1) {
                throw new IllegalArgumentException("Gateway attempts are not source ordered");
            }
            if (queuedSeen) {
                throw new IllegalArgumentException("Gateway queued attempt must be the final attempt");
            }
            if (!physicalIds.add(attempt.physicalAttemptId())) {
                throw new IllegalArgumentException("Gateway physical attempt identity is duplicated");
            }
            if (attempt.retryRequestId() != null && !retryIds.add(attempt.retryRequestId())) {
                throw new IllegalArgumentException("Gateway retry request identity is duplicated");
            }
            if (attempt.state() == GatewayPhysicalAttemptStateV1.STARTED) {
                startedCount++;
                if (index != attempts.size() - 1) {
                    throw new IllegalArgumentException("Gateway STARTED attempt must be the final attempt");
                }
                hasStarted = true;
            }
            if (attempt.state() == GatewayPhysicalAttemptStateV1.QUEUED) {
                queuedSeen = true;
            }
        }
        if (startedCount > 1) {
            throw new IllegalArgumentException("Gateway ACTIVE record must contain one STARTED attempt");
        }
        final GatewayIdempotencyPhaseV1 expectedPhase = attempts.isEmpty()
                ? GatewayIdempotencyPhaseV1.PREPARED
                : hasStarted ? GatewayIdempotencyPhaseV1.ACTIVE : GatewayIdempotencyPhaseV1.QUIESCENT;
        if (phase != expectedPhase) {
            throw new IllegalArgumentException("Gateway idempotency phase does not match attempts");
        }
        if (attempts.isEmpty() && aggregateBytes != null) {
            throw new IllegalArgumentException("Gateway prepared record cannot carry an aggregate");
        }
        if (!attempts.isEmpty() && !hasStarted && aggregateBytes == null) {
            throw new IllegalArgumentException("Gateway quiescent record must carry an aggregate");
        }
    }

    private void validateStoredProjection() {
        final PreparedSubmissionV1 prepared;
        try {
            prepared = PreparedSubmissionV1.decode(preparedSubmissionBytes);
        } catch (RuntimeException malformed) {
            throw new IllegalArgumentException("Gateway prepared submission is malformed", malformed);
        }
        validatePreparedOperation(prepared);
        for (int index = 0; index < attempts.size(); index++) {
            final GatewayPhysicalAttemptV1 attempt = attempts.get(index);
            validateRetryRequestHash(index, attempt);
            if (attempt.state() == GatewayPhysicalAttemptStateV1.STARTED) {
                continue;
            }
            final SubmissionOutcomeMessageV1 outcome = decodeAggregate(requireOutcomeBytes(attempt));
            validateOutcome(outcome, attempt.physicalAttemptId());
            if (stateFor(outcome) != attempt.state()) {
                throw new IllegalArgumentException("Gateway attempt state does not match outcome");
            }
        }
        final byte[] expectedAggregate = aggregate(attempts, null).outcomeBytes();
        if (!Arrays.equals(expectedAggregate, aggregateOutcomeBytes)) {
            throw new IllegalArgumentException("Gateway aggregate does not match attempt history");
        }
    }

    private void validatePreparedOperation(final PreparedSubmissionV1 prepared) {
        final boolean matches = prepared.isManaged()
                ? switch (preparedCommand(prepared).type()) {
                    case SCHEDULE -> operation == GatewayOperationKindV1.SCHEDULE;
                    case PREPARE_LARGE_SCHEDULE -> operation == GatewayOperationKindV1.PREPARE_LARGE_SCHEDULE;
                    case COMMIT_LARGE_SCHEDULE -> operation == GatewayOperationKindV1.COMMIT_LARGE_SCHEDULE;
                    case CANCEL -> operation == GatewayOperationKindV1.CANCEL;
                    case RESCHEDULE -> operation == GatewayOperationKindV1.RESCHEDULE;
                }
                : operation == GatewayOperationKindV1.SCHEDULE;
        if (!matches) {
            throw new IllegalArgumentException("Gateway operation does not match prepared submission");
        }
    }

    private static PreparedCommand preparedCommand(final PreparedSubmissionV1 prepared) {
        return CommandCodec.decodeFrameV1(prepared.managedFrame());
    }

    private void validateRetryRequestHash(final int attemptIndex, final GatewayPhysicalAttemptV1 attempt) {
        if (attempt.retryRequestId() == null) {
            return;
        }
        for (int priorIndex = 0; priorIndex < attemptIndex; priorIndex++) {
            final GatewayPhysicalAttemptV1 prior = attempts.get(priorIndex);
            final Digest32 expected = GatewayIdempotencyHashV1.retryRequestHash(
                    gatewayKeyHash, prior.physicalAttemptId(), attempt.retryRequestId());
            if (expected.equals(attempt.retryRequestHash())) {
                return;
            }
        }
        throw new IllegalArgumentException("Gateway retry request hash does not bind to an earlier attempt");
    }

    private static GatewayPhysicalAttemptStateV1 stateFor(final SubmissionOutcomeMessageV1 outcome) {
        return switch (outcome.kind()) {
            case MANAGED ->
                switch (outcome.managed().kind()) {
                    case QUEUED -> GatewayPhysicalAttemptStateV1.QUEUED;
                    case DEFINITELY_NOT_QUEUED -> GatewayPhysicalAttemptStateV1.DEFINITELY_NOT_QUEUED;
                    case ENQUEUE_UNCERTAIN -> GatewayPhysicalAttemptStateV1.UNCERTAIN;
                };
            case NATIVE_RECEIPT -> GatewayPhysicalAttemptStateV1.QUEUED;
            case NATIVE_DEFINITELY_NOT_QUEUED -> GatewayPhysicalAttemptStateV1.DEFINITELY_NOT_QUEUED;
            case NATIVE_ENQUEUE_UNCERTAIN -> GatewayPhysicalAttemptStateV1.UNCERTAIN;
        };
    }

    private static long checkedIncrement(final long value) {
        if (value <= 0 || value == Long.MAX_VALUE) {
            throw new IllegalStateException("Gateway idempotency revision is exhausted");
        }
        return value + 1;
    }

    private record Aggregate(GatewayIdempotencyPhaseV1 phase, byte[] outcomeBytes) {}

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

    private static CanonicalProtobuf.Reader.Field field(
            final List<CanonicalProtobuf.Reader.Field> fields, final int index, final int number) {
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

    record PhysicalEnqueueAttemptIdMatch(com.nereusstream.delay.transport.PhysicalEnqueueAttemptId id) {}
}
