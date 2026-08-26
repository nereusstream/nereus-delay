package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.SourceActivationBarrier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Explicit successor proof for a Pulsar source connection reactivation.
 *
 * <p>A reconnect creates a new guarded source connection generation. That
 * generation is part of the immutable assignment barrier, so it cannot be
 * written back into the Route or the existing assignment in place. This
 * record binds the new assignment to the old assignment while keeping the
 * resource, physical topic, cursor, batch shape and resource attestation
 * immutable.</p>
 */
public record PulsarSourceReactivation(
        byte[] routeSnapshotDigest, SourceAssignment previousAssignment, SourceAssignment successorAssignment) {
    private static final int VERSION = 1;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-pulsar-source-reactivation\0");

    public PulsarSourceReactivation {
        routeSnapshotDigest = nonZero(routeSnapshotDigest, "routeSnapshotDigest");
        Objects.requireNonNull(previousAssignment, "previousAssignment");
        Objects.requireNonNull(successorAssignment, "successorAssignment");
        validateSuccessor(previousAssignment, successorAssignment);
    }

    @Override
    public byte[] routeSnapshotDigest() {
        return Bytes.copy(routeSnapshotDigest);
    }

    /** Returns the immutable old Pulsar barrier carried by the transition. */
    public PulsarActivationBarrier previousBarrier() {
        return pulsarBarrier(previousAssignment.activationBarrier(), "previousAssignment");
    }

    /** Returns the fresh guarded-connection barrier carried by the transition. */
    public PulsarActivationBarrier successorBarrier() {
        return pulsarBarrier(successorAssignment.activationBarrier(), "successorAssignment");
    }

    /** Canonical, digest-bound transition bytes stored or logged as evidence. */
    public byte[] canonicalBytes() {
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, routeSnapshotDigest);
            CanonicalProtobuf.bytes(output, 3, previousAssignment.canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, successorAssignment.canonicalBytes());
        });
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(body);
            CanonicalProtobuf.bytes(output, 5, Bytes.sha256(DIGEST_DOMAIN, body));
        });
    }

    /** Decodes one exact transition and rejects aliases or digest drift. */
    public static PulsarSourceReactivation decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = fields(encoded);
        requireNumbers(fields, 1, 2, 3, 4, 5);
        if (uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported PulsarSourceReactivation version");
        }
        final byte[] body = CanonicalProtobuf.message(output -> {
            for (int index = 0; index < 4; index++) {
                writeField(output, fields.get(index));
            }
        });
        final byte[] digest = fixed(fields.get(4), 5, 32);
        if (!Bytes.constantTimeEquals(digest, Bytes.sha256(DIGEST_DOMAIN, body))) {
            throw new IllegalArgumentException("Pulsar source reactivation digest mismatch");
        }
        final PulsarSourceReactivation result = new PulsarSourceReactivation(
                fixed(fields.get(1), 2, 32),
                SourceAssignment.decode(bytes(fields.get(2), 3)),
                SourceAssignment.decode(bytes(fields.get(3), 4)));
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical Pulsar source reactivation");
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PulsarSourceReactivation that
                && Arrays.equals(routeSnapshotDigest, that.routeSnapshotDigest)
                && previousAssignment.sameIdentity(that.previousAssignment)
                && successorAssignment.sameIdentity(that.successorAssignment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(routeSnapshotDigest), previousAssignment, successorAssignment);
    }

    private static void validateSuccessor(final SourceAssignment previous, final SourceAssignment successor) {
        if (!previous.shardId().equals(successor.shardId())) {
            throw new IllegalArgumentException("Pulsar source reactivation changed the shard");
        }
        if (Arrays.equals(previous.assignmentId(), successor.assignmentId())) {
            throw new IllegalArgumentException("Pulsar source reactivation reused the assignment id");
        }
        if (Long.compareUnsigned(successor.assignmentEpoch(), previous.assignmentEpoch()) <= 0) {
            throw new IllegalArgumentException("Pulsar source reactivation assignment epoch is not newer");
        }
        final PulsarActivationBarrier oldBarrier = pulsarBarrier(previous.activationBarrier(), "previousAssignment");
        final PulsarActivationBarrier newBarrier = pulsarBarrier(successor.activationBarrier(), "successorAssignment");
        if (!Arrays.equals(oldBarrier.brokerResourceIncarnation(), newBarrier.brokerResourceIncarnation())
                || !oldBarrier.physicalTopic().equals(newBarrier.physicalTopic())
                || oldBarrier.ledgerId() != newBarrier.ledgerId()
                || oldBarrier.entryId() != newBarrier.entryId()
                || oldBarrier.normalizedLastBatchIndex() != newBarrier.normalizedLastBatchIndex()
                || oldBarrier.batchSize() != newBarrier.batchSize()
                || !Arrays.equals(
                        oldBarrier.resourceGuardAttestationDigest(), newBarrier.resourceGuardAttestationDigest())
                || oldBarrier.empty() != newBarrier.empty()) {
            throw new IllegalArgumentException("Pulsar source reactivation changed immutable source identity");
        }
        if (oldBarrier.guardedSourceConnectionGeneration() == newBarrier.guardedSourceConnectionGeneration()) {
            throw new IllegalArgumentException("Pulsar source reactivation did not create a new connection generation");
        }
    }

    private static PulsarActivationBarrier pulsarBarrier(final SourceActivationBarrier barrier, final String name) {
        if (!(Objects.requireNonNull(barrier, name) instanceof PulsarActivationBarrier pulsar)) {
            throw new IllegalArgumentException(name + " is not a Pulsar activation barrier");
        }
        return pulsar;
    }

    private static byte[] nonZero(final byte[] value, final String name) {
        Bytes.requireLength(value, 32, name);
        for (byte element : value) {
            if (element != 0) {
                return Bytes.copy(value);
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }

    private static List<CanonicalProtobuf.Reader.Field> fields(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final List<CanonicalProtobuf.Reader.Field> result = new ArrayList<>();
        while (reader.hasRemaining()) {
            result.add(reader.next());
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException("Pulsar source reactivation is empty");
        }
        return result;
    }

    private static void requireNumbers(final List<CanonicalProtobuf.Reader.Field> fields, final int... numbers) {
        if (fields.size() != numbers.length) {
            throw new IllegalArgumentException("Pulsar source reactivation field count mismatch");
        }
        for (int index = 0; index < numbers.length; index++) {
            if (fields.get(index).number() != numbers[index]) {
                throw new IllegalArgumentException("Pulsar source reactivation field order mismatch");
            }
        }
    }

    private static void writeField(
            final java.io.ByteArrayOutputStream output, final CanonicalProtobuf.Reader.Field field) {
        if (field.wireType() == 0) {
            CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
        } else if (field.wireType() == 2) {
            CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
        } else {
            throw new IllegalArgumentException("unsupported Pulsar source reactivation field wire type");
        }
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid Pulsar source reactivation bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, length, "Pulsar source reactivation field " + number);
        return value;
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid Pulsar source reactivation uint field " + number);
        }
        return field.unsignedValue();
    }
}
