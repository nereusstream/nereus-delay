package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.RouteSnapshot;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

/** Fixed-size lineage depth: the signed Route source root and the last authority-CAS transition. */
public record PulsarSourceReactivationReceipt(
        SourceAssignment routeSourceRoot,
        long previousPublicationRevision,
        byte[] previousPublicationDigest,
        WorkerAssignment previousAssignment,
        WorkerAssignment successorAssignment) {
    private static final int MAX_BYTES = 256 * 1024;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-pulsar-source-reactivation-receipt\0");

    public PulsarSourceReactivationReceipt {
        Objects.requireNonNull(routeSourceRoot);
        Objects.requireNonNull(previousAssignment);
        Objects.requireNonNull(successorAssignment);
        if (previousPublicationRevision <= 0 || previousPublicationRevision == Long.MAX_VALUE) {
            throw new IllegalArgumentException("reactivation needs a finite previous publication revision");
        }
        Bytes.requireLength(previousPublicationDigest, 32, "previous publication digest");
        previousPublicationDigest = Bytes.copy(previousPublicationDigest);
        if (!previousAssignment.routeBound() || !successorAssignment.routeBound()
                || !Bytes.constantTimeEquals(previousAssignment.routeSnapshotDigest(), successorAssignment.routeSnapshotDigest())
                || !previousAssignment.workerId().equals(successorAssignment.workerId())
                || !Arrays.equals(previousAssignment.capacityEnvelopeDigest(), successorAssignment.capacityEnvelopeDigest())
                || Long.compareUnsigned(successorAssignment.placementEpoch(), previousAssignment.placementEpoch()) <= 0) {
            throw new IllegalArgumentException("reactivation changed the Worker or Route/capacity binding");
        }
        new PulsarSourceReactivation(previousAssignment.routeSnapshotDigest(),
                previousAssignment.sourceAssignment(), successorAssignment.sourceAssignment());
        requireStableRoot(routeSourceRoot, previousAssignment.sourceAssignment());
        requireStableRoot(routeSourceRoot, successorAssignment.sourceAssignment());
    }

    @Override public byte[] previousPublicationDigest() { return Bytes.copy(previousPublicationDigest); }

    @Override public boolean equals(Object other) {
        return other instanceof PulsarSourceReactivationReceipt that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override public int hashCode() { return Arrays.hashCode(canonicalBytes()); }

    static PulsarSourceReactivationReceipt next(WorkerAssignmentAuthority.Publication previous, WorkerAssignment next) {
        return new PulsarSourceReactivationReceipt(previous.reactivation() == null
                ? previous.assignment().sourceAssignment() : previous.reactivation().routeSourceRoot(),
                previous.revision(), previous.digest(), previous.assignment(), next);
    }

    void requireSuccessor(long revision, WorkerAssignment assignment) {
        if (revision != Math.addExact(previousPublicationRevision, 1) || !successorAssignment.sameIdentity(assignment)) {
            throw new IllegalArgumentException("reactivation receipt does not name this authority publication");
        }
    }

    void requireRoute(RouteSnapshot route) {
        if (!Bytes.constantTimeEquals(route.snapshotDigest(), successorAssignment.routeSnapshotDigest())
                || !RouteSourceAssignmentFactory.fromRoute(route, routeSourceRoot.shardId().partition(),
                        routeSourceRoot.assignmentId(), routeSourceRoot.assignmentEpoch()).sameIdentity(routeSourceRoot)) {
            throw new IllegalStateException("reactivation receipt differs from its signed Route root");
        }
    }

    public byte[] canonicalBytes() {
        final byte[] body = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, routeSourceRoot.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 3, previousPublicationRevision);
            CanonicalProtobuf.bytes(out, 4, previousPublicationDigest);
            CanonicalProtobuf.bytes(out, 5, previousAssignment.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, successorAssignment.canonicalBytes());
        });
        final byte[] encoded = CanonicalProtobuf.message(out -> {
            out.writeBytes(body);
            CanonicalProtobuf.bytes(out, 7, Bytes.sha256(DOMAIN, body));
        });
        if (encoded.length > MAX_BYTES) throw new IllegalArgumentException("reactivation receipt exceeds its bound");
        return encoded;
    }

    public static PulsarSourceReactivationReceipt decode(byte[] encoded) {
        if (encoded == null || encoded.length == 0 || encoded.length > MAX_BYTES) {
            throw new IllegalArgumentException("reactivation receipt exceeds its bound");
        }
        final var reader = new CanonicalProtobuf.Reader(encoded);
        final var fields = new ArrayList<CanonicalProtobuf.Reader.Field>(7);
        while (reader.hasRemaining()) {
            if (fields.size() == 7) throw new IllegalArgumentException("extra reactivation receipt field");
            final var field = reader.next();
            if (field.number() != fields.size() + 1 || field.wireType() != (field.number() == 1 || field.number() == 3 ? 0 : 2)) {
                throw new IllegalArgumentException("non-canonical reactivation receipt field");
            }
            fields.add(field);
        }
        if (fields.size() != 7 || fields.get(0).unsignedValue() != 1) {
            throw new IllegalArgumentException("unsupported reactivation receipt version or field count");
        }
        final var result = new PulsarSourceReactivationReceipt(SourceAssignment.decode(fields.get(1).rawValue()),
                fields.get(2).unsignedValue(), fields.get(3).rawValue(),
                WorkerAssignment.decode(fields.get(4).rawValue()), WorkerAssignment.decode(fields.get(5).rawValue()));
        if (!Arrays.equals(encoded, result.canonicalBytes())) throw new IllegalArgumentException("reactivation receipt digest or encoding changed");
        return result;
    }

    private static void requireStableRoot(SourceAssignment root, SourceAssignment assignment) {
        if (!root.shardId().equals(assignment.shardId())
                || !(root.activationBarrier() instanceof PulsarActivationBarrier original)
                || !(assignment.activationBarrier() instanceof PulsarActivationBarrier current)) {
            throw new IllegalArgumentException("reactivation lost its Pulsar Route source root");
        }
        final var projected = new PulsarActivationBarrier(original.shardId(), original.brokerResourceIncarnation(),
                original.physicalTopic(), original.ledgerId(), original.entryId(), original.normalizedLastBatchIndex(),
                original.batchSize(), current.guardedSourceConnectionGeneration(), original.resourceGuardAttestationDigest(), original.empty());
        if (!projected.equals(current) || Long.compareUnsigned(assignment.assignmentEpoch(), root.assignmentEpoch()) < 0) {
            throw new IllegalArgumentException("reactivation changed its stable Route barrier");
        }
    }
}
