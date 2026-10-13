package com.nereusstream.delay.transport;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.EvidenceVerificationStatus;
import com.nereusstream.delay.protocol.ExternalDeliveryIdentity;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishEvidenceKind;
import com.nereusstream.delay.protocol.TargetKafkaReceiptMapping;
import com.nereusstream.delay.protocol.TargetKafkaReceiptWire;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** One actual guarded poll per caller-driven turn after same-ID fencing; the first response freezes the finite range. */
public final class KafkaClientArtifactTargetReceiptRecovery implements AutoCloseable {
    public enum Status { WAITING, ADVANCED, PUBLISHED, NOT_PUBLISHED, BUDGET_EXHAUSTED }
    public record Turn(Status status, long nextOffset, Observation observation) {}

    /** Private actual scan result; canonical/hash equality never substitutes for current independent recovery rights. */
    public static final class Observation {
        private final TargetKafkaReceiptMapping mapping;
        private final PublishEvidence evidence;
        private final byte[] image;
        private final Runnable rights;
        private Observation(TargetKafkaReceiptMapping mapping, PublishEvidence evidence, byte[] image, Runnable rights) {
            this.mapping = mapping; this.evidence = evidence; this.image = Bytes.copy(image); this.rights = rights;
            rights.run();
        }
        public TargetKafkaReceiptMapping mapping() { return mapping; }
        public PublishEvidence evidence() { return evidence; }
        public byte[] canonicalBytes() { return Bytes.copy(image); }
        public void requireCurrent(PublishEvidence actual) {
            if (!Arrays.equals(evidence.canonicalBytes(), actual.canonicalBytes())) {
                throw new IllegalArgumentException("recovery evidence differs from its actual fenced scan");
            }
            rights.run();
        }
    }

    private final TargetKafkaReceiptMapping mapping;
    private final KafkaClientArtifactTargetTransactionalFactory.Fence fence;
    private final KafkaClientArtifactTargetReceiptReader reader;
    private final Runnable rights;
    private final int maximumTurns;
    private int turns;
    private int segments;
    private long lastNextOffset;
    private byte[] scanDigest = Bytes.sha256(Bytes.utf8("nereus-delay-target-receipt-scan\0"));
    private Observation observation;
    private boolean closed;

    public KafkaClientArtifactTargetReceiptRecovery(TargetKafkaReceiptMapping mapping,
            KafkaClientArtifactTargetTransactionalTransport resource, Map<String, Object> readerConfiguration,
            KafkaClientArtifactTargetReceiptReader.Protection protection,
            KafkaClientArtifactTargetTransactionalFactory.Limits limits, int maximumTurns) {
        this.mapping = Objects.requireNonNull(mapping, "actual retained mapping");
        if (maximumTurns <= 0) { throw new IllegalArgumentException("recovery requires a finite positive turn bound"); }
        this.maximumTurns = maximumTurns;
        fence = resource.fence().orElseThrow(() -> new IllegalStateException("recovery lacks actual same-ID fencing"));
        rights = () -> {
            resource.fence().orElseThrow(() -> new IllegalStateException("fencing resource is unavailable")).requireCurrent(mapping);
            protection.requireCurrent(mapping, mapping.admission().kafkaReceipt().scanStartOffset());
        };
        rights.run();
        lastNextOffset = mapping.admission().kafkaReceipt().scanStartOffset();
        reader = new KafkaClientArtifactTargetReceiptReader(readerConfiguration, mapping,
                (actual, offset) -> { fence.requireCurrent(actual); protection.requireCurrent(actual, offset); },
                limits.pollTimeout(), limits.maximumRecords(), limits.maximumBytes(), true);
    }

    public synchronized Turn runTurn() {
        if (closed) { throw new IllegalStateException("receipt recovery is closed"); }
        try {
            rights.run();
            if (observation != null) { return finished(); }
            if (turns >= maximumTurns) {
                reader.close();
                return new Turn(Status.BUDGET_EXHAUSTED, lastNextOffset, null);
            }
            turns++;
            final var turn = reader.runTurn();
            final var response = reader.rangeObservation().orElse(null);
            if (turn.status() == KafkaClientArtifactTargetReceiptReader.Status.PUBLISHED) {
                final var actual = turn.observation();
                final var image = CanonicalProtobuf.message(out -> {
                    CanonicalProtobuf.uint32(out, 1, 1);
                    CanonicalProtobuf.bytes(out, 2, mapping.mappingId());
                    CanonicalProtobuf.bytes(out, 3, fence.canonicalBytes());
                    CanonicalProtobuf.bytes(out, 4, actual.evidence().canonicalBytes());
                    CanonicalProtobuf.bytes(out, 5, reader.rangeObservationImage().orElseThrow());
                });
                reader.close();
                observation = new Observation(mapping, actual.evidence(), image, rights);
                return finished();
            }
            if (response == null) { return new Turn(Status.WAITING, lastNextOffset, null); }
            final long next = reader.processedNextOffset();
            if (next < lastNextOffset || next > response.lastStableOffset()) {
                throw new IllegalStateException("fenced recovery lost its contiguous processed range");
            }
            final long start = lastNextOffset;
            final var segment = CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.int64(out, 1, start);
                CanonicalProtobuf.int64(out, 2, next);
                CanonicalProtobuf.bytes(out, 3, reader.rangeObservationImage().orElseThrow());
            });
            scanDigest = Bytes.sha256(scanDigest, Bytes.lp32(segment));
            segments++;
            lastNextOffset = next;
            final long upper = reader.recoveryUpperBound();
            // An actual empty range can complete only with successful fencing, fixed U, paired processing,
            // covering LSO and continuous retention/compaction/delete rights; timeout has no response.
            if (next >= upper && response.lastStableOffset() >= upper) {
                final var receipt = mapping.admission().kafkaReceipt();
                final var id = receipt.receiptResource().receiptTopicUuid();
                final var cursor = EvidenceCursor.targetKafka(TargetKafkaReceiptWire.scope(mapping),
                        ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array(),
                        receipt.receiptPartition(), mapping.admission().channel().context().evidenceGeneration(),
                        reader.recoveryMaxBrokerTime(), upper, response.lastStableOffset());
                final var p = mapping.admission().publication();
                final var evidence = PublishEvidence.create(PublishEvidenceKind.KAFKA_RECEIPT_ABSENCE,
                        EvidenceVerificationStatus.VERIFIED_NOT_PUBLISHED, CanonicalProtobuf.message(out -> {
                            CanonicalProtobuf.bytes(out, 1, cursor.canonicalBytes());
                            CanonicalProtobuf.bytes(out, 2, fence.channel().canonicalBytes());
                            CanonicalProtobuf.bytes(out, 3, ExternalDeliveryIdentity.publishAttempt(p.publishAttemptId()).canonicalBytes());
                            CanonicalProtobuf.bytes(out, 4, p.preparedPublishHash());
                            CanonicalProtobuf.bytes(out, 5, Bytes.sha256(fence.channel().context().producerIdentity()));
                        }));
                TargetKafkaReceiptWire.requireNotPublished(mapping, evidence);
                final var image = CanonicalProtobuf.message(out -> {
                    CanonicalProtobuf.uint32(out, 1, 2);
                    CanonicalProtobuf.bytes(out, 2, mapping.mappingId());
                    CanonicalProtobuf.bytes(out, 3, fence.canonicalBytes());
                    CanonicalProtobuf.int64(out, 4, receipt.scanStartOffset());
                    CanonicalProtobuf.int64(out, 5, upper);
                    CanonicalProtobuf.int64(out, 6, next);
                    CanonicalProtobuf.int64(out, 7, response.lastStableOffset());
                    CanonicalProtobuf.bytes(out, 8, scanDigest);
                    CanonicalProtobuf.uint32(out, 9, segments);
                    CanonicalProtobuf.bytes(out, 10, evidence.canonicalBytes());
                });
                reader.close();
                observation = new Observation(mapping, evidence, image, rights);
                return finished();
            }
            return new Turn(turn.status() == KafkaClientArtifactTargetReceiptReader.Status.ADVANCED
                    ? Status.ADVANCED : Status.WAITING, lastNextOffset, null);
        } catch (RuntimeException | Error failure) {
            try { close(); } catch (RuntimeException | Error closeFailure) { failure.addSuppressed(closeFailure); }
            throw failure;
        }
    }

    private Turn finished() {
        observation.requireCurrent(observation.evidence());
        return new Turn(observation.evidence().verificationStatus() == EvidenceVerificationStatus.VERIFIED_PUBLISHED
                ? Status.PUBLISHED : Status.NOT_PUBLISHED, lastNextOffset, observation);
    }

    public synchronized Optional<Observation> observation() { return Optional.ofNullable(observation); }
    @Override public synchronized void close() { closed = true; reader.close(); }
}
