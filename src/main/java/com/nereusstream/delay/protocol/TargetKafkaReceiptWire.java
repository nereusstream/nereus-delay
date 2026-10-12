package com.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** Independent Target receipt namespace. A mapping is intent; only a same-transaction committed receipt proves publication. */
public final class TargetKafkaReceiptWire {
    private static final byte[] KEY_DOMAIN = Bytes.utf8("nereus-delay-target-kafka-receipt-key\0");
    private static final byte[] VALUE_DOMAIN = Bytes.utf8("nereus-delay-target-kafka-receipt-value\0");
    private static final byte[] RECORD_DOMAIN = Bytes.utf8("nereus-delay-target-kafka-receipt-wire-record\0");
    private TargetKafkaReceiptWire() {}

    /** One immutable key per mapping keeps compaction from overwriting an earlier unresolved attempt. */
    public static byte[] key(final TargetKafkaReceiptMapping mapping) { return Bytes.sha256(KEY_DOMAIN, mapping.mappingId()); }
    public static byte[] value(final TargetKafkaReceiptMapping mapping) {
        final var p = mapping.admission().publication();
        return Bytes.concat(VALUE_DOMAIN, CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, mapping.mappingId());
            CanonicalProtobuf.uint64Bits(out, 3, mapping.logicalSequence());
            CanonicalProtobuf.bytes(out, 4, p.publishAttemptId());
            CanonicalProtobuf.bytes(out, 5, p.preparedPublishHash());
            CanonicalProtobuf.bytes(out, 6, mapping.mutation().source().canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, Bytes.sha256(p.channel().context().producerIdentity()));
            CanonicalProtobuf.bytes(out, 8, Bytes.sha256(mapping.admission().kafkaReceipt().canonicalBytes()));
        }));
    }
    public static byte[] recordHash(final TargetKafkaReceiptMapping mapping) {
        return Bytes.sha256(RECORD_DOMAIN, Bytes.lp32(key(mapping)), Bytes.lp32(value(mapping)));
    }
    public static void requireRecord(final TargetKafkaReceiptMapping mapping, final byte[] key, final byte[] value) {
        if (!Arrays.equals(key(mapping), key) || !Arrays.equals(value(mapping), value)) {
            throw new IllegalArgumentException("Target receipt changes exact mapping/key/value namespace");
        }
    }
    public static EvidenceCursor.TargetScope scope(final TargetKafkaReceiptMapping mapping) {
        final var c = mapping.admission().channel().context();
        return new EvidenceCursor.TargetScope(c.sourceShard(), c.target(), c.domain(), c.accountingIncarnation(),
                Bytes.sha256(c.producerIdentity()));
    }
    public static void requireCursor(final TargetKafkaReceiptMapping mapping, final EvidenceCursor cursor) {
        final var r = mapping.admission().kafkaReceipt();
        final byte[] uuid = ByteBuffer.allocate(16).putLong(r.receiptResource().receiptTopicUuid().getMostSignificantBits())
                .putLong(r.receiptResource().receiptTopicUuid().getLeastSignificantBits()).array();
        if (!cursor.isTarget() || cursor.evidenceKind() != EvidenceKind.KAFKA_RECEIPT_CONTIGUOUS
                || !Arrays.equals(scope(mapping).canonicalBytes(), cursor.targetScope().canonicalBytes())
                || !Arrays.equals(uuid, cursor.topicUuid()) || cursor.physicalPartition() != r.receiptPartition()
                || cursor.evidenceGeneration() != mapping.admission().channel().context().evidenceGeneration()) {
            throw new IllegalArgumentException("Target receipt cursor changes its frozen resource/recovery scope");
        }
    }
    public static PublishEvidence published(final TargetKafkaReceiptMapping mapping, final EvidenceCursor cursor, final long offset) {
        requireCursor(mapping, cursor);
        if (offset < mapping.admission().kafkaReceipt().scanStartOffset()
                || cursor.nextOffsetExclusive() <= offset || cursor.lastObservedLsoExclusive() <= offset) {
            throw new IllegalArgumentException("committed Target receipt is outside its protected cursor/LSO");
        }
        final var p = mapping.admission().publication();
        return PublishEvidence.create(PublishEvidenceKind.KAFKA_TRANSACTIONAL_RECEIPT,
                EvidenceVerificationStatus.VERIFIED_PUBLISHED, CanonicalProtobuf.message(out -> {
                    CanonicalProtobuf.bytes(out, 1, cursor.canonicalBytes());
                    CanonicalProtobuf.uint64(out, 2, offset);
                    CanonicalProtobuf.bytes(out, 3, ExternalDeliveryIdentity.publishAttempt(p.publishAttemptId()).canonicalBytes());
                    CanonicalProtobuf.bytes(out, 4, p.preparedPublishHash());
                    CanonicalProtobuf.bytes(out, 5, p.physical().resource().canonicalBytes());
                    CanonicalProtobuf.uint32(out, 6, p.physical().physicalPartition());
                    CanonicalProtobuf.bytes(out, 7, Bytes.sha256(p.channel().context().producerIdentity()));
                    CanonicalProtobuf.bytes(out, 8, recordHash(mapping));
                }));
    }
    public static void requirePublished(final TargetKafkaReceiptMapping mapping, final PublishEvidence evidence) {
        if (evidence.evidenceKind() != PublishEvidenceKind.KAFKA_TRANSACTIONAL_RECEIPT) {
            throw new IllegalArgumentException("Target K2 cannot downgrade to a Produce ACK");
        }
        final var f = QueryCodecSupport.read(evidence.branch(), "Target receipt evidence");
        final var cursor = EvidenceCursor.decode(QueryCodecSupport.nested(f.getFirst(), 1));
        final long offset = QueryCodecSupport.uint(f.get(1), 2);
        if (!Arrays.equals(evidence.canonicalBytes(), published(mapping, cursor, offset).canonicalBytes())) {
            throw new IllegalArgumentException("Target receipt evidence differs from exact retained mapping");
        }
    }
    public static void requireNotPublished(final TargetKafkaReceiptMapping mapping, final PublishEvidence evidence) {
        if (evidence.evidenceKind() != PublishEvidenceKind.KAFKA_RECEIPT_ABSENCE) {
            throw new IllegalArgumentException("Target K2 negative result requires protected transaction/receipt absence");
        }
        evidence.requireOrdinaryTargetNotPublishedBinding(mapping.admission().publication());
        final var f = QueryCodecSupport.read(evidence.branch(), "Target receipt absence");
        requireCursor(mapping, EvidenceCursor.decode(QueryCodecSupport.nested(f.getFirst(), 1)));
    }
}
