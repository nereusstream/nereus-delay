package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.Bytes;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Objects;

/**
 * Exact target-plus-receipt input for one Kafka transaction.
 *
 * <p>The transport receives both records as one closed request. It cannot
 * send the target through one producer and the receipt through another
 * producer without violating the V1 atomicity contract.</p>
 */
public record KafkaTransactionalDestinationRequest(
        String targetPhysicalTopic,
        KafkaDestinationRequest target,
        String receiptPhysicalTopic,
        KafkaReceiptResource receiptResource,
        KafkaReceiptJournal.Mapping mapping,
        byte[] receiptKey,
        byte[] receiptValue) {
    private static final byte[] KEY_DOMAIN = Bytes.utf8("nereus-delay-kafka-receipt-key-v1\0");
    private static final byte[] VALUE_DOMAIN = Bytes.utf8("nereus-delay-kafka-receipt-value-v1\0");
    private static final byte[] RECORD_HASH_DOMAIN = Bytes.utf8("nereus-delay-kafka-receipt-wire-record-v1\0");

    public KafkaTransactionalDestinationRequest {
        targetPhysicalTopic = canonicalText(targetPhysicalTopic, "targetPhysicalTopic");
        Objects.requireNonNull(target, "target");
        receiptPhysicalTopic = canonicalText(receiptPhysicalTopic, "receiptPhysicalTopic");
        Objects.requireNonNull(receiptResource, "receiptResource");
        Objects.requireNonNull(mapping, "mapping");
        Bytes.requireLength(receiptKey, 32, "receiptKey");
        Objects.requireNonNull(receiptValue, "receiptValue");
        if (receiptValue.length == 0) {
            throw new IllegalArgumentException("receiptValue must not be empty");
        }
        if (!mapping.producer().target().authenticatedClusterId().equals(target.authenticatedClusterId())
                || !mapping.producer().target().nativeTopicUuid().equals(target.nativeTopicUuid())
                || mapping.producer().target().partition() != target.partition()) {
            throw new IllegalArgumentException("target request does not match receipt mapping producer identity");
        }
        if (!target.authenticatedClusterId().equals(receiptResource.authenticatedClusterId())
                || mapping.shard().partition() != receiptResource.shardPartition()
                || !mapping.shard().routeIncarnation().equals(receiptResource.routeIncarnation())) {
            throw new IllegalArgumentException("target and receipt resources do not share the mapping identity");
        }
        receiptKey = Bytes.copy(receiptKey);
        receiptValue = Bytes.copy(receiptValue);
    }

    /** Creates the canonical keyed receipt record for the exact mapping. */
    public static KafkaTransactionalDestinationRequest create(
            final String targetPhysicalTopic,
            final KafkaDestinationRequest target,
            final String receiptPhysicalTopic,
            final KafkaReceiptResource receiptResource,
            final KafkaReceiptJournal.Mapping mapping) {
        Objects.requireNonNull(mapping, "mapping");
        final byte[] targetTopicUuid = uuidBytes(mapping.producer().target().nativeTopicUuid());
        final byte[] receiptTopicUuid = uuidBytes(receiptResource.nativeTopicUuid());
        final byte[] laneId = mapping.producer().laneId().bytes();
        final byte[] laneIncarnation = mapping.producer().laneIncarnation();
        final byte[] transactionIdentity = mapping.producer().transactionalIdentitySha256();
        final byte[] receiptKey = Bytes.sha256(
                KEY_DOMAIN,
                laneId,
                laneIncarnation,
                transactionIdentity,
                targetTopicUuid,
                Bytes.u32beBits(mapping.producer().target().partition()),
                receiptTopicUuid,
                Bytes.u32beBits(receiptResource.receiptPartition()));
        final byte[] receiptValue = Bytes.concat(
                VALUE_DOMAIN,
                Bytes.u32be(1),
                Bytes.u64be(mapping.sequenceId()),
                Bytes.lp32(mapping.publishAttemptId()),
                Bytes.lp32(mapping.preparedPublishHash()),
                targetTopicUuid,
                Bytes.u32beBits(mapping.producer().target().partition()),
                receiptTopicUuid,
                Bytes.u32beBits(receiptResource.receiptPartition()),
                laneId,
                laneIncarnation,
                transactionIdentity,
                mapping.mappingId());
        return new KafkaTransactionalDestinationRequest(
                targetPhysicalTopic, target, receiptPhysicalTopic, receiptResource, mapping, receiptKey, receiptValue);
    }

    @Override
    public byte[] receiptKey() {
        return Bytes.copy(receiptKey);
    }

    @Override
    public byte[] receiptValue() {
        return Bytes.copy(receiptValue);
    }

    /**
     * Returns the canonical digest of the exact keyed receipt record sent by
     * this request. The physical reader uses the same digest after a guarded
     * read_committed Fetch, so a typed receipt evidence branch cannot be built
     * from a receipt offset alone.
     */
    public byte[] canonicalReceiptRecordHash() {
        return canonicalReceiptRecordHash(receiptKey, receiptValue);
    }

    /** Computes the canonical digest for one keyed receipt record. */
    public static byte[] canonicalReceiptRecordHash(final byte[] key, final byte[] value) {
        Bytes.requireLength(key, 32, "receiptRecordKey");
        Objects.requireNonNull(value, "receiptRecordValue");
        if (value.length == 0) {
            throw new IllegalArgumentException("receiptRecordValue must not be empty");
        }
        return Bytes.sha256(RECORD_HASH_DOMAIN, Bytes.lp32(key), Bytes.lp32(value));
    }

    private static byte[] uuidBytes(final java.util.UUID value) {
        return ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        final String decoded = new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        if (!decoded.equals(value)
                || value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }
}
