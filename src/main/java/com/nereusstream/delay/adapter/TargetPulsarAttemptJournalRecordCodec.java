package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryContract;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

/** Independent Target Journal v1; legacy Lane Journal generation 3 remains byte-identical. */
public final class TargetPulsarAttemptJournalRecordCodec {
    public static final int VERSION = 1;
    public static final int MAX_CANONICAL_BYTES = TargetChannelIdentity.MAX_CANONICAL_BYTES
            + CanonicalTargetPartition.MAX_CANONICAL_BYTES + TargetSourcePosition.MAX_CANONICAL_BYTES + 512;

    public static final int SETTLEMENT_VERSION = 2;
    public static final int MAX_SETTLEMENT_BYTES = MAX_CANONICAL_BYTES
            + com.nereusstream.delay.protocol.TargetJournalNegativeSettlement.MAX_CANONICAL_BYTES + 5;

    private TargetPulsarAttemptJournalRecordCodec() {}

    static byte[] mappingBody(final PulsarAttemptJournal.Mapping mapping) {
        final var p = mapping.producer();
        if (!p.isTarget() || !mapping.isCurrentGeneration()) {
            throw new IllegalArgumentException("Target Journal requires its typed current Producer/mapping");
        }
        final var t = p.target();
        final var physical = new CanonicalTargetPartition(
                com.nereusstream.delay.protocol.BrokerResourceIdentity.pulsar(
                        new com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity(
                                t.authenticatedClusterId(), t.resourceIncarnation(), t.physicalTopic(),
                                t.physicalTopicCreationTimestamp())), Integer.toUnsignedLong(t.partition()));
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 3, p.targetChannel().canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, physical.canonicalBytes());
            CanonicalProtobuf.uint64(out, 5, mapping.sequenceId());
            CanonicalProtobuf.bytes(out, 6, mapping.delayMessageId().bytes());
            CanonicalProtobuf.uint32Bits(out, 7, mapping.generation());
            CanonicalProtobuf.bytes(out, 8, mapping.publishAttemptId());
            CanonicalProtobuf.bytes(out, 9, mapping.preparedPublishHash());
            CanonicalProtobuf.bytes(out, 10, mapping.recordTemplateHash());
            CanonicalProtobuf.uint32(out, 11, mapping.deliveryContract().wireValue());
            CanonicalProtobuf.bytes(out, 12, mapping.sourcePosition());
            CanonicalProtobuf.bytes(out, 13, mapping.artifactGenerationSetDigest());
        });
    }

    public static byte[] encode(final PulsarAttemptJournal.AppendRequest request) {
        Objects.requireNonNull(request, "request");
        final byte[] result = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, request.settlement() == null ? VERSION : SETTLEMENT_VERSION);
            CanonicalProtobuf.uint32(out, 2, request.kind().wireValue());
            out.writeBytes(mappingBody(request.mapping()));
            CanonicalProtobuf.bytes(out, 14, request.mapping().mappingId());
            if (request.settlement() != null) { CanonicalProtobuf.bytes(out, 15, request.settlement().canonicalBytes()); }
        });
        if (result.length > (request.settlement() == null ? MAX_CANONICAL_BYTES : MAX_SETTLEMENT_BYTES)) {
            throw new IllegalArgumentException("Target Journal record exceeds its byte bound");
        }
        return result;
    }

    public static PulsarAttemptJournal.JournalRecord decode(
            final byte[] encoded, final PulsarAttemptJournal.JournalPosition position) {
        if (encoded == null || encoded.length > MAX_SETTLEMENT_BYTES) {
            throw new IllegalArgumentException("Target Journal record exceeds its byte bound");
        }
        final var fields = new ArrayList<CanonicalProtobuf.Reader.Field>(14);
        final var reader = new CanonicalProtobuf.Reader(encoded);
        while (reader.hasRemaining()) {
            if (fields.size() == 15) {
                throw new IllegalArgumentException("Target Journal record exceeds its field bound");
            }
            fields.add(reader.next());
        }
        final long version = QueryCodecSupport.uint32(fields.getFirst(), 1);
        final boolean settlementRecord = version == SETTLEMENT_VERSION;
        QueryCodecSupport.requireNumbers(fields, settlementRecord
                ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
                : new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, "Target Pulsar Journal");
        if (!settlementRecord && (version != VERSION || encoded.length > MAX_CANONICAL_BYTES)) {
            throw new IllegalArgumentException("unsupported Target Journal version/bound");
        }
        final long kindNumber = QueryCodecSupport.uint32(fields.get(1), 2);
        final var kind = Arrays.stream(PulsarAttemptJournal.RecordKind.values())
                .filter(k -> k.wireValue() == kindNumber && (settlementRecord
                        ? k == PulsarAttemptJournal.RecordKind.RESOLVED_NOT_PUBLISHED : k.wireValue() <= 4))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("unsupported Target Journal kind/version"));
        final var settlement = settlementRecord ? com.nereusstream.delay.protocol.TargetJournalNegativeSettlement
                .decode(QueryCodecSupport.bytes(fields.get(14), 15)) : null;
        final var channel = TargetChannelIdentity.decode(QueryCodecSupport.bytes(fields.get(2), 3));
        final var physical = CanonicalTargetPartition.decode(QueryCodecSupport.bytes(fields.get(3), 4));
        final var source = TargetSourcePosition.decode(QueryCodecSupport.bytes(fields.get(11), 12));
        final var identity = new PulsarAttemptJournal.CurrentAttemptIdentity(
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(5), 6, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint32Bits(fields.get(6), 7), QueryCodecSupport.fixed(fields.get(7), 8, 32),
                QueryCodecSupport.fixed(fields.get(8), 9, 32), QueryCodecSupport.fixed(fields.get(9), 10, 32),
                DeliveryContract.fromWire(QueryCodecSupport.uint32(fields.get(10), 11)), source.canonicalBytes(),
                QueryCodecSupport.fixed(fields.get(12), 13, 32));
        final var mapping = PulsarAttemptJournal.Mapping.createCurrent(
                channel.context().sourceShard(), PulsarAttemptJournal.ProducerKey.target(channel, physical),
                QueryCodecSupport.uint(fields.get(4), 5), identity);
        if (!Bytes.constantTimeEquals(mapping.mappingId(), QueryCodecSupport.fixed(fields.get(13), 14, 32))) {
            throw new IllegalArgumentException("Target Journal mapping ID differs from its closed body");
        }
        final var result = new PulsarAttemptJournal.JournalRecord(kind, mapping,
                Objects.requireNonNull(position, "position"), settlement);
        QueryCodecSupport.requireCanonical(encoded, encode(new PulsarAttemptJournal.AppendRequest(kind, mapping, settlement)),
                "Target Pulsar Journal");
        return result;
    }
}
