package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryContract;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Closed physical payload codec for current Pulsar Attempt Journal records. */
public final class PulsarAttemptJournalRecordCodec {
    private static final int HASH_LENGTH = 32;
    private static final int LANE_INCARNATION_LENGTH = 16;

    private PulsarAttemptJournalRecordCodec() {}

    /** Encodes one append request without its Broker-assigned Journal position. */
    public static byte[] encode(final PulsarAttemptJournal.AppendRequest request) {
        final PulsarAttemptJournal.AppendRequest exact = Objects.requireNonNull(request, "request");
        final PulsarAttemptJournal.Mapping mapping = exact.mapping();
        if (!mapping.isCurrentGeneration()) {
            throw new IllegalArgumentException("physical Attempt Journal accepts only current mappings");
        }
        final PulsarAttemptJournal.ProducerKey producer = mapping.producer();
        final PulsarTargetResource target = producer.target();
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, ArtifactGenerationSet.ATTEMPT_JOURNAL_GENERATION);
            CanonicalProtobuf.uint32(output, 2, exact.kind().wireValue());
            CanonicalProtobuf.bytes(
                    output, 3, mapping.shard().routeIncarnation().bytes());
            CanonicalProtobuf.uint32Bits(output, 4, mapping.shard().partition());
            CanonicalProtobuf.bytes(output, 5, producer.laneId().bytes());
            CanonicalProtobuf.bytes(output, 6, producer.laneIncarnation());
            CanonicalProtobuf.bytes(output, 7, producer.stableProducerNameHash());
            CanonicalProtobuf.bytes(output, 8, Bytes.utf8(target.authenticatedClusterId()));
            CanonicalProtobuf.bytes(output, 9, target.resourceIncarnation());
            CanonicalProtobuf.bytes(output, 10, Bytes.utf8(target.physicalTopic()));
            CanonicalProtobuf.uint64Bits(output, 11, target.physicalTopicCreationTimestamp());
            CanonicalProtobuf.uint32Bits(output, 12, target.partition());
            CanonicalProtobuf.uint64(output, 13, mapping.sequenceId());
            CanonicalProtobuf.bytes(output, 14, mapping.delayMessageId().bytes());
            CanonicalProtobuf.uint32Bits(output, 15, mapping.generation());
            CanonicalProtobuf.bytes(output, 16, mapping.publishAttemptId());
            CanonicalProtobuf.bytes(output, 17, mapping.preparedPublishHash());
            CanonicalProtobuf.bytes(output, 18, mapping.recordTemplateHash());
            CanonicalProtobuf.uint32(output, 19, mapping.deliveryContract().wireValue());
            CanonicalProtobuf.bytes(output, 20, mapping.sourcePosition());
            CanonicalProtobuf.bytes(output, 21, mapping.artifactGenerationSetDigest());
            CanonicalProtobuf.bytes(output, 22, mapping.mappingId());
        });
    }

    /** Decodes one Broker record and binds the externally observed durable position. */
    public static PulsarAttemptJournal.JournalRecord decode(
            final byte[] encoded, final PulsarAttemptJournal.JournalPosition position) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "PulsarAttemptJournalRecord");
        QueryCodecSupport.requireNumbers(
                fields,
                new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22},
                "PulsarAttemptJournalRecord");
        if (QueryCodecSupport.uint(fields.get(0), 1) != ArtifactGenerationSet.ATTEMPT_JOURNAL_GENERATION) {
            throw new IllegalArgumentException("unsupported Pulsar Attempt Journal generation");
        }
        final PulsarAttemptJournal.RecordKind kind = recordKind(QueryCodecSupport.uint(fields.get(1), 2));
        final ShardId shard = new ShardId(
                new RouteIncarnation(QueryCodecSupport.fixed(fields.get(2), 3, RouteIncarnation.LENGTH)),
                QueryCodecSupport.uint32Bits(fields.get(3), 4));
        final PulsarTargetResource target = new PulsarTargetResource(
                text(fields.get(7), 8),
                QueryCodecSupport.fixed(fields.get(8), 9, HASH_LENGTH),
                text(fields.get(9), 10),
                QueryCodecSupport.uint64Bits(fields.get(10), 11),
                QueryCodecSupport.uint32Bits(fields.get(11), 12));
        final PulsarAttemptJournal.ProducerKey producer = new PulsarAttemptJournal.ProducerKey(
                new DestinationLaneId(QueryCodecSupport.fixed(fields.get(4), 5, HASH_LENGTH)),
                QueryCodecSupport.fixed(fields.get(5), 6, LANE_INCARNATION_LENGTH),
                QueryCodecSupport.fixed(fields.get(6), 7, HASH_LENGTH),
                target);
        final long sequenceId = QueryCodecSupport.uint64Bits(fields.get(12), 13);
        if (sequenceId < 0) {
            throw new IllegalArgumentException("Attempt Journal sequence exceeds the supported domain");
        }
        final PulsarAttemptJournal.CurrentAttemptIdentity identity = new PulsarAttemptJournal.CurrentAttemptIdentity(
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(13), 14, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint32Bits(fields.get(14), 15),
                QueryCodecSupport.fixed(fields.get(15), 16, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(16), 17, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(17), 18, HASH_LENGTH),
                DeliveryContract.fromWire(QueryCodecSupport.uint(fields.get(18), 19)),
                QueryCodecSupport.bytes(fields.get(19), 20),
                QueryCodecSupport.fixed(fields.get(20), 21, HASH_LENGTH));
        final PulsarAttemptJournal.Mapping mapping =
                PulsarAttemptJournal.Mapping.createCurrent(shard, producer, sequenceId, identity);
        if (!Arrays.equals(mapping.mappingId(), QueryCodecSupport.fixed(fields.get(21), 22, HASH_LENGTH))) {
            throw new IllegalArgumentException("Attempt Journal mapping ID does not match its closed body");
        }
        final PulsarAttemptJournal.JournalRecord record =
                new PulsarAttemptJournal.JournalRecord(kind, mapping, Objects.requireNonNull(position, "position"));
        QueryCodecSupport.requireCanonical(
                encoded,
                encode(new PulsarAttemptJournal.AppendRequest(record.kind(), record.mapping())),
                "PulsarAttemptJournalRecord");
        return record;
    }

    private static PulsarAttemptJournal.RecordKind recordKind(final long wireValue) {
        for (PulsarAttemptJournal.RecordKind kind : PulsarAttemptJournal.RecordKind.values()) {
            if (kind.wireValue() == wireValue) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown Pulsar Attempt Journal record kind: " + wireValue);
    }

    private static String text(final CanonicalProtobuf.Reader.Field field, final int number) {
        return new String(QueryCodecSupport.bytes(field, number), StandardCharsets.UTF_8);
    }
}
