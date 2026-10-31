package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.TargetOrdinaryPublishRequest;
import com.nereusstream.delay.protocol.Bytes;
import java.util.ArrayList;
import java.util.Objects;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

/** The exact ordinary Target projection shared by baseline and transactional K1 sends. */
public final class KafkaClientArtifactTargetRecordEncoder {
    private KafkaClientArtifactTargetRecordEncoder() {}

    public static ProducerRecord<byte[], byte[]> configure(
            final String physicalTopic, final TargetOrdinaryPublishRequest request) {
        final var p = Objects.requireNonNull(request, "request").publication();
        final var metadata = p.adapterMetadata().kafka();
        final var reserved = p.reservedMetadata();
        final ArrayList<Header> headers = new ArrayList<>(metadata.headers().size() + 9);
        for (var header : metadata.headers()) {
            headers.add(new RecordHeader(header.name(), header.value()));
        }
        headers.add(new RecordHeader(
                "nereus.delay.route", reserved.routeIncarnation().bytes()));
        headers.add(new RecordHeader("nereus.delay.partition", Bytes.u32be(reserved.shardPartition())));
        headers.add(
                new RecordHeader("nereus.delay.message_id", reserved.messageId().bytes()));
        headers.add(new RecordHeader("nereus.delay.generation", Bytes.u32be(reserved.generation())));
        headers.add(new RecordHeader("nereus.delay.attempt_id", p.publishAttemptId()));
        headers.add(
                new RecordHeader("nereus.delay.destination_profile_hash", reserved.destinationProfileSemanticHash()));
        headers.add(new RecordHeader("nereus.delay.capability_profile_hash", reserved.capabilityProfileSemanticHash()));
        headers.add(new RecordHeader("nereus.delay.deliver_at", Bytes.i64be(reserved.deliverAtEpochMs())));
        headers.add(new RecordHeader("nereus.delay.prepared_hash", p.preparedPublishHash()));
        return new ProducerRecord<>(
                Objects.requireNonNull(physicalTopic, "physicalTopic"),
                Math.toIntExact(p.physical().physicalPartition()),
                p.eventTimeEpochMs(),
                metadata.key(),
                request.resolved().bytes(),
                headers);
    }
}
