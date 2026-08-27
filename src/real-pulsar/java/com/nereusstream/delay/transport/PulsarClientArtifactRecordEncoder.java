package com.nereusstream.delay.transport;

import com.nereusstream.delay.protocol.PulsarKey;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.PulsarRecordTemplate;
import com.nereusstream.delay.protocol.PulsarSequenceAuthority;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;

/**
 * The only source-locked projection from a validated logical record to a P1
 * {@link TypedMessageBuilder}. The prepared-record envelope is never sent as
 * the business payload.
 */
public final class PulsarClientArtifactRecordEncoder {
    private PulsarClientArtifactRecordEncoder() {}

    /** Configures a fresh builder without invoking {@code sendAsync()}. */
    public static TypedMessageBuilder<byte[]> configure(
            final Producer<byte[]> producer, final PulsarPreparedRecord record) {
        Objects.requireNonNull(producer, "producer");
        final PulsarPreparedRecord exact = Objects.requireNonNull(record, "record");
        final PulsarRecordTemplate template = exact.template();
        final TypedMessageBuilder<byte[]> builder =
                Objects.requireNonNull(producer.newMessage(), "Pulsar producer returned no message builder");
        builder.value(exact.resolvedPayload().bytes());

        final PulsarKey key = template.key();
        switch (key.kind()) {
            case NONE -> {
                // Pulsar's builder has no explicit clear-key operation; a new
                // builder starts without a partition key.
            }
            case UTF8 -> builder.key(key.utf8Value());
            case BINARY -> builder.keyBytes(key.value());
        }
        final byte[] orderingKey = template.orderingKey();
        if (orderingKey != null) {
            builder.orderingKey(orderingKey);
        }

        final Map<String, String> properties = new LinkedHashMap<>();
        for (PulsarMetadata.Property property : template.callerProperties()) {
            putProperty(properties, property);
        }
        for (PulsarMetadata.Property property : exact.finalReservedProperties()) {
            putProperty(properties, property);
        }
        builder.properties(properties);

        if (template.eventTimeEpochMs() != null) {
            builder.eventTime(template.eventTimeEpochMs());
        }
        final PulsarSequenceAuthority authority = exact.sequenceAuthority();
        if (authority.kind() == PulsarSequenceAuthority.Kind.MANAGED_JOURNAL) {
            builder.sequenceId(authority.sequenceId());
        }
        if (template.deliveryContract().isNative()) {
            final Long deliverAt = template.nativeDeliverAtEpochMs();
            if (deliverAt == null || deliverAt != template.reservedMetadata().deliverAtEpochMs()) {
                throw new IllegalArgumentException("native record has no exact business deliverAt");
            }
            builder.deliverAt(deliverAt);
        }
        return builder;
    }

    /** Configures and sends the exact logical record through the P1 producer. */
    public static CompletableFuture<MessageId> send(
            final Producer<byte[]> producer, final PulsarPreparedRecord record) {
        return configure(producer, record).sendAsync();
    }

    private static void putProperty(final Map<String, String> properties, final PulsarMetadata.Property property) {
        Objects.requireNonNull(property, "Pulsar property");
        final String previous = properties.putIfAbsent(property.key(), property.value());
        if (previous != null) {
            throw new IllegalArgumentException("duplicate final Pulsar property: " + property.key());
        }
    }
}
