package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Closed adapter-metadata union; free-form metadata maps are not allowed. */
public final class AdapterMetadata {
    public enum Kind {
        KAFKA(1),
        PULSAR(2);

        private final int wireValue;

        Kind(final int wireValue) {
            this.wireValue = wireValue;
        }

        public int wireValue() {
            return wireValue;
        }

        private static Kind fromWire(final long value) {
            for (Kind kind : values()) {
                if (kind.wireValue == value) {
                    return kind;
                }
            }
            throw new IllegalArgumentException("unknown AdapterMetadata branch: " + value);
        }
    }

    private final Kind kind;
    private final KafkaMetadata kafka;
    private final PulsarMetadata pulsar;

    private AdapterMetadata(final Kind kind, final KafkaMetadata kafka, final PulsarMetadata pulsar) {
        this.kind = Objects.requireNonNull(kind, "kind");
        if ((kind == Kind.KAFKA) == (kafka == null) || (kind == Kind.PULSAR) == (pulsar == null)) {
            throw new IllegalArgumentException("AdapterMetadata branch does not match payload");
        }
        this.kafka = kafka;
        this.pulsar = pulsar;
    }

    public static AdapterMetadata kafka(final KafkaMetadata metadata) {
        return new AdapterMetadata(Kind.KAFKA, Objects.requireNonNull(metadata, "metadata"), null);
    }

    public static AdapterMetadata pulsar(final PulsarMetadata metadata) {
        return new AdapterMetadata(Kind.PULSAR, null, Objects.requireNonNull(metadata, "metadata"));
    }

    public Kind kind() {
        return kind;
    }

    public KafkaMetadata kafka() {
        if (kind != Kind.KAFKA) {
            throw new IllegalStateException("metadata branch is not Kafka");
        }
        return kafka;
    }

    public PulsarMetadata pulsar() {
        if (kind != Kind.PULSAR) {
            throw new IllegalStateException("metadata branch is not Pulsar");
        }
        return pulsar;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(
                output, kind.wireValue(), kind == Kind.KAFKA ? kafka.canonicalBytes() : pulsar.canonicalBytes()));
    }

    public static AdapterMetadata decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "AdapterMetadata");
        if (fields.size() != 1) {
            throw new IllegalArgumentException("AdapterMetadata must select exactly one branch");
        }
        final Kind kind = Kind.fromWire(fields.get(0).number());
        final AdapterMetadata result = kind == Kind.KAFKA
                ? kafka(KafkaMetadata.decode(QueryCodecSupport.nested(fields.get(0), kind.wireValue())))
                : pulsar(PulsarMetadata.decode(QueryCodecSupport.nested(fields.get(0), kind.wireValue())));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "AdapterMetadata");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof AdapterMetadata that
                && kind == that.kind
                && Objects.equals(kafka, that.kafka)
                && Objects.equals(pulsar, that.pulsar);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, kafka, pulsar);
    }
}
