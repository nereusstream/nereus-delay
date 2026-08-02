package io.nereusstream.delay.protocol;

import java.util.Objects;

/** Closed adapter-metadata union; free-form metadata maps are not allowed. */
public final class AdapterMetadataV1 {
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
            throw new IllegalArgumentException("unknown AdapterMetadataV1 branch: " + value);
        }
    }

    private final Kind kind;
    private final KafkaMetadataV1 kafka;
    private final PulsarMetadataV1 pulsar;

    private AdapterMetadataV1(final Kind kind, final KafkaMetadataV1 kafka, final PulsarMetadataV1 pulsar) {
        this.kind = Objects.requireNonNull(kind, "kind");
        if ((kind == Kind.KAFKA) == (kafka == null) || (kind == Kind.PULSAR) == (pulsar == null)) {
            throw new IllegalArgumentException("AdapterMetadataV1 branch does not match payload");
        }
        this.kafka = kafka;
        this.pulsar = pulsar;
    }

    public static AdapterMetadataV1 kafka(final KafkaMetadataV1 metadata) {
        return new AdapterMetadataV1(Kind.KAFKA, Objects.requireNonNull(metadata, "metadata"), null);
    }

    public static AdapterMetadataV1 pulsar(final PulsarMetadataV1 metadata) {
        return new AdapterMetadataV1(Kind.PULSAR, null, Objects.requireNonNull(metadata, "metadata"));
    }

    public Kind kind() {
        return kind;
    }

    public KafkaMetadataV1 kafka() {
        if (kind != Kind.KAFKA) {
            throw new IllegalStateException("metadata branch is not Kafka");
        }
        return kafka;
    }

    public PulsarMetadataV1 pulsar() {
        if (kind != Kind.PULSAR) {
            throw new IllegalStateException("metadata branch is not Pulsar");
        }
        return pulsar;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, kind.wireValue(),
                kind == Kind.KAFKA ? kafka.canonicalBytes() : pulsar.canonicalBytes()));
    }

    public static AdapterMetadataV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "AdapterMetadataV1");
        if (fields.size() != 1) {
            throw new IllegalArgumentException("AdapterMetadataV1 must select exactly one branch");
        }
        final Kind kind = Kind.fromWire(fields.get(0).number());
        final AdapterMetadataV1 result = kind == Kind.KAFKA
                ? kafka(KafkaMetadataV1.decode(QueryCodecSupport.nested(fields.get(0), kind.wireValue())))
                : pulsar(PulsarMetadataV1.decode(QueryCodecSupport.nested(fields.get(0), kind.wireValue())));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "AdapterMetadataV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof AdapterMetadataV1 that && kind == that.kind
                && Objects.equals(kafka, that.kafka) && Objects.equals(pulsar, that.pulsar);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, kafka, pulsar);
    }
}
