package io.nereusstream.delay.protocol;

import java.util.Objects;

/** Closed Kafka/Pulsar Broker resource identity oneof. */
public final class BrokerResourceIdentityV1 {
    public enum Kind {
        KAFKA,
        PULSAR
    }

    private final KafkaBrokerResourceIdentityV1 kafka;
    private final PulsarBrokerResourceIdentityV1 pulsar;

    private BrokerResourceIdentityV1(final KafkaBrokerResourceIdentityV1 kafka,
                                     final PulsarBrokerResourceIdentityV1 pulsar) {
        if ((kafka == null) == (pulsar == null)) {
            throw new IllegalArgumentException("BrokerResourceIdentityV1 must select exactly one branch");
        }
        this.kafka = kafka;
        this.pulsar = pulsar;
    }

    public static BrokerResourceIdentityV1 kafka(final KafkaBrokerResourceIdentityV1 resource) {
        return new BrokerResourceIdentityV1(Objects.requireNonNull(resource, "resource"), null);
    }

    public static BrokerResourceIdentityV1 pulsar(final PulsarBrokerResourceIdentityV1 resource) {
        return new BrokerResourceIdentityV1(null, Objects.requireNonNull(resource, "resource"));
    }

    public Kind kind() {
        return kafka != null ? Kind.KAFKA : Kind.PULSAR;
    }

    public KafkaBrokerResourceIdentityV1 kafka() {
        if (kafka == null) {
            throw new IllegalStateException("Broker resource is not Kafka");
        }
        return kafka;
    }

    public PulsarBrokerResourceIdentityV1 pulsar() {
        if (pulsar == null) {
            throw new IllegalStateException("Broker resource is not Pulsar");
        }
        return pulsar;
    }

    public byte[] canonicalBytes() {
        return kafka != null ? kafka.canonicalBytes() : pulsar.canonicalBytes();
    }

    public static BrokerResourceIdentityV1 decode(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        if (!reader.hasRemaining()) {
            throw new IllegalArgumentException("BrokerResourceIdentityV1 is empty");
        }
        final CanonicalProtobuf.Reader.Field field = reader.next();
        if (reader.hasRemaining()) {
            throw new IllegalArgumentException("BrokerResourceIdentityV1 selects multiple branches");
        }
        return switch (field.number()) {
            case 1 -> kafka(KafkaBrokerResourceIdentityV1.decode(encoded));
            case 2 -> pulsar(PulsarBrokerResourceIdentityV1.decode(encoded));
            default -> throw new IllegalArgumentException("unknown BrokerResourceIdentityV1 branch");
        };
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof BrokerResourceIdentityV1 that && Objects.equals(kafka, that.kafka)
                && Objects.equals(pulsar, that.pulsar);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kafka, pulsar);
    }
}
