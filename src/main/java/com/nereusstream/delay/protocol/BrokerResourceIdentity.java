package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Closed Kafka/Pulsar Broker resource identity oneof. */
public final class BrokerResourceIdentity {
    public enum Kind {
        KAFKA,
        PULSAR
    }

    private final KafkaBrokerResourceIdentity kafka;
    private final PulsarBrokerResourceIdentity pulsar;

    private BrokerResourceIdentity(final KafkaBrokerResourceIdentity kafka, final PulsarBrokerResourceIdentity pulsar) {
        if ((kafka == null) == (pulsar == null)) {
            throw new IllegalArgumentException("BrokerResourceIdentity must select exactly one branch");
        }
        this.kafka = kafka;
        this.pulsar = pulsar;
    }

    public static BrokerResourceIdentity kafka(final KafkaBrokerResourceIdentity resource) {
        return new BrokerResourceIdentity(Objects.requireNonNull(resource, "resource"), null);
    }

    public static BrokerResourceIdentity pulsar(final PulsarBrokerResourceIdentity resource) {
        return new BrokerResourceIdentity(null, Objects.requireNonNull(resource, "resource"));
    }

    public Kind kind() {
        return kafka != null ? Kind.KAFKA : Kind.PULSAR;
    }

    public KafkaBrokerResourceIdentity kafka() {
        if (kafka == null) {
            throw new IllegalStateException("Broker resource is not Kafka");
        }
        return kafka;
    }

    public PulsarBrokerResourceIdentity pulsar() {
        if (pulsar == null) {
            throw new IllegalStateException("Broker resource is not Pulsar");
        }
        return pulsar;
    }

    public byte[] canonicalBytes() {
        return kafka != null ? kafka.canonicalBytes() : pulsar.canonicalBytes();
    }

    public static BrokerResourceIdentity decode(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        if (!reader.hasRemaining()) {
            throw new IllegalArgumentException("BrokerResourceIdentity is empty");
        }
        final CanonicalProtobuf.Reader.Field field = reader.next();
        if (reader.hasRemaining()) {
            throw new IllegalArgumentException("BrokerResourceIdentity selects multiple branches");
        }
        return switch (field.number()) {
            case 1 -> kafka(KafkaBrokerResourceIdentity.decode(encoded));
            case 2 -> pulsar(PulsarBrokerResourceIdentity.decode(encoded));
            default -> throw new IllegalArgumentException("unknown BrokerResourceIdentity branch");
        };
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof BrokerResourceIdentity that
                && Objects.equals(kafka, that.kafka)
                && Objects.equals(pulsar, that.pulsar);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kafka, pulsar);
    }
}
