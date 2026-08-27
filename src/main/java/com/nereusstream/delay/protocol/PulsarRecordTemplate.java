package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Admission-frozen Pulsar record projection. Journal mapping, sequence
 * authority, final reserved metadata and resolved payload bytes are excluded.
 */
public final class PulsarRecordTemplate {
    public static final int SCHEMA_GENERATION = 1;
    public static final int HASH_LENGTH = 32;
    private static final String HASH_DOMAIN = "nereus-delay-pulsar-record-template\0";

    private final BrokerResourceIdentity targetResource;
    private final long physicalPartition;
    private final PulsarKey key;
    private final byte[] orderingKey;
    private final List<PulsarMetadata.Property> callerProperties;
    private final Long eventTimeEpochMs;
    private final ReservedPublishMetadata reservedMetadata;
    private final DeliveryContract deliveryContract;
    private final Long nativeDeliverAtEpochMs;
    private final PayloadForPublish payload;
    private final byte[] artifactGenerationSetDigest;

    public PulsarRecordTemplate(
            final BrokerResourceIdentity targetResource,
            final long physicalPartition,
            final PulsarKey key,
            final byte[] orderingKey,
            final List<PulsarMetadata.Property> callerProperties,
            final Long eventTimeEpochMs,
            final ReservedPublishMetadata reservedMetadata,
            final DeliveryContract deliveryContract,
            final Long nativeDeliverAtEpochMs,
            final PayloadForPublish payload,
            final byte[] artifactGenerationSetDigest) {
        this.targetResource = Objects.requireNonNull(targetResource, "targetResource");
        if (targetResource.kind() != BrokerResourceIdentity.Kind.PULSAR) {
            throw new IllegalArgumentException("PulsarRecordTemplate requires a Pulsar target");
        }
        if (physicalPartition < 0 || physicalPartition > 0xffff_ffffL) {
            throw new IllegalArgumentException("physicalPartition is outside uint32 range");
        }
        this.physicalPartition = physicalPartition;
        this.key = Objects.requireNonNull(key, "key");
        this.orderingKey = orderingKey == null ? null : Bytes.copy(orderingKey);
        this.callerProperties = sortedProperties(callerProperties);
        if (eventTimeEpochMs != null && eventTimeEpochMs < 0) {
            throw new IllegalArgumentException("eventTime must be non-negative");
        }
        this.eventTimeEpochMs = eventTimeEpochMs;
        this.reservedMetadata = Objects.requireNonNull(reservedMetadata, "reservedMetadata");
        this.deliveryContract = Objects.requireNonNull(deliveryContract, "deliveryContract");
        if (deliveryContract.isNative()) {
            if (nativeDeliverAtEpochMs == null || nativeDeliverAtEpochMs < 0) {
                throw new IllegalArgumentException("native contract requires a business deliverAt");
            }
            if (nativeDeliverAtEpochMs != reservedMetadata.deliverAtEpochMs()) {
                throw new IllegalArgumentException("native deliverAt disagrees with reserved metadata");
            }
        } else if (nativeDeliverAtEpochMs != null) {
            throw new IllegalArgumentException("managed contract cannot carry native deliverAt");
        }
        this.nativeDeliverAtEpochMs = nativeDeliverAtEpochMs;
        this.payload = Objects.requireNonNull(payload, "payload");
        this.artifactGenerationSetDigest = fixed(artifactGenerationSetDigest, "artifactGenerationSetDigest");
    }

    public BrokerResourceIdentity targetResource() {
        return targetResource;
    }

    public long physicalPartition() {
        return physicalPartition;
    }

    public PulsarKey key() {
        return key;
    }

    public byte[] orderingKey() {
        return orderingKey == null ? null : Bytes.copy(orderingKey);
    }

    public List<PulsarMetadata.Property> callerProperties() {
        return callerProperties;
    }

    public Long eventTimeEpochMs() {
        return eventTimeEpochMs;
    }

    public ReservedPublishMetadata reservedMetadata() {
        return reservedMetadata;
    }

    public DeliveryContract deliveryContract() {
        return deliveryContract;
    }

    public Long nativeDeliverAtEpochMs() {
        return nativeDeliverAtEpochMs;
    }

    public PayloadForPublish payload() {
        return payload;
    }

    public byte[] artifactGenerationSetDigest() {
        return Bytes.copy(artifactGenerationSetDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, SCHEMA_GENERATION);
            CanonicalProtobuf.bytes(output, 2, targetResource.canonicalBytes());
            CanonicalProtobuf.uint32(output, 3, physicalPartition);
            CanonicalProtobuf.bytes(output, 4, key.canonicalBytes());
            if (orderingKey != null) {
                CanonicalProtobuf.bytes(output, 5, orderingKey);
            }
            for (PulsarMetadata.Property property : callerProperties) {
                CanonicalProtobuf.bytes(output, 6, property.canonicalBytes());
            }
            if (eventTimeEpochMs != null) {
                CanonicalProtobuf.int64(output, 7, eventTimeEpochMs);
            }
            CanonicalProtobuf.bytes(output, 8, reservedMetadata.canonicalBytes());
            CanonicalProtobuf.uint32(output, 9, deliveryContract.wireValue());
            if (nativeDeliverAtEpochMs != null) {
                CanonicalProtobuf.int64(output, 10, nativeDeliverAtEpochMs);
            }
            CanonicalProtobuf.bytes(output, 11, payload.canonicalBytes());
            CanonicalProtobuf.bytes(output, 12, artifactGenerationSetDigest);
        });
    }

    public byte[] recordTemplateHash() {
        return Bytes.sha256(Bytes.utf8(HASH_DOMAIN), canonicalBytes());
    }

    public static PulsarRecordTemplate decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "PulsarRecordTemplate", true);
        // The minimal legal shape omits ordering key, caller properties and
        // event time: 1,2,3,4,8,9,11,12.
        if (fields.size() < 8) {
            throw new IllegalArgumentException("PulsarRecordTemplate is incomplete");
        }
        int index = 0;
        if (QueryCodecSupport.uint(fields.get(index), 1) != SCHEMA_GENERATION) {
            throw new IllegalArgumentException("unsupported PulsarRecordTemplate generation");
        }
        index++;
        final BrokerResourceIdentity target =
                BrokerResourceIdentity.decode(QueryCodecSupport.nested(fields.get(index++), 2));
        final long partition = QueryCodecSupport.uint(fields.get(index++), 3);
        final PulsarKey key = PulsarKey.decode(QueryCodecSupport.nested(fields.get(index++), 4));
        byte[] orderingKey = null;
        if (index < fields.size() && fields.get(index).number() == 5) {
            orderingKey = QueryCodecSupport.bytes(fields.get(index++), 5);
        }
        final List<PulsarMetadata.Property> properties = new ArrayList<>();
        while (index < fields.size() && fields.get(index).number() == 6) {
            properties.add(PulsarPropertyCodec.decode(QueryCodecSupport.nested(fields.get(index++), 6)));
        }
        Long eventTime = null;
        if (index < fields.size() && fields.get(index).number() == 7) {
            eventTime = QueryCodecSupport.uint(fields.get(index++), 7);
        }
        final ReservedPublishMetadata reserved =
                ReservedPublishMetadata.decode(QueryCodecSupport.nested(fields.get(index++), 8));
        final DeliveryContract contract = DeliveryContract.fromWire(QueryCodecSupport.uint(fields.get(index++), 9));
        Long nativeDeliverAt = null;
        if (index < fields.size() && fields.get(index).number() == 10) {
            nativeDeliverAt = QueryCodecSupport.uint(fields.get(index++), 10);
        }
        final PayloadForPublish payload = PayloadForPublish.decode(QueryCodecSupport.nested(fields.get(index++), 11));
        final byte[] artifactDigest = QueryCodecSupport.fixed(fields.get(index++), 12, HASH_LENGTH);
        if (index != fields.size()) {
            throw new IllegalArgumentException("PulsarRecordTemplate field order mismatch");
        }
        final PulsarRecordTemplate result = new PulsarRecordTemplate(
                target,
                partition,
                key,
                orderingKey,
                properties,
                eventTime,
                reserved,
                contract,
                nativeDeliverAt,
                payload,
                artifactDigest);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PulsarRecordTemplate");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PulsarRecordTemplate that
                && physicalPartition == that.physicalPartition
                && targetResource.equals(that.targetResource)
                && key.equals(that.key)
                && Arrays.equals(orderingKey, that.orderingKey)
                && callerProperties.equals(that.callerProperties)
                && Objects.equals(eventTimeEpochMs, that.eventTimeEpochMs)
                && reservedMetadata.equals(that.reservedMetadata)
                && deliveryContract == that.deliveryContract
                && Objects.equals(nativeDeliverAtEpochMs, that.nativeDeliverAtEpochMs)
                && payload.equals(that.payload)
                && Arrays.equals(artifactGenerationSetDigest, that.artifactGenerationSetDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                targetResource,
                physicalPartition,
                key,
                Arrays.hashCode(orderingKey),
                callerProperties,
                eventTimeEpochMs,
                reservedMetadata,
                deliveryContract,
                nativeDeliverAtEpochMs,
                payload,
                Arrays.hashCode(artifactGenerationSetDigest));
    }

    private static List<PulsarMetadata.Property> sortedProperties(final List<PulsarMetadata.Property> values) {
        Objects.requireNonNull(values, "callerProperties");
        final List<PulsarMetadata.Property> copy = new ArrayList<>(values);
        for (PulsarMetadata.Property property : copy) {
            Objects.requireNonNull(property, "caller property");
        }
        for (int index = 1; index < copy.size(); index++) {
            if (compareUnsigned(copy.get(index - 1).keyUtf8(), copy.get(index).keyUtf8()) >= 0) {
                throw new IllegalArgumentException("Pulsar caller properties must be sorted and unique");
            }
        }
        return Collections.unmodifiableList(copy);
    }

    private static int compareUnsigned(final byte[] left, final byte[] right) {
        return Arrays.compareUnsigned(left, right);
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static final class PulsarPropertyCodec {
        private static PulsarMetadata.Property decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "PulsarProperty");
            QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "PulsarProperty");
            final byte[] key = QueryCodecSupport.bytes(fields.get(0), 1);
            final byte[] value = QueryCodecSupport.bytes(fields.get(1), 2);
            final PulsarMetadata.Property property = new PulsarMetadata.Property(key, value);
            QueryCodecSupport.requireCanonical(encoded, property.canonicalBytes(), "PulsarProperty");
            return property;
        }
    }
}
