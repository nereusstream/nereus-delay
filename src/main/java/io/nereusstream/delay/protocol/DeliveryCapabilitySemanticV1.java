package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Immutable destination delivery-capability semantic value from Registry §5.1.1.
 *
 * <p>The surrounding ProfileSemanticEnvelope supplies the profile identity and
 * semantic hash. This value owns the adapter/capability/evidence prerequisite
 * invariants and has a strict canonical representation.</p>
 */
public final class DeliveryCapabilitySemanticV1 implements ProfileSemanticBodyV1 {
    public static final int ADAPTER_CONFORMANCE_VERSION_MIN = 1;
    public static final int REJECTION_CLASSIFIER_VERSION_MIN = 1;
    public static final int HASH_LENGTH = 32;

    private final AdapterKindV1 adapterKind;
    private final OutcomeCapabilityV1 outcomeCapability;
    private final int timingCapabilityBits;
    private final BrokerResourceIdentityV1 evidenceResource;
    private final int evidencePartitionCount;
    private final long minimumEvidenceRetentionMs;
    private final long minimumDedupHorizonMs;
    private final long maximumCertifiedProducerKeys;
    private final byte[] brokerPrerequisiteDigest;
    private final byte[] sourceLockDigest;
    private final int adapterConformanceVersion;
    private final int rejectionClassifierVersion;

    public DeliveryCapabilitySemanticV1(final AdapterKindV1 adapterKind,
                                        final OutcomeCapabilityV1 outcomeCapability,
                                        final int timingCapabilityBits,
                                        final BrokerResourceIdentityV1 evidenceResource,
                                        final int evidencePartitionCount,
                                        final long minimumEvidenceRetentionMs,
                                        final long minimumDedupHorizonMs,
                                        final long maximumCertifiedProducerKeys,
                                        final byte[] brokerPrerequisiteDigest,
                                        final byte[] sourceLockDigest,
                                        final int adapterConformanceVersion,
                                        final int rejectionClassifierVersion) {
        this.adapterKind = Objects.requireNonNull(adapterKind, "adapterKind");
        this.outcomeCapability = Objects.requireNonNull(outcomeCapability, "outcomeCapability");
        TimingCapabilityV1.requireValid(timingCapabilityBits);
        if (adapterKind == AdapterKindV1.KAFKA
                && (timingCapabilityBits & (TimingCapabilityV1.PULSAR_GUARDED_HANDOFF
                | TimingCapabilityV1.PULSAR_AUTO_FAST)) != 0) {
            throw new IllegalArgumentException("Kafka capability cannot carry Pulsar timing bits");
        }
        if (adapterKind == AdapterKindV1.PULSAR && outcomeCapability == OutcomeCapabilityV1.KAFKA_TRANSACTIONAL_RECEIPT) {
            throw new IllegalArgumentException("Kafka transactional capability requires a Kafka adapter");
        }
        if (adapterKind == AdapterKindV1.KAFKA && outcomeCapability == OutcomeCapabilityV1.PULSAR_BROKER_DEDUP) {
            throw new IllegalArgumentException("Pulsar dedup capability requires a Pulsar adapter");
        }
        this.timingCapabilityBits = timingCapabilityBits;
        this.evidenceResource = evidenceResource;
        if (evidencePartitionCount < 0 || minimumEvidenceRetentionMs < 0 || minimumDedupHorizonMs < 0
                || maximumCertifiedProducerKeys < 0) {
            throw new IllegalArgumentException("evidence capability bounds must be non-negative");
        }
        this.evidencePartitionCount = evidencePartitionCount;
        this.minimumEvidenceRetentionMs = minimumEvidenceRetentionMs;
        this.minimumDedupHorizonMs = minimumDedupHorizonMs;
        this.maximumCertifiedProducerKeys = maximumCertifiedProducerKeys;
        this.brokerPrerequisiteDigest = fixed(brokerPrerequisiteDigest, "brokerPrerequisiteDigest");
        this.sourceLockDigest = fixed(sourceLockDigest, "sourceLockDigest");
        if (outcomeCapability == OutcomeCapabilityV1.AT_LEAST_ONCE) {
            if (evidenceResource != null || evidencePartitionCount != 0 || minimumEvidenceRetentionMs != 0
                    || minimumDedupHorizonMs != 0 || maximumCertifiedProducerKeys != 0
                    || adapterConformanceVersion != 0 || rejectionClassifierVersion != 0) {
                throw new IllegalArgumentException("baseline capability cannot carry evidence prerequisites");
            }
        } else {
            if (evidenceResource == null || evidencePartitionCount <= 0 || minimumEvidenceRetentionMs <= 0
                    || minimumDedupHorizonMs <= 0 || maximumCertifiedProducerKeys <= 0
                    || adapterConformanceVersion < ADAPTER_CONFORMANCE_VERSION_MIN
                    || rejectionClassifierVersion < REJECTION_CLASSIFIER_VERSION_MIN) {
                throw new IllegalArgumentException("strong capability requires evidence prerequisites");
            }
            final BrokerResourceIdentityV1.Kind expected = outcomeCapability
                    == OutcomeCapabilityV1.KAFKA_TRANSACTIONAL_RECEIPT
                    ? BrokerResourceIdentityV1.Kind.KAFKA : BrokerResourceIdentityV1.Kind.PULSAR;
            if (evidenceResource.kind() != expected) {
                throw new IllegalArgumentException("evidence resource does not match outcome capability");
            }
        }
        this.adapterConformanceVersion = adapterConformanceVersion;
        this.rejectionClassifierVersion = rejectionClassifierVersion;
    }

    public AdapterKindV1 adapterKind() {
        return adapterKind;
    }

    public OutcomeCapabilityV1 outcomeCapability() {
        return outcomeCapability;
    }

    public int timingCapabilityBits() {
        return timingCapabilityBits;
    }

    public BrokerResourceIdentityV1 evidenceResource() {
        return evidenceResource;
    }

    public int evidencePartitionCount() {
        return evidencePartitionCount;
    }

    public long minimumEvidenceRetentionMs() {
        return minimumEvidenceRetentionMs;
    }

    public long minimumDedupHorizonMs() {
        return minimumDedupHorizonMs;
    }

    public long maximumCertifiedProducerKeys() {
        return maximumCertifiedProducerKeys;
    }

    public byte[] brokerPrerequisiteDigest() {
        return Bytes.copy(brokerPrerequisiteDigest);
    }

    public byte[] sourceLockDigest() {
        return Bytes.copy(sourceLockDigest);
    }

    public int adapterConformanceVersion() {
        return adapterConformanceVersion;
    }

    public int rejectionClassifierVersion() {
        return rejectionClassifierVersion;
    }

    public boolean requiresEvidenceResource() {
        return outcomeCapability != OutcomeCapabilityV1.AT_LEAST_ONCE;
    }

    @Override
    public ProfileKindV1 profileKind() {
        return ProfileKindV1.DELIVERY_CAPABILITY;
    }

    @Override
    public int schemaVersion() {
        return 1;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, adapterKind.wireValue());
            CanonicalProtobuf.uint32(output, 2, outcomeCapability.wireValue());
            CanonicalProtobuf.uint32(output, 3, timingCapabilityBits);
            if (evidenceResource != null) {
                CanonicalProtobuf.bytes(output, 4, evidenceResource.canonicalBytes());
            }
            CanonicalProtobuf.uint32(output, 5, evidencePartitionCount);
            CanonicalProtobuf.uint64(output, 6, minimumEvidenceRetentionMs);
            CanonicalProtobuf.uint64(output, 7, minimumDedupHorizonMs);
            CanonicalProtobuf.uint64(output, 8, maximumCertifiedProducerKeys);
            CanonicalProtobuf.bytes(output, 9, brokerPrerequisiteDigest);
            CanonicalProtobuf.bytes(output, 10, sourceLockDigest);
            CanonicalProtobuf.uint32(output, 11, adapterConformanceVersion);
            CanonicalProtobuf.uint32(output, 12, rejectionClassifierVersion);
        });
    }

    public static DeliveryCapabilitySemanticV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(
                encoded, "DeliveryCapabilitySemanticV1");
        if (fields.size() != 11 && fields.size() != 12) {
            throw new IllegalArgumentException("DeliveryCapabilitySemanticV1 has an unexpected field count");
        }
        final boolean hasEvidence = fields.size() == 12;
        final int offset = hasEvidence ? 1 : 0;
        if (hasEvidence && fields.get(3).number() != 4) {
            throw new IllegalArgumentException("invalid DeliveryCapabilitySemanticV1 evidence field");
        }
        final DeliveryCapabilitySemanticV1 result = new DeliveryCapabilitySemanticV1(
                AdapterKindV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1)),
                OutcomeCapabilityV1.fromWire(QueryCodecSupport.uint(fields.get(1), 2)),
                QueryCodecSupport.uint32(fields.get(2), 3),
                hasEvidence ? BrokerResourceIdentityV1.decode(QueryCodecSupport.nested(fields.get(3), 4)) : null,
                QueryCodecSupport.uint32(fields.get(3 + offset), 5),
                QueryCodecSupport.uint(fields.get(4 + offset), 6),
                QueryCodecSupport.uint(fields.get(5 + offset), 7),
                QueryCodecSupport.uint(fields.get(6 + offset), 8),
                QueryCodecSupport.fixed(fields.get(7 + offset), 9, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(8 + offset), 10, HASH_LENGTH),
                QueryCodecSupport.uint32(fields.get(9 + offset), 11),
                QueryCodecSupport.uint32(fields.get(10 + offset), 12));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "DeliveryCapabilitySemanticV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof DeliveryCapabilitySemanticV1 that
                && adapterKind == that.adapterKind && outcomeCapability == that.outcomeCapability
                && timingCapabilityBits == that.timingCapabilityBits
                && Objects.equals(evidenceResource, that.evidenceResource)
                && evidencePartitionCount == that.evidencePartitionCount
                && minimumEvidenceRetentionMs == that.minimumEvidenceRetentionMs
                && minimumDedupHorizonMs == that.minimumDedupHorizonMs
                && maximumCertifiedProducerKeys == that.maximumCertifiedProducerKeys
                && Arrays.equals(brokerPrerequisiteDigest, that.brokerPrerequisiteDigest)
                && Arrays.equals(sourceLockDigest, that.sourceLockDigest)
                && adapterConformanceVersion == that.adapterConformanceVersion
                && rejectionClassifierVersion == that.rejectionClassifierVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(adapterKind, outcomeCapability, timingCapabilityBits, evidenceResource,
                evidencePartitionCount, minimumEvidenceRetentionMs, minimumDedupHorizonMs,
                maximumCertifiedProducerKeys, Arrays.hashCode(brokerPrerequisiteDigest),
                Arrays.hashCode(sourceLockDigest), adapterConformanceVersion, rejectionClassifierVersion);
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        if (allZero(value)) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return Bytes.copy(value);
    }

    private static boolean allZero(final byte[] value) {
        for (byte element : value) {
            if (element != 0) {
                return false;
            }
        }
        return true;
    }
}
