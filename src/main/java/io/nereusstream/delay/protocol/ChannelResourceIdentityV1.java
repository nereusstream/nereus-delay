package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical lane-scoped destination channel identity and credential lease binding. */
public final class ChannelResourceIdentityV1 {
    public static final int HASH_LENGTH = 32;
    public static final int LANE_INCARNATION_LENGTH = 16;

    private final AdapterKindV1 adapterKind;
    private final ChannelKindV1 channelKind;
    private final byte[] destinationLaneId;
    private final byte[] laneIncarnation;
    private final BrokerResourceIdentityV1 targetResource;
    private final long physicalPartition;
    private final long channelGeneration;
    private final long channelSlot;
    private final byte[] producerOrTransactionalIdentity;
    private final byte[] producerOrTransactionalIdentitySha256;
    private final BrokerResourceIdentityV1 evidenceResource;
    private final Long evidenceGeneration;
    private final byte[] resourceGuardAttestationDigest;
    private final long credentialBindingGeneration;
    private final byte[] credentialBindingDigest;
    private final byte[] resolvedCredentialVersionFingerprintDigest;
    private final CredentialUseLeaseV1 credentialUseLease;

    public ChannelResourceIdentityV1(final AdapterKindV1 adapterKind, final ChannelKindV1 channelKind,
                                    final byte[] destinationLaneId, final byte[] laneIncarnation,
                                    final BrokerResourceIdentityV1 targetResource, final long physicalPartition,
                                    final long channelGeneration, final long channelSlot,
                                    final byte[] producerOrTransactionalIdentity,
                                    final byte[] producerOrTransactionalIdentitySha256,
                                    final BrokerResourceIdentityV1 evidenceResource,
                                    final Long evidenceGeneration,
                                    final byte[] resourceGuardAttestationDigest,
                                    final long credentialBindingGeneration,
                                    final byte[] credentialBindingDigest,
                                    final byte[] resolvedCredentialVersionFingerprintDigest,
                                    final CredentialUseLeaseV1 credentialUseLease) {
        this.adapterKind = Objects.requireNonNull(adapterKind, "adapterKind");
        this.channelKind = Objects.requireNonNull(channelKind, "channelKind");
        this.destinationLaneId = fixed(destinationLaneId, HASH_LENGTH, "destinationLaneId");
        this.laneIncarnation = fixed(laneIncarnation, LANE_INCARNATION_LENGTH, "laneIncarnation");
        this.targetResource = Objects.requireNonNull(targetResource, "targetResource");
        if (physicalPartition < 0 || physicalPartition > 0xffff_ffffL) {
            throw new IllegalArgumentException("physicalPartition is outside uint32 range");
        }
        this.physicalPartition = physicalPartition;
        if (channelGeneration == 0) {
            throw new IllegalArgumentException("channelGeneration must be non-zero");
        }
        this.channelGeneration = channelGeneration;
        if (channelSlot < 0 || channelSlot > 0xffff_ffffL) {
            throw new IllegalArgumentException("channelSlot is outside uint32 range");
        }
        this.channelSlot = channelSlot;
        this.producerOrTransactionalIdentity = nonEmpty(producerOrTransactionalIdentity,
                "producerOrTransactionalIdentity");
        this.producerOrTransactionalIdentitySha256 = fixed(producerOrTransactionalIdentitySha256, HASH_LENGTH,
                "producerOrTransactionalIdentitySha256");
        if (!Arrays.equals(this.producerOrTransactionalIdentitySha256,
                Bytes.sha256(this.producerOrTransactionalIdentity))) {
            throw new IllegalArgumentException("producer identity digest mismatch");
        }
        if ((evidenceResource == null) != (evidenceGeneration == null)) {
            throw new IllegalArgumentException("evidence resource and generation must be paired");
        }
        if (channelKind.requiresEvidenceResource() != (evidenceResource != null)) {
            throw new IllegalArgumentException("channel kind/evidence resource presence mismatch");
        }
        if (evidenceResource != null) {
            if (evidenceResource.kind() != targetResource.kind() || evidenceGeneration == 0) {
                throw new IllegalArgumentException("evidence resource identity mismatch");
            }
        }
        this.evidenceResource = evidenceResource;
        this.evidenceGeneration = evidenceGeneration;
        this.resourceGuardAttestationDigest = fixed(resourceGuardAttestationDigest, HASH_LENGTH,
                "resourceGuardAttestationDigest");
        if (credentialBindingGeneration <= 0) {
            throw new IllegalArgumentException("credentialBindingGeneration must be positive");
        }
        this.credentialBindingGeneration = credentialBindingGeneration;
        this.credentialBindingDigest = fixed(credentialBindingDigest, HASH_LENGTH, "credentialBindingDigest");
        this.resolvedCredentialVersionFingerprintDigest = fixed(resolvedCredentialVersionFingerprintDigest,
                HASH_LENGTH, "resolvedCredentialVersionFingerprintDigest");
        this.credentialUseLease = Objects.requireNonNull(credentialUseLease, "credentialUseLease");
        if (credentialUseLease.kind() != CredentialUseKindV1.DESTINATION_CHANNEL
                || credentialUseLease.profile().profileKind() != ProfileKindV1.DESTINATION
                || credentialUseLease.secretGeneration() != credentialBindingGeneration
                || !Arrays.equals(credentialUseLease.credentialBindingDigest(), this.credentialBindingDigest)
                || !Arrays.equals(credentialUseLease.resolvedCredentialFingerprintDigest(),
                this.resolvedCredentialVersionFingerprintDigest)) {
            throw new IllegalArgumentException("channel credential lease binding mismatch");
        }
        if (!Arrays.equals(credentialUseLease.holderScopeDigest(),
                CredentialUseLeaseV1.destinationChannelHolderScope(canonicalFieldsThrough13()))) {
            throw new IllegalArgumentException("channel credential lease holder scope mismatch");
        }
        validateAdapterAndChannel();
    }

    public AdapterKindV1 adapterKind() {
        return adapterKind;
    }

    public ChannelKindV1 channelKind() {
        return channelKind;
    }

    public byte[] destinationLaneId() {
        return Bytes.copy(destinationLaneId);
    }

    public byte[] laneIncarnation() {
        return Bytes.copy(laneIncarnation);
    }

    public BrokerResourceIdentityV1 targetResource() {
        return targetResource;
    }

    public long physicalPartition() {
        return physicalPartition;
    }

    public long channelGeneration() {
        return channelGeneration;
    }

    public long channelSlot() {
        return channelSlot;
    }

    public byte[] producerOrTransactionalIdentity() {
        return Bytes.copy(producerOrTransactionalIdentity);
    }

    public byte[] producerOrTransactionalIdentitySha256() {
        return Bytes.copy(producerOrTransactionalIdentitySha256);
    }

    public BrokerResourceIdentityV1 evidenceResource() {
        return evidenceResource;
    }

    public Long evidenceGeneration() {
        return evidenceGeneration;
    }

    public byte[] resourceGuardAttestationDigest() {
        return Bytes.copy(resourceGuardAttestationDigest);
    }

    public long credentialBindingGeneration() {
        return credentialBindingGeneration;
    }

    public byte[] credentialBindingDigest() {
        return Bytes.copy(credentialBindingDigest);
    }

    public byte[] resolvedCredentialVersionFingerprintDigest() {
        return Bytes.copy(resolvedCredentialVersionFingerprintDigest);
    }

    public CredentialUseLeaseV1 credentialUseLease() {
        return credentialUseLease;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            writeFieldsThrough13(output);
            CanonicalProtobuf.uint64(output, 14, credentialBindingGeneration);
            CanonicalProtobuf.bytes(output, 15, credentialBindingDigest);
            CanonicalProtobuf.bytes(output, 16, resolvedCredentialVersionFingerprintDigest);
            CanonicalProtobuf.bytes(output, 17, credentialUseLease.canonicalBytes());
        });
    }

    public static ChannelResourceIdentityV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "ChannelResourceIdentityV1");
        if (fields.size() != 15 && fields.size() != 17) {
            throw new IllegalArgumentException("ChannelResourceIdentityV1 has an unexpected field count");
        }
        final boolean hasEvidence = fields.size() == 17;
        final int[] expected = hasEvidence
                ? new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}
                : new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13, 14, 15, 16, 17};
        QueryCodecSupport.requireNumbers(fields, expected, "ChannelResourceIdentityV1");
        final ChannelResourceIdentityV1 result = new ChannelResourceIdentityV1(
                AdapterKindV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1)),
                ChannelKindV1.fromWire(QueryCodecSupport.uint(fields.get(1), 2)),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(3), 4, LANE_INCARNATION_LENGTH),
                BrokerResourceIdentityV1.decode(QueryCodecSupport.nested(fields.get(4), 5)),
                QueryCodecSupport.uint(fields.get(5), 6),
                nonZero(QueryCodecSupport.uint(fields.get(6), 7), "channelGeneration"),
                QueryCodecSupport.uint(fields.get(7), 8),
                QueryCodecSupport.bytes(fields.get(8), 9),
                QueryCodecSupport.fixed(fields.get(9), 10, HASH_LENGTH),
                hasEvidence ? BrokerResourceIdentityV1.decode(QueryCodecSupport.nested(fields.get(10), 11)) : null,
                hasEvidence ? nonZero(QueryCodecSupport.uint(fields.get(11), 12), "evidenceGeneration") : null,
                QueryCodecSupport.fixed(fields.get(hasEvidence ? 12 : 10), 13, HASH_LENGTH),
                positive(QueryCodecSupport.uint(fields.get(hasEvidence ? 13 : 11), 14),
                        "credentialBindingGeneration"),
                QueryCodecSupport.fixed(fields.get(hasEvidence ? 14 : 12), 15, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(hasEvidence ? 15 : 13), 16, HASH_LENGTH),
                CredentialUseLeaseV1.decode(QueryCodecSupport.nested(fields.get(hasEvidence ? 16 : 14), 17)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ChannelResourceIdentityV1");
        return result;
    }

    private void writeFieldsThrough13(final java.io.ByteArrayOutputStream output) {
        CanonicalProtobuf.uint32(output, 1, adapterKind.wireValue());
        CanonicalProtobuf.uint32(output, 2, channelKind.wireValue());
        CanonicalProtobuf.bytes(output, 3, destinationLaneId);
        CanonicalProtobuf.bytes(output, 4, laneIncarnation);
        CanonicalProtobuf.bytes(output, 5, targetResource.canonicalBytes());
        CanonicalProtobuf.uint32(output, 6, physicalPartition);
        CanonicalProtobuf.uint64Bits(output, 7, channelGeneration);
        CanonicalProtobuf.uint32(output, 8, channelSlot);
        CanonicalProtobuf.bytes(output, 9, producerOrTransactionalIdentity);
        CanonicalProtobuf.bytes(output, 10, producerOrTransactionalIdentitySha256);
        if (evidenceResource != null) {
            CanonicalProtobuf.bytes(output, 11, evidenceResource.canonicalBytes());
            CanonicalProtobuf.uint64Bits(output, 12, evidenceGeneration);
        }
        CanonicalProtobuf.bytes(output, 13, resourceGuardAttestationDigest);
    }

    private byte[] canonicalFieldsThrough13() {
        return CanonicalProtobuf.message(this::writeFieldsThrough13);
    }

    private void validateAdapterAndChannel() {
        final boolean targetMatches = (adapterKind == AdapterKindV1.KAFKA && targetResource.kind() == BrokerResourceIdentityV1.Kind.KAFKA)
                || (adapterKind == AdapterKindV1.PULSAR && targetResource.kind() == BrokerResourceIdentityV1.Kind.PULSAR);
        if (!targetMatches) {
            throw new IllegalArgumentException("channel adapter and target resource branch mismatch");
        }
        if (channelKind == ChannelKindV1.KAFKA_TRANSACTIONAL_RECEIPT && adapterKind != AdapterKindV1.KAFKA) {
            throw new IllegalArgumentException("Kafka transactional channel requires Kafka adapter");
        }
        if ((channelKind == ChannelKindV1.PULSAR_DEDUP_PRODUCER
                || channelKind == ChannelKindV1.PULSAR_NATIVE_DELAYED) && adapterKind != AdapterKindV1.PULSAR) {
            throw new IllegalArgumentException("Pulsar channel requires Pulsar adapter");
        }
    }

    private static long positive(final long value, final String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static long nonZero(final long value, final String name) {
        if (value == 0) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return value;
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    private static byte[] nonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ChannelResourceIdentityV1 that
                && adapterKind == that.adapterKind
                && channelKind == that.channelKind
                && Arrays.equals(destinationLaneId, that.destinationLaneId)
                && Arrays.equals(laneIncarnation, that.laneIncarnation)
                && Objects.equals(targetResource, that.targetResource)
                && physicalPartition == that.physicalPartition
                && channelGeneration == that.channelGeneration
                && channelSlot == that.channelSlot
                && Arrays.equals(producerOrTransactionalIdentity, that.producerOrTransactionalIdentity)
                && Arrays.equals(producerOrTransactionalIdentitySha256,
                that.producerOrTransactionalIdentitySha256)
                && Objects.equals(evidenceResource, that.evidenceResource)
                && Objects.equals(evidenceGeneration, that.evidenceGeneration)
                && Arrays.equals(resourceGuardAttestationDigest, that.resourceGuardAttestationDigest)
                && credentialBindingGeneration == that.credentialBindingGeneration
                && Arrays.equals(credentialBindingDigest, that.credentialBindingDigest)
                && Arrays.equals(resolvedCredentialVersionFingerprintDigest,
                that.resolvedCredentialVersionFingerprintDigest)
                && Objects.equals(credentialUseLease, that.credentialUseLease);
    }

    @Override
    public int hashCode() {
        return Objects.hash(adapterKind, channelKind, Arrays.hashCode(destinationLaneId),
                Arrays.hashCode(laneIncarnation), targetResource, physicalPartition, channelGeneration, channelSlot,
                Arrays.hashCode(producerOrTransactionalIdentity), Arrays.hashCode(producerOrTransactionalIdentitySha256),
                evidenceResource, evidenceGeneration, Arrays.hashCode(resourceGuardAttestationDigest),
                credentialBindingGeneration, Arrays.hashCode(credentialBindingDigest),
                Arrays.hashCode(resolvedCredentialVersionFingerprintDigest), credentialUseLease);
    }
}
