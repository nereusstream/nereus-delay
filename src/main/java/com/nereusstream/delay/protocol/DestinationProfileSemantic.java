package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Immutable Destination Profile semantic body from Registry §5.1.1. */
public final class DestinationProfileSemantic implements ProfileSemanticBody {
    public static final int SCHEMA_VERSION = 2;
    public static final int ALLOWED_ORDERING_MODE_BITS = 0x03;
    public static final int CREDENTIAL_BINDING_PROTOCOL_VERSION = 1;
    public static final int MAX_ALIAS_BYTES = 256;
    public static final int HASH_LENGTH = 32;

    private final AdapterKind adapterKind;
    private final BrokerResourceIdentity targetResource;
    private final int targetPartitionCount;
    private final TargetPartitionPolicy targetPartitionPolicy;
    private final TargetPartitionHashInput targetPartitionHashInput;
    private final List<Integer> allowedExplicitPartitions;
    private final ProfileRef deliveryCapability;
    private final int allowedOrderingModeBits;
    private final long handoffLeadMs;
    private final long targetClockAheadBoundMs;
    private final byte[] credentialAuthorizationScopeDigest;
    private final long maxTargetRecordBytes;
    private final long maxAdapterMetadataBytes;
    private final long maxPayloadBytes;
    private final int unorderedLaneBucketCount;
    private final byte[] destinationAliasUtf8Nfc;
    private final long minimumTopicTtlMs;
    private final long minimumTopicRetentionMs;
    private final int adapterEncodingVersion;
    private final byte[] prerequisitePolicyDigest;

    public DestinationProfileSemantic(
            final AdapterKind adapterKind,
            final BrokerResourceIdentity targetResource,
            final int targetPartitionCount,
            final TargetPartitionPolicy targetPartitionPolicy,
            final TargetPartitionHashInput targetPartitionHashInput,
            final List<Integer> allowedExplicitPartitions,
            final ProfileRef deliveryCapability,
            final int allowedOrderingModeBits,
            final long handoffLeadMs,
            final long targetClockAheadBoundMs,
            final byte[] credentialAuthorizationScopeDigest,
            final long maxTargetRecordBytes,
            final long maxAdapterMetadataBytes,
            final long maxPayloadBytes,
            final int unorderedLaneBucketCount,
            final byte[] destinationAliasUtf8Nfc,
            final long minimumTopicTtlMs,
            final long minimumTopicRetentionMs,
            final int adapterEncodingVersion,
            final byte[] prerequisitePolicyDigest) {
        this.adapterKind = Objects.requireNonNull(adapterKind, "adapterKind");
        this.targetResource = Objects.requireNonNull(targetResource, "targetResource");
        if ((adapterKind == AdapterKind.KAFKA && targetResource.kind() != BrokerResourceIdentity.Kind.KAFKA)
                || (adapterKind == AdapterKind.PULSAR && targetResource.kind() != BrokerResourceIdentity.Kind.PULSAR)) {
            throw new IllegalArgumentException("Destination Profile adapter does not match target resource");
        }
        if (targetPartitionCount == 0) {
            throw new IllegalArgumentException("target partition count must be positive");
        }
        this.targetPartitionCount = targetPartitionCount;
        this.targetPartitionPolicy = Objects.requireNonNull(targetPartitionPolicy, "targetPartitionPolicy");
        this.targetPartitionHashInput = Objects.requireNonNull(targetPartitionHashInput, "targetPartitionHashInput");
        this.allowedExplicitPartitions =
                validatePartitions(allowedExplicitPartitions, targetPartitionCount, targetPartitionPolicy);
        this.deliveryCapability = Objects.requireNonNull(deliveryCapability, "deliveryCapability");
        if (deliveryCapability.profileKind() != ProfileKind.DELIVERY_CAPABILITY) {
            throw new IllegalArgumentException("destination Profile requires a delivery capability reference");
        }
        if (allowedOrderingModeBits <= 0 || (allowedOrderingModeBits & ~ALLOWED_ORDERING_MODE_BITS) != 0) {
            throw new IllegalArgumentException("invalid allowed ordering mode bits");
        }
        this.allowedOrderingModeBits = allowedOrderingModeBits;
        if (handoffLeadMs < 0 || targetClockAheadBoundMs < 0) {
            throw new IllegalArgumentException("timing bounds must be non-negative");
        }
        if (adapterKind == AdapterKind.KAFKA && handoffLeadMs != 0) {
            throw new IllegalArgumentException("Kafka Destination Profile cannot carry a handoff lead");
        }
        this.handoffLeadMs = handoffLeadMs;
        this.targetClockAheadBoundMs = targetClockAheadBoundMs;
        this.credentialAuthorizationScopeDigest =
                fixed(credentialAuthorizationScopeDigest, "credentialAuthorizationScopeDigest");
        if (maxTargetRecordBytes <= 0 || maxAdapterMetadataBytes <= 0 || maxPayloadBytes <= 0) {
            throw new IllegalArgumentException("Destination Profile size limits must be positive");
        }
        this.maxTargetRecordBytes = maxTargetRecordBytes;
        this.maxAdapterMetadataBytes = maxAdapterMetadataBytes;
        this.maxPayloadBytes = maxPayloadBytes;
        if (unorderedLaneBucketCount <= 0) {
            throw new IllegalArgumentException("unordered lane bucket count must be positive");
        }
        this.unorderedLaneBucketCount = unorderedLaneBucketCount;
        this.destinationAliasUtf8Nfc = alias(destinationAliasUtf8Nfc);
        if (minimumTopicTtlMs < 0 || minimumTopicRetentionMs < 0 || adapterEncodingVersion <= 0) {
            throw new IllegalArgumentException("invalid Destination Profile lifetime/encoding bounds");
        }
        this.minimumTopicTtlMs = minimumTopicTtlMs;
        this.minimumTopicRetentionMs = minimumTopicRetentionMs;
        this.adapterEncodingVersion = adapterEncodingVersion;
        this.prerequisitePolicyDigest = fixed(prerequisitePolicyDigest, "prerequisitePolicyDigest");
    }

    /** Generation-2 constructor; native delivery no longer shifts a Broker timestamp. */
    public DestinationProfileSemantic(
            final AdapterKind adapterKind,
            final BrokerResourceIdentity targetResource,
            final int targetPartitionCount,
            final TargetPartitionPolicy targetPartitionPolicy,
            final TargetPartitionHashInput targetPartitionHashInput,
            final List<Integer> allowedExplicitPartitions,
            final ProfileRef deliveryCapability,
            final int allowedOrderingModeBits,
            final long maxHandoffLeadMs,
            final byte[] credentialAuthorizationScopeDigest,
            final long maxTargetRecordBytes,
            final long maxAdapterMetadataBytes,
            final long maxPayloadBytes,
            final int unorderedLaneBucketCount,
            final byte[] destinationAliasUtf8Nfc,
            final long minimumTopicTtlMs,
            final long minimumTopicRetentionMs,
            final int adapterEncodingVersion,
            final byte[] prerequisitePolicyDigest) {
        this(
                adapterKind,
                targetResource,
                targetPartitionCount,
                targetPartitionPolicy,
                targetPartitionHashInput,
                allowedExplicitPartitions,
                deliveryCapability,
                allowedOrderingModeBits,
                maxHandoffLeadMs,
                0,
                credentialAuthorizationScopeDigest,
                maxTargetRecordBytes,
                maxAdapterMetadataBytes,
                maxPayloadBytes,
                unorderedLaneBucketCount,
                destinationAliasUtf8Nfc,
                minimumTopicTtlMs,
                minimumTopicRetentionMs,
                adapterEncodingVersion,
                prerequisitePolicyDigest);
    }

    @Override
    public ProfileKind profileKind() {
        return ProfileKind.DESTINATION;
    }

    @Override
    public int schemaVersion() {
        return SCHEMA_VERSION;
    }

    public AdapterKind adapterKind() {
        return adapterKind;
    }

    public BrokerResourceIdentity targetResource() {
        return targetResource;
    }

    public int targetPartitionCount() {
        return targetPartitionCount;
    }

    public TargetPartitionPolicy targetPartitionPolicy() {
        return targetPartitionPolicy;
    }

    public TargetPartitionHashInput targetPartitionHashInput() {
        return targetPartitionHashInput;
    }

    public List<Integer> allowedExplicitPartitions() {
        return allowedExplicitPartitions;
    }

    public ProfileRef deliveryCapability() {
        return deliveryCapability;
    }

    public int allowedOrderingModeBits() {
        return allowedOrderingModeBits;
    }

    public long handoffLeadMs() {
        return handoffLeadMs;
    }

    /** Generation-2 name for the immutable upper bound on native handoff lead. */
    public long maxHandoffLeadMs() {
        return handoffLeadMs;
    }

    public long targetClockAheadBoundMs() {
        return targetClockAheadBoundMs;
    }

    public byte[] credentialAuthorizationScopeDigest() {
        return Bytes.copy(credentialAuthorizationScopeDigest);
    }

    public long maxTargetRecordBytes() {
        return maxTargetRecordBytes;
    }

    public long maxAdapterMetadataBytes() {
        return maxAdapterMetadataBytes;
    }

    public long maxPayloadBytes() {
        return maxPayloadBytes;
    }

    public int unorderedLaneBucketCount() {
        return unorderedLaneBucketCount;
    }

    public byte[] destinationAliasUtf8Nfc() {
        return Bytes.copy(destinationAliasUtf8Nfc);
    }

    public long minimumTopicTtlMs() {
        return minimumTopicTtlMs;
    }

    public long minimumTopicRetentionMs() {
        return minimumTopicRetentionMs;
    }

    public int adapterEncodingVersion() {
        return adapterEncodingVersion;
    }

    public byte[] prerequisitePolicyDigest() {
        return Bytes.copy(prerequisitePolicyDigest);
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, adapterKind.wireValue());
            CanonicalProtobuf.bytes(output, 2, targetResource.canonicalBytes());
            CanonicalProtobuf.uint32Bits(output, 3, targetPartitionCount);
            CanonicalProtobuf.uint32(output, 4, targetPartitionPolicy.wireValue());
            CanonicalProtobuf.uint32(output, 5, targetPartitionHashInput.wireValue());
            for (int partition : allowedExplicitPartitions) {
                CanonicalProtobuf.uint32Bits(output, 6, partition);
            }
            CanonicalProtobuf.bytes(output, 7, deliveryCapability.canonicalBytes());
            CanonicalProtobuf.uint32(output, 8, allowedOrderingModeBits);
            CanonicalProtobuf.uint64(output, 9, handoffLeadMs);
            CanonicalProtobuf.bytes(output, 11, credentialAuthorizationScopeDigest);
            CanonicalProtobuf.uint32(output, 12, CREDENTIAL_BINDING_PROTOCOL_VERSION);
            CanonicalProtobuf.uint64(output, 13, maxTargetRecordBytes);
            CanonicalProtobuf.uint64(output, 14, maxAdapterMetadataBytes);
            CanonicalProtobuf.uint64(output, 15, maxPayloadBytes);
            CanonicalProtobuf.uint32(output, 16, unorderedLaneBucketCount);
            CanonicalProtobuf.bytes(output, 17, destinationAliasUtf8Nfc);
            CanonicalProtobuf.uint64(output, 18, minimumTopicTtlMs);
            CanonicalProtobuf.uint64(output, 19, minimumTopicRetentionMs);
            CanonicalProtobuf.uint32(output, 20, adapterEncodingVersion);
            CanonicalProtobuf.bytes(output, 21, prerequisitePolicyDigest);
        });
    }

    public static DestinationProfileSemantic decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = readAll(encoded);
        if (fields.size() < 19) {
            throw new IllegalArgumentException("DestinationProfileSemantic is incomplete");
        }
        int index = 0;
        require(fields.get(index++), 1);
        require(fields.get(index++), 2);
        require(fields.get(index++), 3);
        require(fields.get(index++), 4);
        require(fields.get(index++), 5);
        final List<Integer> partitions = new ArrayList<>();
        while (index < fields.size() && fields.get(index).number() == 6) {
            partitions.add(QueryCodecSupport.uint32Bits(fields.get(index++), 6));
        }
        final int remaining = fields.size() - index;
        if (remaining != 14) {
            throw new IllegalArgumentException("DestinationProfileSemantic has an unexpected field count");
        }
        int cursor = index;
        final ProfileRef capability = ProfileRef.decode(QueryCodecSupport.nested(fields.get(cursor++), 7));
        final int orderingBits = QueryCodecSupport.uint32(fields.get(cursor++), 8);
        final long handoffLead = QueryCodecSupport.uint(fields.get(cursor++), 9);
        final byte[] scopeDigest = QueryCodecSupport.fixed(fields.get(cursor++), 11, HASH_LENGTH);
        final int bindingVersion = QueryCodecSupport.uint32(fields.get(cursor++), 12);
        final long maxRecord = QueryCodecSupport.uint(fields.get(cursor++), 13);
        final long maxMetadata = QueryCodecSupport.uint(fields.get(cursor++), 14);
        final long maxPayload = QueryCodecSupport.uint(fields.get(cursor++), 15);
        final int bucketCount = QueryCodecSupport.uint32(fields.get(cursor++), 16);
        final byte[] alias = QueryCodecSupport.bytes(fields.get(cursor++), 17);
        final long minimumTtl = QueryCodecSupport.uint(fields.get(cursor++), 18);
        final long minimumRetention = QueryCodecSupport.uint(fields.get(cursor++), 19);
        final int encodingVersion = QueryCodecSupport.uint32(fields.get(cursor++), 20);
        final byte[] prerequisiteDigest = QueryCodecSupport.fixed(fields.get(cursor++), 21, HASH_LENGTH);
        if (cursor != fields.size()) {
            throw new IllegalArgumentException("DestinationProfileSemantic field order mismatch");
        }
        final DestinationProfileSemantic result = new DestinationProfileSemantic(
                AdapterKind.fromWire(QueryCodecSupport.uint(fields.get(0), 1)),
                BrokerResourceIdentity.decode(QueryCodecSupport.nested(fields.get(1), 2)),
                QueryCodecSupport.uint32Bits(fields.get(2), 3),
                TargetPartitionPolicy.fromWire(QueryCodecSupport.uint(fields.get(3), 4)),
                TargetPartitionHashInput.fromWire(QueryCodecSupport.uint(fields.get(4), 5)),
                partitions,
                capability,
                orderingBits,
                handoffLead,
                0,
                scopeDigest,
                maxRecord,
                maxMetadata,
                maxPayload,
                bucketCount,
                alias,
                minimumTtl,
                minimumRetention,
                encodingVersion,
                prerequisiteDigest);
        if (bindingVersion != CREDENTIAL_BINDING_PROTOCOL_VERSION) {
            throw new IllegalArgumentException("unsupported Destination Profile credential binding version");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "DestinationProfileSemantic");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof DestinationProfileSemantic that
                && adapterKind == that.adapterKind
                && targetResource.equals(that.targetResource)
                && targetPartitionCount == that.targetPartitionCount
                && targetPartitionPolicy == that.targetPartitionPolicy
                && targetPartitionHashInput == that.targetPartitionHashInput
                && allowedExplicitPartitions.equals(that.allowedExplicitPartitions)
                && deliveryCapability.equals(that.deliveryCapability)
                && allowedOrderingModeBits == that.allowedOrderingModeBits
                && handoffLeadMs == that.handoffLeadMs
                && Arrays.equals(credentialAuthorizationScopeDigest, that.credentialAuthorizationScopeDigest)
                && maxTargetRecordBytes == that.maxTargetRecordBytes
                && maxAdapterMetadataBytes == that.maxAdapterMetadataBytes
                && maxPayloadBytes == that.maxPayloadBytes
                && unorderedLaneBucketCount == that.unorderedLaneBucketCount
                && Arrays.equals(destinationAliasUtf8Nfc, that.destinationAliasUtf8Nfc)
                && minimumTopicTtlMs == that.minimumTopicTtlMs
                && minimumTopicRetentionMs == that.minimumTopicRetentionMs
                && adapterEncodingVersion == that.adapterEncodingVersion
                && Arrays.equals(prerequisitePolicyDigest, that.prerequisitePolicyDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                adapterKind,
                targetResource,
                targetPartitionCount,
                targetPartitionPolicy,
                targetPartitionHashInput,
                allowedExplicitPartitions,
                deliveryCapability,
                allowedOrderingModeBits,
                handoffLeadMs,
                Arrays.hashCode(credentialAuthorizationScopeDigest),
                maxTargetRecordBytes,
                maxAdapterMetadataBytes,
                maxPayloadBytes,
                unorderedLaneBucketCount,
                Arrays.hashCode(destinationAliasUtf8Nfc),
                minimumTopicTtlMs,
                minimumTopicRetentionMs,
                adapterEncodingVersion,
                Arrays.hashCode(prerequisitePolicyDigest));
    }

    private static List<Integer> validatePartitions(
            final List<Integer> values, final int count, final TargetPartitionPolicy policy) {
        Objects.requireNonNull(values, "allowedExplicitPartitions");
        final List<Integer> result = List.copyOf(values);
        Integer previous = null;
        for (Integer value : result) {
            if (value == null
                    || Integer.compareUnsigned(value, count) >= 0
                    || previous != null && Integer.compareUnsigned(value, previous) <= 0) {
                throw new IllegalArgumentException("explicit target partitions are not sorted/in range");
            }
            previous = value;
        }
        if ((policy == TargetPartitionPolicy.EXPLICIT_ONLY || policy == TargetPartitionPolicy.EXPLICIT_OR_HASH)
                && result.isEmpty()) {
            throw new IllegalArgumentException("partition policy requires explicit partitions");
        }
        if (policy == TargetPartitionPolicy.HASH_ONLY && !result.isEmpty()) {
            throw new IllegalArgumentException("HASH_ONLY cannot carry explicit partitions");
        }
        return result;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        if (allZero(value)) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return Bytes.copy(value);
    }

    private static byte[] alias(final byte[] value) {
        Objects.requireNonNull(value, "destinationAliasUtf8Nfc");
        if (value.length == 0 || value.length > MAX_ALIAS_BYTES) {
            throw new IllegalArgumentException("destination alias is outside the bound");
        }
        final String decoded = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(decoded.getBytes(StandardCharsets.UTF_8), value)
                || decoded.isBlank()
                || decoded.indexOf('\0') >= 0
                || !Normalizer.normalize(decoded, Normalizer.Form.NFC).equals(decoded)) {
            throw new IllegalArgumentException("destination alias must be valid NFC UTF-8");
        }
        return Bytes.copy(value);
    }

    private static List<CanonicalProtobuf.Reader.Field> readAll(final byte[] encoded) {
        Objects.requireNonNull(encoded, "DestinationProfileSemantic");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("DestinationProfileSemantic is empty");
        }
        return fields;
    }

    private static void require(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number) {
            throw new IllegalArgumentException("DestinationProfileSemantic field order mismatch");
        }
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
