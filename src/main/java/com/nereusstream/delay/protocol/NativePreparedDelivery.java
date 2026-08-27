package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Exact Pulsar AUTO_FAST prepared submission; no Broker I/O is performed
 * here.
 *
 * <p>The current generation is the only production representation. The old
 * policy-free factory is retained as a reader-only compatibility seam for
 * callers compiled against the initial H0 API. It emits generation 1 and is
 * rejected by the physical submission adapter. New code must use
 * {@link #createCurrent}.</p>
 */
public final class NativePreparedDelivery {
    public static final int PREPARED_VERSION = 2;
    public static final int NATIVE_ENCODING_VERSION = 2;
    public static final int HASH_LENGTH = 32;
    private static final int LEGACY_PREPARED_VERSION = 1;
    private static final int LEGACY_NATIVE_ENCODING_VERSION = 1;
    private static final int NATIVE_DELIVERY_ID_LENGTH = 32;
    private static final String HASH_DOMAIN = "nereus-delay-native-submission\0";

    private final int preparedVersion;
    private final int nativeEncodingVersion;
    private final byte[] nativeDeliveryId;
    private final ProfileRef destination;
    private final ProfileRef capability;
    private final PulsarBrokerResourceIdentity target;
    private final int physicalPartition;
    private final byte[] inlinePayload;
    private final PulsarMetadata metadata;
    private final Long eventTimeEpochMs;
    private final long deliverAtEpochMs;
    private final long legacyBrokerDeliverAtEpochMs;
    private final NativeDeliveryPolicy nativeDeliveryPolicy;
    private final DeliveryContract deliveryContract;
    private final HandoffPolicySnapshot handoffPolicySnapshot;
    private final NativeCapabilitySnapshot capabilitySnapshot;
    private final byte[] resourceGuardAttestationSha256;
    private final long capabilityExpiryEpochMs;
    private final byte[] submissionHash;

    private NativePreparedDelivery(
            final int preparedVersion,
            final int nativeEncodingVersion,
            final byte[] nativeDeliveryId,
            final ProfileRef destination,
            final ProfileRef capability,
            final PulsarBrokerResourceIdentity target,
            final int physicalPartition,
            final byte[] inlinePayload,
            final PulsarMetadata metadata,
            final Long eventTimeEpochMs,
            final long deliverAtEpochMs,
            final long legacyBrokerDeliverAtEpochMs,
            final NativeDeliveryPolicy nativeDeliveryPolicy,
            final DeliveryContract deliveryContract,
            final HandoffPolicySnapshot handoffPolicySnapshot,
            final NativeCapabilitySnapshot capabilitySnapshot,
            final byte[] resourceGuardAttestationSha256,
            final long capabilityExpiryEpochMs,
            final byte[] submissionHash) {
        if (preparedVersion != LEGACY_PREPARED_VERSION && preparedVersion != PREPARED_VERSION) {
            throw new IllegalArgumentException("unsupported NativePreparedDelivery generation");
        }
        if (nativeEncodingVersion != LEGACY_NATIVE_ENCODING_VERSION
                && nativeEncodingVersion != NATIVE_ENCODING_VERSION) {
            throw new IllegalArgumentException("unsupported NativePreparedDelivery encoding generation");
        }
        this.preparedVersion = preparedVersion;
        this.nativeEncodingVersion = nativeEncodingVersion;
        requireNonZero(nativeDeliveryId, NATIVE_DELIVERY_ID_LENGTH, "nativeDeliveryId");
        this.nativeDeliveryId = Bytes.copy(nativeDeliveryId);
        this.destination = requireProfile(destination, ProfileKind.DESTINATION, "destination");
        this.capability = requireProfile(capability, ProfileKind.DELIVERY_CAPABILITY, "capability");
        this.target = Objects.requireNonNull(target, "target");
        this.physicalPartition = physicalPartition;
        this.inlinePayload = Bytes.copy(Objects.requireNonNull(inlinePayload, "inlinePayload"));
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        if (deliverAtEpochMs < 0 || (eventTimeEpochMs != null && eventTimeEpochMs < 0) || capabilityExpiryEpochMs < 0) {
            throw new IllegalArgumentException("invalid NativePreparedDelivery timestamps");
        }
        this.eventTimeEpochMs = eventTimeEpochMs;
        this.deliverAtEpochMs = deliverAtEpochMs;
        this.legacyBrokerDeliverAtEpochMs = legacyBrokerDeliverAtEpochMs;
        this.nativeDeliveryPolicy = Objects.requireNonNull(nativeDeliveryPolicy, "nativeDeliveryPolicy");
        this.deliveryContract = Objects.requireNonNull(deliveryContract, "deliveryContract");
        this.handoffPolicySnapshot = handoffPolicySnapshot;
        this.capabilitySnapshot = Objects.requireNonNull(capabilitySnapshot, "capabilitySnapshot");
        if (!destination.equals(capabilitySnapshot.destination())
                || !capability.equals(capabilitySnapshot.capability())
                || !target.equals(capabilitySnapshot.target())
                || physicalPartition != capabilitySnapshot.physicalPartition()) {
            throw new IllegalArgumentException("NativePreparedDelivery projection disagrees with capability snapshot");
        }
        this.resourceGuardAttestationSha256 = fixed(resourceGuardAttestationSha256, "resourceGuardAttestationSha256");
        if (!Arrays.equals(this.resourceGuardAttestationSha256, capabilitySnapshot.resourceGuardAttestationSha256())) {
            throw new IllegalArgumentException("NativePreparedDelivery guard attestation disagrees with snapshot");
        }
        if (capabilityExpiryEpochMs != capabilitySnapshot.notAfterEpochMs()) {
            throw new IllegalArgumentException("NativePreparedDelivery capability expiry disagrees with snapshot");
        }
        this.capabilityExpiryEpochMs = capabilityExpiryEpochMs;
        if (preparedVersion == PREPARED_VERSION) {
            requireCurrentProjection();
        } else if (legacyBrokerDeliverAtEpochMs < deliverAtEpochMs) {
            throw new IllegalArgumentException("legacy Broker delivery time must not precede business delivery time");
        }
        this.submissionHash = fixed(submissionHash, "submissionHash");
    }

    /**
     * Reader-only compatibility factory for the original policy-free native
     * envelope. It is deliberately not used by the current production
     * physical adapter.
     */
    public static NativePreparedDelivery create(
            final byte[] nativeDeliveryId,
            final ProfileRef destination,
            final ProfileRef capability,
            final PulsarBrokerResourceIdentity target,
            final int physicalPartition,
            final byte[] inlinePayload,
            final PulsarMetadata metadata,
            final Long eventTimeEpochMs,
            final long deliverAtEpochMs,
            final long brokerDeliverAtEpochMs,
            final NativeCapabilitySnapshot capabilitySnapshot) {
        final NativePreparedDelivery fields = new NativePreparedDelivery(
                LEGACY_PREPARED_VERSION,
                LEGACY_NATIVE_ENCODING_VERSION,
                nativeDeliveryId,
                destination,
                capability,
                target,
                physicalPartition,
                inlinePayload,
                metadata,
                eventTimeEpochMs,
                deliverAtEpochMs,
                brokerDeliverAtEpochMs,
                NativeDeliveryPolicy.FORBID,
                DeliveryContract.NEREUS_MANAGED_NOT_BEFORE,
                null,
                capabilitySnapshot,
                capabilitySnapshot.resourceGuardAttestationSha256(),
                capabilitySnapshot.notAfterEpochMs(),
                new byte[HASH_LENGTH]);
        return withSubmissionHash(fields, fields.legacyCanonicalFields());
    }

    /** Creates the generation-2 native envelope with one exact business timestamp. */
    public static NativePreparedDelivery createCurrent(
            final byte[] nativeDeliveryId,
            final ProfileRef destination,
            final ProfileRef capability,
            final PulsarBrokerResourceIdentity target,
            final int physicalPartition,
            final byte[] inlinePayload,
            final PulsarMetadata metadata,
            final Long eventTimeEpochMs,
            final long deliverAtEpochMs,
            final NativeDeliveryPolicy nativeDeliveryPolicy,
            final DeliveryContract deliveryContract,
            final HandoffPolicySnapshot handoffPolicySnapshot,
            final NativeCapabilitySnapshot capabilitySnapshot) {
        final NativePreparedDelivery fields = new NativePreparedDelivery(
                PREPARED_VERSION,
                NATIVE_ENCODING_VERSION,
                nativeDeliveryId,
                destination,
                capability,
                target,
                physicalPartition,
                inlinePayload,
                metadata,
                eventTimeEpochMs,
                deliverAtEpochMs,
                deliverAtEpochMs,
                nativeDeliveryPolicy,
                deliveryContract,
                handoffPolicySnapshot,
                capabilitySnapshot,
                capabilitySnapshot.resourceGuardAttestationSha256(),
                capabilitySnapshot.notAfterEpochMs(),
                new byte[HASH_LENGTH]);
        return withSubmissionHash(fields, fields.currentCanonicalFields());
    }

    private static NativePreparedDelivery withSubmissionHash(
            final NativePreparedDelivery fields, final byte[] canonicalFields) {
        final byte[] submissionHash = Bytes.sha256(Bytes.utf8(HASH_DOMAIN), canonicalFields);
        return new NativePreparedDelivery(
                fields.preparedVersion,
                fields.nativeEncodingVersion,
                fields.nativeDeliveryId,
                fields.destination,
                fields.capability,
                fields.target,
                fields.physicalPartition,
                fields.inlinePayload,
                fields.metadata,
                fields.eventTimeEpochMs,
                fields.deliverAtEpochMs,
                fields.legacyBrokerDeliverAtEpochMs,
                fields.nativeDeliveryPolicy,
                fields.deliveryContract,
                fields.handoffPolicySnapshot,
                fields.capabilitySnapshot,
                fields.resourceGuardAttestationSha256,
                fields.capabilityExpiryEpochMs,
                submissionHash);
    }

    public static NativePreparedDelivery decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "NativePreparedDelivery");
        if (fields.isEmpty() || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("NativePreparedDelivery is missing its generation");
        }
        return QueryCodecSupport.uint(fields.get(0), 1) == LEGACY_PREPARED_VERSION
                ? decodeLegacy(fields, encoded)
                : decodeCurrent(fields, encoded);
    }

    private static NativePreparedDelivery decodeLegacy(
            final List<CanonicalProtobuf.Reader.Field> fields, final byte[] encoded) {
        if (fields.size() != 15 && fields.size() != 16) {
            throw new IllegalArgumentException("legacy NativePreparedDelivery fields are incomplete or unknown");
        }
        final byte[] nativeDeliveryId = QueryCodecSupport.fixed(fields.get(1), 2, NATIVE_DELIVERY_ID_LENGTH);
        final ProfileRef destination = ProfileRef.decode(QueryCodecSupport.nested(fields.get(2), 3));
        final ProfileRef capability = ProfileRef.decode(QueryCodecSupport.nested(fields.get(3), 4));
        final PulsarBrokerResourceIdentity target =
                PulsarBrokerResourceIdentity.decode(QueryCodecSupport.nested(fields.get(4), 5));
        final int partition = QueryCodecSupport.uint32Bits(fields.get(5), 6);
        final byte[] payload = QueryCodecSupport.bytes(fields.get(6), 7);
        final PulsarMetadata metadata = PulsarMetadata.decode(QueryCodecSupport.nested(fields.get(7), 8));
        int index = 8;
        Long eventTime = null;
        if (fields.get(index).number() == 9) {
            eventTime = QueryCodecSupport.uint(fields.get(index), 9);
            index++;
        }
        if (fields.size() != index + 7 || fields.get(index).number() != 10) {
            throw new IllegalArgumentException("legacy NativePreparedDelivery timestamp fields are invalid");
        }
        final long deliverAt = QueryCodecSupport.uint(fields.get(index), 10);
        final long brokerDeliverAt = QueryCodecSupport.uint(fields.get(index + 1), 11);
        final NativeCapabilitySnapshot snapshot =
                NativeCapabilitySnapshot.decode(QueryCodecSupport.nested(fields.get(index + 2), 12));
        final byte[] guard = QueryCodecSupport.fixed(fields.get(index + 3), 13, HASH_LENGTH);
        final long expiry = QueryCodecSupport.uint(fields.get(index + 4), 14);
        if (QueryCodecSupport.uint(fields.get(index + 5), 15) != LEGACY_NATIVE_ENCODING_VERSION) {
            throw new IllegalArgumentException("unsupported legacy NativePreparedDelivery encoding version");
        }
        final byte[] submissionHash = QueryCodecSupport.fixed(fields.get(index + 6), 16, HASH_LENGTH);
        final NativePreparedDelivery result = new NativePreparedDelivery(
                LEGACY_PREPARED_VERSION,
                LEGACY_NATIVE_ENCODING_VERSION,
                nativeDeliveryId,
                destination,
                capability,
                target,
                partition,
                payload,
                metadata,
                eventTime,
                deliverAt,
                brokerDeliverAt,
                NativeDeliveryPolicy.FORBID,
                DeliveryContract.NEREUS_MANAGED_NOT_BEFORE,
                null,
                snapshot,
                guard,
                expiry,
                submissionHash);
        final byte[] expected = Bytes.sha256(Bytes.utf8(HASH_DOMAIN), result.legacyCanonicalFields());
        if (!Bytes.constantTimeEquals(submissionHash, expected)) {
            throw new IllegalArgumentException("NativePreparedDelivery submission hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "NativePreparedDelivery");
        return result;
    }

    private static NativePreparedDelivery decodeCurrent(
            final List<CanonicalProtobuf.Reader.Field> fields, final byte[] encoded) {
        if (fields.size() != 17 && fields.size() != 18) {
            throw new IllegalArgumentException("NativePreparedDelivery generation 2 has an invalid field count");
        }
        final byte[] nativeDeliveryId = QueryCodecSupport.fixed(fields.get(1), 2, NATIVE_DELIVERY_ID_LENGTH);
        final ProfileRef destination = ProfileRef.decode(QueryCodecSupport.nested(fields.get(2), 3));
        final ProfileRef capability = ProfileRef.decode(QueryCodecSupport.nested(fields.get(3), 4));
        final PulsarBrokerResourceIdentity target =
                PulsarBrokerResourceIdentity.decode(QueryCodecSupport.nested(fields.get(4), 5));
        final int partition = QueryCodecSupport.uint32Bits(fields.get(5), 6);
        final byte[] payload = QueryCodecSupport.bytes(fields.get(6), 7);
        final PulsarMetadata metadata = PulsarMetadata.decode(QueryCodecSupport.nested(fields.get(7), 8));
        int index = 8;
        Long eventTime = null;
        if (fields.get(index).number() == 9) {
            eventTime = QueryCodecSupport.uint(fields.get(index++), 9);
        }
        requireField(fields, index, 10);
        final long deliverAt = QueryCodecSupport.uint(fields.get(index++), 10);
        final NativeDeliveryPolicy policy =
                NativeDeliveryPolicy.fromWire(QueryCodecSupport.uint(fields.get(index++), 11));
        final DeliveryContract contract = DeliveryContract.fromWire(QueryCodecSupport.uint(fields.get(index++), 12));
        final HandoffPolicySnapshot handoff =
                HandoffPolicySnapshot.decode(QueryCodecSupport.nested(fields.get(index++), 13));
        final NativeCapabilitySnapshot capabilitySnapshot =
                NativeCapabilitySnapshot.decode(QueryCodecSupport.nested(fields.get(index++), 14));
        final byte[] guard = QueryCodecSupport.fixed(fields.get(index++), 15, HASH_LENGTH);
        final long expiry = QueryCodecSupport.uint(fields.get(index++), 16);
        if (QueryCodecSupport.uint(fields.get(index++), 17) != NATIVE_ENCODING_VERSION) {
            throw new IllegalArgumentException("unsupported NativePreparedDelivery encoding version");
        }
        if (index + 1 != fields.size() || fields.get(index).number() != 18) {
            throw new IllegalArgumentException("NativePreparedDelivery submission hash field is missing");
        }
        final byte[] submissionHash = QueryCodecSupport.fixed(fields.get(index), 18, HASH_LENGTH);
        final NativePreparedDelivery result = new NativePreparedDelivery(
                PREPARED_VERSION,
                NATIVE_ENCODING_VERSION,
                nativeDeliveryId,
                destination,
                capability,
                target,
                partition,
                payload,
                metadata,
                eventTime,
                deliverAt,
                deliverAt,
                policy,
                contract,
                handoff,
                capabilitySnapshot,
                guard,
                expiry,
                submissionHash);
        final byte[] expected = Bytes.sha256(Bytes.utf8(HASH_DOMAIN), result.currentCanonicalFields());
        if (!Bytes.constantTimeEquals(submissionHash, expected)) {
            throw new IllegalArgumentException("NativePreparedDelivery submission hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "NativePreparedDelivery");
        return result;
    }

    public int preparedVersion() {
        return preparedVersion;
    }

    public int nativeEncodingVersion() {
        return nativeEncodingVersion;
    }

    public boolean isCurrentGeneration() {
        return preparedVersion == PREPARED_VERSION && nativeEncodingVersion == NATIVE_ENCODING_VERSION;
    }

    public byte[] nativeDeliveryId() {
        return Bytes.copy(nativeDeliveryId);
    }

    public ProfileRef destination() {
        return destination;
    }

    public ProfileRef capability() {
        return capability;
    }

    public PulsarBrokerResourceIdentity target() {
        return target;
    }

    public int physicalPartition() {
        return physicalPartition;
    }

    public byte[] inlinePayload() {
        return Bytes.copy(inlinePayload);
    }

    public PulsarMetadata metadata() {
        return metadata;
    }

    public Long eventTimeEpochMs() {
        return eventTimeEpochMs;
    }

    public long deliverAtEpochMs() {
        return deliverAtEpochMs;
    }

    /** Compatibility accessor; generation 2 returns the same business timestamp. */
    public long brokerDeliverAtEpochMs() {
        return preparedVersion == LEGACY_PREPARED_VERSION ? legacyBrokerDeliverAtEpochMs : deliverAtEpochMs;
    }

    public NativeDeliveryPolicy nativeDeliveryPolicy() {
        return nativeDeliveryPolicy;
    }

    public DeliveryContract deliveryContract() {
        return deliveryContract;
    }

    public HandoffPolicySnapshot handoffPolicySnapshot() {
        return handoffPolicySnapshot;
    }

    public NativeCapabilitySnapshot capabilitySnapshot() {
        return capabilitySnapshot;
    }

    public byte[] resourceGuardAttestationSha256() {
        return Bytes.copy(resourceGuardAttestationSha256);
    }

    public long capabilityExpiryEpochMs() {
        return capabilityExpiryEpochMs;
    }

    public byte[] submissionHash() {
        return Bytes.copy(submissionHash);
    }

    public byte[] canonicalBytes() {
        return preparedVersion == LEGACY_PREPARED_VERSION
                ? CanonicalProtobuf.message(output -> {
                    output.writeBytes(legacyCanonicalFields());
                    CanonicalProtobuf.bytes(output, 16, submissionHash);
                })
                : CanonicalProtobuf.message(output -> {
                    output.writeBytes(currentCanonicalFields());
                    CanonicalProtobuf.bytes(output, 18, submissionHash);
                });
    }

    public NativePreparedRef preparedRef() {
        return new NativePreparedRef(
                nativeDeliveryId,
                submissionHash,
                destination,
                target,
                physicalPartition,
                capabilitySnapshot.snapshotDigest(),
                capabilityExpiryEpochMs,
                Bytes.sha256(canonicalBytes()));
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof NativePreparedDelivery that)) {
            return false;
        }
        return preparedVersion == that.preparedVersion
                && nativeEncodingVersion == that.nativeEncodingVersion
                && physicalPartition == that.physicalPartition
                && Objects.equals(eventTimeEpochMs, that.eventTimeEpochMs)
                && deliverAtEpochMs == that.deliverAtEpochMs
                && legacyBrokerDeliverAtEpochMs == that.legacyBrokerDeliverAtEpochMs
                && nativeDeliveryPolicy == that.nativeDeliveryPolicy
                && deliveryContract == that.deliveryContract
                && capabilityExpiryEpochMs == that.capabilityExpiryEpochMs
                && Arrays.equals(nativeDeliveryId, that.nativeDeliveryId)
                && destination.equals(that.destination)
                && capability.equals(that.capability)
                && target.equals(that.target)
                && Arrays.equals(inlinePayload, that.inlinePayload)
                && metadata.equals(that.metadata)
                && Objects.equals(handoffPolicySnapshot, that.handoffPolicySnapshot)
                && capabilitySnapshot.equals(that.capabilitySnapshot)
                && Arrays.equals(resourceGuardAttestationSha256, that.resourceGuardAttestationSha256)
                && Arrays.equals(submissionHash, that.submissionHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                preparedVersion,
                nativeEncodingVersion,
                Arrays.hashCode(nativeDeliveryId),
                destination,
                capability,
                target,
                physicalPartition,
                Arrays.hashCode(inlinePayload),
                metadata,
                eventTimeEpochMs,
                deliverAtEpochMs,
                legacyBrokerDeliverAtEpochMs,
                nativeDeliveryPolicy,
                deliveryContract,
                handoffPolicySnapshot,
                capabilitySnapshot,
                Arrays.hashCode(resourceGuardAttestationSha256),
                capabilityExpiryEpochMs,
                Arrays.hashCode(submissionHash));
    }

    private byte[] legacyCanonicalFields() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, LEGACY_PREPARED_VERSION);
            CanonicalProtobuf.bytes(output, 2, nativeDeliveryId);
            CanonicalProtobuf.bytes(output, 3, destination.canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, capability.canonicalBytes());
            CanonicalProtobuf.bytes(output, 5, target.canonicalBytes());
            CanonicalProtobuf.uint32Bits(output, 6, physicalPartition);
            CanonicalProtobuf.bytes(output, 7, inlinePayload);
            CanonicalProtobuf.bytes(output, 8, metadata.canonicalBytes());
            if (eventTimeEpochMs != null) {
                CanonicalProtobuf.int64(output, 9, eventTimeEpochMs);
            }
            CanonicalProtobuf.int64(output, 10, deliverAtEpochMs);
            CanonicalProtobuf.int64(output, 11, legacyBrokerDeliverAtEpochMs);
            CanonicalProtobuf.bytes(output, 12, capabilitySnapshot.canonicalBytes());
            CanonicalProtobuf.bytes(output, 13, resourceGuardAttestationSha256);
            CanonicalProtobuf.int64(output, 14, capabilityExpiryEpochMs);
            CanonicalProtobuf.uint32(output, 15, LEGACY_NATIVE_ENCODING_VERSION);
        });
    }

    private byte[] currentCanonicalFields() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, PREPARED_VERSION);
            CanonicalProtobuf.bytes(output, 2, nativeDeliveryId);
            CanonicalProtobuf.bytes(output, 3, destination.canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, capability.canonicalBytes());
            CanonicalProtobuf.bytes(output, 5, target.canonicalBytes());
            CanonicalProtobuf.uint32Bits(output, 6, physicalPartition);
            CanonicalProtobuf.bytes(output, 7, inlinePayload);
            CanonicalProtobuf.bytes(output, 8, metadata.canonicalBytes());
            if (eventTimeEpochMs != null) {
                CanonicalProtobuf.int64(output, 9, eventTimeEpochMs);
            }
            CanonicalProtobuf.int64(output, 10, deliverAtEpochMs);
            CanonicalProtobuf.uint32(output, 11, nativeDeliveryPolicy.wireValue());
            CanonicalProtobuf.uint32(output, 12, deliveryContract.wireValue());
            CanonicalProtobuf.bytes(output, 13, handoffPolicySnapshot.canonicalBytes());
            CanonicalProtobuf.bytes(output, 14, capabilitySnapshot.canonicalBytes());
            CanonicalProtobuf.bytes(output, 15, resourceGuardAttestationSha256);
            CanonicalProtobuf.int64(output, 16, capabilityExpiryEpochMs);
            CanonicalProtobuf.uint32(output, 17, NATIVE_ENCODING_VERSION);
        });
    }

    private void requireCurrentProjection() {
        if (nativeDeliveryPolicy != NativeDeliveryPolicy.ALLOW_AUTO_FAST_AND_MANAGED_HANDOFF
                || deliveryContract != DeliveryContract.PULSAR_NATIVE_DELIVERY
                || handoffPolicySnapshot == null
                || handoffPolicySnapshot.mode() != HandoffPolicyMode.ENABLED
                || !handoffPolicySnapshot.allows(HandoffPath.AUTO_FAST)
                || deliverAtEpochMs >= capabilityExpiryEpochMs
                || deliverAtEpochMs >= handoffPolicySnapshot.validUntilEpochMs()) {
            throw new IllegalArgumentException("NativePreparedDelivery generation 2 has an invalid native projection");
        }
    }

    private static void requireField(
            final List<CanonicalProtobuf.Reader.Field> fields, final int index, final int number) {
        if (index >= fields.size() || fields.get(index).number() != number) {
            throw new IllegalArgumentException("NativePreparedDelivery field order mismatch at " + number);
        }
    }

    private static ProfileRef requireProfile(final ProfileRef value, final ProfileKind kind, final String name) {
        Objects.requireNonNull(value, name);
        if (value.profileKind() != kind) {
            throw new IllegalArgumentException(name + " has the wrong Profile kind");
        }
        return value;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static void requireNonZero(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        for (byte item : value) {
            if (item != 0) {
                return;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }
}
