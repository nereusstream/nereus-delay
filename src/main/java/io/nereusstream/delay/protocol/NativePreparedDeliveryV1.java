package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Exact Pulsar AUTO_FAST prepared submission; no Broker I/O is performed here. */
public final class NativePreparedDeliveryV1 {
    public static final int PREPARED_VERSION = 1;
    public static final int NATIVE_ENCODING_VERSION = 1;
    public static final int HASH_LENGTH = 32;
    private static final int NATIVE_DELIVERY_ID_LENGTH = 32;

    private final byte[] nativeDeliveryId;
    private final ProfileRefV1 destination;
    private final ProfileRefV1 capability;
    private final PulsarBrokerResourceIdentityV1 target;
    private final int physicalPartition;
    private final byte[] inlinePayload;
    private final PulsarMetadataV1 metadata;
    private final Long eventTimeEpochMs;
    private final long deliverAtEpochMs;
    private final long brokerDeliverAtEpochMs;
    private final NativeCapabilitySnapshotV1 capabilitySnapshot;
    private final byte[] resourceGuardAttestationSha256;
    private final long capabilityExpiryEpochMs;
    private final byte[] submissionHash;

    private NativePreparedDeliveryV1(final byte[] nativeDeliveryId, final ProfileRefV1 destination,
                                     final ProfileRefV1 capability, final PulsarBrokerResourceIdentityV1 target,
                                     final int physicalPartition, final byte[] inlinePayload,
                                     final PulsarMetadataV1 metadata, final Long eventTimeEpochMs,
                                     final long deliverAtEpochMs, final long brokerDeliverAtEpochMs,
                                     final NativeCapabilitySnapshotV1 capabilitySnapshot,
                                     final byte[] resourceGuardAttestationSha256,
                                     final long capabilityExpiryEpochMs, final byte[] submissionHash) {
        requireNonZero(nativeDeliveryId, NATIVE_DELIVERY_ID_LENGTH, "nativeDeliveryId");
        this.nativeDeliveryId = Bytes.copy(nativeDeliveryId);
        this.destination = Objects.requireNonNull(destination, "destination");
        if (destination.profileKind() != ProfileKindV1.DESTINATION) {
            throw new IllegalArgumentException("native prepared destination must be a DESTINATION profile");
        }
        this.capability = Objects.requireNonNull(capability, "capability");
        if (capability.profileKind() != ProfileKindV1.DELIVERY_CAPABILITY) {
            throw new IllegalArgumentException("native prepared capability must be a DELIVERY_CAPABILITY profile");
        }
        this.target = Objects.requireNonNull(target, "target");
        if (physicalPartition < 0 || deliverAtEpochMs < 0 || brokerDeliverAtEpochMs < deliverAtEpochMs
                || (eventTimeEpochMs != null && eventTimeEpochMs < 0)) {
            throw new IllegalArgumentException("invalid NativePreparedDelivery timestamps or partition");
        }
        this.physicalPartition = physicalPartition;
        this.inlinePayload = Bytes.copy(Objects.requireNonNull(inlinePayload, "inlinePayload"));
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.eventTimeEpochMs = eventTimeEpochMs;
        this.deliverAtEpochMs = deliverAtEpochMs;
        this.brokerDeliverAtEpochMs = brokerDeliverAtEpochMs;
        this.capabilitySnapshot = Objects.requireNonNull(capabilitySnapshot, "capabilitySnapshot");
        if (!destination.equals(capabilitySnapshot.destination()) || !capability.equals(capabilitySnapshot.capability())
                || !target.equals(capabilitySnapshot.target())
                || physicalPartition != capabilitySnapshot.physicalPartition()) {
            throw new IllegalArgumentException("NativePreparedDelivery projection disagrees with capability snapshot");
        }
        this.resourceGuardAttestationSha256 = fixed(resourceGuardAttestationSha256,
                "resourceGuardAttestationSha256");
        if (!Arrays.equals(this.resourceGuardAttestationSha256,
                capabilitySnapshot.resourceGuardAttestationSha256())) {
            throw new IllegalArgumentException("NativePreparedDelivery guard attestation disagrees with snapshot");
        }
        if (capabilityExpiryEpochMs != capabilitySnapshot.notAfterEpochMs()) {
            throw new IllegalArgumentException("NativePreparedDelivery capability expiry disagrees with snapshot");
        }
        if (capabilityExpiryEpochMs < 0) {
            throw new IllegalArgumentException("capability expiry must be non-negative");
        }
        this.capabilityExpiryEpochMs = capabilityExpiryEpochMs;
        this.submissionHash = fixed(submissionHash, "submissionHash");
    }

    public static NativePreparedDeliveryV1 create(final byte[] nativeDeliveryId, final ProfileRefV1 destination,
                                                  final ProfileRefV1 capability,
                                                  final PulsarBrokerResourceIdentityV1 target,
                                                  final int physicalPartition, final byte[] inlinePayload,
                                                  final PulsarMetadataV1 metadata, final Long eventTimeEpochMs,
                                                  final long deliverAtEpochMs, final long brokerDeliverAtEpochMs,
                                                  final NativeCapabilitySnapshotV1 capabilitySnapshot) {
        final byte[] fields = canonicalFields(nativeDeliveryId, destination, capability, target, physicalPartition,
                inlinePayload, metadata, eventTimeEpochMs, deliverAtEpochMs, brokerDeliverAtEpochMs,
                capabilitySnapshot, capabilitySnapshot.resourceGuardAttestationSha256(),
                capabilitySnapshot.notAfterEpochMs());
        final byte[] submissionHash = Bytes.sha256(Bytes.utf8("nereus-delay-native-submission-v1\0"), fields);
        return new NativePreparedDeliveryV1(nativeDeliveryId, destination, capability, target, physicalPartition,
                inlinePayload, metadata, eventTimeEpochMs, deliverAtEpochMs, brokerDeliverAtEpochMs,
                capabilitySnapshot, capabilitySnapshot.resourceGuardAttestationSha256(),
                capabilitySnapshot.notAfterEpochMs(), submissionHash);
    }

    public static NativePreparedDeliveryV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "NativePreparedDeliveryV1");
        if (fields.size() != 15 && fields.size() != 16) {
            throw new IllegalArgumentException("NativePreparedDeliveryV1 fields are incomplete or unknown");
        }
        if (QueryCodecSupport.uint(fields.get(0), 1) != PREPARED_VERSION
                || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("unsupported NativePreparedDeliveryV1 version");
        }
        final byte[] nativeDeliveryId = QueryCodecSupport.fixed(fields.get(1), 2, NATIVE_DELIVERY_ID_LENGTH);
        final ProfileRefV1 destination = ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(2), 3));
        final ProfileRefV1 capability = ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(3), 4));
        final PulsarBrokerResourceIdentityV1 target = PulsarBrokerResourceIdentityV1.decode(
                QueryCodecSupport.nested(fields.get(4), 5));
        final int partition = QueryCodecSupport.uint32(fields.get(5), 6);
        final byte[] payload = QueryCodecSupport.bytes(fields.get(6), 7);
        final PulsarMetadataV1 metadata = PulsarMetadataV1.decode(QueryCodecSupport.nested(fields.get(7), 8));
        int index = 8;
        Long eventTime = null;
        if (fields.get(index).number() == 9) {
            eventTime = QueryCodecSupport.uint(fields.get(index), 9);
            index++;
        }
        if (fields.size() != index + 7 || fields.get(index).number() != 10) {
            throw new IllegalArgumentException("NativePreparedDeliveryV1 timestamp fields are invalid");
        }
        final long deliverAt = QueryCodecSupport.uint(fields.get(index), 10);
        final long brokerDeliverAt = QueryCodecSupport.uint(fields.get(index + 1), 11);
        final NativeCapabilitySnapshotV1 snapshot = NativeCapabilitySnapshotV1.decode(
                QueryCodecSupport.nested(fields.get(index + 2), 12));
        final byte[] guardAttestation = QueryCodecSupport.fixed(fields.get(index + 3), 13, HASH_LENGTH);
        final long expiry = QueryCodecSupport.uint(fields.get(index + 4), 14);
        if (QueryCodecSupport.uint(fields.get(index + 5), 15) != NATIVE_ENCODING_VERSION) {
            throw new IllegalArgumentException("unsupported NativePreparedDelivery encoding version");
        }
        final byte[] submissionHash = QueryCodecSupport.fixed(fields.get(index + 6), 16, HASH_LENGTH);
        final NativePreparedDeliveryV1 result = new NativePreparedDeliveryV1(nativeDeliveryId, destination,
                capability, target, partition, payload, metadata, eventTime, deliverAt, brokerDeliverAt, snapshot,
                guardAttestation, expiry, submissionHash);
        final byte[] expected = Bytes.sha256(Bytes.utf8("nereus-delay-native-submission-v1\0"),
                canonicalFields(nativeDeliveryId, destination, capability, target, partition, payload, metadata,
                        eventTime, deliverAt, brokerDeliverAt, snapshot, guardAttestation, expiry));
        if (!Bytes.constantTimeEquals(submissionHash, expected)) {
            throw new IllegalArgumentException("NativePreparedDelivery submission hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "NativePreparedDeliveryV1");
        return result;
    }

    public byte[] nativeDeliveryId() {
        return Bytes.copy(nativeDeliveryId);
    }

    public ProfileRefV1 destination() {
        return destination;
    }

    public ProfileRefV1 capability() {
        return capability;
    }

    public PulsarBrokerResourceIdentityV1 target() {
        return target;
    }

    public int physicalPartition() {
        return physicalPartition;
    }

    public byte[] inlinePayload() {
        return Bytes.copy(inlinePayload);
    }

    public PulsarMetadataV1 metadata() {
        return metadata;
    }

    public Long eventTimeEpochMs() {
        return eventTimeEpochMs;
    }

    public long deliverAtEpochMs() {
        return deliverAtEpochMs;
    }

    public long brokerDeliverAtEpochMs() {
        return brokerDeliverAtEpochMs;
    }

    public NativeCapabilitySnapshotV1 capabilitySnapshot() {
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
        return CanonicalProtobuf.message(output -> {
            writeFields(output);
            CanonicalProtobuf.bytes(output, 16, submissionHash);
        });
    }

    public NativePreparedRefV1 preparedRef() {
        return new NativePreparedRefV1(nativeDeliveryId, submissionHash, destination, target, physicalPartition,
                capabilitySnapshot.snapshotDigest(), capabilityExpiryEpochMs, Bytes.sha256(canonicalBytes()));
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof NativePreparedDeliveryV1 that)) {
            return false;
        }
        return physicalPartition == that.physicalPartition && Objects.equals(eventTimeEpochMs, that.eventTimeEpochMs)
                && deliverAtEpochMs == that.deliverAtEpochMs && brokerDeliverAtEpochMs == that.brokerDeliverAtEpochMs
                && capabilityExpiryEpochMs == that.capabilityExpiryEpochMs
                && Arrays.equals(nativeDeliveryId, that.nativeDeliveryId)
                && destination.equals(that.destination) && capability.equals(that.capability)
                && target.equals(that.target) && Arrays.equals(inlinePayload, that.inlinePayload)
                && metadata.equals(that.metadata) && capabilitySnapshot.equals(that.capabilitySnapshot)
                && Arrays.equals(resourceGuardAttestationSha256, that.resourceGuardAttestationSha256)
                && Arrays.equals(submissionHash, that.submissionHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(nativeDeliveryId), destination, capability, target, physicalPartition,
                Arrays.hashCode(inlinePayload), metadata, eventTimeEpochMs, deliverAtEpochMs,
                brokerDeliverAtEpochMs, capabilitySnapshot, Arrays.hashCode(resourceGuardAttestationSha256),
                capabilityExpiryEpochMs, Arrays.hashCode(submissionHash));
    }

    private void writeFields(final java.io.ByteArrayOutputStream output) {
        CanonicalProtobuf.uint32(output, 1, PREPARED_VERSION);
        CanonicalProtobuf.bytes(output, 2, nativeDeliveryId);
        CanonicalProtobuf.bytes(output, 3, destination.canonicalBytes());
        CanonicalProtobuf.bytes(output, 4, capability.canonicalBytes());
        CanonicalProtobuf.bytes(output, 5, target.canonicalBytes());
        CanonicalProtobuf.uint32(output, 6, physicalPartition);
        CanonicalProtobuf.bytes(output, 7, inlinePayload);
        CanonicalProtobuf.bytes(output, 8, metadata.canonicalBytes());
        if (eventTimeEpochMs != null) {
            CanonicalProtobuf.int64(output, 9, eventTimeEpochMs);
        }
        CanonicalProtobuf.int64(output, 10, deliverAtEpochMs);
        CanonicalProtobuf.int64(output, 11, brokerDeliverAtEpochMs);
        CanonicalProtobuf.bytes(output, 12, capabilitySnapshot.canonicalBytes());
        CanonicalProtobuf.bytes(output, 13, resourceGuardAttestationSha256);
        CanonicalProtobuf.int64(output, 14, capabilityExpiryEpochMs);
        CanonicalProtobuf.uint32(output, 15, NATIVE_ENCODING_VERSION);
    }

    private static byte[] canonicalFields(final byte[] nativeDeliveryId, final ProfileRefV1 destination,
                                           final ProfileRefV1 capability, final PulsarBrokerResourceIdentityV1 target,
                                           final int physicalPartition, final byte[] inlinePayload,
                                           final PulsarMetadataV1 metadata, final Long eventTimeEpochMs,
                                           final long deliverAtEpochMs, final long brokerDeliverAtEpochMs,
                                           final NativeCapabilitySnapshotV1 capabilitySnapshot,
                                           final byte[] resourceGuardAttestationSha256,
                                           final long capabilityExpiryEpochMs) {
        final NativePreparedDeliveryV1 fields = new NativePreparedDeliveryV1(nativeDeliveryId, destination,
                capability, target, physicalPartition, inlinePayload, metadata, eventTimeEpochMs, deliverAtEpochMs,
                brokerDeliverAtEpochMs, capabilitySnapshot, resourceGuardAttestationSha256, capabilityExpiryEpochMs,
                new byte[HASH_LENGTH]);
        return CanonicalProtobuf.message(fields::writeFields);
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
