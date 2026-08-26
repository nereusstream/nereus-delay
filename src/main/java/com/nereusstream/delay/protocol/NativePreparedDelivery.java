package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Exact Pulsar AUTO_FAST prepared submission; no Broker I/O is performed here. */
public final class NativePreparedDelivery {
    public static final int PREPARED_VERSION = 1;
    public static final int NATIVE_ENCODING_VERSION = 1;
    public static final int HASH_LENGTH = 32;
    private static final int NATIVE_DELIVERY_ID_LENGTH = 32;

    private final byte[] nativeDeliveryId;
    private final ProfileRef destination;
    private final ProfileRef capability;
    private final PulsarBrokerResourceIdentity target;
    private final int physicalPartition;
    private final byte[] inlinePayload;
    private final PulsarMetadata metadata;
    private final Long eventTimeEpochMs;
    private final long deliverAtEpochMs;
    private final long brokerDeliverAtEpochMs;
    private final NativeCapabilitySnapshot capabilitySnapshot;
    private final byte[] resourceGuardAttestationSha256;
    private final long capabilityExpiryEpochMs;
    private final byte[] submissionHash;

    private NativePreparedDelivery(
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
            final NativeCapabilitySnapshot capabilitySnapshot,
            final byte[] resourceGuardAttestationSha256,
            final long capabilityExpiryEpochMs,
            final byte[] submissionHash) {
        requireNonZero(nativeDeliveryId, NATIVE_DELIVERY_ID_LENGTH, "nativeDeliveryId");
        this.nativeDeliveryId = Bytes.copy(nativeDeliveryId);
        this.destination = Objects.requireNonNull(destination, "destination");
        if (destination.profileKind() != ProfileKind.DESTINATION) {
            throw new IllegalArgumentException("native prepared destination must be a DESTINATION profile");
        }
        this.capability = Objects.requireNonNull(capability, "capability");
        if (capability.profileKind() != ProfileKind.DELIVERY_CAPABILITY) {
            throw new IllegalArgumentException("native prepared capability must be a DELIVERY_CAPABILITY profile");
        }
        this.target = Objects.requireNonNull(target, "target");
        if (deliverAtEpochMs < 0
                || brokerDeliverAtEpochMs < deliverAtEpochMs
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
        if (capabilityExpiryEpochMs < 0) {
            throw new IllegalArgumentException("capability expiry must be non-negative");
        }
        this.capabilityExpiryEpochMs = capabilityExpiryEpochMs;
        this.submissionHash = fixed(submissionHash, "submissionHash");
    }

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
        final byte[] fields = canonicalFields(
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
                capabilitySnapshot,
                capabilitySnapshot.resourceGuardAttestationSha256(),
                capabilitySnapshot.notAfterEpochMs());
        final byte[] submissionHash = Bytes.sha256(Bytes.utf8("nereus-delay-native-submission\0"), fields);
        return new NativePreparedDelivery(
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
                capabilitySnapshot,
                capabilitySnapshot.resourceGuardAttestationSha256(),
                capabilitySnapshot.notAfterEpochMs(),
                submissionHash);
    }

    public static NativePreparedDelivery decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "NativePreparedDelivery");
        if (fields.size() != 15 && fields.size() != 16) {
            throw new IllegalArgumentException("NativePreparedDelivery fields are incomplete or unknown");
        }
        if (QueryCodecSupport.uint(fields.get(0), 1) != PREPARED_VERSION
                || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("unsupported NativePreparedDelivery version");
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
            throw new IllegalArgumentException("NativePreparedDelivery timestamp fields are invalid");
        }
        final long deliverAt = QueryCodecSupport.uint(fields.get(index), 10);
        final long brokerDeliverAt = QueryCodecSupport.uint(fields.get(index + 1), 11);
        final NativeCapabilitySnapshot snapshot =
                NativeCapabilitySnapshot.decode(QueryCodecSupport.nested(fields.get(index + 2), 12));
        final byte[] guardAttestation = QueryCodecSupport.fixed(fields.get(index + 3), 13, HASH_LENGTH);
        final long expiry = QueryCodecSupport.uint(fields.get(index + 4), 14);
        if (QueryCodecSupport.uint(fields.get(index + 5), 15) != NATIVE_ENCODING_VERSION) {
            throw new IllegalArgumentException("unsupported NativePreparedDelivery encoding version");
        }
        final byte[] submissionHash = QueryCodecSupport.fixed(fields.get(index + 6), 16, HASH_LENGTH);
        final NativePreparedDelivery result = new NativePreparedDelivery(
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
                snapshot,
                guardAttestation,
                expiry,
                submissionHash);
        final byte[] expected = Bytes.sha256(
                Bytes.utf8("nereus-delay-native-submission\0"),
                canonicalFields(
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
                        snapshot,
                        guardAttestation,
                        expiry));
        if (!Bytes.constantTimeEquals(submissionHash, expected)) {
            throw new IllegalArgumentException("NativePreparedDelivery submission hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "NativePreparedDelivery");
        return result;
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

    public long brokerDeliverAtEpochMs() {
        return brokerDeliverAtEpochMs;
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
        return CanonicalProtobuf.message(output -> {
            writeFields(output);
            CanonicalProtobuf.bytes(output, 16, submissionHash);
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
        return physicalPartition == that.physicalPartition
                && Objects.equals(eventTimeEpochMs, that.eventTimeEpochMs)
                && deliverAtEpochMs == that.deliverAtEpochMs
                && brokerDeliverAtEpochMs == that.brokerDeliverAtEpochMs
                && capabilityExpiryEpochMs == that.capabilityExpiryEpochMs
                && Arrays.equals(nativeDeliveryId, that.nativeDeliveryId)
                && destination.equals(that.destination)
                && capability.equals(that.capability)
                && target.equals(that.target)
                && Arrays.equals(inlinePayload, that.inlinePayload)
                && metadata.equals(that.metadata)
                && capabilitySnapshot.equals(that.capabilitySnapshot)
                && Arrays.equals(resourceGuardAttestationSha256, that.resourceGuardAttestationSha256)
                && Arrays.equals(submissionHash, that.submissionHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                Arrays.hashCode(nativeDeliveryId),
                destination,
                capability,
                target,
                physicalPartition,
                Arrays.hashCode(inlinePayload),
                metadata,
                eventTimeEpochMs,
                deliverAtEpochMs,
                brokerDeliverAtEpochMs,
                capabilitySnapshot,
                Arrays.hashCode(resourceGuardAttestationSha256),
                capabilityExpiryEpochMs,
                Arrays.hashCode(submissionHash));
    }

    private void writeFields(final java.io.ByteArrayOutputStream output) {
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
        CanonicalProtobuf.int64(output, 11, brokerDeliverAtEpochMs);
        CanonicalProtobuf.bytes(output, 12, capabilitySnapshot.canonicalBytes());
        CanonicalProtobuf.bytes(output, 13, resourceGuardAttestationSha256);
        CanonicalProtobuf.int64(output, 14, capabilityExpiryEpochMs);
        CanonicalProtobuf.uint32(output, 15, NATIVE_ENCODING_VERSION);
    }

    private static byte[] canonicalFields(
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
            final NativeCapabilitySnapshot capabilitySnapshot,
            final byte[] resourceGuardAttestationSha256,
            final long capabilityExpiryEpochMs) {
        final NativePreparedDelivery fields = new NativePreparedDelivery(
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
                capabilitySnapshot,
                resourceGuardAttestationSha256,
                capabilityExpiryEpochMs,
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
