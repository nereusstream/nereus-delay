package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Opaque, bounded upload capability; its bytes never become a DB/receipt locator. */
public final class OpaquePayloadUploadHandle {
    public static final int VERSION = 1;
    public static final int HASH_LENGTH = 32;
    public static final int MAX_CAPABILITY_ENVELOPE_BYTES = 1 << 20;

    private final byte[] reservationId;
    private final ProfileRef objectStoreProfile;
    private final UploadHandleKind kind;
    private final long expiresAtEpochMs;
    private final byte[] capabilityEnvelope;
    private final byte[] capabilityEnvelopeSha256;

    private OpaquePayloadUploadHandle(
            final byte[] reservationId,
            final ProfileRef objectStoreProfile,
            final UploadHandleKind kind,
            final long expiresAtEpochMs,
            final byte[] capabilityEnvelope,
            final byte[] capabilityEnvelopeSha256) {
        requireNonZero(reservationId, "reservationId");
        this.reservationId = Bytes.copy(reservationId);
        this.objectStoreProfile = Objects.requireNonNull(objectStoreProfile, "objectStoreProfile");
        if (objectStoreProfile.profileKind() != ProfileKind.OBJECT_STORE) {
            throw new IllegalArgumentException("upload handle requires an OBJECT_STORE profile");
        }
        this.kind = Objects.requireNonNull(kind, "kind");
        if (expiresAtEpochMs < 0) {
            throw new IllegalArgumentException("upload handle expiry must be non-negative");
        }
        Objects.requireNonNull(capabilityEnvelope, "capabilityEnvelope");
        if (capabilityEnvelope.length == 0 || capabilityEnvelope.length > MAX_CAPABILITY_ENVELOPE_BYTES) {
            throw new IllegalArgumentException("capability envelope is outside the bounded range");
        }
        this.expiresAtEpochMs = expiresAtEpochMs;
        this.capabilityEnvelope = Bytes.copy(capabilityEnvelope);
        Bytes.requireLength(capabilityEnvelopeSha256, HASH_LENGTH, "capabilityEnvelopeSha256");
        this.capabilityEnvelopeSha256 = Bytes.copy(capabilityEnvelopeSha256);
        if (!Bytes.constantTimeEquals(this.capabilityEnvelopeSha256, Bytes.sha256(this.capabilityEnvelope))) {
            throw new IllegalArgumentException("capability envelope digest mismatch");
        }
    }

    public static OpaquePayloadUploadHandle create(
            final byte[] reservationId,
            final ProfileRef objectStoreProfile,
            final UploadHandleKind kind,
            final long expiresAtEpochMs,
            final byte[] capabilityEnvelope) {
        return new OpaquePayloadUploadHandle(
                reservationId,
                objectStoreProfile,
                kind,
                expiresAtEpochMs,
                capabilityEnvelope,
                Bytes.sha256(capabilityEnvelope));
    }

    public static OpaquePayloadUploadHandle decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "OpaquePayloadUploadHandle");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7}, "OpaquePayloadUploadHandle");
        if (QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported upload handle version");
        }
        final OpaquePayloadUploadHandle result = new OpaquePayloadUploadHandle(
                QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                ProfileRef.decode(QueryCodecSupport.nested(fields.get(2), 3)),
                UploadHandleKind.fromWire(QueryCodecSupport.uint(fields.get(3), 4)),
                QueryCodecSupport.uint(fields.get(4), 5),
                QueryCodecSupport.bytes(fields.get(5), 6),
                QueryCodecSupport.fixed(fields.get(6), 7, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "OpaquePayloadUploadHandle");
        return result;
    }

    public byte[] reservationId() {
        return Bytes.copy(reservationId);
    }

    public ProfileRef objectStoreProfile() {
        return objectStoreProfile;
    }

    public UploadHandleKind kind() {
        return kind;
    }

    public long expiresAtEpochMs() {
        return expiresAtEpochMs;
    }

    public byte[] capabilityEnvelope() {
        return Bytes.copy(capabilityEnvelope);
    }

    public byte[] capabilityEnvelopeSha256() {
        return Bytes.copy(capabilityEnvelopeSha256);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, reservationId);
            CanonicalProtobuf.bytes(output, 3, objectStoreProfile.canonicalBytes());
            CanonicalProtobuf.uint32(output, 4, kind.wireValue());
            CanonicalProtobuf.int64(output, 5, expiresAtEpochMs);
            CanonicalProtobuf.bytes(output, 6, capabilityEnvelope);
            CanonicalProtobuf.bytes(output, 7, capabilityEnvelopeSha256);
        });
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof OpaquePayloadUploadHandle that)) {
            return false;
        }
        return expiresAtEpochMs == that.expiresAtEpochMs
                && kind == that.kind
                && objectStoreProfile.equals(that.objectStoreProfile)
                && Arrays.equals(reservationId, that.reservationId)
                && Arrays.equals(capabilityEnvelope, that.capabilityEnvelope)
                && Arrays.equals(capabilityEnvelopeSha256, that.capabilityEnvelopeSha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                Arrays.hashCode(reservationId),
                objectStoreProfile,
                kind,
                expiresAtEpochMs,
                Arrays.hashCode(capabilityEnvelope),
                Arrays.hashCode(capabilityEnvelopeSha256));
    }

    private static void requireNonZero(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        for (byte item : value) {
            if (item != 0) {
                return;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }
}
