package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Public, read-only view of the canonical {@code ReadyCertificateV1} nested
 * value.  Admission and Lane-state parsing intentionally share the same
 * validator so certificate digests and channel projections cannot diverge.
 */
public final class ReadyCertificateV1 {
    private final PublishAdmissionBody.ReadyCertificate delegate;

    private ReadyCertificateV1(final PublishAdmissionBody.ReadyCertificate delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        final PublishAdmissionBody.Channel channel = PublishAdmissionBody.decodeChannelIdentity(delegate.channel());
        if (!Arrays.equals(delegate.destinationLaneId(), channel.laneId())
                || !Arrays.equals(delegate.laneIncarnation(), channel.laneIncarnation())) {
            throw new IllegalArgumentException("ReadyCertificate lane does not match ChannelResourceIdentity");
        }
        if (delegate.validUntilEpochMs() <= delegate.issuedAt().latestEpochMs()) {
            throw new IllegalArgumentException("ReadyCertificate valid-until must exceed issued UTC interval");
        }
        validateNestedCanonical(delegate.canonicalBytes());
    }

    public static ReadyCertificateV1 decode(final byte[] encoded) {
        return new ReadyCertificateV1(PublishAdmissionBody.decodeReadyCertificate(
                Objects.requireNonNull(encoded, "encoded")));
    }

    public byte[] canonicalBytes() {
        return delegate.canonicalBytes();
    }

    public byte[] ownerIdentity() {
        return delegate.ownerIdentity();
    }

    public byte[] storeIncarnation() {
        return delegate.storeIncarnation();
    }

    public byte[] destinationLaneId() {
        return delegate.destinationLaneId();
    }

    public byte[] laneIncarnation() {
        return delegate.laneIncarnation();
    }

    public byte[] channel() {
        return delegate.channel();
    }

    public long validUntilEpochMs() {
        return delegate.validUntilEpochMs();
    }

    public TrustedUtcIntervalEvidence issuedAt() {
        return delegate.issuedAt();
    }

    public long credentialBindingGeneration() {
        return delegate.credentialBindingGeneration();
    }

    public byte[] credentialBindingDigest() {
        return delegate.credentialBindingDigest();
    }

    public byte[] resolvedCredentialVersionFingerprintDigest() {
        return delegate.credentialFingerprint();
    }

    public byte[] certificateDigest() {
        return delegate.certificateDigest();
    }

    private static void validateNestedCanonical(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        while (reader.hasRemaining()) {
            final CanonicalProtobuf.Reader.Field field = reader.next();
            if (field.number() == 7 || field.number() == 8) {
                final byte[] nested = QueryCodecSupport.bytes(field, field.number());
                final java.util.List<CanonicalProtobuf.Reader.Field> nestedFields =
                        QueryCodecSupport.read(nested, "ReadyCertificate nested field " + field.number());
                final byte[] canonical = CanonicalProtobuf.message(output -> {
                    for (CanonicalProtobuf.Reader.Field nestedField : nestedFields) {
                        if (nestedField.wireType() == 0) {
                            CanonicalProtobuf.uint64(output, nestedField.number(), nestedField.unsignedValue());
                        } else {
                            CanonicalProtobuf.bytes(output, nestedField.number(), nestedField.rawValue());
                        }
                    }
                });
                QueryCodecSupport.requireCanonical(nested, canonical,
                        "ReadyCertificate nested field " + field.number());
            }
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ReadyCertificateV1 that
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
