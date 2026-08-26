package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 request that publishes one immutable Destination Profile version. */
public final class PublishDestinationProfileRequest implements ControlOperationRequestBranch {
    public static final int INITIAL_SECRET_GENERATION = 1;

    private final ProfileSemanticEnvelope profile;
    private final CredentialBinding credentialBinding;

    public PublishDestinationProfileRequest(
            final ProfileSemanticEnvelope profile, final CredentialBinding credentialBinding) {
        this.profile = Objects.requireNonNull(profile, "profile");
        if (profile.profileKind() != ProfileKind.DESTINATION) {
            throw new IllegalArgumentException("published Profile must have DESTINATION kind");
        }
        this.credentialBinding = Objects.requireNonNull(credentialBinding, "credentialBinding");
        if (!profile.ref().equals(credentialBinding.profile())
                || credentialBinding.secretGeneration() != INITIAL_SECRET_GENERATION) {
            throw new IllegalArgumentException("published Destination binding must be generation 1 for the Profile");
        }
    }

    public ProfileSemanticEnvelope profile() {
        return profile;
    }

    public CredentialBinding credentialBinding() {
        return credentialBinding;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, credentialBinding.canonicalBytes());
        });
    }

    public static PublishDestinationProfileRequest decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "PublishDestinationProfileRequest");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "PublishDestinationProfileRequest");
        final PublishDestinationProfileRequest result = new PublishDestinationProfileRequest(
                ProfileSemanticEnvelope.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                CredentialBinding.decode(QueryCodecSupport.nested(fields.get(1), 2)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PublishDestinationProfileRequest");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PublishDestinationProfileRequest that
                && profile.equals(that.profile)
                && credentialBinding.equals(that.credentialBinding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(profile, credentialBinding);
    }
}
