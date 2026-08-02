package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Registry §6.3 request that publishes one immutable Destination Profile version. */
public final class PublishDestinationProfileRequestV1 implements ControlOperationRequestBranchV1 {
    public static final int INITIAL_SECRET_GENERATION = 1;

    private final ProfileSemanticEnvelopeV1 profile;
    private final CredentialBindingV1 credentialBinding;

    public PublishDestinationProfileRequestV1(final ProfileSemanticEnvelopeV1 profile,
                                              final CredentialBindingV1 credentialBinding) {
        this.profile = Objects.requireNonNull(profile, "profile");
        if (profile.profileKind() != ProfileKindV1.DESTINATION) {
            throw new IllegalArgumentException("published Profile must have DESTINATION kind");
        }
        this.credentialBinding = Objects.requireNonNull(credentialBinding, "credentialBinding");
        if (!profile.ref().equals(credentialBinding.profile())
                || credentialBinding.secretGeneration() != INITIAL_SECRET_GENERATION) {
            throw new IllegalArgumentException("published Destination binding must be generation 1 for the Profile");
        }
    }

    public ProfileSemanticEnvelopeV1 profile() {
        return profile;
    }

    public CredentialBindingV1 credentialBinding() {
        return credentialBinding;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, profile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, credentialBinding.canonicalBytes());
        });
    }

    public static PublishDestinationProfileRequestV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "PublishDestinationProfileRequestV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2}, "PublishDestinationProfileRequestV1");
        final PublishDestinationProfileRequestV1 result = new PublishDestinationProfileRequestV1(
                ProfileSemanticEnvelopeV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                CredentialBindingV1.decode(QueryCodecSupport.nested(fields.get(1), 2)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(),
                "PublishDestinationProfileRequestV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PublishDestinationProfileRequestV1 that
                && profile.equals(that.profile)
                && credentialBinding.equals(that.credentialBinding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(profile, credentialBinding);
    }
}
