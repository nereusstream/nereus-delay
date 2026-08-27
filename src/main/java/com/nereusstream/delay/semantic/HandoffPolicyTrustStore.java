package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.HandoffPolicySnapshot;
import com.nereusstream.delay.protocol.SourcePosition;
import java.security.PublicKey;
import java.util.Objects;
import java.util.Optional;

/**
 * Historical trust view for signed handoff policy snapshots.
 *
 * <p>Implementations must resolve a key and activation marker as of the
 * supplied source position. A mutable "current key" is not sufficient for
 * replay because a source record may legitimately reference an older lease.</p>
 */
public interface HandoffPolicyTrustStore {
    Optional<PublicKey> issuerKey(int issuerKeyGeneration, SourcePosition sourcePosition);

    Optional<SourcePosition> activationPosition(byte[] policyScopeDigest, long policyGeneration);

    default void requireTrusted(
            final HandoffPolicySnapshot snapshot,
            final byte[] expectedScopeDigest,
            final byte[] expectedArtifactGenerationSetDigest,
            final SourcePosition sourcePosition) {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(expectedScopeDigest, "expectedScopeDigest");
        Objects.requireNonNull(expectedArtifactGenerationSetDigest, "expectedArtifactGenerationSetDigest");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        if (!java.util.Arrays.equals(snapshot.policyScopeDigest(), expectedScopeDigest)
                || !java.util.Arrays.equals(
                        snapshot.artifactGenerationSetDigest(), expectedArtifactGenerationSetDigest)) {
            throw new IllegalArgumentException("handoff policy scope or artifact generation mismatch");
        }
        final PublicKey key = issuerKey(snapshot.issuerKeyGeneration(), sourcePosition)
                .orElseThrow(() ->
                        new IllegalArgumentException("handoff policy issuer key is not trusted at source position"));
        final SourcePosition activation = activationPosition(snapshot.policyScopeDigest(), snapshot.generation())
                .orElseThrow(() -> new IllegalArgumentException("handoff policy generation has no activation marker"));
        if (!activation.sameSourceIdentity(sourcePosition)
                || activation.shardId() == null
                || activation.compareTo(sourcePosition) > 0) {
            throw new IllegalArgumentException("handoff policy was not active at source position");
        }
        if (!snapshot.verifySignature(key)) {
            throw new IllegalArgumentException("handoff policy signature is invalid");
        }
    }
}
