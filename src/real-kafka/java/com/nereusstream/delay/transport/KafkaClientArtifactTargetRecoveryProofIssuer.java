package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.TargetRecoveryProofTrust;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.TargetRecoveryProof;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Objects;

/** Privileged deployment key owner. It cannot sign a caller's arbitrary PUBLISHED/NOT_PUBLISHED DTO or raw bytes. */
public final class KafkaClientArtifactTargetRecoveryProofIssuer {
    private final AuthorIdentity service;
    private final int version;
    private volatile PrivateKey key;
    private final TargetRecoveryProofTrust trust;

    public KafkaClientArtifactTargetRecoveryProofIssuer(
            AuthorIdentity service, int version, PrivateKey key, TargetRecoveryProofTrust trust) {
        if (Objects.requireNonNull(service).kind() != AuthorIdentity.Kind.SERVICE || version == 0) {
            throw new IllegalArgumentException("explicit recovery proof issuer required");
        }
        this.service = service;
        this.version = version;
        this.key = Objects.requireNonNull(key);
        this.trust = Objects.requireNonNull(trust);
    }
    /** Closing does not invalidate verification of already signed historical proofs. */
    public void invalidate() {
        key = null;
    }

    public TargetRecoveryProof sign(KafkaClientArtifactTargetRecoveryRuntime.ArchiveObservation observation) {
        final var actual = Objects.requireNonNull(observation, "private actual typed recovery observation");
        actual.requireCurrent();
        trust.requireCurrentIssuer(service, version, actual.currentTime());
        try {
            final byte[] body = actual.unsigned(service, version);
            final var signer = Signature.getInstance("Ed25519");
            signer.initSign(Objects.requireNonNull(key, "recovery proof signing owner closed"));
            signer.update(TargetRecoveryProof.signingDigest(body));
            final var proof = TargetRecoveryProof.assemble(body, signer.sign());
            trust.verify(trust.objectKey(proof.digest()), proof.canonicalBytes());
            actual.requireCurrent();
            trust.requireCurrentIssuer(service, version, actual.currentTime());
            return proof;
        } catch (RuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IllegalStateException("actual recovery proof signing unavailable", failure);
        }
    }
}
