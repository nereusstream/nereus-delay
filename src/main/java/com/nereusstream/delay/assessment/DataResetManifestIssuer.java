package com.nereusstream.delay.assessment;

import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.PrivateKey;
import java.util.List;
import java.util.Objects;

/** Cutover tooling that signs one freshly collected DataResetManifest. */
public final class DataResetManifestIssuer {
    private final PrivateKey issuerKey;
    private final int issuerKeyGeneration;

    public DataResetManifestIssuer(final PrivateKey issuerKey, final int issuerKeyGeneration) {
        this.issuerKey = Objects.requireNonNull(issuerKey, "issuerKey");
        if (issuerKeyGeneration <= 0) {
            throw new IllegalArgumentException("issuerKeyGeneration must be positive");
        }
        this.issuerKeyGeneration = issuerKeyGeneration;
    }

    /**
     * Issues a manifest only from caller-supplied fresh evidence. This class
     * does not enumerate, mutate, drain, migrate or reset runtime resources.
     */
    public DataResetManifest issue(
            final DataResetManifest.ManifestScope scope,
            final String sourceBaselineCommit,
            final long resetGeneration,
            final ArtifactGenerationSet artifacts,
            final List<DataResetManifest.ResourceIncarnation> resources,
            final byte[] freshResourceEvidenceDigest,
            final DataResetManifest.ObligationZeroProof obligationZeroProof,
            final List<DataResetManifest.WorkerCapability> workerCapabilities,
            final TrustedUtcIntervalEvidence createdAt,
            final DataResetManifest.ActivationWindow activationWindow) {
        return DataResetManifest.create(
                scope,
                sourceBaselineCommit,
                resetGeneration,
                artifacts,
                resources,
                freshResourceEvidenceDigest,
                obligationZeroProof,
                workerCapabilities,
                createdAt,
                activationWindow,
                issuerKeyGeneration,
                issuerKey);
    }
}
