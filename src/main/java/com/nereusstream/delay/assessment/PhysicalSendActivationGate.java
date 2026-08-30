package com.nereusstream.delay.assessment;

import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import java.util.Objects;
import java.util.Optional;

/**
 * Closed lifecycle authority for a physical native send.
 *
 * <p>A disposable integration run is authorized by one exact disposable
 * attestation. A persistent deployment is authorized only when Gate C,
 * SHADOW requirements, the signed cutover manifest, and the running source
 * baseline all agree. The two modes cannot be inferred from missing inputs.</p>
 */
public final class PhysicalSendActivationGate {
    private final Mode mode;
    private final String environmentId;
    private final ArtifactGenerationSet artifacts;
    private final DataResetActivationGate persistentManifestGate;
    private final LiveAuthorityGate liveAuthorityGate;

    private PhysicalSendActivationGate(
            final Mode mode,
            final String environmentId,
            final ArtifactGenerationSet artifacts,
            final DataResetActivationGate persistentManifestGate,
            final LiveAuthorityGate liveAuthorityGate) {
        this.mode = Objects.requireNonNull(mode, "mode");
        this.environmentId = Objects.requireNonNull(environmentId, "environmentId");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.persistentManifestGate = persistentManifestGate;
        this.liveAuthorityGate = Objects.requireNonNull(liveAuthorityGate, "liveAuthorityGate");
    }

    /** Authorizes synthetic physical testing in one explicitly disposable execution. */
    public static PhysicalSendActivationGate disposableLocal(
            final DeploymentSafetyGate.GateBStatus gateBStatus,
            final DisposableEnvironmentAttestation attestation,
            final ArtifactGenerationSet artifacts) {
        final DisposableEnvironmentAttestation exact = Objects.requireNonNull(attestation, "attestation");
        final DeploymentSafetyGate.Decision decision = DeploymentSafetyGate.localDisposable(
                gateBStatus,
                exact.environmentId(),
                EnvironmentClassification.DISPOSABLE_LOCAL,
                Optional.of(exact),
                DeploymentSafetyGate.LocalOperation.INTEGRATION_TEST);
        requireAuthorized(decision, "disposable physical testing");
        return new PhysicalSendActivationGate(
                Mode.DISPOSABLE_LOCAL,
                exact.environmentId(),
                Objects.requireNonNull(artifacts, "artifacts"),
                null,
                (ignoredArtifacts, ignoredTime) -> {});
    }

    /**
     * Authorizes an enabled persistent deployment only after all independent
     * lifecycle authorities and the exact running source baseline agree.
     */
    public static PhysicalSendActivationGate persistentEnabled(
            final DeploymentSafetyGate.GateBStatus gateBStatus,
            final EnvironmentClassification classification,
            final GateCAuthorization gateCAuthorization,
            final DeploymentSafetyGate.ShadowReadiness shadowReadiness,
            final DataResetActivationGate manifestGate,
            final String runningSourceBaselineCommit,
            final LiveAuthorityGate liveAuthorityGate) {
        final GateCAuthorization gateC = Objects.requireNonNull(gateCAuthorization, "gateCAuthorization");
        final DataResetActivationGate exactManifestGate = Objects.requireNonNull(manifestGate, "manifestGate");
        final DeploymentSafetyGate.Decision decision = DeploymentSafetyGate.deployment(
                gateBStatus,
                gateC.environmentId(),
                Objects.requireNonNull(classification, "classification"),
                Optional.of(gateC),
                DeploymentSafetyGate.DeploymentOperation.ENTER_ENABLED,
                Objects.requireNonNull(shadowReadiness, "shadowReadiness"));
        requireAuthorized(decision, "persistent physical activation");
        if (!gateC.environmentId().equals(exactManifestGate.manifest().scope().environmentId())) {
            throw new IllegalArgumentException("Gate C and DataResetManifest environments differ");
        }
        final String runningCommit = Objects.requireNonNull(runningSourceBaselineCommit, "runningSourceBaselineCommit");
        if (!runningCommit.equals(exactManifestGate.manifest().sourceBaselineCommit())) {
            throw new IllegalArgumentException("running source baseline differs from DataResetManifest");
        }
        return new PhysicalSendActivationGate(
                Mode.PERSISTENT_ENABLED,
                gateC.environmentId(),
                exactManifestGate.artifacts(),
                exactManifestGate,
                Objects.requireNonNull(liveAuthorityGate, "liveAuthorityGate"));
    }

    public Mode mode() {
        return mode;
    }

    public String environmentId() {
        return environmentId;
    }

    public ArtifactGenerationSet artifacts() {
        return artifacts;
    }

    /** Revalidates the exact artifact generation immediately before Producer ownership. */
    public void requirePhysicalSend(final ArtifactGenerationSet candidateArtifacts, final long trustedNowEpochMs) {
        final ArtifactGenerationSet candidate = Objects.requireNonNull(candidateArtifacts, "candidateArtifacts");
        if (!artifacts.equals(candidate) || !Bytes.constantTimeEquals(artifacts.setDigest(), candidate.setDigest())) {
            throw new IllegalStateException("physical send ArtifactGenerationSet is stale or mixed");
        }
        if (trustedNowEpochMs < 0) {
            throw new IllegalArgumentException("trustedNowEpochMs must be non-negative");
        }
        if (persistentManifestGate != null) {
            persistentManifestGate.requirePhysicalSend(
                    candidate, persistentManifestGate.manifest().manifestDigest(), trustedNowEpochMs);
        }
        liveAuthorityGate.requireCurrent(candidate, trustedNowEpochMs);
    }

    private static void requireAuthorized(final DeploymentSafetyGate.Decision decision, final String operation) {
        if (!decision.authorized()) {
            throw new IllegalStateException(operation + " denied: " + decision.code());
        }
    }

    public enum Mode {
        DISPOSABLE_LOCAL,
        PERSISTENT_ENABLED
    }

    /** Revalidates bounded deployment authority immediately before physical ownership. */
    @FunctionalInterface
    public interface LiveAuthorityGate {
        void requireCurrent(ArtifactGenerationSet candidateArtifacts, long trustedNowEpochMs);
    }
}
