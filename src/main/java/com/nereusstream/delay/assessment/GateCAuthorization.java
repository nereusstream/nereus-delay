package com.nereusstream.delay.assessment;

import com.nereusstream.delay.protocol.Bytes;
import java.util.Arrays;
import java.util.Objects;

/** Positive Gate C authority bound to one exact persistent environment and assessment scope. */
public record GateCAuthorization(
        String environmentId,
        EnvironmentClassification environmentClassification,
        Resolution resolution,
        byte[] assessmentScopeDigest,
        byte[] assessmentReceiptDigest,
        byte[] gateReceiptDigest) {
    public GateCAuthorization {
        environmentId = AssessmentCanonical.text(environmentId, "environmentId");
        Objects.requireNonNull(environmentClassification, "environmentClassification");
        if (!environmentClassification.requiresDeploymentSafetyAssessment()) {
            throw new IllegalArgumentException("Gate C authority requires a persistent environment classification");
        }
        Objects.requireNonNull(resolution, "resolution");
        assessmentScopeDigest = AssessmentCanonical.digest(assessmentScopeDigest, "assessmentScopeDigest");
        assessmentReceiptDigest = AssessmentCanonical.digest(assessmentReceiptDigest, "assessmentReceiptDigest");
        gateReceiptDigest = AssessmentCanonical.digest(gateReceiptDigest, "gateReceiptDigest");
    }

    @Override
    public byte[] assessmentScopeDigest() {
        return Bytes.copy(assessmentScopeDigest);
    }

    @Override
    public byte[] assessmentReceiptDigest() {
        return Bytes.copy(assessmentReceiptDigest);
    }

    @Override
    public byte[] gateReceiptDigest() {
        return Bytes.copy(gateReceiptDigest);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof GateCAuthorization that
                && environmentId.equals(that.environmentId)
                && environmentClassification == that.environmentClassification
                && resolution == that.resolution
                && Arrays.equals(assessmentScopeDigest, that.assessmentScopeDigest)
                && Arrays.equals(assessmentReceiptDigest, that.assessmentReceiptDigest)
                && Arrays.equals(gateReceiptDigest, that.gateReceiptDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                environmentId,
                environmentClassification,
                resolution,
                Arrays.hashCode(assessmentScopeDigest),
                Arrays.hashCode(assessmentReceiptDigest),
                Arrays.hashCode(gateReceiptDigest));
    }

    public enum Resolution {
        RESET,
        RETAIN,
        MIGRATED
    }
}
