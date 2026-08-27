package com.nereusstream.delay.assessment;

import com.nereusstream.delay.protocol.Bytes;
import java.util.Arrays;
import java.util.Objects;

/** Exact evidence that one local environment is isolated, synthetic, owned, and disposable. */
public record DisposableEnvironmentAttestation(
        String environmentId,
        String executionId,
        boolean isolatedResourceScope,
        boolean syntheticDataOnly,
        boolean exclusiveResourceOwnership,
        boolean cleanupAuthorized,
        byte[] evidenceSha256) {
    public DisposableEnvironmentAttestation {
        environmentId = AssessmentCanonical.text(environmentId, "environmentId");
        executionId = AssessmentCanonical.text(executionId, "executionId");
        evidenceSha256 = AssessmentCanonical.digest(evidenceSha256, "evidenceSha256");
    }

    public boolean complete() {
        return isolatedResourceScope && syntheticDataOnly && exclusiveResourceOwnership && cleanupAuthorized;
    }

    @Override
    public byte[] evidenceSha256() {
        return Bytes.copy(evidenceSha256);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof DisposableEnvironmentAttestation that
                && environmentId.equals(that.environmentId)
                && executionId.equals(that.executionId)
                && isolatedResourceScope == that.isolatedResourceScope
                && syntheticDataOnly == that.syntheticDataOnly
                && exclusiveResourceOwnership == that.exclusiveResourceOwnership
                && cleanupAuthorized == that.cleanupAuthorized
                && Arrays.equals(evidenceSha256, that.evidenceSha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                environmentId,
                executionId,
                isolatedResourceScope,
                syntheticDataOnly,
                exclusiveResourceOwnership,
                cleanupAuthorized,
                Arrays.hashCode(evidenceSha256));
    }
}
