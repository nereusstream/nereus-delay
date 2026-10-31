package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationRequest;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.runtime.TargetPayloadObligationSummaryBootstrap;
import java.util.Objects;

/** Reuses the original protected immutable image; request hashes and stored counts never supply its authority. */
public final class TargetCheckpointSummaryInitializationMaterials {
    /** Original image/Source/Pin and management-history loan, acquired and retained by the existing I/O owner. */
    public interface HistoricalImage {
        TargetCheckpointRootVerifier.AuthenticatedImage image();

        void requireExactResource(CheckpointResource resource);

        void requireHistoricalQualification(
                TargetPayloadSummaryInitializationRequest request, TargetQuotaMutation initializedAt);

        void requireCurrent();
    }

    @FunctionalInterface
    public interface OriginalImages {
        /** Resolves an already held exact historical image, not a caller's newly declared successful fold. */
        HistoricalImage resolve(CheckpointResource exact);
    }

    private final OriginalImages originals;

    public TargetCheckpointSummaryInitializationMaterials(OriginalImages originals) {
        this.originals = Objects.requireNonNull(originals);
    }

    public void requireAuthenticated(
            TargetPayloadSummaryInitializationRequest request, TargetQuotaMutation initializedAt) {
        final var held =
                Objects.requireNonNull(originals.resolve(request.protectedImage()), "original protected summary image");
        held.requireCurrent();
        held.requireExactResource(request.protectedImage());
        held.requireHistoricalQualification(request, initializedAt);
        if (com.nereusstream.delay.protocol.TargetQuotaMutation.increment(
                        request.auditedCut().sequence())
                != initializedAt.sequence()) {
            throw new IllegalStateException("summary initialization no longer follows its exact protected cut");
        }
        initializedAt.requireAfter(request.auditedCut());
        TargetPayloadObligationSummaryBootstrap.requireColdProtectedFold(Objects.requireNonNull(held.image()), request);
        held.requireExactResource(request.protectedImage());
        held.requireCurrent();
    }
}
