package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.store.CatalogInitialization;
import com.nereusstream.delay.store.CatalogInitializationAuthority;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.List;
import java.util.Objects;

/** Protects the exact eligible reader set and namespace permission before Target Catalog schema-2 initialization. */
public final class ProtocolCompatibleCatalogInitializationAuthority implements CatalogInitializationAuthority {
    /**
     * Authenticates/protects the complete eligible membership, exact declarations/revisions/sessions and activated
     * schema bundle until initialization ends. Acquisition may perform finite provider I/O; requireCurrent must
     * check held snapshots only, including membership/session loss, without a remote read under Catalog monitors.
     */
    @FunctionalInterface
    public interface ReaderSetProtection {
        TargetStoreBackend.CommitGuard acquire(CatalogInitialization initialization, ArtifactGenerationSet artifacts,
                ProtocolActivationAuthorityCoordinator.EligibleReaderSet readers);
    }

    private final ProtocolActivationAuthorityCoordinator capabilities;
    private final ArtifactGenerationSet artifacts;
    private final List<String> eligibleReaders;
    private final ReaderSetProtection readers;
    private final CatalogInitializationAuthority namespace;

    /** Artifact/schema trust and eligibility come from the activated deployment, not from a codec version constant. */
    public ProtocolCompatibleCatalogInitializationAuthority(ProtocolActivationAuthorityCoordinator capabilities,
            ArtifactGenerationSet artifacts, List<String> eligibleReaders, ReaderSetProtection readers,
            CatalogInitializationAuthority namespace) {
        this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.eligibleReaders = List.copyOf(Objects.requireNonNull(eligibleReaders, "eligibleReaders"));
        if (this.eligibleReaders.isEmpty()) {
            throw new IllegalArgumentException("eligible Catalog readers are empty");
        }
        this.readers = Objects.requireNonNull(readers, "readers");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
    }

    @Override
    public TargetStoreBackend.CommitGuard acquire(CatalogInitialization initialization,
            TargetCheckpointRootVerifier.AuthenticatedImage image) {
        Objects.requireNonNull(initialization, "initialization");
        final var compatible = capabilities.requireEligibleReaders(
                ProtocolTuple.currentSystemMutation(), artifacts, eligibleReaders);
        final var readerGuard = Objects.requireNonNull(readers.acquire(initialization, artifacts, compatible),
                "reader protection guard");
        TargetStoreBackend.CommitGuard namespaceGuard = null;
        try {
            readerGuard.requireCurrent();
            namespaceGuard = Objects.requireNonNull(namespace.acquire(initialization, image), "namespace guard");
            readerGuard.requireCurrent();
            namespaceGuard.requireCurrent();
            return combined(readerGuard, namespaceGuard);
        } catch (RuntimeException | Error failure) {
            closeAfterFailure(namespaceGuard, failure);
            closeAfterFailure(readerGuard, failure);
            throw failure;
        }
    }

    private static TargetStoreBackend.CommitGuard combined(TargetStoreBackend.CommitGuard readers,
            TargetStoreBackend.CommitGuard namespace) {
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {
                readers.requireCurrent();
                namespace.requireCurrent();
                readers.requireCurrent();
            }

            @Override
            public void close() {
                try {
                    namespace.close();
                } catch (RuntimeException | Error failure) {
                    closeAfterFailure(readers, failure);
                    throw failure;
                }
                readers.close();
            }
        };
    }

    private static void closeAfterFailure(TargetStoreBackend.CommitGuard guard, Throwable failure) {
        if (guard == null) { return; }
        try { guard.close(); }
        catch (RuntimeException | Error closeFailure) { failure.addSuppressed(closeFailure); }
    }
}
