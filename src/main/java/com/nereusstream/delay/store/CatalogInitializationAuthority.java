package com.nereusstream.delay.store;

/** Trusted namespace/genesis permission and current Owner/Source/session protection, separate from image auth. */
@FunctionalInterface
public interface CatalogInitializationAuthority {
    /** Authenticate outside Catalog monitors; the returned short guard holds/rechecks rights until the CAS ends. */
    TargetStoreBackend.CommitGuard acquire(CatalogInitialization initialization,
            TargetCheckpointRootVerifier.AuthenticatedImage image);
}
