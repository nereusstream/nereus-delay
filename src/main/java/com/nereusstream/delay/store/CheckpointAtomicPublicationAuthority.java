package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;

/**
 * Optional upload-intent authority that atomically binds the uploaded object
 * to the Recovery Catalog in one authority-record CAS.
 *
 * <p>The provider upload has already completed when this method is called.
 * Implementations must validate the exact pending intent, object identity,
 * manifest and catalog generation before committing the PUBLISHED intent and
 * catalog manifest together. A response-loss retry must reread the exact
 * successor state before reporting success.</p>
 */
public interface CheckpointAtomicPublicationAuthority
        extends CheckpointUploadIntentAuthority, RecoveryCatalogAuthority {

    /**
     * Commits the PUBLISHED intent and its catalog binding as one authority
     * operation.
     *
     * @param expectedPending exact PENDING_UPLOAD value created before the
     * checkpoint attempt
     * @param resource immutable provider object identity returned by upload
     * @param manifest exact local checkpoint manifest bound to that object
     * @param expectedCatalogGeneration catalog generation carried by the intent
     * @return the exact PUBLISHED successor
     */
    CheckpointUploadIntent publishUploadedCheckpointAtomically(
            CheckpointUploadIntent expectedPending,
            CheckpointResource resource,
            CheckpointManifest manifest,
            long expectedCatalogGeneration);
}
