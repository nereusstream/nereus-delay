package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.Optional;

/**
 * CAS authority for one checkpoint upload intent.
 *
 * <p>The embedded file/in-memory implementation and the Oxia implementation
 * share these exact value transitions. Catalog publication, Object Store
 * upload and Owner Lease/session authorization remain separate authorities.</p>
 */
public interface CheckpointUploadIntentAuthority {
    CheckpointUploadIntent create(CheckpointUploadIntent pending);

    CheckpointUploadIntent publish(CheckpointUploadIntent expectedPending, CheckpointResource resource);

    Optional<CheckpointUploadIntent> currentPublishedFor(CheckpointUploadIntent expectedPending);

    CheckpointUploadIntent beginReaping(CheckpointUploadIntent expectedPending, TrustedUtcIntervalEvidence evidence);

    CheckpointUploadIntent beginReaping(
            CheckpointUploadIntent expectedPending,
            TrustedUtcIntervalEvidence evidence,
            RecoveryCatalogAuthority catalog);

    Optional<CheckpointUploadIntent> current(CheckpointUploadIntent identity);
}
