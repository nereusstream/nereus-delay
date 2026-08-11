package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;

import java.util.Optional;

/**
 * CAS authority for one checkpoint upload intent.
 *
 * <p>The embedded file/in-memory implementation and the Oxia implementation
 * share these exact value transitions. Catalog publication, Object Store
 * upload and Owner Lease/session authorization remain separate authorities.</p>
 */
public interface CheckpointUploadIntentAuthority {
    CheckpointUploadIntentV1 create(CheckpointUploadIntentV1 pending);

    CheckpointUploadIntentV1 publish(CheckpointUploadIntentV1 expectedPending,
                                     CheckpointResourceV1 resource);

    Optional<CheckpointUploadIntentV1> currentPublishedFor(CheckpointUploadIntentV1 expectedPending);

    CheckpointUploadIntentV1 beginReaping(CheckpointUploadIntentV1 expectedPending,
                                          TrustedUtcIntervalEvidence evidence);

    CheckpointUploadIntentV1 beginReaping(CheckpointUploadIntentV1 expectedPending,
                                          TrustedUtcIntervalEvidence evidence,
                                          RecoveryCatalogAuthority catalog);

    Optional<CheckpointUploadIntentV1> current(CheckpointUploadIntentV1 identity);
}
