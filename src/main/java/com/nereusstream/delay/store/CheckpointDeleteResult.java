package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResourceV1;
import com.nereusstream.delay.protocol.ResourceDeleteConfirmedBody;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.Objects;

/** Provider evidence returned after the complete checkpoint object set is deleted. */
public record CheckpointDeleteResult(
        CheckpointResourceV1 resource,
        ResourceDeleteConfirmedBody.DeleteOutcome outcome,
        byte[] providerRequestIdHash,
        byte[] responseHash) {
    private static final int HASH_LENGTH = 32;

    public CheckpointDeleteResult {
        Objects.requireNonNull(resource, "resource");
        Objects.requireNonNull(outcome, "outcome");
        Bytes.requireLength(providerRequestIdHash, HASH_LENGTH, "providerRequestIdHash");
        Bytes.requireLength(responseHash, HASH_LENGTH, "responseHash");
        if (outcome == ResourceDeleteConfirmedBody.DeleteOutcome.DELETED && resource.immutableVersion().length == 0) {
            throw new IllegalArgumentException("deleted checkpoint must retain an immutable manifest version");
        }
        providerRequestIdHash = Bytes.copy(providerRequestIdHash);
        responseHash = Bytes.copy(responseHash);
    }

    @Override
    public byte[] providerRequestIdHash() {
        return Bytes.copy(providerRequestIdHash);
    }

    @Override
    public byte[] responseHash() {
        return Bytes.copy(responseHash);
    }

    /** Adapts this provider result to the canonical delete-confirmed evidence body. */
    public ResourceDeleteConfirmedBody.ExternalDeleteEvidence externalEvidence(
            final byte[] resourceIdentityHash, final TrustedUtcIntervalEvidence observedAt) {
        Bytes.requireLength(resourceIdentityHash, HASH_LENGTH, "resourceIdentityHash");
        Objects.requireNonNull(observedAt, "observedAt");
        return new ResourceDeleteConfirmedBody.ExternalDeleteEvidence(
                resourceIdentityHash,
                providerRequestIdHash,
                outcome,
                outcome == ResourceDeleteConfirmedBody.DeleteOutcome.DELETED
                        ? resource.immutableVersion()
                        : new byte[0],
                new byte[0],
                responseHash,
                observedAt);
    }
}
