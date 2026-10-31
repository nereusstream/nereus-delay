package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import java.security.PrivateKey;
import java.util.Objects;

/** Signs a materialized request after the exact Worker/Store Claim read; Source APPLIED still owns SEND permission. */
public final class TargetPublishAdmissionMutationFactory {
    private final int keyVersion;
    private final PrivateKey key;
    private final long maximumFrameBytes;

    public TargetPublishAdmissionMutationFactory(int keyVersion, PrivateKey key, long maximumFrameBytes) {
        if (keyVersion == 0 || maximumFrameBytes <= 0 || maximumFrameBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException("Admission signing requires an assigned key and finite Source frame");
        }
        this.keyVersion = keyVersion;
        this.key = Objects.requireNonNull(key, "key");
        this.maximumFrameBytes = maximumFrameBytes;
    }

    public SystemMutation sign(final TargetPublishAdmissionBody body) {
        if (!Objects.requireNonNull(body, "body").materialized()) {
            throw new IllegalArgumentException("new Target Admission writer requires v4/v5 materialization");
        }
        final var owner = body.owner();
        final var image = SystemMutation.signed(
                body.shard(),
                SystemMutationType.TARGET_PUBLISH_ADMISSION,
                body.retryUntilEpochMs(),
                body.publishAttemptId(),
                body.canonicalBytes(),
                AuthorIdentity.owner(
                                owner.deploymentId(),
                                owner.workerRunId(),
                                owner.ownerEpoch(),
                                owner.leaseFencingDigest())
                        .canonicalBytes(),
                keyVersion,
                key);
        if (image.encodeFrame().length > maximumFrameBytes) {
            throw new IllegalArgumentException("materialized Admission exceeds configured Source frame bytes");
        }
        return image;
    }
}
