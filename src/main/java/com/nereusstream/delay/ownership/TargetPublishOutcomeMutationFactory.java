package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.runtime.TargetPublishAdmissionStore;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.Objects;

/** Signs exact Target initial outcomes; context/physical authentication and source ordering remain external. */
public final class TargetPublishOutcomeMutationFactory {
    private final int keyVersion;
    private final PrivateKey key;

    public TargetPublishOutcomeMutationFactory(int keyVersion, PrivateKey key) {
        if (keyVersion == 0) {
            throw new IllegalArgumentException("Target Outcome signing version is unassigned");
        }
        this.keyVersion = keyVersion;
        this.key = Objects.requireNonNull(key, "key");
    }

    public SystemMutation create(
            final TargetPublishAdmissionStore.Applied applied, final DestinationPublishResult result,
            final WorkerPublishOutcomeMutationFactory.OutcomeContext context) {
        final var body = Objects.requireNonNull(applied, "applied").body();
        final var physical = Objects.requireNonNull(result, "result");
        final var c = Objects.requireNonNull(context, "context");
        final int effect = switch (physical.disposition()) {
            case PUBLISHED -> 1;
            case DEFINITIVELY_NOT_PUBLISHED -> 2;
            case UNKNOWN -> 3;
        };
        if (effect != 3) {
            final var evidence = PublishEvidence.decode(physical.evidence());
            evidence.requireBusinessMutation(body.publishAttemptId(), effect == 1);
            if (!Arrays.equals(c.transfer(), body.outcomeTransfer())) {
                throw new IllegalArgumentException("Target Outcome changes frozen Admission charge transfer");
            }
            if (effect == 1) {
                evidence.requireOrdinaryTargetPublishedBinding(body.publication());
                if (physical.brokerResource() == null
                        || !physical.brokerResource().equals(body.publication().physical().resource())
                        || Integer.toUnsignedLong(physical.brokerPartition())
                                != body.publication().physical().physicalPartition()
                        || !Arrays.equals(physical.externalDeliveryIdentity(), body.publishAttemptId())) {
                    throw new IllegalArgumentException("Target Outcome result changes physical request identity");
                }
            }
        } else if (physical.evidence() != null && physical.evidence().length != 0) {
            throw new IllegalArgumentException("Target UNKNOWN cannot carry definitive/opaque evidence");
        }
        final var owner = body.owner();
        final byte[] encoded = PublishOutcomeBody.encodeInitial(
                body.shard(), c.retryUntilEpochMs(), body.publishAttemptId(), effect, c.disposition(),
                physical.stableCode(), effect == 3 ? null : physical.evidence(), c.transfer(),
                c.observedAt(), c.retryDecision());
        return SystemMutation.signed(body.shard(), SystemMutationType.PUBLISH_OUTCOME, c.retryUntilEpochMs(),
                body.publishAttemptId(), encoded,
                AuthorIdentity.owner(owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch(),
                        owner.leaseFencingDigest()).canonicalBytes(), keyVersion, key);
    }
}
