package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.ActivationBarrierV1;
import io.nereusstream.delay.protocol.ChannelResourceIdentityV1;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.ReadyCertificateV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;

import java.util.List;
import java.util.Objects;

/**
 * Immutable result returned by the external Lane prerequisite authority.
 *
 * <p>This is deliberately not a readiness boolean.  The result carries the
 * exact channel identity, its certificate, the evidence cursors that the
 * authority caught up, and the trusted interval used for the activation
 * decision.  The shard persists only the certificate as part of its typed
 * READY projection; the authority remains responsible for producing these
 * inputs from live Profile, credential, Broker and evidence state.</p>
 */
public record LaneActivationPrerequisites(
        ChannelResourceIdentityV1 channel,
        ReadyCertificateV1 readyCertificate,
        List<EvidenceCursorV1> evidenceCursors,
        TrustedUtcIntervalEvidence verifiedAt) {
    public LaneActivationPrerequisites {
        channel = Objects.requireNonNull(channel, "channel");
        readyCertificate = Objects.requireNonNull(readyCertificate, "readyCertificate");
        evidenceCursors = List.copyOf(Objects.requireNonNull(evidenceCursors, "evidenceCursors"));
        verifiedAt = Objects.requireNonNull(verifiedAt, "verifiedAt");

        final ChannelResourceIdentityV1 certificateChannel = ChannelResourceIdentityV1.decode(
                readyCertificate.channel());
        if (!channel.equals(certificateChannel)) {
            throw new IllegalArgumentException("Lane activation channel differs from Ready Certificate");
        }
        if (!evidenceCursors.equals(readyCertificate.evidenceCursors())) {
            throw new IllegalArgumentException("Lane activation evidence differs from Ready Certificate");
        }
        final ActivationBarrierV1 barrier = readyCertificate.activationBarrier();
        if (!channel.targetResource().equals(barrier.resource())
                || channel.physicalPartition() != barrier.partition()) {
            throw new IllegalArgumentException("Lane activation barrier differs from channel resource");
        }
        if (evidenceCursors.isEmpty()) {
            throw new IllegalArgumentException("Lane activation requires evidence cursors");
        }
        for (EvidenceCursorV1 cursor : evidenceCursors) {
            if (!java.util.Arrays.equals(cursor.destinationLaneId(), readyCertificate.destinationLaneId())
                    || !java.util.Arrays.equals(cursor.laneIncarnation(), readyCertificate.laneIncarnation())) {
                throw new IllegalArgumentException("Lane activation evidence is bound to another Lane");
            }
        }
        verifiedAt.requireEarliestAtLeast(readyCertificate.issuedAt().latestEpochMs());
        if (verifiedAt.latestEpochMs() >= readyCertificate.validUntilEpochMs()) {
            throw new IllegalArgumentException("Lane activation verification is at or after certificate expiry");
        }
    }

    /** Requires the activation interval to contain the owner execution time. */
    public void requireCurrentAt(final long nowEpochMs) {
        if (nowEpochMs < verifiedAt.earliestEpochMs() || nowEpochMs > verifiedAt.latestEpochMs()) {
            throw new IllegalStateException("Lane activation trusted interval does not contain owner time");
        }
        if (nowEpochMs >= readyCertificate.validUntilEpochMs()) {
            throw new IllegalStateException("Lane activation Ready Certificate has expired");
        }
    }
}
