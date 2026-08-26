package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ActiveLaneState;
import com.nereusstream.delay.protocol.ChannelResourceIdentity;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ReadyCertificate;
import com.nereusstream.delay.runtime.AdmissionGate;
import com.nereusstream.delay.runtime.ClaimRecord;
import com.nereusstream.delay.runtime.RuntimeReadiness;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Binds external Publish preparation to the exact typed Lane that produced a
 * successful Claim.
 *
 * <p>The external authority still owns credential/channel resolution, signing
 * keys and trusted publish timing. This coordinator owns the local identity
 * boundary around that callback: it rereads the live Owner/Claim, then reads
 * the persisted typed READY Lane and passes its exact certificate/channel to
 * the authority. A returned preparation must carry those same immutable
 * identities before it can enter Publish Admission.</p>
 */
public final class WorkerPublishPreparationCoordinator implements WorkerShardRuntime.PublishPreparationProvider {
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;
    private final LongSupplier ownerClock;
    private final PrerequisiteAuthority prerequisiteAuthority;

    public WorkerPublishPreparationCoordinator(
            final OwnedDelayShard ownedShard,
            final OxiaOwnerLeaseStore authority,
            final LongSupplier ownerClock,
            final PrerequisiteAuthority prerequisiteAuthority) {
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
        this.prerequisiteAuthority = Objects.requireNonNull(prerequisiteAuthority, "prerequisiteAuthority");
    }

    @Override
    public Optional<WorkerCommandRuntime.PublishPreparation> prepare(
            final ClaimHandoffWorkClassExecutor.ClaimHandoffResult claimResult) {
        final ClaimHandoffWorkClassExecutor.ClaimHandoffResult exactResult =
                Objects.requireNonNull(claimResult, "claimResult");
        if (exactResult.kind() != ClaimHandoffWorkClassExecutor.ResultKind.CLAIMED
                || exactResult.claim() == null
                || exactResult.reservation() == null) {
            throw new IllegalArgumentException("Publish preparation requires a successful Claim");
        }
        final ClaimRecord claim = exactResult.claim();
        ownedShard.requirePublishAdmissionAuthoritativelyStrict(authority, claim, ownerClock);

        final ActiveLaneState lane = ownedShard.shard().getActiveLaneState(claim.laneId());
        if (lane == null
                || lane.admissionGate() != AdmissionGate.OPEN
                || lane.runtimeReadiness() != RuntimeReadiness.READY
                || lane.readyCertificate() == null) {
            throw new IllegalStateException("Claim Lane is not backed by a typed READY certificate");
        }
        final ReadyCertificate certificate = ReadyCertificate.decode(lane.readyCertificate());
        final ChannelResourceIdentity channel = ChannelResourceIdentity.decode(certificate.channel());
        validateClaimBinding(claim, lane, certificate, channel);

        final Optional<WorkerCommandRuntime.PublishPreparation> prepared = Objects.requireNonNull(
                prerequisiteAuthority.prepare(new PreparationRequest(claim, lane, channel, certificate)),
                "publish preparation result");
        if (prepared.isEmpty()) {
            return Optional.empty();
        }
        final WorkerCommandRuntime.PublishPreparation exact = prepared.orElseThrow();
        if (!channel.equals(exact.channel()) || !certificate.equals(exact.readyCertificate())) {
            throw new IllegalArgumentException("Publish preparation changed the persisted Lane channel/certificate");
        }
        return Optional.of(exact);
    }

    private static void validateClaimBinding(
            final ClaimRecord claim,
            final ActiveLaneState lane,
            final ReadyCertificate certificate,
            final ChannelResourceIdentity channel) {
        final OwnerIdentity owner = OwnerIdentity.decode(claim.ownerIdentity());
        if (!Arrays.equals(owner.canonicalBytes(), certificate.ownerIdentity())
                || !Arrays.equals(claim.storeIncarnation(), certificate.storeIncarnation())
                || !Arrays.equals(claim.laneId().bytes(), certificate.destinationLaneId())
                || !Arrays.equals(claim.laneIncarnation(), certificate.laneIncarnation())
                || !claim.laneId().equals(lane.laneId())
                || !Arrays.equals(claim.laneIncarnation(), lane.laneIncarnation())) {
            throw new IllegalArgumentException("Claim does not match the typed READY Lane certificate");
        }
        if (!claim.materialization().targetResource().equals(channel.targetResource())
                || claim.materialization().physicalPartition() != channel.physicalPartition()
                || !channel.targetResource()
                        .equals(certificate.activationBarrier().resource())
                || channel.physicalPartition()
                        != certificate.activationBarrier().partition()) {
            throw new IllegalArgumentException("Claim target does not match the READY channel barrier");
        }
        if (!lane.destinationProfile().equals(claim.materialization().destinationProfile())
                || !lane.capabilityProfile().equals(claim.materialization().capabilityProfile())) {
            throw new IllegalArgumentException("Claim materialization Profiles differ from the typed Lane");
        }
    }

    /** External authority for live channel, credential, signing and timing inputs. */
    @FunctionalInterface
    public interface PrerequisiteAuthority {
        Optional<WorkerCommandRuntime.PublishPreparation> prepare(PreparationRequest request);
    }

    /** Immutable identity bundle supplied to the external prerequisite authority. */
    public record PreparationRequest(
            ClaimRecord claim,
            ActiveLaneState lane,
            ChannelResourceIdentity channel,
            ReadyCertificate readyCertificate) {
        public PreparationRequest {
            Objects.requireNonNull(claim, "claim");
            Objects.requireNonNull(lane, "lane");
            Objects.requireNonNull(channel, "channel");
            Objects.requireNonNull(readyCertificate, "readyCertificate");
            if (!claim.laneId().equals(lane.laneId())
                    || !Arrays.equals(claim.laneIncarnation(), lane.laneIncarnation())) {
                throw new IllegalArgumentException("preparation request Lane identity mismatch");
            }
        }
    }
}
