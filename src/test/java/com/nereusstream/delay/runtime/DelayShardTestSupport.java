package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.ActivationBarrier;
import com.nereusstream.delay.protocol.ActiveLaneState;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalLaneTuple;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.ChannelResourceIdentity;
import com.nereusstream.delay.protocol.CredentialUseKind;
import com.nereusstream.delay.protocol.CredentialUseLease;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ReadyCertificate;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.UUID;

/** Test-classpath-only bridge for package-local runtime compatibility seams. */
public final class DelayShardTestSupport {
    private DelayShardTestSupport() {}

    public static RetiredMessageIdentityRecord retireMessageIdentity(
            final DelayShard shard, final DelayMessageId messageId, final long messageIdentityReuseUntilEpochMs) {
        return shard.retireMessageIdentity(messageId, messageIdentityReuseUntilEpochMs);
    }

    public static LaneRecord updateLaneReadiness(
            final DelayShard shard, final DestinationLaneId laneId, final RuntimeReadiness readiness) {
        return shard.updateLaneReadiness(laneId, readiness);
    }

    /**
     * Test-only setup that still exercises the strict typed activation API.
     * The certificate is built from the persisted tuple and Store incarnation;
     * production code has no equivalent synthetic bridge.
     */
    public static LaneRecord activateTypedLaneReadinessForTest(final DelayShard shard, final DestinationLaneId laneId) {
        final ActiveLaneState current = shard.getActiveLaneState(laneId);
        if (current == null) {
            throw new IllegalStateException("test activation requires a typed Lane");
        }
        final CanonicalLaneTuple.Projection tuple = CanonicalLaneTuple.project(current.canonicalLaneTuple());
        final ChannelResourceIdentity channel = testChannel(tuple, laneId.bytes(), current.laneIncarnation());
        final ReadyCertificate certificate = testCertificate(shard, laneId, current.laneIncarnation(), channel);
        return shard.activateLaneReadiness(
                laneId, current.laneIncarnation(), channel, certificate, certificate.evidenceCursors());
    }

    public static ClaimRecord claimForPublish(
            final DelayShard shard,
            final DelayMessageId messageId,
            final AuthorIdentity owner,
            final long claimDeadlineEpochMs,
            final byte[] materialization,
            final byte[] claimedCharge) {
        return shard.claimForPublish(messageId, owner, claimDeadlineEpochMs, materialization, claimedCharge);
    }

    public static List<DelayShard.ExpiryWork> discoverExpiry(
            final DelayShard shard, final long earliestEpochMs, final int limit) {
        return shard.discoverExpiry(earliestEpochMs, limit);
    }

    public static List<DelayShard.ReadyWork> discoverReady(
            final DelayShard shard, final long earliestEpochMs, final int limit) {
        return shard.discoverReady(earliestEpochMs, limit);
    }

    public static List<DelayShard.ReservationExpiryWork> discoverReservationExpiry(
            final DelayShard shard, final long earliestEpochMs, final int limit) {
        return shard.discoverReservationExpiry(earliestEpochMs, limit);
    }

    public static List<DelayShard.LaneCloseMaterializationWork> discoverLaneCloseMaterialization(
            final DelayShard shard, final int limit) {
        return shard.discoverLaneCloseMaterialization(limit);
    }

    private static ChannelResourceIdentity testChannel(
            final CanonicalLaneTuple.Projection tuple, final byte[] laneId, final byte[] laneIncarnation) {
        final byte[] producer = Bytes.utf8("delay-test-activation-producer");
        final byte[] guard = Bytes.sha256(Bytes.utf8("delay-test-activation-guard"));
        final byte[] binding = Bytes.sha256(Bytes.utf8("delay-test-activation-binding"));
        final byte[] fingerprint = Bytes.sha256(Bytes.utf8("delay-test-activation-fingerprint"));
        final TrustedUtcIntervalEvidence issuedAt = testEvidence(1_000, 1_001);
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, AdapterKind.KAFKA.wireValue());
            CanonicalProtobuf.uint32(output, 2, ChannelKind.BASELINE_PRODUCER.wireValue());
            CanonicalProtobuf.bytes(output, 3, laneId);
            CanonicalProtobuf.bytes(output, 4, laneIncarnation);
            CanonicalProtobuf.bytes(output, 5, tuple.targetResource().canonicalBytes());
            CanonicalProtobuf.uint32(output, 6, tuple.physicalPartition());
            CanonicalProtobuf.uint64(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, 0);
            CanonicalProtobuf.bytes(output, 9, producer);
            CanonicalProtobuf.bytes(output, 10, Bytes.sha256(producer));
            CanonicalProtobuf.bytes(output, 13, guard);
        });
        final CredentialUseLease lease = new CredentialUseLease(
                tuple.destinationProfile(),
                CredentialUseKind.DESTINATION_CHANNEL,
                CredentialUseLease.destinationChannelHolderScope(prefix),
                1,
                binding,
                fingerprint,
                issuedAt,
                9_000,
                1);
        return new ChannelResourceIdentity(
                AdapterKind.KAFKA,
                ChannelKind.BASELINE_PRODUCER,
                laneId,
                laneIncarnation,
                tuple.targetResource(),
                tuple.physicalPartition(),
                1,
                0,
                producer,
                Bytes.sha256(producer),
                null,
                null,
                guard,
                1,
                binding,
                fingerprint,
                lease);
    }

    private static ReadyCertificate testCertificate(
            final DelayShard shard,
            final DestinationLaneId laneId,
            final byte[] laneIncarnation,
            final ChannelResourceIdentity channel) {
        final OwnerIdentity owner = new OwnerIdentity(
                Bytes.utf8("delay-test-activation-deployment"),
                Bytes.utf8("delay-test-activation-worker"),
                1,
                Bytes.sha256(Bytes.utf8("delay-test-activation-owner")));
        final UUID topic = channel.targetResource().kafka().nativeTopicUuid();
        final byte[] barrier = ActivationBarrier.kafka(
                        channel.targetResource(), (int) channel.physicalPartition(), 0, 0)
                .canonicalBytes();
        final byte[] cursor = EvidenceCursor.kafka(
                        laneId.bytes(),
                        laneIncarnation,
                        uuidBytes(topic),
                        (int) channel.physicalPartition(),
                        1,
                        2_000,
                        1,
                        1)
                .canonicalBytes();
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, owner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, shard.storeIncarnation());
            CanonicalProtobuf.bytes(output, 4, laneId.bytes());
            CanonicalProtobuf.bytes(output, 5, laneIncarnation);
            CanonicalProtobuf.bytes(output, 6, channel.canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, barrier);
            CanonicalProtobuf.bytes(output, 8, cursor);
            CanonicalProtobuf.uint32(output, 9, 1);
            CanonicalProtobuf.uint32(output, 10, 1);
            CanonicalProtobuf.int64(output, 11, 8_000);
            CanonicalProtobuf.bytes(output, 12, testEvidence(1_000, 1_001).canonicalBytes());
            CanonicalProtobuf.uint64(output, 13, channel.credentialBindingGeneration());
            CanonicalProtobuf.bytes(output, 14, channel.credentialBindingDigest());
            CanonicalProtobuf.bytes(output, 15, channel.resolvedCredentialVersionFingerprintDigest());
        });
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(prefix);
            while (reader.hasRemaining()) {
                writeField(output, reader.next());
            }
            CanonicalProtobuf.bytes(output, 16, Bytes.sha256(Bytes.utf8("nereus-delay-ready-certificate\0"), prefix));
        });
        return ReadyCertificate.decode(encoded);
    }

    private static TrustedUtcIntervalEvidence testEvidence(final long earliest, final long latest) {
        return new TrustedUtcIntervalEvidence(
                earliest,
                latest,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("delay-test-activation-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("delay-test-activation-time")),
                0,
                null);
    }

    private static byte[] uuidBytes(final UUID value) {
        return java.nio.ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    private static void writeField(final ByteArrayOutputStream output, final CanonicalProtobuf.Reader.Field field) {
        if (field.wireType() == 0) {
            CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
        } else {
            CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
        }
    }
}
