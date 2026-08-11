package io.nereusstream.delay.client;

import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DeliveryCapabilitySemanticV1;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.DestinationProfileSemanticV1;
import io.nereusstream.delay.protocol.FailureStageV1;
import io.nereusstream.delay.protocol.NativeCapabilitySnapshotV1;
import io.nereusstream.delay.protocol.NativePreparedDeliveryV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.OutcomeCapabilityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.PulsarMetadataV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.StableErrorV1;
import io.nereusstream.delay.protocol.TargetPartitionHashInputV1;
import io.nereusstream.delay.protocol.TargetPartitionHashV1;
import io.nereusstream.delay.protocol.TargetPartitionPolicyV1;
import io.nereusstream.delay.protocol.TimingCapabilityV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.store.ShardStoreConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AutoFastScheduleTest {
    @TempDir
    Path tempDir;

    @Test
    void preparesEligibleNativeBranchBeforeAnyCommandAdmission() throws Exception {
        final Fixture fixture = fixture();
        try (EmbeddedDelayService service = fixture.service(tempDir.resolve("eligible"))) {
            final PreparedSubmissionV1 prepared = service.prepareAutoFast(
                    AutoFastSchedule.withNativeCandidate(fixture.command, fixture.candidate));

            assertFalse(prepared.isManaged());
            final NativePreparedDeliveryV1 nativePrepared = prepared.nativePrepared();
            assertNotNull(nativePrepared);
            assertEquals(fixture.target, nativePrepared.target());
            assertEquals(4_020, nativePrepared.brokerDeliverAtEpochMs());
            assertTrue(nativePrepared.nativeDeliveryId().length == 32);
            assertTrue(java.util.Arrays.stream(toUnsigned(nativePrepared.nativeDeliveryId()))
                    .anyMatch(value -> value != 0));
            assertEquals(prepared, PreparedSubmissionV1.decode(prepared.canonicalBytes()));
            assertEquals(0, service.pendingCommandCount());
        }
    }

    @Test
    void ineligibleNativeCandidateReturnsTheExactManagedFrame() throws Exception {
        final Fixture fixture = fixture();
        final AutoFastSchedule.NativeCandidate noDirectAuthority = new AutoFastSchedule.NativeCandidate(
                fixture.candidate.destinationProfile(), fixture.candidate.capabilityProfile(), fixture.target,
                0, fixture.payload, fixture.metadata, null, 4_000, 5_000,
                fixture.snapshot, fixture.keyPair.getPublic(), false);
        try (EmbeddedDelayService service = fixture.service(tempDir.resolve("fallback"))) {
            final PreparedSubmissionV1 prepared = service.prepareAutoFast(
                    AutoFastSchedule.withNativeCandidate(fixture.command, noDirectAuthority));
            final PreparedSubmissionV1 expected = service.prepareManagedSubmissionV1(fixture.command);

            assertTrue(prepared.isManaged());
            assertArrayEquals(expected.managedFrame(), prepared.managedFrame());
            assertEquals(expected, PreparedSubmissionV1.decode(prepared.canonicalBytes()));
            assertEquals(0, service.pendingCommandCount());
        }
    }

    @Test
    void batchSelectionIsIndependentAndKeepsInputOrder() throws Exception {
        final Fixture fixture = fixture();
        try (EmbeddedDelayService service = fixture.service(tempDir.resolve("batch"))) {
            final List<PreparedSubmissionV1> prepared = service.prepareAutoFastBatch(List.of(
                    AutoFastSchedule.withNativeCandidate(fixture.command, fixture.candidate),
                    AutoFastSchedule.managed(fixture.command)));

            assertEquals(2, prepared.size());
            assertFalse(prepared.get(0).isManaged());
            assertTrue(prepared.get(1).isManaged());
            assertArrayEquals(prepared.get(1).managedFrame(),
                    service.prepareManagedSubmissionV1(fixture.command).managedFrame());
            assertEquals(0, service.pendingCommandCount());
        }
    }

    @Test
    void hashOnlyNativeSelectionRecomputesTheSignedPartition() throws Exception {
        final Fixture base = fixture();
        final PulsarMetadataV1 metadata = new PulsarMetadataV1(null, null, Bytes.utf8("native-ordering-key"),
                List.of());
        final ProfileSemanticEnvelopeV1 destination = destination(base.candidate.capabilityProfile().ref(),
                base.target, TargetPartitionPolicyV1.HASH_ONLY, List.of(), TargetPartitionHashInputV1.ORDERING_KEY);
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination.ref(),
                new RetryPolicyRefV1(Bytes.utf8("autofast-hash-retry"), 1, bytes(32, 30)),
                4_000, 9_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0], base.payload,
                null, AdapterMetadataV1.pulsar(metadata), null, null);
        final PreparedCommand command = PreparedCommand.scheduleV1(base.shard, intent, 10_000);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(2_000, 2_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("autofast-hash-clock"),
                1, 2, 3, Bytes.sha256(Bytes.utf8("autofast-hash-sample")), 0, null);
        final int expectedPartition = (int) TargetPartitionHashV1.partition(destination.ref(), 2,
                metadata.orderingKey());
        final NativeCapabilitySnapshotV1 expectedSnapshot = NativeCapabilitySnapshotV1.create(destination.ref(),
                base.candidate.capabilityProfile().ref(), base.target, expectedPartition, bytes(32, 40), 1, 1,
                bytes(32, 41), bytes(32, 42), bytes(32, 43), issuedAt, 9_000, 1, base.keyPair.getPrivate());
        final AutoFastSchedule.NativeCandidate expectedCandidate = new AutoFastSchedule.NativeCandidate(destination,
                base.candidate.capabilityProfile(), base.target, expectedPartition, base.payload, metadata, null,
                4_000, 5_000, expectedSnapshot, base.keyPair.getPublic(), true);
        final int wrongPartition = expectedPartition == 0 ? 1 : 0;
        final NativeCapabilitySnapshotV1 wrongSnapshot = NativeCapabilitySnapshotV1.create(destination.ref(),
                base.candidate.capabilityProfile().ref(), base.target, wrongPartition, bytes(32, 40), 1, 1,
                bytes(32, 41), bytes(32, 42), bytes(32, 43), issuedAt, 9_000, 1, base.keyPair.getPrivate());
        final AutoFastSchedule.NativeCandidate wrongCandidate = new AutoFastSchedule.NativeCandidate(destination,
                base.candidate.capabilityProfile(), base.target, wrongPartition, base.payload, metadata, null,
                4_000, 5_000, wrongSnapshot, base.keyPair.getPublic(), true);

        try (EmbeddedDelayService service = base.service(tempDir.resolve("hash-only-valid"))) {
            assertFalse(service.prepareAutoFast(AutoFastSchedule.withNativeCandidate(command, expectedCandidate))
                    .isManaged());
        }
        try (EmbeddedDelayService service = base.service(tempDir.resolve("hash-only-wrong"))) {
            final PreparedSubmissionV1 prepared = service.prepareAutoFast(
                    AutoFastSchedule.withNativeCandidate(command, wrongCandidate));
            assertTrue(prepared.isManaged());
            assertArrayEquals(service.prepareManagedSubmissionV1(command).managedFrame(), prepared.managedFrame());
        }
    }

    @Test
    void nativeSelectionPreservesUnsignedHighBitPhysicalPartition() throws Exception {
        final Fixture base = fixture();
        final int highBitPartition = Integer.MIN_VALUE;
        final int partitionCount = Integer.MIN_VALUE + 1;
        final ProfileSemanticEnvelopeV1 destination = destination(base.candidate.capabilityProfile().ref(),
                base.target, partitionCount, TargetPartitionPolicyV1.EXPLICIT_ONLY,
                List.of(highBitPartition), TargetPartitionHashInputV1.ORDERING_KEY);
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination.ref(),
                new RetryPolicyRefV1(Bytes.utf8("autofast-high-bit-retry"), 1, bytes(32, 70)),
                4_000, 9_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0], base.payload,
                null, AdapterMetadataV1.pulsar(base.metadata), null, null);
        final PreparedCommand command = PreparedCommand.scheduleV1(base.shard, intent, 10_000);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(2_000, 2_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("autofast-high-bit-clock"),
                1, 2, 3, Bytes.sha256(Bytes.utf8("autofast-high-bit-sample")), 0, null);
        final NativeCapabilitySnapshotV1 snapshot = NativeCapabilitySnapshotV1.create(destination.ref(),
                base.candidate.capabilityProfile().ref(), base.target, highBitPartition, bytes(32, 71), 1, 1,
                bytes(32, 72), bytes(32, 73), bytes(32, 74), issuedAt, 9_000, 1,
                base.keyPair.getPrivate());
        final AutoFastSchedule.NativeCandidate candidate = new AutoFastSchedule.NativeCandidate(destination,
                base.candidate.capabilityProfile(), base.target, highBitPartition, base.payload, base.metadata,
                null, 4_000, 5_000, snapshot, base.keyPair.getPublic(), true);

        try (EmbeddedDelayService service = base.service(tempDir.resolve("high-bit-partition"))) {
            final PreparedSubmissionV1 prepared = service.prepareAutoFast(
                    AutoFastSchedule.withNativeCandidate(command, candidate));
            assertFalse(prepared.isManaged());
            assertEquals(highBitPartition, prepared.nativePrepared().physicalPartition());
        }
    }

    @Test
    void malformedPreparationExposesAStablePreparationFailure() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 10);
        final PreparedCommand legacy = PreparedCommand.schedule(shard,
                new ScheduleIntent(DestinationLaneId.derive(Bytes.utf8("legacy-autofast")),
                        2_000, 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);

        final PreparationFailure failure = assertThrows(PreparationFailure.class,
                () -> AutoFastSchedule.managed(legacy));
        assertEquals(FailureStageV1.PREPARATION, failure.error().stage());
        assertEquals(StableCode.INVALID_COMMAND, failure.error().code());
        final StableErrorV1 decoded = StableErrorV1.decode(failure.error().canonicalBytes());
        assertEquals(failure.error(), decoded);
    }

    private static Fixture fixture() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 9);
        final PulsarBrokerResourceIdentityV1 target = new PulsarBrokerResourceIdentityV1(
                "autofast-cluster", bytes(32, 3), "persistent://tenant/ns/autofast", 17);
        final ProfileSemanticEnvelopeV1 capability = capability();
        final ProfileSemanticEnvelopeV1 destination = destination(capability.ref(), target);
        final byte[] payload = Bytes.utf8("autofast-payload");
        final PulsarMetadataV1 metadata = new PulsarMetadataV1(null, null, null, List.of());
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination.ref(),
                new RetryPolicyRefV1(Bytes.utf8("autofast-retry"), 1, bytes(32, 30)),
                4_000, 9_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0], payload,
                null, AdapterMetadataV1.pulsar(metadata), null, null);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, intent, 10_000);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(2_000, 2_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("autofast-clock"),
                1, 2, 3, Bytes.sha256(Bytes.utf8("autofast-sample")), 0, null);
        final NativeCapabilitySnapshotV1 snapshot = NativeCapabilitySnapshotV1.create(destination.ref(),
                capability.ref(), target, 0, bytes(32, 40), 1, 1, bytes(32, 41), bytes(32, 42),
                bytes(32, 43), issuedAt, 9_000, 1, keyPair.getPrivate());
        final AutoFastSchedule.NativeCandidate candidate = new AutoFastSchedule.NativeCandidate(destination,
                capability, target, 0, payload, metadata, null, 4_000, 5_000, snapshot,
                keyPair.getPublic(), true);
        return new Fixture(shard, target, payload, metadata, command, candidate, snapshot, keyPair);
    }

    private static ProfileSemanticEnvelopeV1 destination(final io.nereusstream.delay.protocol.ProfileRefV1 capability,
                                                         final PulsarBrokerResourceIdentityV1 target) {
        return destination(capability, target, 2, TargetPartitionPolicyV1.EXPLICIT_ONLY, List.of(0),
                TargetPartitionHashInputV1.ORDERING_KEY);
    }

    private static ProfileSemanticEnvelopeV1 destination(final io.nereusstream.delay.protocol.ProfileRefV1 capability,
                                                         final PulsarBrokerResourceIdentityV1 target,
                                                         final TargetPartitionPolicyV1 policy,
                                                         final List<Integer> allowedPartitions,
                                                         final TargetPartitionHashInputV1 hashInput) {
        return destination(capability, target, 2, policy, allowedPartitions, hashInput);
    }

    private static ProfileSemanticEnvelopeV1 destination(final io.nereusstream.delay.protocol.ProfileRefV1 capability,
                                                         final PulsarBrokerResourceIdentityV1 target,
                                                         final int targetPartitionCount,
                                                         final TargetPartitionPolicyV1 policy,
                                                         final List<Integer> allowedPartitions,
                                                         final TargetPartitionHashInputV1 hashInput) {
        final DestinationProfileSemanticV1 body = new DestinationProfileSemanticV1(AdapterKindV1.PULSAR,
                BrokerResourceIdentityV1.pulsar(target), targetPartitionCount, policy, hashInput, allowedPartitions,
                capability, 1, 0, 20,
                bytes(32, 50), 1_024, 512, 512, 1, Bytes.utf8("autofast-destination"), 0, 0, 1,
                bytes(32, 51));
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.DESTINATION, Bytes.utf8("autofast-destination"),
                1, body);
    }

    private static ProfileSemanticEnvelopeV1 capability() {
        final DeliveryCapabilitySemanticV1 body = new DeliveryCapabilitySemanticV1(AdapterKindV1.PULSAR,
                OutcomeCapabilityV1.AT_LEAST_ONCE,
                TimingCapabilityV1.ORDINARY_MANAGED | TimingCapabilityV1.PULSAR_AUTO_FAST,
                null, 0, 0, 0, 0, bytes(32, 60), bytes(32, 61), 0, 0);
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.DELIVERY_CAPABILITY,
                Bytes.utf8("autofast-capability"), 1, body);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static int[] toUnsigned(final byte[] value) {
        final int[] result = new int[value.length];
        for (int index = 0; index < value.length; index++) {
            result[index] = value[index] & 0xff;
        }
        return result;
    }

    private record Fixture(ShardId shard, PulsarBrokerResourceIdentityV1 target, byte[] payload,
                           PulsarMetadataV1 metadata, PreparedCommand command,
                           AutoFastSchedule.NativeCandidate candidate,
                           NativeCapabilitySnapshotV1 snapshot, KeyPair keyPair) {
        private EmbeddedDelayService service(final Path path) {
            return new EmbeddedDelayService(ShardStoreConfig.defaults(path), shard,
                    Clock.fixed(Instant.ofEpochMilli(3_000), ZoneOffset.UTC));
        }
    }
}
