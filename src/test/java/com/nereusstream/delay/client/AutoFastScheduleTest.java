package com.nereusstream.delay.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemantic;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.FailureStage;
import com.nereusstream.delay.protocol.NativeCapabilitySnapshot;
import com.nereusstream.delay.protocol.NativePreparedDelivery;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OutcomeCapability;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleIntent;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.StableError;
import com.nereusstream.delay.protocol.TargetPartitionHash;
import com.nereusstream.delay.protocol.TargetPartitionHashInput;
import com.nereusstream.delay.protocol.TargetPartitionPolicy;
import com.nereusstream.delay.protocol.TimingCapability;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.store.ShardStoreConfig;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AutoFastScheduleTest {
    @TempDir
    Path tempDir;

    @Test
    void preparesEligibleNativeBranchBeforeAnyCommandAdmission() throws Exception {
        final Fixture fixture = fixture();
        try (EmbeddedDelayService service = fixture.service(tempDir.resolve("eligible"))) {
            final PreparedSubmission prepared =
                    service.prepareAutoFast(AutoFastSchedule.withNativeCandidate(fixture.command, fixture.candidate));

            assertFalse(prepared.isManaged());
            final NativePreparedDelivery nativePrepared = prepared.nativePrepared();
            assertNotNull(nativePrepared);
            assertEquals(fixture.target, nativePrepared.target());
            assertEquals(4_020, nativePrepared.brokerDeliverAtEpochMs());
            assertTrue(nativePrepared.nativeDeliveryId().length == 32);
            assertTrue(java.util.Arrays.stream(toUnsigned(nativePrepared.nativeDeliveryId()))
                    .anyMatch(value -> value != 0));
            assertEquals(prepared, PreparedSubmission.decode(prepared.canonicalBytes()));
            assertEquals(0, service.pendingCommandCount());
        }
    }

    @Test
    void nativeBrokerTimestampNeverExceedsTheActivatedTargetClockBound() throws Exception {
        final Fixture fixture = fixture();
        try (EmbeddedDelayService service = fixture.service(tempDir.resolve("target-clock-bound"))) {
            final PreparedSubmission prepared =
                    service.prepareAutoFast(AutoFastSchedule.withNativeCandidate(fixture.command, fixture.candidate));
            final DestinationProfileSemantic destination = (DestinationProfileSemantic)
                    fixture.candidate.destinationProfile().body();

            assertFalse(prepared.isManaged());
            assertEquals(20, destination.targetClockAheadBoundMs());
            assertEquals(
                    fixture.candidate.deliverAtEpochMs() + destination.targetClockAheadBoundMs(),
                    prepared.nativePrepared().brokerDeliverAtEpochMs());
            assertTrue(prepared.nativePrepared().brokerDeliverAtEpochMs()
                            - prepared.nativePrepared().deliverAtEpochMs()
                    <= destination.targetClockAheadBoundMs());
        }
    }

    @Test
    void ineligibleNativeCandidateReturnsTheExactManagedFrame() throws Exception {
        final Fixture fixture = fixture();
        final AutoFastSchedule.NativeCandidate noDirectAuthority = new AutoFastSchedule.NativeCandidate(
                fixture.candidate.destinationProfile(),
                fixture.candidate.capabilityProfile(),
                fixture.target,
                0,
                fixture.payload,
                fixture.metadata,
                null,
                4_000,
                5_000,
                fixture.snapshot,
                fixture.keyPair.getPublic(),
                false);
        try (EmbeddedDelayService service = fixture.service(tempDir.resolve("fallback"))) {
            final PreparedSubmission prepared =
                    service.prepareAutoFast(AutoFastSchedule.withNativeCandidate(fixture.command, noDirectAuthority));
            final PreparedSubmission expected = service.prepareManagedSubmission(fixture.command);

            assertTrue(prepared.isManaged());
            assertArrayEquals(expected.managedFrame(), prepared.managedFrame());
            assertEquals(expected, PreparedSubmission.decode(prepared.canonicalBytes()));
            assertEquals(0, service.pendingCommandCount());
        }
    }

    @Test
    void batchSelectionIsIndependentAndKeepsInputOrder() throws Exception {
        final Fixture fixture = fixture();
        try (EmbeddedDelayService service = fixture.service(tempDir.resolve("batch"))) {
            final List<PreparedSubmission> prepared = service.prepareAutoFastBatch(List.of(
                    AutoFastSchedule.withNativeCandidate(fixture.command, fixture.candidate),
                    AutoFastSchedule.managed(fixture.command)));

            assertEquals(2, prepared.size());
            assertFalse(prepared.get(0).isManaged());
            assertTrue(prepared.get(1).isManaged());
            assertArrayEquals(
                    prepared.get(1).managedFrame(),
                    service.prepareManagedSubmission(fixture.command).managedFrame());
            assertEquals(0, service.pendingCommandCount());
        }
    }

    @Test
    void hashOnlyNativeSelectionRecomputesTheSignedPartition() throws Exception {
        final Fixture base = fixture();
        final PulsarMetadata metadata = new PulsarMetadata(null, null, Bytes.utf8("native-ordering-key"), List.of());
        final ProfileSemanticEnvelope destination = destination(
                base.candidate.capabilityProfile().ref(),
                base.target,
                TargetPartitionPolicy.HASH_ONLY,
                List.of(),
                TargetPartitionHashInput.ORDERING_KEY);
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination.ref(),
                new RetryPolicyRef(Bytes.utf8("autofast-hash-retry"), 1, bytes(32, 30)),
                4_000,
                9_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                base.payload,
                null,
                AdapterMetadata.pulsar(metadata),
                null,
                null);
        final PreparedCommand command = PreparedCommand.schedule(base.shard, intent, 10_000);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                2_000,
                2_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("autofast-hash-clock"),
                1,
                2,
                3,
                Bytes.sha256(Bytes.utf8("autofast-hash-sample")),
                0,
                null);
        final int expectedPartition = (int) TargetPartitionHash.partition(destination.ref(), 2, metadata.orderingKey());
        final NativeCapabilitySnapshot expectedSnapshot = NativeCapabilitySnapshot.create(
                destination.ref(),
                base.candidate.capabilityProfile().ref(),
                base.target,
                expectedPartition,
                bytes(32, 40),
                1,
                1,
                bytes(32, 41),
                bytes(32, 42),
                bytes(32, 43),
                issuedAt,
                9_000,
                1,
                base.keyPair.getPrivate());
        final AutoFastSchedule.NativeCandidate expectedCandidate = new AutoFastSchedule.NativeCandidate(
                destination,
                base.candidate.capabilityProfile(),
                base.target,
                expectedPartition,
                base.payload,
                metadata,
                null,
                4_000,
                5_000,
                expectedSnapshot,
                base.keyPair.getPublic(),
                true);
        final int wrongPartition = expectedPartition == 0 ? 1 : 0;
        final NativeCapabilitySnapshot wrongSnapshot = NativeCapabilitySnapshot.create(
                destination.ref(),
                base.candidate.capabilityProfile().ref(),
                base.target,
                wrongPartition,
                bytes(32, 40),
                1,
                1,
                bytes(32, 41),
                bytes(32, 42),
                bytes(32, 43),
                issuedAt,
                9_000,
                1,
                base.keyPair.getPrivate());
        final AutoFastSchedule.NativeCandidate wrongCandidate = new AutoFastSchedule.NativeCandidate(
                destination,
                base.candidate.capabilityProfile(),
                base.target,
                wrongPartition,
                base.payload,
                metadata,
                null,
                4_000,
                5_000,
                wrongSnapshot,
                base.keyPair.getPublic(),
                true);

        try (EmbeddedDelayService service = base.service(tempDir.resolve("hash-only-valid"))) {
            assertFalse(service.prepareAutoFast(AutoFastSchedule.withNativeCandidate(command, expectedCandidate))
                    .isManaged());
        }
        try (EmbeddedDelayService service = base.service(tempDir.resolve("hash-only-wrong"))) {
            final PreparedSubmission prepared =
                    service.prepareAutoFast(AutoFastSchedule.withNativeCandidate(command, wrongCandidate));
            assertTrue(prepared.isManaged());
            assertArrayEquals(service.prepareManagedSubmission(command).managedFrame(), prepared.managedFrame());
        }
    }

    @Test
    void nativeSelectionPreservesUnsignedHighBitPhysicalPartition() throws Exception {
        final Fixture base = fixture();
        final int highBitPartition = Integer.MIN_VALUE;
        final int partitionCount = Integer.MIN_VALUE + 1;
        final ProfileSemanticEnvelope destination = destination(
                base.candidate.capabilityProfile().ref(),
                base.target,
                partitionCount,
                TargetPartitionPolicy.EXPLICIT_ONLY,
                List.of(highBitPartition),
                TargetPartitionHashInput.ORDERING_KEY);
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination.ref(),
                new RetryPolicyRef(Bytes.utf8("autofast-high-bit-retry"), 1, bytes(32, 70)),
                4_000,
                9_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                base.payload,
                null,
                AdapterMetadata.pulsar(base.metadata),
                null,
                null);
        final PreparedCommand command = PreparedCommand.schedule(base.shard, intent, 10_000);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                2_000,
                2_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("autofast-high-bit-clock"),
                1,
                2,
                3,
                Bytes.sha256(Bytes.utf8("autofast-high-bit-sample")),
                0,
                null);
        final NativeCapabilitySnapshot snapshot = NativeCapabilitySnapshot.create(
                destination.ref(),
                base.candidate.capabilityProfile().ref(),
                base.target,
                highBitPartition,
                bytes(32, 71),
                1,
                1,
                bytes(32, 72),
                bytes(32, 73),
                bytes(32, 74),
                issuedAt,
                9_000,
                1,
                base.keyPair.getPrivate());
        final AutoFastSchedule.NativeCandidate candidate = new AutoFastSchedule.NativeCandidate(
                destination,
                base.candidate.capabilityProfile(),
                base.target,
                highBitPartition,
                base.payload,
                base.metadata,
                null,
                4_000,
                5_000,
                snapshot,
                base.keyPair.getPublic(),
                true);

        try (EmbeddedDelayService service = base.service(tempDir.resolve("high-bit-partition"))) {
            final PreparedSubmission prepared =
                    service.prepareAutoFast(AutoFastSchedule.withNativeCandidate(command, candidate));
            assertFalse(prepared.isManaged());
            assertEquals(highBitPartition, prepared.nativePrepared().physicalPartition());
        }
    }

    @Test
    void malformedPreparationExposesAStablePreparationFailure() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 10);
        final PreparedCommand legacy = PreparedCommand.schedule(
                shard,
                new ScheduleIntent(
                        DestinationLaneId.derive(Bytes.utf8("legacy-autofast")),
                        2_000,
                        5_000,
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8("payload")),
                10_000);

        final PreparationFailure failure =
                assertThrows(PreparationFailure.class, () -> AutoFastSchedule.managed(legacy));
        assertEquals(FailureStage.PREPARATION, failure.error().stage());
        assertEquals(StableCode.INVALID_COMMAND, failure.error().code());
        final StableError decoded = StableError.decode(failure.error().canonicalBytes());
        assertEquals(failure.error(), decoded);
    }

    private static Fixture fixture() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 9);
        final PulsarBrokerResourceIdentity target = new PulsarBrokerResourceIdentity(
                "autofast-cluster", bytes(32, 3), "persistent://tenant/ns/autofast", 17);
        final ProfileSemanticEnvelope capability = capability();
        final ProfileSemanticEnvelope destination = destination(capability.ref(), target);
        final byte[] payload = Bytes.utf8("autofast-payload");
        final PulsarMetadata metadata = new PulsarMetadata(null, null, null, List.of());
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination.ref(),
                new RetryPolicyRef(Bytes.utf8("autofast-retry"), 1, bytes(32, 30)),
                4_000,
                9_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                payload,
                null,
                AdapterMetadata.pulsar(metadata),
                null,
                null);
        final PreparedCommand command = PreparedCommand.schedule(shard, intent, 10_000);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                2_000,
                2_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("autofast-clock"),
                1,
                2,
                3,
                Bytes.sha256(Bytes.utf8("autofast-sample")),
                0,
                null);
        final NativeCapabilitySnapshot snapshot = NativeCapabilitySnapshot.create(
                destination.ref(),
                capability.ref(),
                target,
                0,
                bytes(32, 40),
                1,
                1,
                bytes(32, 41),
                bytes(32, 42),
                bytes(32, 43),
                issuedAt,
                9_000,
                1,
                keyPair.getPrivate());
        final AutoFastSchedule.NativeCandidate candidate = new AutoFastSchedule.NativeCandidate(
                destination,
                capability,
                target,
                0,
                payload,
                metadata,
                null,
                4_000,
                5_000,
                snapshot,
                keyPair.getPublic(),
                true);
        return new Fixture(shard, target, payload, metadata, command, candidate, snapshot, keyPair);
    }

    private static ProfileSemanticEnvelope destination(
            final com.nereusstream.delay.protocol.ProfileRef capability, final PulsarBrokerResourceIdentity target) {
        return destination(
                capability,
                target,
                2,
                TargetPartitionPolicy.EXPLICIT_ONLY,
                List.of(0),
                TargetPartitionHashInput.ORDERING_KEY);
    }

    private static ProfileSemanticEnvelope destination(
            final com.nereusstream.delay.protocol.ProfileRef capability,
            final PulsarBrokerResourceIdentity target,
            final TargetPartitionPolicy policy,
            final List<Integer> allowedPartitions,
            final TargetPartitionHashInput hashInput) {
        return destination(capability, target, 2, policy, allowedPartitions, hashInput);
    }

    private static ProfileSemanticEnvelope destination(
            final com.nereusstream.delay.protocol.ProfileRef capability,
            final PulsarBrokerResourceIdentity target,
            final int targetPartitionCount,
            final TargetPartitionPolicy policy,
            final List<Integer> allowedPartitions,
            final TargetPartitionHashInput hashInput) {
        final DestinationProfileSemantic body = new DestinationProfileSemantic(
                AdapterKind.PULSAR,
                BrokerResourceIdentity.pulsar(target),
                targetPartitionCount,
                policy,
                hashInput,
                allowedPartitions,
                capability,
                1,
                0,
                20,
                bytes(32, 50),
                1_024,
                512,
                512,
                1,
                Bytes.utf8("autofast-destination"),
                0,
                0,
                1,
                bytes(32, 51));
        return new ProfileSemanticEnvelope(ProfileKind.DESTINATION, Bytes.utf8("autofast-destination"), 1, body);
    }

    private static ProfileSemanticEnvelope capability() {
        final DeliveryCapabilitySemantic body = new DeliveryCapabilitySemantic(
                AdapterKind.PULSAR,
                OutcomeCapability.AT_LEAST_ONCE,
                TimingCapability.ORDINARY_MANAGED | TimingCapability.PULSAR_AUTO_FAST,
                null,
                0,
                0,
                0,
                0,
                bytes(32, 60),
                bytes(32, 61),
                0,
                0);
        return new ProfileSemanticEnvelope(ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("autofast-capability"), 1, body);
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

    private record Fixture(
            ShardId shard,
            PulsarBrokerResourceIdentity target,
            byte[] payload,
            PulsarMetadata metadata,
            PreparedCommand command,
            AutoFastSchedule.NativeCandidate candidate,
            NativeCapabilitySnapshot snapshot,
            KeyPair keyPair) {
        private EmbeddedDelayService service(final Path path) {
            return new EmbeddedDelayService(
                    ShardStoreConfig.defaults(path), shard, Clock.fixed(Instant.ofEpochMilli(3_000), ZoneOffset.UTC));
        }
    }
}
