package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PublishAdmissionBodyTest {
    @Test
    void validatesCanonicalAdmissionProjectionsAndTiming() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final Fixture fixture = Fixture.create(shard);

        final PublishAdmissionBody admission = PublishAdmissionBody.decode(fixture.body());

        assertArrayEquals(fixture.owner(), admission.ownerIdentity());
        assertArrayEquals(fixture.messageId().bytes(), admission.messageId());
        assertEquals(1, admission.descriptor().attemptNo());
        assertArrayEquals(fixture.descriptor(), admission.descriptor().canonicalBytes());
        admission.requireTiming(2_000, 5_000);
    }

    @Test
    void validatesBrokerPersistenceTimeAgainstDecisionAndExpiryBounds() {
        final PublishAdmissionBody admission = PublishAdmissionBody.decode(
                Fixture.create(new ShardId(RouteIncarnation.random(), 10)).body());

        admission.requireBrokerTiming(2_001, 0, 0);
        admission.requireBrokerTiming(3_000, 0, 1_000);
        assertThrows(IllegalArgumentException.class, () -> admission.requireBrokerTiming(3_000, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> admission.requireBrokerTiming(4_999, 1, 0));
        assertThrows(IllegalArgumentException.class,
                () -> admission.requireBrokerTiming(Long.MAX_VALUE, 1, 0));
    }

    @Test
    void rejectsDescriptorProjectionDrift() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final Fixture fixture = Fixture.create(shard);
        final byte[] drifted = tamperPreparedHash(fixture.body());

        assertThrows(IllegalArgumentException.class, () -> PublishAdmissionBody.decode(drifted));
    }

    @Test
    void rejectsDescriptorAdapterIdentityDrift() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final Fixture fixture = Fixture.create(shard);
        final byte[] drifted = tamperDescriptorScalar(fixture.body(), 2, AdapterKindV1.PULSAR.wireValue());

        assertThrows(IllegalArgumentException.class, () -> PublishAdmissionBody.decode(drifted));
    }

    @Test
    void rejectsDescriptorMetadataAdapterIdentityDrift() {
        final BrokerResourceIdentityV1 target = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1("cluster", bytes(32, 70),
                        "persistent://tenant/ns/metadata-drift", 1));
        final Fixture fixture = Fixture.createWithProfiles(new ShardId(RouteIncarnation.random(), 12),
                Fixture.profileRef("destination", 1), Fixture.profileRef("capability", 2), target,
                AdapterKindV1.PULSAR, 2_000);

        assertThrows(IllegalArgumentException.class,
                () -> PublishAdmissionBody.decode(tamperDescriptorMetadata(fixture.body())));
    }

    @Test
    void retainsCertifiedHandoffTimingForProfileSemanticValidation() {
        final Fixture fixture = Fixture.createWithActionAt(
                new ShardId(RouteIncarnation.random(), 6), 1_500);
        final PublishAdmissionBody admission = PublishAdmissionBody.decode(fixture.body());

        assertEquals(1_500, admission.descriptor().actionAtEpochMs());
        assertThrows(IllegalArgumentException.class, admission::requireOrdinaryManagedTiming);
    }

    @Test
    void rejectsActionAfterBusinessVisibilityTimeBeforeProfileLookup() {
        final Fixture fixture = Fixture.createWithActionAt(
                new ShardId(RouteIncarnation.random(), 7), 2_001);

        assertThrows(IllegalArgumentException.class, () -> PublishAdmissionBody.decode(fixture.body()));
    }

    @Test
    void acceptsOnlyThePinnedPulsarHandoffLead() {
        final BrokerResourceIdentityV1 target = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1("cluster", bytes(32, 40),
                        "persistent://tenant/ns/topic", 1));
        final DeliveryCapabilitySemanticV1 capabilityBody = new DeliveryCapabilitySemanticV1(
                AdapterKindV1.PULSAR, OutcomeCapabilityV1.AT_LEAST_ONCE,
                TimingCapabilityV1.ORDINARY_MANAGED | TimingCapabilityV1.PULSAR_GUARDED_HANDOFF,
                null, 0, 0, 0, 0, bytes(32, 41), bytes(32, 42), 0, 0);
        final ProfileSemanticEnvelopeV1 capability = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.DELIVERY_CAPABILITY, Bytes.utf8("pulsar-capability"), 1, capabilityBody);
        final DestinationProfileSemanticV1 destinationBody = new DestinationProfileSemanticV1(
                AdapterKindV1.PULSAR, target, 1, TargetPartitionPolicyV1.HASH_ONLY,
                TargetPartitionHashInputV1.ORDERING_KEY, List.of(), capability.ref(), 1, 500, 100,
                bytes(32, 43), 1_000, 128, 512, 1, Bytes.utf8("pulsar-destination"), 0, 0, 1,
                bytes(32, 44));
        final ProfileSemanticEnvelopeV1 destination = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.DESTINATION, Bytes.utf8("pulsar-destination"), 1, destinationBody);
        final Fixture fixture = Fixture.createWithProfiles(new ShardId(RouteIncarnation.random(), 8),
                destination.ref().canonicalBytes(), capability.ref().canonicalBytes(), target,
                AdapterKindV1.PULSAR, 1_500);
        final PublishAdmissionBody admission = PublishAdmissionBody.decode(fixture.body());

        admission.requireTimingPolicy(destinationBody, capabilityBody);
        assertThrows(IllegalArgumentException.class, () -> admission.requireTimingPolicy(destinationBody,
                new DeliveryCapabilitySemanticV1(AdapterKindV1.PULSAR, OutcomeCapabilityV1.AT_LEAST_ONCE,
                        TimingCapabilityV1.ORDINARY_MANAGED, null, 0, 0, 0, 0,
                        bytes(32, 45), bytes(32, 46), 0, 0)));
        final DestinationProfileSemanticV1 encodingMismatchBody = new DestinationProfileSemanticV1(
                AdapterKindV1.PULSAR, target, 1, TargetPartitionPolicyV1.HASH_ONLY,
                TargetPartitionHashInputV1.ORDERING_KEY, List.of(), capability.ref(), 1, 500, 100,
                bytes(32, 49), 1_000, 128, 512, 1, Bytes.utf8("pulsar-encoding-mismatch"), 0, 0, 2,
                bytes(32, 50));
        assertThrows(IllegalArgumentException.class, () -> admission.requireTimingPolicy(
                encodingMismatchBody, capabilityBody));

        final DestinationProfileSemanticV1 partitionMismatchBody = new DestinationProfileSemanticV1(
                AdapterKindV1.PULSAR, target, 2, TargetPartitionPolicyV1.EXPLICIT_ONLY,
                TargetPartitionHashInputV1.ORDERING_KEY, List.of(1), capability.ref(), 1, 500, 100,
                bytes(32, 47), 1_000, 128, 512, 1, Bytes.utf8("pulsar-partition-mismatch"), 0, 0, 1,
                bytes(32, 48));
        final ProfileSemanticEnvelopeV1 partitionMismatch = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.DESTINATION, Bytes.utf8("pulsar-partition-mismatch"), 1,
                partitionMismatchBody);
        final Fixture partitionFixture = Fixture.createWithProfiles(new ShardId(RouteIncarnation.random(), 9),
                partitionMismatch.ref().canonicalBytes(), capability.ref().canonicalBytes(), target,
                AdapterKindV1.PULSAR, 1_500);
        final PublishAdmissionBody partitionAdmission = PublishAdmissionBody.decode(partitionFixture.body());
        assertThrows(IllegalArgumentException.class, () -> partitionAdmission.requireTimingPolicy(
                partitionMismatchBody, capabilityBody));
    }

    @Test
    void rejectsAProfileHashPartitionThatDoesNotMatchDescriptorMetadata() {
        final BrokerResourceIdentityV1 target = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1("cluster", java.util.UUID.nameUUIDFromBytes(
                        Bytes.utf8("hash-partition-target"))));
        final DeliveryCapabilitySemanticV1 capabilityBody = new DeliveryCapabilitySemanticV1(
                AdapterKindV1.KAFKA, OutcomeCapabilityV1.AT_LEAST_ONCE, TimingCapabilityV1.ORDINARY_MANAGED,
                null, 0, 0, 0, 0, bytes(32, 61), bytes(32, 62), 0, 0);
        final ProfileSemanticEnvelopeV1 capability = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.DELIVERY_CAPABILITY, Bytes.utf8("hash-capability"), 1, capabilityBody);
        final DestinationProfileSemanticV1 destinationBody = new DestinationProfileSemanticV1(
                AdapterKindV1.KAFKA, target, 2, TargetPartitionPolicyV1.HASH_ONLY,
                TargetPartitionHashInputV1.ADAPTER_MESSAGE_KEY, List.of(), capability.ref(), 1, 0, 0,
                bytes(32, 63), 1_000, 128, 512, 1, Bytes.utf8("hash-destination"), 0, 0, 1,
                bytes(32, 64));
        final ProfileSemanticEnvelopeV1 destination = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.DESTINATION, Bytes.utf8("hash-destination"), 1, destinationBody);
        final byte[] digest = Bytes.sha256(Bytes.utf8("nereus-delay-target-partition-v1"),
                Bytes.lp32(destination.ref().profileId()), Bytes.u64be(destination.ref().version()),
                Bytes.lp32(Bytes.utf8("key")));
        final long expected = Long.remainderUnsigned(Bytes.readU64be(digest, 0), 2);
        final long wrongPartition = expected == 0 ? 1 : 0;
        final Fixture fixture = Fixture.createWithProfiles(new ShardId(RouteIncarnation.random(), 11),
                destination.ref().canonicalBytes(), capability.ref().canonicalBytes(), target,
                AdapterKindV1.KAFKA, 2_000, wrongPartition);
        final PublishAdmissionBody admission = PublishAdmissionBody.decode(fixture.body());

        assertThrows(IllegalArgumentException.class, () -> admission.requireTimingPolicy(destinationBody,
                capabilityBody));
    }

    @Test
    void rejectsMessageRoutedToDifferentBodyShard() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final DelayMessageId messageId = DelayMessageId.random(new ShardId(RouteIncarnation.random(), 4));

        assertThrows(IllegalArgumentException.class,
                () -> PublishAdmissionBody.decode(Fixture.create(shard, messageId).body()));
    }

    private static byte[] tamperPreparedHash(final byte[] body) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(body);
        return CanonicalProtobuf.message(output -> {
            while (reader.hasRemaining()) {
                final CanonicalProtobuf.Reader.Field field = reader.next();
                if (field.number() == 18) {
                    final byte[] hash = Arrays.copyOf(field.rawValue(), field.rawValue().length);
                    hash[0] ^= 1;
                    CanonicalProtobuf.bytes(output, 18, hash);
                } else if (field.wireType() == 0) {
                    CanonicalProtobuf.int64(output, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                }
            }
        });
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static byte[] tamperDescriptorScalar(final byte[] body, final int descriptorField,
                                                 final long value) {
        final List<CanonicalProtobuf.Reader.Field> outerFields = new ArrayList<>();
        final CanonicalProtobuf.Reader outerReader = new CanonicalProtobuf.Reader(body);
        while (outerReader.hasRemaining()) {
            outerFields.add(outerReader.next());
        }
        final byte[] descriptor = outerFields.stream().filter(field -> field.number() == 22)
                .findFirst().orElseThrow().rawValue();
        final byte[] driftedDescriptor = CanonicalProtobuf.message(output -> {
            final CanonicalProtobuf.Reader descriptorReader = new CanonicalProtobuf.Reader(descriptor);
            while (descriptorReader.hasRemaining()) {
                final CanonicalProtobuf.Reader.Field field = descriptorReader.next();
                if (field.number() == descriptorField) {
                    CanonicalProtobuf.uint64(output, descriptorField, value);
                } else if (field.wireType() == 0) {
                    CanonicalProtobuf.uint64(output, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                }
            }
        });
        final byte[] driftedHash = Bytes.sha256(Bytes.utf8("nereus-delay-prepared-publish-v1\0"),
                driftedDescriptor);
        return CanonicalProtobuf.message(output -> {
            for (CanonicalProtobuf.Reader.Field field : outerFields) {
                if (field.number() == 18) {
                    CanonicalProtobuf.bytes(output, 18, driftedHash);
                } else if (field.number() == 22) {
                    CanonicalProtobuf.bytes(output, 22, driftedDescriptor);
                } else if (field.wireType() == 0) {
                    CanonicalProtobuf.uint64(output, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                }
            }
        });
    }

    private static byte[] tamperDescriptorMetadata(final byte[] body) {
        final List<CanonicalProtobuf.Reader.Field> outerFields = new ArrayList<>();
        final CanonicalProtobuf.Reader outerReader = new CanonicalProtobuf.Reader(body);
        while (outerReader.hasRemaining()) {
            outerFields.add(outerReader.next());
        }
        final byte[] descriptor = outerFields.stream().filter(field -> field.number() == 22)
                .findFirst().orElseThrow().rawValue();
        final byte[] driftedDescriptor = CanonicalProtobuf.message(output -> {
            final CanonicalProtobuf.Reader descriptorReader = new CanonicalProtobuf.Reader(descriptor);
            while (descriptorReader.hasRemaining()) {
                final CanonicalProtobuf.Reader.Field field = descriptorReader.next();
                if (field.number() == 16) {
                    CanonicalProtobuf.bytes(output, 16, Fixture.metadata());
                } else if (field.wireType() == 0) {
                    CanonicalProtobuf.uint64(output, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                }
            }
        });
        final byte[] driftedHash = Bytes.sha256(Bytes.utf8("nereus-delay-prepared-publish-v1\0"),
                driftedDescriptor);
        return CanonicalProtobuf.message(output -> {
            for (CanonicalProtobuf.Reader.Field field : outerFields) {
                if (field.number() == 18) {
                    CanonicalProtobuf.bytes(output, 18, driftedHash);
                } else if (field.number() == 22) {
                    CanonicalProtobuf.bytes(output, 22, driftedDescriptor);
                } else if (field.wireType() == 0) {
                    CanonicalProtobuf.uint64(output, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                }
            }
        });
    }

    public record Fixture(byte[] body, byte[] owner, DelayMessageId messageId, byte[] descriptor, byte[] lane) {
        public static Fixture create(final ShardId shard) {
            return create(shard, DelayMessageId.random(shard));
        }

        public static Fixture create(final ShardId shard, final DelayMessageId messageId) {
            return createInternal(shard, messageId, new byte[16], Bytes.sha256(Bytes.utf8("timeline")), 1, 0, 0,
                    Bytes.sha256(Bytes.utf8("obligations")), Bytes.sha256(Bytes.utf8("semantic")), 1, 1);
        }

        public static Fixture createWithActionAt(final ShardId shard, final long actionAt) {
            return createInternal(shard, DelayMessageId.random(shard), new byte[16],
                    Bytes.sha256(Bytes.utf8("timeline")), 1, 0, 0,
                    Bytes.sha256(Bytes.utf8("obligations")), Bytes.sha256(Bytes.utf8("semantic")), 1, 1,
                    actionAt);
        }

        public static Fixture createWithProfiles(final ShardId shard, final byte[] destinationProfile,
                                                 final byte[] capabilityProfile,
                                                 final BrokerResourceIdentityV1 target,
                                                 final AdapterKindV1 adapterKind, final long actionAt) {
            return createWithProfiles(shard, destinationProfile, capabilityProfile, target, adapterKind, actionAt, 0);
        }

        public static Fixture createWithProfiles(final ShardId shard, final byte[] destinationProfile,
                                                 final byte[] capabilityProfile,
                                                 final BrokerResourceIdentityV1 target,
                                                 final AdapterKindV1 adapterKind, final long actionAt,
                                                 final long physicalPartition) {
            final DelayMessageId messageId = DelayMessageId.random(shard);
            final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 7,
                    Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
            final byte[] lane = Bytes.sha256(Bytes.utf8("lane"));
            final byte[] targetBytes = target.canonicalBytes();
            final byte[] bindingDigest = Bytes.sha256(Bytes.utf8("binding"));
            final byte[] fingerprint = Bytes.sha256(Bytes.utf8("fingerprint"));
            final byte[] time = trustedTime(1_000, 1_000);
            final byte[] laneIncarnation = new byte[16];
            final byte[] channelPrefix = channelPrefix(adapterKind, lane, laneIncarnation, targetBytes,
                    physicalPartition);
            final byte[] lease = lease(destinationProfile, bindingDigest, fingerprint, time,
                    CredentialUseLeaseV1.destinationChannelHolderScope(channelPrefix));
            final byte[] channel = channel(channelPrefix, bindingDigest, fingerprint, lease);
            final byte[] payload = payload(Bytes.utf8("hello"));
            final byte[] metadata = metadata(adapterKind);
            final byte[] attempt = Bytes.sha256(Bytes.utf8("attempt"));
            final byte[] reserved = reserved(shard, messageId, attempt, destinationProfile, capabilityProfile);
            final byte[] descriptor = descriptor(adapterKind, lane, laneIncarnation, destinationProfile,
                    capabilityProfile, targetBytes, channel, messageId, attempt, payload, metadata, reserved,
                    1, actionAt);
            final byte[] materialization = materialization(destinationProfile, capabilityProfile, targetBytes,
                    messageId, payload, metadata, descriptor, actionAt);
            final byte[] claim = claim(shard, messageId, lane, laneIncarnation, owner, materialization,
                    Bytes.sha256(Bytes.utf8("timeline")), 1, 0, 0,
                    Bytes.sha256(Bytes.utf8("obligations")), Bytes.sha256(Bytes.utf8("semantic")), 1);
            final byte[] certificate = certificate(owner, shard, lane, laneIncarnation, channel, time,
                    bindingDigest, fingerprint);
            final byte[] body = CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, subject(shard));
                CanonicalProtobuf.uint32(output, 2, SystemMutationType.PUBLISH_ADMISSION.wireValue());
                CanonicalProtobuf.int64(output, 3, 9_000);
                CanonicalProtobuf.bytes(output, 10, ownerBody(owner));
                CanonicalProtobuf.bytes(output, 11, new byte[16]);
                CanonicalProtobuf.bytes(output, 12, Bytes.sha256(Bytes.utf8("claim")));
                CanonicalProtobuf.bytes(output, 13, lane);
                CanonicalProtobuf.bytes(output, 14, laneIncarnation);
                CanonicalProtobuf.bytes(output, 15, messageId.bytes());
                CanonicalProtobuf.uint32(output, 16, 0);
                CanonicalProtobuf.bytes(output, 17, attempt);
                CanonicalProtobuf.bytes(output, 18,
                        Bytes.sha256(Bytes.utf8("nereus-delay-prepared-publish-v1\0"), descriptor));
                CanonicalProtobuf.bytes(output, 19, charge());
                CanonicalProtobuf.bytes(output, 20, certificateDigest(certificate));
                CanonicalProtobuf.bytes(output, 21, channel);
                CanonicalProtobuf.bytes(output, 22, descriptor);
                CanonicalProtobuf.bytes(output, 23, certificate);
                CanonicalProtobuf.bytes(output, 24, trustedTime(2_000, 2_001));
                CanonicalProtobuf.bytes(output, 25, claim);
            });
            return new Fixture(body, owner, messageId, descriptor, lane);
        }

        public static Fixture createForSource(final ShardId shard, final DelayMessageId messageId,
                                               final byte[] laneIncarnation, final byte[] timelineKey,
                                               final int sourceWorkKind,
                                               final int expectedAdmissionsUsed,
                                               final int expectedUncertainRetryAdmissionsUsed,
                                               final byte[] obligationSetDigest, final byte[] semanticDigest) {
            return createForSource(shard, messageId, laneIncarnation, timelineKey, sourceWorkKind,
                    expectedAdmissionsUsed, expectedUncertainRetryAdmissionsUsed, obligationSetDigest,
                    semanticDigest, 1, 1);
        }

        public static Fixture createForSource(final ShardId shard, final DelayMessageId messageId,
                                               final byte[] laneIncarnation, final byte[] timelineKey,
                                               final int sourceWorkKind,
                                               final int expectedAdmissionsUsed,
                                               final int expectedUncertainRetryAdmissionsUsed,
                                               final byte[] obligationSetDigest, final byte[] semanticDigest,
                                               final int attemptNo) {
            return createForSource(shard, messageId, laneIncarnation, timelineKey, sourceWorkKind,
                    expectedAdmissionsUsed, expectedUncertainRetryAdmissionsUsed, obligationSetDigest,
                    semanticDigest, attemptNo, 1);
        }

        public static Fixture createForSource(final ShardId shard, final DelayMessageId messageId,
                                               final byte[] laneIncarnation, final byte[] timelineKey,
                                               final int sourceWorkKind,
                                               final int expectedAdmissionsUsed,
                                               final int expectedUncertainRetryAdmissionsUsed,
                                               final byte[] obligationSetDigest, final byte[] semanticDigest,
                                               final int attemptNo, final long stateVersion) {
            return createInternal(shard, messageId, laneIncarnation, Bytes.sha256(timelineKey), sourceWorkKind,
                    expectedAdmissionsUsed, expectedUncertainRetryAdmissionsUsed, obligationSetDigest,
                    semanticDigest, attemptNo, stateVersion);
        }

        private static Fixture createInternal(final ShardId shard, final DelayMessageId messageId,
                                              final byte[] laneIncarnation, final byte[] timelineKeySha256,
                                              final int sourceWorkKind,
                                              final int expectedAdmissionsUsed,
                                              final int expectedUncertainRetryAdmissionsUsed,
                                              final byte[] obligationSetDigest, final byte[] semanticDigest,
                                              final int attemptNo, final long stateVersion) {
            return createInternal(shard, messageId, laneIncarnation, timelineKeySha256, sourceWorkKind,
                    expectedAdmissionsUsed, expectedUncertainRetryAdmissionsUsed, obligationSetDigest,
                    semanticDigest, attemptNo, stateVersion, 2_000);
        }

        private static Fixture createInternal(final ShardId shard, final DelayMessageId messageId,
                                              final byte[] laneIncarnation, final byte[] timelineKeySha256,
                                              final int sourceWorkKind,
                                              final int expectedAdmissionsUsed,
                                              final int expectedUncertainRetryAdmissionsUsed,
                                              final byte[] obligationSetDigest, final byte[] semanticDigest,
                                              final int attemptNo, final long stateVersion, final long actionAt) {
            final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 7,
                    Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
            final byte[] lane = Bytes.sha256(Bytes.utf8("lane"));
            final byte[] target = brokerResource();
            final byte[] destinationProfile = profileRef("destination", 1);
            final byte[] capabilityProfile = profileRef("capability", 2);
            final byte[] bindingDigest = Bytes.sha256(Bytes.utf8("binding"));
            final byte[] fingerprint = Bytes.sha256(Bytes.utf8("fingerprint"));
            final byte[] time = trustedTime(1_000, 1_000);
            final byte[] channelPrefix = channelPrefix(lane, laneIncarnation, target);
            final byte[] lease = lease(destinationProfile, bindingDigest, fingerprint, time,
                    CredentialUseLeaseV1.destinationChannelHolderScope(channelPrefix));
            final byte[] channel = channel(channelPrefix, bindingDigest, fingerprint, lease);
            final byte[] payload = payload(Bytes.utf8("hello"));
            final byte[] metadata = metadata();
            final byte[] attempt = Bytes.sha256(Bytes.utf8("attempt"));
            final byte[] reserved = reserved(shard, messageId, attempt, destinationProfile, capabilityProfile);
            final byte[] descriptor = descriptor(lane, laneIncarnation, destinationProfile, capabilityProfile,
                    target, channel, messageId, attempt, payload, metadata, reserved, attemptNo, actionAt);
            final byte[] materialization = materialization(destinationProfile, capabilityProfile, target, messageId,
                    payload, metadata, descriptor, actionAt);
            final byte[] claim = claim(shard, messageId, lane, laneIncarnation, owner, materialization,
                    timelineKeySha256, sourceWorkKind, expectedAdmissionsUsed,
                    expectedUncertainRetryAdmissionsUsed, obligationSetDigest, semanticDigest, stateVersion);
            final byte[] certificate = certificate(owner, shard, lane, laneIncarnation, channel, time,
                    bindingDigest, fingerprint);
            final byte[] body = CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, subject(shard));
                CanonicalProtobuf.uint32(output, 2, SystemMutationType.PUBLISH_ADMISSION.wireValue());
                CanonicalProtobuf.int64(output, 3, 9_000);
                CanonicalProtobuf.bytes(output, 10, ownerBody(owner));
                CanonicalProtobuf.bytes(output, 11, new byte[16]);
                CanonicalProtobuf.bytes(output, 12, Bytes.sha256(Bytes.utf8("claim")));
                CanonicalProtobuf.bytes(output, 13, lane);
                CanonicalProtobuf.bytes(output, 14, laneIncarnation);
                CanonicalProtobuf.bytes(output, 15, messageId.bytes());
                CanonicalProtobuf.uint32(output, 16, 0);
                CanonicalProtobuf.bytes(output, 17, attempt);
                CanonicalProtobuf.bytes(output, 18,
                        Bytes.sha256(Bytes.utf8("nereus-delay-prepared-publish-v1\0"), descriptor));
                CanonicalProtobuf.bytes(output, 19, charge());
                CanonicalProtobuf.bytes(output, 20,
                        certificateDigest(certificate));
                CanonicalProtobuf.bytes(output, 21, channel);
                CanonicalProtobuf.bytes(output, 22, descriptor);
                CanonicalProtobuf.bytes(output, 23, certificate);
                CanonicalProtobuf.bytes(output, 24, trustedTime(2_000, 2_001));
                CanonicalProtobuf.bytes(output, 25, claim);
            });
            return new Fixture(body, owner, messageId, descriptor, lane);
        }

        private static byte[] ownerBody(final byte[] encoded) {
            return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1,
                    CanonicalProtobuf.message(inner -> {
                        CanonicalProtobuf.bytes(inner, 1, Bytes.utf8("deployment"));
                        CanonicalProtobuf.bytes(inner, 2, Bytes.utf8("worker"));
                        CanonicalProtobuf.uint32(inner, 3, 7);
                        CanonicalProtobuf.bytes(inner, 4, Bytes.sha256(Bytes.utf8("lease")));
                    })));
        }

        private static byte[] subject(final ShardId shard) {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
                CanonicalProtobuf.uint32(output, 2, shard.partition());
            });
        }

        private static byte[] brokerResource() {
            return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1,
                    CanonicalProtobuf.message(inner -> {
                        CanonicalProtobuf.bytes(inner, 1, Bytes.utf8("cluster"));
                        CanonicalProtobuf.bytes(inner, 2, new byte[16]);
                    })));
        }

        private static byte[] profileRef(final String id, final int kind) {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, Bytes.utf8(id));
                CanonicalProtobuf.uint32(output, 2, 1);
                CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8(id + "-hash")));
                CanonicalProtobuf.uint32(output, 4, kind);
            });
        }

        private static byte[] trustedTime(final long earliest, final long latest) {
            return new TrustedUtcIntervalEvidence(earliest, latest,
                    TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("host"), 1, 1, 1,
                    Bytes.sha256(Bytes.utf8("sample")), 0, null).canonicalBytes();
        }

        private static byte[] lease(final byte[] profile, final byte[] bindingDigest, final byte[] fingerprint,
                                    final byte[] issuedAt, final byte[] holderScope) {
            final byte[] prefix = CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32(output, 1, 1);
                CanonicalProtobuf.bytes(output, 2, profile);
                CanonicalProtobuf.uint32(output, 3, 1);
                CanonicalProtobuf.bytes(output, 4, holderScope);
                CanonicalProtobuf.uint32(output, 5, 1);
                CanonicalProtobuf.bytes(output, 6, bindingDigest);
                CanonicalProtobuf.bytes(output, 7, fingerprint);
                CanonicalProtobuf.bytes(output, 8, issuedAt);
                CanonicalProtobuf.int64(output, 9, 9_000);
                CanonicalProtobuf.uint32(output, 10, 1);
            });
            return appendHash(prefix, 11, "nereus-delay-credential-use-lease-v1\0");
        }

        private static byte[] channelPrefix(final byte[] lane, final byte[] laneIncarnation,
                                            final byte[] target) {
            return channelPrefix(AdapterKindV1.KAFKA, lane, laneIncarnation, target, 0);
        }

        private static byte[] channelPrefix(final AdapterKindV1 adapterKind, final byte[] lane,
                                            final byte[] laneIncarnation, final byte[] target) {
            return channelPrefix(adapterKind, lane, laneIncarnation, target, 0);
        }

        private static byte[] channelPrefix(final AdapterKindV1 adapterKind, final byte[] lane,
                                            final byte[] laneIncarnation, final byte[] target,
                                            final long physicalPartition) {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32(output, 1, adapterKind.wireValue());
                CanonicalProtobuf.uint32(output, 2, 1);
                CanonicalProtobuf.bytes(output, 3, lane);
                CanonicalProtobuf.bytes(output, 4, laneIncarnation);
                CanonicalProtobuf.bytes(output, 5, target);
                CanonicalProtobuf.uint32(output, 6, physicalPartition);
                CanonicalProtobuf.uint32(output, 7, 1);
                CanonicalProtobuf.uint32(output, 8, 0);
                CanonicalProtobuf.bytes(output, 9, Bytes.utf8("producer"));
                CanonicalProtobuf.bytes(output, 10, Bytes.sha256(Bytes.utf8("producer")));
                CanonicalProtobuf.bytes(output, 13, Bytes.sha256(Bytes.utf8("guard")));
            });
        }

        private static byte[] channel(final byte[] channelPrefix, final byte[] bindingDigest,
                                      final byte[] fingerprint, final byte[] lease) {
            return CanonicalProtobuf.message(output -> {
                final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(channelPrefix);
                while (reader.hasRemaining()) {
                    final CanonicalProtobuf.Reader.Field field = reader.next();
                    if (field.wireType() == 0) {
                        CanonicalProtobuf.uint64(output, field.number(), field.unsignedValue());
                    } else {
                        CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                    }
                }
                CanonicalProtobuf.uint32(output, 14, 1);
                CanonicalProtobuf.bytes(output, 15, bindingDigest);
                CanonicalProtobuf.bytes(output, 16, fingerprint);
                CanonicalProtobuf.bytes(output, 17, lease);
            });
        }

        private static byte[] payload(final byte[] value) {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32(output, 1, value.length);
                CanonicalProtobuf.bytes(output, 2, Bytes.sha256(value));
                CanonicalProtobuf.bytes(output, 3, value);
            });
        }

        private static byte[] metadata() {
            return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1,
                    CanonicalProtobuf.message(inner -> CanonicalProtobuf.bytes(inner, 1, Bytes.utf8("key")))));
        }

        private static byte[] metadata(final AdapterKindV1 adapterKind) {
            return adapterKind == AdapterKindV1.KAFKA ? metadata() : CanonicalProtobuf.message(output ->
                    CanonicalProtobuf.bytes(output, 2, CanonicalProtobuf.message(inner ->
                            CanonicalProtobuf.bytes(inner, 3, Bytes.utf8("ordering")))));
        }

        private static byte[] reserved(final ShardId shard, final DelayMessageId messageId, final byte[] attempt,
                                       final byte[] destinationProfile, final byte[] capabilityProfile) {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
                CanonicalProtobuf.uint32(output, 2, shard.partition());
                CanonicalProtobuf.bytes(output, 3, messageId.bytes());
                CanonicalProtobuf.uint32(output, 4, 0);
                CanonicalProtobuf.bytes(output, 5, attempt);
                CanonicalProtobuf.bytes(output, 6, profileHash(destinationProfile));
                CanonicalProtobuf.bytes(output, 7, profileHash(capabilityProfile));
                CanonicalProtobuf.int64(output, 8, 2_000);
                CanonicalProtobuf.uint32(output, 9, 1);
            });
        }

        private static byte[] descriptor(final byte[] lane, final byte[] laneIncarnation,
                                         final byte[] destinationProfile, final byte[] capabilityProfile,
                                         final byte[] target, final byte[] channel, final DelayMessageId messageId,
                                         final byte[] attempt, final byte[] payload, final byte[] metadata,
                                         final byte[] reserved, final int attemptNo) {
            return descriptor(lane, laneIncarnation, destinationProfile, capabilityProfile, target, channel,
                    messageId, attempt, payload, metadata, reserved, attemptNo, 2_000);
        }

        private static byte[] descriptor(final byte[] lane, final byte[] laneIncarnation,
                                         final byte[] destinationProfile, final byte[] capabilityProfile,
                                         final byte[] target, final byte[] channel, final DelayMessageId messageId,
                                         final byte[] attempt, final byte[] payload, final byte[] metadata,
                                         final byte[] reserved, final int attemptNo, final long actionAt) {
            return descriptor(AdapterKindV1.KAFKA, lane, laneIncarnation, destinationProfile, capabilityProfile,
                    target, channel, messageId, attempt, payload, metadata, reserved, attemptNo, actionAt);
        }

        private static byte[] descriptor(final AdapterKindV1 adapterKind, final byte[] lane,
                                         final byte[] laneIncarnation, final byte[] destinationProfile,
                                         final byte[] capabilityProfile, final byte[] target,
                                         final byte[] channel, final DelayMessageId messageId,
                                         final byte[] attempt, final byte[] payload, final byte[] metadata,
                                         final byte[] reserved, final int attemptNo, final long actionAt) {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32(output, 1, 1);
                CanonicalProtobuf.uint32(output, 2, adapterKind.wireValue());
                CanonicalProtobuf.uint32(output, 3, 1);
                CanonicalProtobuf.bytes(output, 4, lane);
                CanonicalProtobuf.bytes(output, 5, laneIncarnation);
                CanonicalProtobuf.bytes(output, 6, destinationProfile);
                CanonicalProtobuf.bytes(output, 7, capabilityProfile);
                CanonicalProtobuf.bytes(output, 8, target);
                CanonicalProtobuf.uint32(output, 9, 0);
                CanonicalProtobuf.bytes(output, 10, channel);
                CanonicalProtobuf.bytes(output, 11, messageId.bytes());
                CanonicalProtobuf.uint32(output, 12, 0);
                CanonicalProtobuf.bytes(output, 13, attempt);
                CanonicalProtobuf.uint32(output, 14, attemptNo);
                CanonicalProtobuf.bytes(output, 15, payload);
                CanonicalProtobuf.bytes(output, 16, metadata);
                CanonicalProtobuf.bytes(output, 17, reserved);
                CanonicalProtobuf.int64(output, 18, 2_000);
                CanonicalProtobuf.int64(output, 19, 5_000);
                CanonicalProtobuf.int64(output, 20, actionAt);
            });
        }

        private static byte[] materialization(final byte[] destinationProfile, final byte[] capabilityProfile,
                                              final byte[] target, final DelayMessageId messageId,
                                              final byte[] payload, final byte[] metadata, final byte[] descriptor) {
            return materialization(destinationProfile, capabilityProfile, target, messageId, payload, metadata,
                    descriptor, 2_000);
        }

        private static byte[] materialization(final byte[] destinationProfile, final byte[] capabilityProfile,
                                              final byte[] target, final DelayMessageId messageId,
                                              final byte[] payload, final byte[] metadata, final byte[] descriptor,
                                              final long actionAt) {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, destinationProfile);
                CanonicalProtobuf.bytes(output, 2, capabilityProfile);
                CanonicalProtobuf.bytes(output, 3, target);
                CanonicalProtobuf.uint32(output, 4, 0);
                CanonicalProtobuf.bytes(output, 5, messageId.bytes());
                CanonicalProtobuf.uint32(output, 6, 0);
                CanonicalProtobuf.bytes(output, 7, payload);
                CanonicalProtobuf.bytes(output, 8, metadata);
                CanonicalProtobuf.int64(output, 9, 2_000);
                CanonicalProtobuf.int64(output, 10, 5_000);
                CanonicalProtobuf.int64(output, 11, actionAt);
            });
        }

        private static byte[] claim(final ShardId shard, final DelayMessageId messageId, final byte[] lane,
                                    final byte[] laneIncarnation, final byte[] owner, final byte[] materialization,
                                    final byte[] timelineKeySha256, final int sourceWorkKind,
                                    final int expectedAdmissionsUsed,
                                    final int expectedUncertainRetryAdmissionsUsed,
                                    final byte[] obligationSetDigest, final byte[] semanticDigest,
                                    final long stateVersion) {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, Bytes.sha256(Bytes.utf8("claim")));
                CanonicalProtobuf.bytes(output, 2, messageId.bytes());
                CanonicalProtobuf.uint32(output, 3, 0);
                CanonicalProtobuf.int64(output, 4, stateVersion);
                CanonicalProtobuf.bytes(output, 5, lane);
                CanonicalProtobuf.bytes(output, 6, laneIncarnation);
                CanonicalProtobuf.uint32(output, 7, 1);
                CanonicalProtobuf.uint32(output, 8, 1);
                CanonicalProtobuf.bytes(output, 9, timelineKeySha256);
                CanonicalProtobuf.bytes(output, 10, materialization);
                CanonicalProtobuf.bytes(output, 11,
                        Bytes.sha256(Bytes.utf8("nereus-delay-claim-materialization-v1\0"), materialization));
                CanonicalProtobuf.bytes(output, 12, charge());
                CanonicalProtobuf.int64(output, 13, 4_000);
                CanonicalProtobuf.bytes(output, 14, ownerBody(owner));
                CanonicalProtobuf.bytes(output, 15, new byte[16]);
                CanonicalProtobuf.uint32(output, 16, sourceWorkKind);
                CanonicalProtobuf.uint32(output, 17, expectedAdmissionsUsed);
                CanonicalProtobuf.uint32(output, 18, expectedUncertainRetryAdmissionsUsed);
                CanonicalProtobuf.bytes(output, 19, obligationSetDigest);
                CanonicalProtobuf.bytes(output, 20, semanticDigest);
            });
        }

        private static byte[] certificate(final byte[] owner, final ShardId shard, final byte[] lane,
                                          final byte[] laneIncarnation, final byte[] channel, final byte[] issuedAt,
                                          final byte[] bindingDigest, final byte[] fingerprint) {
            final byte[] prefix = CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32(output, 1, 1);
                CanonicalProtobuf.bytes(output, 2, ownerBody(owner));
                CanonicalProtobuf.bytes(output, 3, new byte[16]);
                CanonicalProtobuf.bytes(output, 4, lane);
                CanonicalProtobuf.bytes(output, 5, laneIncarnation);
                CanonicalProtobuf.bytes(output, 6, channel);
                CanonicalProtobuf.bytes(output, 7, CanonicalProtobuf.message(barrier ->
                        CanonicalProtobuf.bytes(barrier, 1, CanonicalProtobuf.message(empty -> {
                            CanonicalProtobuf.bytes(empty, 1, brokerResource());
                            CanonicalProtobuf.uint32(empty, 2, 0);
                        }))));
                CanonicalProtobuf.bytes(output, 8, evidenceCursor(lane, laneIncarnation));
                CanonicalProtobuf.uint32(output, 9, 1);
                CanonicalProtobuf.uint32(output, 10, 1);
                CanonicalProtobuf.int64(output, 11, 8_000);
                CanonicalProtobuf.bytes(output, 12, issuedAt);
                CanonicalProtobuf.uint32(output, 13, 1);
                CanonicalProtobuf.bytes(output, 14, bindingDigest);
                CanonicalProtobuf.bytes(output, 15, fingerprint);
            });
            return appendHash(prefix, 16, "nereus-delay-ready-certificate-v1\0");
        }

        private static byte[] evidenceCursor(final byte[] lane, final byte[] laneIncarnation) {
            final byte[] topicUuid = new byte[16];
            return EvidenceCursorV1.kafka(lane, laneIncarnation, topicUuid, 0, 1, 8_000, 1, 1)
                    .canonicalBytes();
        }

        private static byte[] charge() {
            return CanonicalProtobuf.message(output -> {
                for (int number = 1; number <= 17; number++) {
                    CanonicalProtobuf.uint32(output, number, 0);
                }
            });
        }

        private static byte[] appendHash(final byte[] prefix, final int field, final String domain) {
            return CanonicalProtobuf.message(output -> {
                final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(prefix);
                while (reader.hasRemaining()) {
                    final CanonicalProtobuf.Reader.Field current = reader.next();
                    if (current.wireType() == 0) {
                        CanonicalProtobuf.int64(output, current.number(), current.unsignedValue());
                    } else {
                        CanonicalProtobuf.bytes(output, current.number(), current.rawValue());
                    }
                }
                CanonicalProtobuf.bytes(output, field, Bytes.sha256(Bytes.utf8(domain), prefix));
            });
        }

        private static byte[] certificateDigest(final byte[] certificate) {
            final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(certificate);
            final java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
            while (reader.hasRemaining()) {
                final CanonicalProtobuf.Reader.Field field = reader.next();
                if (field.number() == 16) {
                    break;
                }
                if (field.wireType() == 0) {
                    CanonicalProtobuf.int64(output, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                }
            }
            return Bytes.sha256(Bytes.utf8("nereus-delay-ready-certificate-v1\0"), output.toByteArray());
        }

        private static byte[] profileHash(final byte[] profile) {
            final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(profile);
            while (reader.hasRemaining()) {
                final CanonicalProtobuf.Reader.Field field = reader.next();
                if (field.number() == 3) {
                    return field.rawValue();
                }
            }
            throw new IllegalArgumentException("profile hash is missing");
        }
    }
}
