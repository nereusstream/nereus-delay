package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.ValueEnvelope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TargetBindingChannelContractTest {
    private final Properties vectors = load("target-binding-channel-vectors.properties");
    private final Properties compatibility = load("target-compatibility-vectors.properties");
    private final DelayMessageId message = new DelayMessageId(bytes("message.id"));
    private final SourcePosition source = TargetSourcePosition.decode(bytes("source"));
    private final CanonicalTargetPartition physical = CanonicalTargetPartition.decode(other("pulsar.target"));
    private final TargetDispatchCompatibility dispatch =
            TargetDispatchCompatibility.decode(other("pulsar.journal.dispatch"));
    private final TargetControlScope controls = TargetControlScope.decode(other("scope.shared"));
    private final TargetKeyCodec.Domain domain = new TargetKeyCodec.Domain(0, 1);

    @Test
    void allFourChannelBranchesMatchIndependentIdentityProducerLeaseAndReferenceBytes() {
        for (String name : List.of("kafka.baseline", "kafka.receipt", "pulsar.baseline", "pulsar.journal")) {
            final var contract = TargetDispatchCompatibility.decode(other(name + ".dispatch"));
            final var scope = new TargetControlScope(
                    contract.target(), source.shardId(), controls.controls(), controls.permits());
            final var kind =
                    switch (contract.capability().outcomeCapability()) {
                        case AT_LEAST_ONCE -> ChannelKind.BASELINE_PRODUCER;
                        case KAFKA_TRANSACTIONAL_RECEIPT -> ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT;
                        case PULSAR_BROKER_DEDUP -> ChannelKind.PULSAR_DEDUP_PRODUCER;
                    };
            final var context = new TargetChannelIdentity.Context(
                    source.shardId(),
                    contract.target(),
                    domain,
                    repeat(16, 1),
                    contract.digest(),
                    scope.digest(),
                    kind,
                    0,
                    1,
                    kind.requiresEvidenceResource() ? 1L : null,
                    repeat(32, 0x22));
            final var actual = channel(context, profile(1));
            assertArrayEquals(bytes("channel." + name), actual.canonicalBytes(), name);
            assertArrayEquals(bytes("channel." + name + ".producer"), context.producerIdentity());
            assertArrayEquals(bytes("channel." + name + ".holder"), context.credentialHolderScope());
            assertEquals(actual, TargetChannelIdentity.decodeReferenced(actual.digest(), actual.canonicalBytes()));
            assertEquals(
                    actual,
                    TargetChannelIdentity.decodeForStore(
                            actual.encodedKey(), actual.canonicalBytes(), source.shardId()));
        }
    }

    @Test
    void scheduleNativeStrictPrepareAndCommittedObjectPreserveExactIndependentBodies() {
        for (String kind : List.of("best", "native", "strict", "prepare", "object")) {
            final var body = body(kind, intent(kind, profile(1), null));
            assertArrayEquals(bytes("body." + kind), body, kind);
            final var actual = binding(body, kind, source, domain);
            assertArrayEquals(bytes("binding." + kind), actual.canonicalBytes(), kind);
            assertArrayEquals(bytes("binding." + kind + ".key"), actual.encodedKey());
            assertEquals(actual, TargetScheduleBinding.decodeReferenced(actual.digest(), actual.canonicalBytes()));
            assertEquals(
                    actual,
                    TargetScheduleBinding.decodeForStore(
                            actual.encodedKey(), actual.canonicalBytes(), source.shardId()));
            assertArrayEquals(
                    body, TargetScheduleBinding.decode(actual.canonicalBytes()).canonicalBody());
        }
    }

    @Test
    void renewalKeepsProducerAndRecoveryDomainButCannotReplaceAFrozenAttempt() {
        final var old = channel(context(0, 1, 1, domain), profile(1));
        final var next = channel(context(0, 2, 1, domain), profile(1));
        assertArrayEquals(old.context().producerIdentity(), next.context().producerIdentity());
        assertNotEquals(old.context(), next.context());
        assertNotEquals(old.credentialLease(), next.credentialLease());
        assertDoesNotThrow(() -> next.requireSuccessorOf(old));
        assertThrows(IllegalArgumentException.class, () -> next.requireExactFrozenIdentity(old));
        assertDoesNotThrow(() -> old.requireExactFrozenIdentity(TargetChannelIdentity.decode(old.canonicalBytes())));
        assertThrows(
                IllegalArgumentException.class, () -> new TargetChannelIdentity(next.context(), old.credentialLease()));
        final var legacyHolder =
                CredentialUseLease.destinationChannelHolderScope(old.context().canonicalBytes());
        final var legacyLease = new CredentialUseLease(
                profile(1),
                CredentialUseKind.DESTINATION_CHANNEL,
                legacyHolder,
                1,
                repeat(32, 0x33),
                repeat(32, 0x66),
                clock(),
                1000,
                1);
        assertThrows(IllegalArgumentException.class, () -> new TargetChannelIdentity(old.context(), legacyLease));
    }

    @Test
    void producerDomainsSeparateSlotsShardIncarnationAndActualExecutionScope() {
        final var original = context(0, 1, 1, domain);
        final var different = new ArrayList<TargetChannelIdentity.Context>();
        different.add(context(1, 1, 1, domain));
        different.add(context(0, 1, 1, new TargetKeyCodec.Domain(0, 2)));
        different.add(new TargetChannelIdentity.Context(
                new ShardId(source.shardId().routeIncarnation(), 4),
                physical.id(),
                domain,
                repeat(16, 1),
                dispatch.digest(),
                controls.digest(),
                ChannelKind.PULSAR_DEDUP_PRODUCER,
                0,
                1,
                1L,
                repeat(32, 0x22)));
        different.add(new TargetChannelIdentity.Context(
                source.shardId(),
                physical.id(),
                domain,
                repeat(16, 2),
                dispatch.digest(),
                controls.digest(),
                ChannelKind.PULSAR_DEDUP_PRODUCER,
                0,
                1,
                1L,
                repeat(32, 0x22)));
        different.add(new TargetChannelIdentity.Context(
                source.shardId(),
                physical.id(),
                domain,
                repeat(16, 1),
                repeat(32, 0x99),
                controls.digest(),
                ChannelKind.PULSAR_DEDUP_PRODUCER,
                0,
                1,
                1L,
                repeat(32, 0x22)));
        for (var changed : different) {
            assertTrue(!Arrays.equals(original.producerIdentity(), changed.producerIdentity()));
            assertThrows(IllegalArgumentException.class, () -> channel(changed, profile(1))
                    .requireSuccessorOf(channel(original, profile(1))));
        }
    }

    @Test
    void channelGenerationAndEvidenceCannotSkipWrapOrResetDuringRenewal() {
        final var old = channel(context(0, 1, 1, domain), profile(1));
        assertThrows(IllegalArgumentException.class, () -> channel(context(0, 3, 1, domain), profile(1))
                .requireSuccessorOf(old));
        assertThrows(IllegalArgumentException.class, () -> channel(context(0, 2, 2, domain), profile(1))
                .requireSuccessorOf(old));
        assertDoesNotThrow(() -> channel(context(0, Long.MIN_VALUE, 1, domain), profile(1))
                .requireSuccessorOf(channel(context(0, Long.MAX_VALUE, 1, domain), profile(1))));
        assertThrows(
                IllegalStateException.class,
                () -> old.requireSuccessorOf(channel(context(0, -1, 1, domain), profile(1))));
        assertThrows(IllegalArgumentException.class, () -> context(0, 0, 1, domain));
        assertThrows(IllegalArgumentException.class, () -> context(0, 1, 0, domain));
    }

    @Test
    void channelProjectionUsesExactCredentialProviderAuthorityScopeAndOutcomeClass() {
        final var cap = new ProfileSemanticEnvelope(
                ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("cap"), 1, dispatch.capability());
        final var provider = destination("provider", cap, 0xAA, 10000);
        final var actual = channel(context(0, 1, 1, domain), provider.ref());
        assertDoesNotThrow(() -> actual.requireProjection(physical, dispatch, controls, provider));
        assertThrows(
                IllegalArgumentException.class,
                () -> actual.requireProjection(
                        physical, dispatch, controls, destination("new-provider", cap, 0xAA, 10000)));
        final var unauthorized = destination("unauthorized", cap, 0xAB, 10000);
        assertThrows(IllegalArgumentException.class, () -> channel(context(0, 1, 1, domain), unauthorized.ref())
                .requireProjection(physical, dispatch, controls, unauthorized));
        assertThrows(
                IllegalArgumentException.class,
                () -> actual.requireProjection(
                        physical, dispatch, TargetControlScope.decode(other("scope.private")), provider));
        final var baseline = new TargetChannelIdentity.Context(
                source.shardId(),
                physical.id(),
                domain,
                repeat(16, 1),
                dispatch.digest(),
                controls.digest(),
                ChannelKind.BASELINE_PRODUCER,
                0,
                1,
                null,
                repeat(32, 0x22));
        assertThrows(IllegalArgumentException.class, () -> channel(baseline, provider.ref())
                .requireProjection(physical, dispatch, controls, provider));
    }

    @Test
    void newWorkChecksActiveDomainSlotCapacityAndExactStoreShard() {
        final var channel = channel(context(0, 1, 1, domain), profile(1));
        for (var state : TargetQueueState.AdmissionState.values()) {
            for (var lifecycle : List.of(TargetDomainState.Lifecycle.ACTIVE, TargetDomainState.Lifecycle.DRAINING)) {
                final var summary = new TargetDomainState(
                        domain, lifecycle, dispatch.digest(), controls.digest(), null, null, null);
                final var queue =
                        new TargetQueueState(physical.id(), 1, 1, state, repeat(16, 1), 60000, List.of(summary));
                assertDoesNotThrow(() -> channel.requireQueueProjection(queue));
                if (state == TargetQueueState.AdmissionState.OPEN && lifecycle == TargetDomainState.Lifecycle.ACTIVE) {
                    assertDoesNotThrow(() -> channel.requireNewWorkProjection(queue, 1));
                    assertThrows(IllegalArgumentException.class, () -> channel.requireNewWorkProjection(queue, 0));
                    assertThrows(IllegalArgumentException.class, () -> channel(context(1, 1, 1, domain), profile(1))
                            .requireNewWorkProjection(queue, 1));
                } else {
                    assertThrows(IllegalArgumentException.class, () -> channel.requireNewWorkProjection(queue, 1));
                }
            }
        }
        final var wrongShard = new ShardId(source.shardId().routeIncarnation(), 4);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetChannelIdentity.decodeForStore(channel.encodedKey(), channel.canonicalBytes(), wrongShard));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetScheduleBinding.decodeForStore(
                        bytes("binding.best.key"), bytes("binding.best"), wrongShard));
    }

    @Test
    void bindingResolvesExactProfilesAndRetainsPayloadMetadataAndOrderingLimits() {
        final var cap = new ProfileSemanticEnvelope(
                ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("cap"), 1, dispatch.capability());
        final var dest = destination("destination", cap, 0xAA, 10000);
        final var body = body("best", intent("best", dest.ref(), null));
        final var bound = binding(body, "best", source, domain);
        assertDoesNotThrow(() -> bound.requireReferences(physical, dest, cap, dispatch, dispatch, controls));
        assertThrows(
                IllegalArgumentException.class,
                () -> bound.requireReferences(
                        physical, destination("replacement", cap, 0xAA, 10000), cap, dispatch, dispatch, controls));
        assertThrows(
                IllegalArgumentException.class,
                () -> bound.requireReferences(
                        physical, dest, cap, dispatch, dispatch, TargetControlScope.decode(other("scope.private"))));
        final var small = destination("small", cap, 0xAA, 1);
        final var oversized = binding(body("best", intent("best", small.ref(), null)), "best", source, domain);
        assertThrows(
                IllegalArgumentException.class,
                () -> oversized.requireReferences(physical, small, cap, dispatch, dispatch, controls));
    }

    @Test
    void prepareSourceIsRetainedAcrossCommitAndDifferentPhysicalSourceIsRejected() {
        final var prepare = TargetScheduleBinding.decode(bytes("binding.prepare"));
        final var current = (KafkaSourcePosition) source;
        final var commit = new KafkaSourcePosition(
                current.shardId(), current.authenticatedClusterId(), current.nativeTopicUuid(), 8, 4, 110);
        assertDoesNotThrow(() -> prepare.requireMessageSource(commit));
        assertEquals(source, prepare.bindingSource());
        assertArrayEquals(bytes("body.prepare"), prepare.canonicalBody());
        assertThrows(
                IllegalArgumentException.class,
                () -> prepare.requireMessageSource(new KafkaSourcePosition(
                        current.shardId(), current.authenticatedClusterId(), current.nativeTopicUuid(), 6, 4, 80)));
        assertThrows(
                IllegalArgumentException.class,
                () -> prepare.requireMessageSource(new KafkaSourcePosition(
                        current.shardId(), current.authenticatedClusterId(), UUID.randomUUID(), 8, 4, 110)));
    }

    @Test
    void locatorAndQueueProjectionRejectReusedGenerationWrongAccountingOrBinding() {
        final var bound = TargetScheduleBinding.decode(bytes("binding.strict"));
        final var locator = new TargetMessageLocator(
                message,
                2,
                physical.id(),
                domain,
                repeat(16, 1),
                OrderingMode.DELIVERY_TIME_FIFO,
                repeat(32, 0x88),
                bound.digest());
        assertDoesNotThrow(() -> bound.requireLocator(locator));
        assertThrows(
                IllegalArgumentException.class,
                () -> bound.requireLocator(new TargetMessageLocator(
                        message,
                        2,
                        physical.id(),
                        domain,
                        repeat(16, 2),
                        OrderingMode.DELIVERY_TIME_FIFO,
                        repeat(32, 0x88),
                        bound.digest())));
        assertThrows(
                IllegalArgumentException.class,
                () -> bound.requireLocator(new TargetMessageLocator(
                        message,
                        2,
                        physical.id(),
                        domain,
                        repeat(16, 1),
                        OrderingMode.DELIVERY_TIME_FIFO,
                        repeat(32, 0x88),
                        repeat(32, 0x99))));
        final var active = new TargetDomainState(
                domain, TargetDomainState.Lifecycle.ACTIVE, dispatch.digest(), controls.digest(), null, null, null);
        final var queue = new TargetQueueState(
                physical.id(), 1, 1, TargetQueueState.AdmissionState.OPEN, repeat(16, 1), 60000, List.of(active));
        assertDoesNotThrow(() -> bound.requireQueueProjection(queue));
        final var released =
                new TargetDomainState(domain, TargetDomainState.Lifecycle.VACANT, null, null, null, null, null);
        assertThrows(
                IllegalArgumentException.class,
                () -> bound.requireQueueProjection(new TargetQueueState(
                        physical.id(),
                        1,
                        1,
                        TargetQueueState.AdmissionState.OPEN,
                        repeat(16, 1),
                        60000,
                        List.of(released))));
        assertThrows(IllegalArgumentException.class, () -> binding(
                        bound.canonicalBody(), "strict", source, new TargetKeyCodec.Domain(0, 2))
                .requireQueueProjection(queue));
    }

    @Test
    void bindingCannotInventNativePermissionOrMismatchSourceMessageAndCommandBranch() {
        final byte[] body = bytes("body.best");
        assertThrows(IllegalArgumentException.class, () -> binding(body, "native", source, domain));
        assertThrows(IllegalArgumentException.class, () -> binding(body, "prepare", source, domain));
        assertThrows(IllegalArgumentException.class, () -> binding(body, "strict", source, domain));
        final var otherSource = new KafkaSourcePosition(
                new ShardId(source.shardId().routeIncarnation(), 4), "source-cluster", UUID.randomUUID(), 7, 4, 90);
        assertThrows(IllegalArgumentException.class, () -> binding(body, "best", otherSource, domain));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetScheduleBinding(
                        DelayMessageId.random(source.shardId()),
                        CommandType.SCHEDULE,
                        body,
                        source,
                        physical.id(),
                        domain,
                        repeat(16, 1),
                        dispatch.digest(),
                        dispatch.digest(),
                        controls.digest(),
                        repeat(32, 0x77),
                        null,
                        null));
    }

    @Test
    void exactLegacyIntentOmissionSurvivesWithoutGainingNativePermission() {
        final var legacy = CanonicalScheduleIntent.create(
                profile(1),
                retry(),
                100,
                200,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("key"),
                Bytes.utf8("payload"),
                null,
                metadata(),
                null,
                null);
        assertTrue(legacy.legacyPolicyDefault());
        final byte[] body = body("best", legacy);
        final var bound = binding(body, "best", source, domain);
        assertArrayEquals(
                body, TargetScheduleBinding.decode(bound.canonicalBytes()).canonicalBody());
        assertTrue(bound.intent().legacyPolicyDefault());
        assertEquals(NativeDeliveryPolicy.FORBID, bound.intent().nativeDeliveryPolicy());
        assertThrows(IllegalArgumentException.class, () -> binding(body, "native", source, domain));
    }

    @Test
    void metadataAndReferenceBoundsApplyBeforeUnboundedLegacyCollectionDecoding() {
        final List<PulsarMetadata.Property> properties = new ArrayList<>();
        for (int n = 0; n < 1024; n++) {
            properties.add(new PulsarMetadata.Property(String.format("k%04d", n), "v"));
        }
        final var legal = AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, properties));
        assertDoesNotThrow(() -> binding(body("best", intent("best", profile(1), legal)), "best", source, domain));
        properties.add(new PulsarMetadata.Property("k1024", "v"));
        final var tooMany = AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, properties));
        assertThrows(
                IllegalArgumentException.class,
                () -> binding(body("best", intent("best", profile(1), tooMany)), "best", source, domain));
        final var longProfile = new ProfileRef(new byte[257], 1, repeat(32, 0x61), ProfileKind.DESTINATION);
        assertThrows(
                IllegalArgumentException.class,
                () -> binding(body("best", intent("best", longProfile, null)), "best", source, domain));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetScheduleBinding.decode(new byte[TargetScheduleBinding.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetChannelIdentity.decode(new byte[TargetChannelIdentity.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void maximumChannelIncludesFullWidthGenerationAndBoundedSignedTimeLease() {
        final var context = context(0xffff_ffffL, -1, -1, new TargetKeyCodec.Domain(63, -1));
        final var time = new TrustedUtcIntervalEvidence(
                Long.MAX_VALUE - 3,
                Long.MAX_VALUE - 2,
                TrustedUtcIntervalEvidence.Source.SIGNED_TIME_SERVICE,
                Bytes.utf8("x".repeat(256)),
                -1,
                -1,
                -1,
                repeat(32, 0x44),
                -1,
                repeat(64, 0x55));
        final var profile = new ProfileRef(Bytes.utf8("x".repeat(256)), -1, repeat(32, 0x61), ProfileKind.DESTINATION);
        final var lease = new CredentialUseLease(
                profile,
                CredentialUseKind.DESTINATION_CHANNEL,
                context.credentialHolderScope(),
                -1,
                repeat(32, 0x33),
                repeat(32, 0x66),
                time,
                Long.MAX_VALUE,
                -1);
        final var actual = new TargetChannelIdentity(context, lease);
        assertArrayEquals(bytes("channel.maximum"), actual.canonicalBytes());
        assertArrayEquals(bytes("channel.maximum.sha256"), Bytes.sha256(actual.canonicalBytes()));
        assertTrue(actual.canonicalBytes().length <= TargetChannelIdentity.MAX_CANONICAL_BYTES);
        assertTrue(lease.canonicalBytes().length <= TargetChannelIdentity.MAX_LEASE_BYTES);
        assertEquals(actual, TargetChannelIdentity.decode(actual.canonicalBytes()));
        final var oversizedClock = new TrustedUtcIntervalEvidence(
                100,
                101,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                new byte[257],
                1,
                2,
                3,
                repeat(32, 0x44),
                0,
                null);
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetChannelIdentity(
                        context,
                        new CredentialUseLease(
                                profile,
                                CredentialUseKind.DESTINATION_CHANNEL,
                                context.credentialHolderScope(),
                                1,
                                repeat(32, 0x33),
                                repeat(32, 0x66),
                                oversizedClock,
                                1000,
                                1)));
    }

    @Test
    void maximumBindingIncludesInlinePayloadMetadataBusinessOrderKeysAndFullSourceIdentity() {
        final var profile = new ProfileRef(Bytes.utf8("x".repeat(256)), -1, repeat(32, 0x61), ProfileKind.DESTINATION);
        final var retry = new RetryPolicyRef(Bytes.utf8("x".repeat(256)), -1, repeat(32, 0x62));
        final var metadata = AdapterMetadata.pulsar(new PulsarMetadata(
                null, null, null, List.of(new PulsarMetadata.Property("p", "x".repeat((1 << 20) - 15)))));
        assertEquals(1 << 20, metadata.canonicalBytes().length);
        final var intent = CanonicalScheduleIntent.create(
                profile,
                retry,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                DeliveryMode.MANAGED,
                OrderingMode.DELIVERY_TIME_FIFO,
                repeat(1 << 20, 'x'),
                repeat(1 << 24, 'x'),
                null,
                metadata,
                repeat(1 << 20, 'x'),
                Long.MAX_VALUE,
                NativeDeliveryPolicy.FORBID);
        final byte[] body = new ScheduleCommandBody(message, Long.MAX_VALUE, intent).canonicalBytes();
        final byte[] token = new byte[32];
        for (int n = 0; n < token.length; n++) {
            token[n] = (byte) n;
        }
        final var source = new PulsarSourcePosition(
                this.source.shardId(),
                token,
                "x".repeat(1 << 20),
                -1,
                -1,
                -2,
                -1,
                PulsarSourcePosition.EntryKind.BATCH,
                Long.MAX_VALUE);
        final var actual = binding(body, "strict", source, new TargetKeyCodec.Domain(63, -1));
        assertArrayEquals(bytes("body.maximum.sha256"), Bytes.sha256(body));
        assertArrayEquals(bytes("binding.maximum.sha256"), Bytes.sha256(actual.canonicalBytes()));
        assertEquals(Integer.parseInt(vectors.getProperty("binding.maximum.length")), actual.canonicalBytes().length);
        assertTrue(actual.canonicalBytes().length <= TargetScheduleBinding.MAX_CANONICAL_BYTES);
        assertEquals(actual, TargetScheduleBinding.decode(actual.canonicalBytes()));
    }

    @Test
    void unknownMissingAndTamperedFieldsFailEvenWithFreshOuterDigests() {
        for (int number = 1; number <= 16; number++) {
            if (number == 14 || number == 15) {
                continue;
            }
            final int omitted = number;
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetScheduleBinding.decode(rewrite(
                            bytes("binding.native"), omitted, null, 16, "nereus-delay-target-schedule-binding\0")),
                    "missing field " + number);
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetScheduleBinding.decode(Bytes.concat(bytes("binding.best"), uint(17, 1))));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetChannelIdentity.decode(Bytes.concat(bytes("channel.pulsar.journal"), uint(18, 1))));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetChannelIdentity.decode(rewrite(
                        bytes("channel.pulsar.journal"),
                        12,
                        field(12, Bytes.utf8("other-producer")),
                        17,
                        "nereus-delay-target-channel-identity\0")));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetChannelIdentity.decode(rewrite(
                        bytes("channel.pulsar.journal"), 14, null, 17, "nereus-delay-target-channel-identity\0")));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetChannelIdentity.decode(rewrite(
                        bytes("channel.pulsar.journal"), 9, uint(9, 4), 17, "nereus-delay-target-channel-identity\0")));
    }

    @Test
    void immutableReferencedRecordsRemainUnreadableToTheActiveLaneEnvelopeReader() {
        assertEquals(TargetChannelIdentity.VALUE_TYPE, bytes("channel.value")[2] & 0xff);
        assertEquals(TargetScheduleBinding.VALUE_TYPE, bytes("binding.value")[2] & 0xff);
        assertArrayEquals(
                bytes("channel.key"),
                channel(context(0, 1, 1, domain), profile(1)).encodedKey());
        for (String name : List.of("channel.value", "binding.value")) {
            final byte[] value = bytes(name);
            assertEquals(
                    Bytes.readU32be(value, value.length - 4), Bytes.crc32c(Arrays.copyOf(value, value.length - 4)));
            assertThrows(IllegalArgumentException.class, () -> ValueEnvelope.decodeAny(value));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetScheduleBinding.decodeReferenced(repeat(32, 9), bytes("binding.best")));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetChannelIdentity.decodeForStore(
                        bytes("binding.best.key"), bytes("channel.pulsar.journal"), source.shardId()));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetChannelIdentity.decodeReferenced(repeat(32, 9), bytes("channel.pulsar.journal")));
    }

    @Test
    void callersCannotMutateBindingBodiesChannelScopesOrFrozenDigests() {
        final byte[] body = bytes("body.best");
        final var bound = binding(body, "best", source, domain);
        final byte[] frozen = bound.canonicalBytes();
        Arrays.fill(body, (byte) 0);
        Arrays.fill(bound.canonicalBody(), (byte) 0);
        Arrays.fill(bound.membershipGrantRef(), (byte) 0);
        assertArrayEquals(frozen, bound.canonicalBytes());
        final var channel = channel(context(0, 1, 1, domain), profile(1));
        final byte[] encoded = channel.canonicalBytes();
        Arrays.fill(channel.context().accountingIncarnation(), (byte) 0);
        Arrays.fill(channel.context().dispatchCompatibilityRef(), (byte) 0);
        Arrays.fill(channel.context().producerIdentity(), (byte) 0);
        Arrays.fill(channel.digest(), (byte) 0);
        assertArrayEquals(encoded, channel.canonicalBytes());
    }

    private TargetScheduleBinding binding(
            final byte[] body, final String kind, final SourcePosition at, final TargetKeyCodec.Domain domain) {
        return new TargetScheduleBinding(
                message,
                kind.equals("prepare") ? CommandType.PREPARE_LARGE_SCHEDULE : CommandType.SCHEDULE,
                body,
                at,
                physical.id(),
                domain,
                repeat(16, 1),
                dispatch.digest(),
                dispatch.digest(),
                controls.digest(),
                repeat(32, 0x77),
                kind.equals("native") ? repeat(32, 0xEE) : null,
                kind.equals("strict") ? repeat(32, 0x88) : null);
    }

    private TargetChannelIdentity.Context context(
            final long slot, final long generation, final long evidence, final TargetKeyCodec.Domain domain) {
        return new TargetChannelIdentity.Context(
                source.shardId(),
                physical.id(),
                domain,
                repeat(16, 1),
                dispatch.digest(),
                controls.digest(),
                ChannelKind.PULSAR_DEDUP_PRODUCER,
                slot,
                generation,
                evidence,
                repeat(32, 0x22));
    }

    private static TargetChannelIdentity channel(
            final TargetChannelIdentity.Context context, final ProfileRef profile) {
        return new TargetChannelIdentity(
                context,
                new CredentialUseLease(
                        profile,
                        CredentialUseKind.DESTINATION_CHANNEL,
                        context.credentialHolderScope(),
                        1,
                        repeat(32, 0x33),
                        repeat(32, 0x66),
                        clock(),
                        1000,
                        1));
    }

    private static TrustedUtcIntervalEvidence clock() {
        return new TrustedUtcIntervalEvidence(
                100,
                101,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("clock"),
                1,
                2,
                3,
                repeat(32, 0x44),
                0,
                null);
    }

    private static ProfileRef profile(final int kind) {
        return new ProfileRef(Bytes.utf8(kind == 1 ? "dest" : "obj"), 1, repeat(32, 0x61), ProfileKind.fromWire(kind));
    }

    private static RetryPolicyRef retry() {
        return new RetryPolicyRef(Bytes.utf8("retry"), 1, repeat(32, 0x62));
    }

    private static AdapterMetadata metadata() {
        return AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, List.of()));
    }

    private static CanonicalScheduleIntent intent(
            final String kind, final ProfileRef profile, final AdapterMetadata metadata) {
        if (kind.equals("prepare")) {
            return CanonicalScheduleIntent.forPrepare(
                    profile,
                    retry(),
                    100,
                    200,
                    DeliveryMode.MANAGED,
                    OrderingMode.BEST_EFFORT,
                    Bytes.utf8("key"),
                    metadata(),
                    Bytes.utf8("biz"),
                    99L,
                    NativeDeliveryPolicy.FORBID);
        }
        final var object = kind.equals("object")
                ? new CommittedPayloadDescriptor(
                        profile(3),
                        Bytes.utf8("container"),
                        Bytes.utf8("key"),
                        Bytes.utf8("version"),
                        Bytes.utf8("etag"),
                        7,
                        Bytes.sha256(Bytes.utf8("payload")),
                        repeat(32, 0x63),
                        repeat(32, 0x64))
                : null;
        return CanonicalScheduleIntent.create(
                profile,
                retry(),
                100,
                200,
                DeliveryMode.MANAGED,
                kind.equals("strict") ? OrderingMode.DELIVERY_TIME_FIFO : OrderingMode.BEST_EFFORT,
                Bytes.utf8("key"),
                object == null ? Bytes.utf8("payload") : null,
                object,
                metadata == null ? metadata() : metadata,
                Bytes.utf8("biz"),
                99L,
                kind.equals("native") ? NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF : NativeDeliveryPolicy.FORBID);
    }

    private byte[] body(final String kind, final CanonicalScheduleIntent intent) {
        return kind.equals("prepare")
                ? new PrepareLargeScheduleBody(
                                message,
                                500,
                                intent,
                                7,
                                Bytes.sha256(Bytes.utf8("payload")),
                                1000,
                                new PayloadProofTrustSetRef(1, repeat(32, 0x65)),
                                profile(3))
                        .canonicalBytes()
                : new ScheduleCommandBody(message, 500, intent).canonicalBytes();
    }

    private ProfileSemanticEnvelope destination(
            final String id, final ProfileSemanticEnvelope cap, final int auth, final int maximumPayload) {
        return new ProfileSemanticEnvelope(
                ProfileKind.DESTINATION,
                Bytes.utf8(id),
                1,
                new DestinationProfileSemantic(
                        AdapterKind.PULSAR,
                        physical.resource(),
                        8,
                        TargetPartitionPolicy.EXPLICIT_ONLY,
                        TargetPartitionHashInput.DELAY_MESSAGE_ID,
                        List.of(5),
                        cap.ref(),
                        3,
                        60000,
                        repeat(32, auth),
                        20000,
                        10000,
                        maximumPayload,
                        1,
                        Bytes.utf8(id),
                        86400000,
                        172800000,
                        2,
                        repeat(32, 0xBB)));
    }

    private byte[] bytes(final String key) {
        return HexFormat.of().parseHex(vectors.getProperty(key));
    }

    private byte[] other(final String key) {
        return HexFormat.of().parseHex(compatibility.getProperty(key));
    }

    private static byte[] repeat(final int count, final int value) {
        final byte[] out = new byte[count];
        Arrays.fill(out, (byte) value);
        return out;
    }

    private static byte[] field(final int n, final byte[] value) {
        return CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, n, value));
    }

    private static byte[] uint(final int n, final long value) {
        return CanonicalProtobuf.message(out -> CanonicalProtobuf.uint64Bits(out, n, value));
    }

    private static byte[] rewrite(
            final byte[] encoded,
            final int number,
            final byte[] replacement,
            final int digestNumber,
            final String domain) {
        final var fields = QueryCodecSupport.read(encoded, "test rewrite");
        final byte[] body = CanonicalProtobuf.message(out -> {
            for (var f : fields) {
                if (f.number() == digestNumber) {
                    continue;
                }
                if (f.number() == number) {
                    if (replacement != null) {
                        out.writeBytes(replacement);
                    }
                    continue;
                }
                if (f.wireType() == 2) {
                    CanonicalProtobuf.bytes(out, f.number(), f.rawValue());
                } else {
                    CanonicalProtobuf.uint64Bits(out, f.number(), QueryCodecSupport.uint(f, f.number()));
                }
            }
        });
        return number == digestNumber && replacement == null
                ? body
                : Bytes.concat(body, field(digestNumber, Bytes.sha256(Bytes.utf8(domain), body)));
    }

    private static Properties load(final String name) {
        final var result = new Properties();
        try (var input = TargetBindingChannelContractTest.class.getResourceAsStream("/ndip3/" + name)) {
            result.load(input);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return result;
    }
}
