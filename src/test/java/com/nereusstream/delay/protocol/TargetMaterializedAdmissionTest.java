package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.runtime.AttemptLedgerState;
import com.nereusstream.delay.runtime.AttemptObligationRef;
import com.nereusstream.delay.runtime.CurrentSendWorkKind;
import com.nereusstream.delay.runtime.GenerationAggregateState;
import com.nereusstream.delay.runtime.TargetClaimRecord;
import com.nereusstream.delay.runtime.TargetGenerationRuntimeIndex;
import com.nereusstream.delay.runtime.TargetMessageRecord;
import com.nereusstream.delay.runtime.TargetPublishAdmissionVerifier;
import com.nereusstream.delay.runtime.TargetTimelineWorkRef;
import com.nereusstream.delay.runtime.TimelineWorkKind;
import com.nereusstream.delay.runtime.UncertainRetryAuthority;
import com.nereusstream.delay.store.KeyCodec;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class TargetMaterializedAdmissionTest {
    @Test
    void publicationFreezesExactClaimChannelPayloadAndRecordIdentity() throws Exception {
        final var f = fixture();
        final var publication = publication(f);
        final var decoded = TargetOrdinaryPublicationBinding.decode(publication.canonicalBytes());
        assertEquals(publication, decoded);
        assertArrayEquals(publication.preparedPublishHash(), decoded.preparedPublishHash());
        assertDoesNotThrow(() -> decoded.requireClaim(f.claim(), f.claimed(), f.binding()));
        assertDoesNotThrow(() -> decoded.requirePayload(f.claimed().inlinePayload()));
        assertThrows(IllegalArgumentException.class, () -> decoded.requirePayload(Bytes.utf8("wrong")));
        assertThrows(IllegalStateException.class, () -> decoded.requireClaim(f.claim(), f.initial(), f.binding()));
        final byte[] changed = publication.canonicalBytes();
        changed[changed.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetOrdinaryPublicationBinding.decode(changed));
        assertThrows(IllegalArgumentException.class, () -> TargetOrdinaryPublicationBinding.decode(
                new byte[TargetOrdinaryPublicationBinding.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void materializedBodyUsesIndependentTupleAndRequiresResolvedAuthority() throws Exception {
        final var f = fixture();
        final var publication = publication(f);
        final var body = admission(f, publication);
        final var decoded = TargetPublishAdmissionBody.decode(body.canonicalBytes());
        assertEquals(4, decoded.bodyVersion());
        assertEquals(3, TargetPublishAdmissionBody.BODY_VERSION);
        assertEquals(publication, decoded.publication());
        assertArrayEquals(f.claim().canonicalBytes(), decoded.claimProof().canonicalBytes());
        final var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var owner = f.claim().owner();
        final var mutation = SystemMutation.signed(
                body.shard(), SystemMutationType.TARGET_PUBLISH_ADMISSION, body.retryUntilEpochMs(),
                body.publishAttemptId(), body.canonicalBytes(),
                AuthorIdentity.owner(owner.deploymentId(), owner.workerRunId(),
                                owner.ownerEpoch(), owner.leaseFencingDigest())
                        .canonicalBytes(),
                1, keys.getPrivate());
        assertArrayEquals(mutation.encodeFrame(), SystemMutation.decodeFrame(mutation.encodeFrame()).encodeFrame());
        final var oldSource = (KafkaSourcePosition) f.binding().bindingSource();
        final var at = new KafkaSourcePosition(oldSource.shardId(), oldSource.authenticatedClusterId(),
                oldSource.nativeTopicUuid(), oldSource.offset() + 1, oldSource.leaderEpoch(),
                body.decisionTime().earliestEpochMs());
        final var scope = new TargetQuotaScope(at.shardId(), repeated(32, 0x51), null);
        final var accepted = TargetPublishAdmissionVerifier.decideFirstApplication(
                scope, mutation, at,
                (a, b, c, d) -> new TargetPublishAdmissionVerifier.Authorization(
                        keys.getPublic(), ProtocolTuple.targetMaterializedPublishAdmission(), 10, 10, 100,
                        (e, g, h, i) -> true, (materialized, source) -> {
                            assertEquals(publication, materialized.publication());
                            assertEquals(at, source);
                        }));
        assertNull(accepted.rejection());
        assertEquals(StableCode.UNAUTHORIZED_SYSTEM_MUTATION,
                TargetPublishAdmissionVerifier.decideFirstApplication(
                                scope, mutation, at,
                                (a, b, c, d) -> new TargetPublishAdmissionVerifier.Authorization(
                                        keys.getPublic(), ProtocolTuple.targetPublishAdmission(),
                                        10, 10, 100, (e, g, h, i) -> true))
                        .rejection());
        assertThrows(IllegalStateException.class, () -> TargetPublishAdmissionVerifier.decideFirstApplication(
                scope, mutation, at,
                (a, b, c, d) -> new TargetPublishAdmissionVerifier.Authorization(
                        keys.getPublic(), ProtocolTuple.targetMaterializedPublishAdmission(),
                        10, 10, 100, (e, g, h, i) -> true)));
    }

    private record Fixture(
            TargetScheduleBinding binding,
            CanonicalTargetPartition physical,
            TargetChannelIdentity channel,
            TargetMessageRecord initial,
            TargetClaimRecord claim,
            TargetMessageRecord claimed) {}

    @Test
    void targetPublishedEvidenceRequiresExactResourcePartitionAndPreparedCommitment() throws Exception {
        final var f = fixture();
        final var publication = publication(f);
        final var body = admission(f, publication);
        final var transfer = PublishAdmissionBody.ChargeVector.decodeCanonical(body.outcomeTransfer());
        assertEquals(1, transfer.inflightMessages());
        assertEquals(body.executionBytes(), transfer.inflightBytes());
        assertEquals(body.commitment().amount(CapacityDimension.RESULT_BYTES), transfer.resultBytes());
        final long part = publication.physical().physicalPartition();
        assertDoesNotThrow(() -> pulsarAck(publication, part, publication.preparedPublishHash())
                .requireOrdinaryTargetPublishedBinding(publication));
        assertThrows(IllegalArgumentException.class, () -> pulsarAck(publication, part + 1,
                publication.preparedPublishHash()).requireOrdinaryTargetPublishedBinding(publication));
        assertThrows(IllegalArgumentException.class, () -> pulsarAck(publication, part, repeated(32, 0x61))
                .requireOrdinaryTargetPublishedBinding(publication));
    }

    private static PublishEvidence pulsarAck(
            TargetOrdinaryPublicationBinding publication, long partition, byte[] preparedHash) {
        final byte[] branch = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, publication.physical().resource().canonicalBytes());
            CanonicalProtobuf.uint32(out, 2, partition);
            CanonicalProtobuf.uint64(out, 3, 1);
            CanonicalProtobuf.uint64(out, 4, 2);
            CanonicalProtobuf.uint32(out, 5, 0);
            CanonicalProtobuf.uint64(out, 6, publication.deliverAtEpochMs());
            CanonicalProtobuf.bytes(out, 7, repeated(32, 0x61));
            CanonicalProtobuf.uint64(out, 8, 1);
            CanonicalProtobuf.bytes(out, 9,
                    ExternalDeliveryIdentity.publishAttempt(publication.publishAttemptId()).canonicalBytes());
            CanonicalProtobuf.bytes(out, 10, preparedHash);
            CanonicalProtobuf.bytes(out, 11, repeated(32, 0x62));
        });
        return PublishEvidence.create(
                PublishEvidenceKind.PULSAR_SEND_ACK, EvidenceVerificationStatus.VERIFIED_PUBLISHED, branch);
    }

    private static Fixture fixture() throws Exception {
        final var binding = TargetScheduleBinding.decode(vector("target-binding-channel", "binding.best"));
        final var physical = CanonicalTargetPartition.decode(vector("target-compatibility", "pulsar.target"));
        final var channel = TargetChannelIdentity.decode(vector("target-binding-channel", "channel.pulsar.journal"));
        final var template = TargetQuotaClaimCharge.decode(vector("target-quota-claim", "native"));
        final var intent = binding.intent();
        final var locator = new TargetMessageLocator(
                binding.messageId(), 0, binding.target(), binding.domain(), binding.accountingIncarnation(),
                intent.orderingMode(), binding.orderingDomain(), binding.digest());
        final var work = new TargetTimelineWorkRef(
                locator, TimelineWorkKind.INITIAL_SCHEDULE, intent.deliverAtEpochMs(), intent.deliverAtEpochMs(),
                binding.bindingSource().sourceOrderToken(), 1, 1, UncertainRetryAuthority.NONE, null, null, false);
        final var initial = new TargetMessageRecord(
                locator, 1, intent.deliverAtEpochMs(), intent.expireAtEpochMs(), intent.deliverAtEpochMs(),
                intent.nativeDeliveryPolicy(), binding.bindingSource(), intent.inlinePayload(), null,
                new TargetGenerationRuntimeIndex(0, GenerationAggregateState.SCHEDULED, CurrentSendWorkKind.TIMELINE,
                        work, null, null, List.of(), 0, 0, false, 1));
        final var claim = new TargetClaimRecord(
                initial.runtime(), initial.stateVersion(), initial.digest(),
                new TargetHeadRef(work.ordinaryKey(), locator.messageId(), 0, work.ordinaryEligibilityAtEpochMs()),
                template.owner(), template.storeIncarnation(), 3, intent.expireAtEpochMs() - 1, 128, 1,
                template.creation());
        return new Fixture(binding, physical, channel, initial, claim, claim.claimed(initial));
    }

    private static TargetOrdinaryPublicationBinding publication(Fixture f) {
        return TargetOrdinaryPublicationBinding.fromClaim(
                f.claim(), f.claimed(), f.binding(), f.physical(), f.channel(),
                new ProfileRef(Bytes.utf8("capability"), 1, repeated(32, 0x41), ProfileKind.DELIVERY_CAPABILITY),
                repeated(32, 0x42));
    }

    private static TargetPublishAdmissionBody admission(Fixture f, TargetOrdinaryPublicationBinding publication) {
        final long[] reserve = new long[CapacityDimension.COUNT];
        reserve[CapacityDimension.RESULT_BYTES.wireValue() - 1] = 256;
        final var time = new TrustedUtcIntervalEvidence(
                f.claimed().deliverAtEpochMs(), f.claimed().deliverAtEpochMs() + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"),
                1, 1, 1, repeated(32, 0x43), 0, null);
        final var ref = new AttemptObligationRef(publication.publishAttemptId(), publication.locator().generation(),
                AttemptLedgerState.PUBLISHING,
                KeyCodec.inflight((byte) 2, f.claim().owner().ownerEpoch(), publication.publishAttemptId()));
        return new TargetPublishAdmissionBody(
                f.binding().bindingSource().shardId(), time.latestEpochMs() + 10_000, f.claim().owner(),
                f.claim().storeIncarnation(), f.claim().claimId(), publication.locator(), publication.attemptNo(),
                publication.publishAttemptId(), ref, 128, new CapacityVector(reserve), CapacityVector.empty(), time,
                publication, f.claim());
    }

    private static byte[] repeated(int count, int value) {
        final byte[] bytes = new byte[count];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static byte[] vector(String name, String key) throws Exception {
        final var values = new Properties();
        try (var input = TargetMaterializedAdmissionTest.class.getResourceAsStream(
                "/ndip3/" + name + "-vectors.properties")) {
            values.load(input);
        }
        return HexFormat.of().parseHex(values.getProperty(key));
    }
}
