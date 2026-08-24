package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.adapter.DestinationPublishRequest;
import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.EvidenceVerificationStatusV1;
import com.nereusstream.delay.protocol.ExternalDeliveryIdentityV1;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PublishEvidenceKindV1;
import com.nereusstream.delay.protocol.PublishEvidenceV1;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.PublishAttemptLedger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class WorkerPublishOutcomeMutationFactoryTest {
    @Test
    void buildsCanonicalSignedPublishedOutcomeFromPhysicalEvidence() {
        final Fixture fixture = new Fixture();
        final KeyPair keyPair = keyPair();
        final AuthorIdentity author = AuthorIdentity.owner(
                Bytes.utf8("deployment"), Bytes.utf8("worker"), 7, Bytes.sha256(Bytes.utf8("fence")));
        final byte[] evidence = evidence(fixture.attempt.publishAttemptId(), true);
        final DestinationPublishResult result =
                DestinationPublishResult.published(Bytes.utf8("delivery"), 2_001, evidence);
        final WorkerPublishOutcomeMutationFactory factory = new WorkerPublishOutcomeMutationFactory(
                (attempt, request, physical) -> new WorkerPublishOutcomeMutationFactory.OutcomeContext(
                        9_000, 0, charge(), observedAt(), retryDecision()),
                author.canonicalBytes(),
                1,
                keyPair.getPrivate());

        final var mutation = factory.create(fixture.attempt, fixture.request, result);
        final PublishOutcomeBody body = PublishOutcomeBody.decode(mutation.canonicalBody());

        assertEquals(com.nereusstream.delay.protocol.SystemMutationType.PUBLISH_OUTCOME, mutation.type());
        assertTrue(mutation.verifySignature(keyPair.getPublic()));
        assertArrayEquals(fixture.attempt.publishAttemptId(), mutation.logicalOperationIdentity());
        assertEquals(1, body.sideEffect());
        assertEquals(StableCode.OK, body.stableCode());
        assertArrayEquals(evidence, body.evidence());
    }

    @Test
    void rejectsUnknownResultThatWouldDropTypedEvidence() {
        final Fixture fixture = new Fixture();
        final KeyPair keyPair = keyPair();
        final AuthorIdentity author = AuthorIdentity.owner(
                Bytes.utf8("deployment"), Bytes.utf8("worker"), 7, Bytes.sha256(Bytes.utf8("fence")));
        final WorkerPublishOutcomeMutationFactory factory = new WorkerPublishOutcomeMutationFactory(
                (attempt, request, physical) -> new WorkerPublishOutcomeMutationFactory.OutcomeContext(
                        9_000, 4, unknownTransfer(), observedAt(), unknownRetryPlaceholder()),
                author.canonicalBytes(),
                1,
                keyPair.getPrivate());

        assertThrows(
                IllegalArgumentException.class,
                () -> factory.create(
                        fixture.attempt,
                        fixture.request,
                        DestinationPublishResult.unknown(
                                StableCode.DESTINATION_OUTCOME_UNKNOWN, Bytes.utf8("typed-unknown-evidence"))));
    }

    @Test
    void rejectsDispositionThatDoesNotMatchPhysicalResult() {
        final Fixture fixture = new Fixture();
        final KeyPair keyPair = keyPair();
        final AuthorIdentity author = AuthorIdentity.owner(
                Bytes.utf8("deployment"), Bytes.utf8("worker"), 7, Bytes.sha256(Bytes.utf8("fence")));
        final WorkerPublishOutcomeMutationFactory factory = new WorkerPublishOutcomeMutationFactory(
                (attempt, request, physical) -> new WorkerPublishOutcomeMutationFactory.OutcomeContext(
                        9_000, 1, charge(), observedAt(), retryDecision()),
                author.canonicalBytes(),
                1,
                keyPair.getPrivate());

        assertThrows(
                IllegalArgumentException.class,
                () -> factory.create(
                        fixture.attempt,
                        fixture.request,
                        DestinationPublishResult.published(
                                Bytes.utf8("delivery"), 2_001, evidence(fixture.attempt.publishAttemptId(), true))));
    }

    private static byte[] charge() {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                .canonicalBytes();
    }

    private static byte[] retryDecision() {
        final byte[] policy = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("policy"));
            CanonicalProtobuf.uint64Bits(output, 2, 1);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("policy-hash")));
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, policy);
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.int64(output, 4, 2_000);
            CanonicalProtobuf.int64(output, 5, 5_000);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, StableCode.OK.wireValue());
            CanonicalProtobuf.uint32(output, 9, 1);
        });
    }

    private static byte[] unknownRetryPlaceholder() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, Bytes.utf8("unknown")));
    }

    private static byte[] unknownTransfer() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, Bytes.utf8("transfer")));
    }

    private static TrustedUtcIntervalEvidence observedAt() {
        return new TrustedUtcIntervalEvidence(
                2_000,
                2_000,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("time-proof")),
                0,
                null);
    }

    private static byte[] evidence(final byte[] attemptId, final boolean published) {
        final ExternalDeliveryIdentityV1 owner = ExternalDeliveryIdentityV1.publishAttempt(attemptId);
        final byte[] branch = published
                ? CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.bytes(
                            output,
                            1,
                            BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1(
                                            "cluster-a", UUID.nameUUIDFromBytes(Bytes.utf8("topic"))))
                                    .canonicalBytes());
                    CanonicalProtobuf.uint32(output, 2, 0);
                    CanonicalProtobuf.uint64(output, 3, 1);
                    CanonicalProtobuf.uint64(output, 5, 2_000);
                    CanonicalProtobuf.bytes(output, 6, owner.canonicalBytes());
                    CanonicalProtobuf.bytes(output, 7, Bytes.sha256(Bytes.utf8("prepared")));
                    CanonicalProtobuf.bytes(output, 8, Bytes.sha256(Bytes.utf8("response")));
                })
                : CanonicalProtobuf.message(output -> {
                    CanonicalProtobuf.bytes(output, 1, Bytes.utf8("channel"));
                    CanonicalProtobuf.bytes(output, 2, owner.canonicalBytes());
                    CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("prepared")));
                    CanonicalProtobuf.uint32(output, 4, 1);
                    CanonicalProtobuf.bytes(output, 5, Bytes.sha256(Bytes.utf8("request")));
                    CanonicalProtobuf.uint32(output, 6, 1);
                    CanonicalProtobuf.uint32(output, 7, StableCode.CAPABILITY_UNAVAILABLE.wireValue());
                });
        return PublishEvidenceV1.create(
                        published
                                ? PublishEvidenceKindV1.KAFKA_PRODUCE_ACK
                                : PublishEvidenceKindV1.ADAPTER_NON_SUBMISSION,
                        published
                                ? EvidenceVerificationStatusV1.VERIFIED_PUBLISHED
                                : EvidenceVerificationStatusV1.VERIFIED_NOT_PUBLISHED,
                        branch)
                .canonicalBytes();
    }

    private static KeyPair keyPair() {
        try {
            return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        } catch (java.security.GeneralSecurityException failure) {
            throw new IllegalStateException("Ed25519 is unavailable", failure);
        }
    }

    private static final class Fixture {
        private final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        private final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("outcome-lane"));
        private final byte[] laneIncarnation = new byte[16];
        private final PublishAttemptLedger attempt = PublishAttemptLedger.publishing(
                com.nereusstream.delay.protocol.DelayMessageId.random(shard),
                0,
                Bytes.sha256(Bytes.utf8("attempt")),
                Bytes.sha256(Bytes.utf8("claim")),
                7,
                1,
                lane,
                laneIncarnation,
                Bytes.utf8("owner"),
                new byte[16],
                Bytes.sha256(Bytes.utf8("prepared")),
                Bytes.utf8("opaque-admission"),
                new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 0, null, 1_000).canonicalBytes());
        private final DestinationPublishRequest request = new DestinationPublishRequest(
                lane,
                laneIncarnation,
                attempt.delayMessageId(),
                0,
                attempt.publishAttemptId(),
                1_000,
                1_000,
                Bytes.utf8("payload"),
                Bytes.utf8("metadata"));
    }
}
