package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ResolveUncertainBodyTest {
    @Test
    void evidenceAttachmentRequiresTypedEvidenceOwnedByAttempt() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final byte[] attemptId = hash("attempt-published");
        final byte[] evidence = evidence(attemptId, true);

        final ResolveUncertainBody decoded = ResolveUncertainBody.decode(body(shard, messageId, attemptId, 1,
                evidence));

        assertEquals(1, decoded.resolutionKind());
        assertArrayEquals(evidence, decoded.evidence());
    }

    @Test
    void canonicalEncoderRoundTripsBothResolutionShapes() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final ControlRef controlRef = new ControlRef(hash("operation"), hash("request"), 0);
        final DestinationLaneId laneId = new DestinationLaneId(Bytes.sha256(Bytes.utf8("resolve-lane")));
        final byte[] laneIncarnation = new byte[16];
        final byte[] attemptId = hash("attempt-published");
        final byte[] evidence = evidence(attemptId, true);

        final byte[] attached = ResolveUncertainBody.encode(shard, 9_000, controlRef, laneId, laneIncarnation,
                messageId, 0, attemptId, 1, evidence, false, false, null);
        final ResolveUncertainBody attachedDecoded = ResolveUncertainBody.decode(attached);
        assertArrayEquals(attached, ResolveUncertainBody.encode(shard, 9_000, attachedDecoded.controlRef(),
                attachedDecoded.laneId(), attachedDecoded.laneIncarnation(), attachedDecoded.messageId(),
                attachedDecoded.generation(), attachedDecoded.publishAttemptId(),
                attachedDecoded.resolutionKind(), attachedDecoded.evidence(),
                attachedDecoded.allowPossibleDuplicate(), attachedDecoded.allowPossibleDeliveryTerminal(),
                attachedDecoded.acknowledgementHash()));

        final byte[] acknowledgement = hash("acknowledgement");
        final byte[] retry = ResolveUncertainBody.encode(shard, 9_000, controlRef, laneId, laneIncarnation,
                messageId, 0, hash("attempt-retry"), 3, null, true, false, acknowledgement);
        final ResolveUncertainBody retryDecoded = ResolveUncertainBody.decode(retry);
        assertEquals(3, retryDecoded.resolutionKind());
        assertArrayEquals(acknowledgement, retryDecoded.acknowledgementHash());
    }

    @Test
    void notPublishedAttachmentRejectsWrongOwnerAndOpaqueBytes() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final byte[] attemptId = hash("attempt-not-published");
        final byte[] evidence = evidence(attemptId, false);

        ResolveUncertainBody.decode(body(shard, messageId, attemptId, 2, evidence));
        assertThrows(IllegalArgumentException.class,
                () -> ResolveUncertainBody.decode(body(shard, messageId, hash("other"), 2, evidence)));
        assertThrows(IllegalArgumentException.class,
                () -> ResolveUncertainBody.decode(body(shard, messageId, attemptId, 1,
                        CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1,
                                Bytes.utf8("opaque"))))));

        final DelayMessageId otherShardMessage = DelayMessageId.random(
                new ShardId(RouteIncarnation.random(), 0));
        assertThrows(IllegalArgumentException.class,
                () -> ResolveUncertainBody.decode(body(shard, otherShardMessage, attemptId, 2, evidence)));
    }

    private static byte[] body(final ShardId shard, final DelayMessageId messageId, final byte[] attemptId,
                               final int resolutionKind, final byte[] evidence) {
        final byte[] lane = Bytes.sha256(Bytes.utf8("resolve-lane"));
        final ControlRef controlRef = new ControlRef(hash("operation"), hash("request"), 0);
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject(shard));
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.RESOLVE_UNCERTAIN.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.bytes(output, 11, lane);
            CanonicalProtobuf.bytes(output, 12, new byte[16]);
            CanonicalProtobuf.bytes(output, 13, messageId.bytes());
            CanonicalProtobuf.uint32(output, 14, 0);
            CanonicalProtobuf.bytes(output, 15, attemptId);
            CanonicalProtobuf.uint32(output, 16, resolutionKind);
            CanonicalProtobuf.bytes(output, 17, evidence);
            CanonicalProtobuf.uint32(output, 18, 0);
            CanonicalProtobuf.uint32(output, 19, 0);
        });
    }

    private static byte[] evidence(final byte[] attemptId, final boolean published) {
        final ExternalDeliveryIdentityV1 owner = ExternalDeliveryIdentityV1.publishAttempt(attemptId);
        final byte[] branch = published ? CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, BrokerResourceIdentityV1.kafka(
                    new KafkaBrokerResourceIdentityV1("cluster-a",
                            UUID.nameUUIDFromBytes(Bytes.utf8("resolve-topic")))).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, 0);
            CanonicalProtobuf.uint64(output, 3, 1);
            CanonicalProtobuf.uint64(output, 5, 1_000);
            CanonicalProtobuf.bytes(output, 6, owner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, hash("prepared"));
            CanonicalProtobuf.bytes(output, 8, hash("response"));
        }) : CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, ProtocolTestFixtures.baselineKafkaChannel());
            CanonicalProtobuf.bytes(output, 2, owner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, hash("prepared"));
            CanonicalProtobuf.uint32(output, 4, 1);
            CanonicalProtobuf.bytes(output, 5, hash("request"));
            CanonicalProtobuf.uint32(output, 6, 1);
            CanonicalProtobuf.uint32(output, 7, StableCode.CAPABILITY_UNAVAILABLE.wireValue());
        });
        return PublishEvidenceV1.create(published ? PublishEvidenceKindV1.KAFKA_PRODUCE_ACK
                        : PublishEvidenceKindV1.ADAPTER_NON_SUBMISSION,
                published ? EvidenceVerificationStatusV1.VERIFIED_PUBLISHED
                        : EvidenceVerificationStatusV1.VERIFIED_NOT_PUBLISHED, branch).canonicalBytes();
    }

    private static byte[] subject(final ShardId shard) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
    }

    private static byte[] hash(final String value) {
        return Bytes.sha256(Bytes.utf8(value));
    }
}
