package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PublishEvidenceV1Test {
    @Test
    void kafkaAckEnvelopeDerivesAndChecksEvidenceId() {
        final byte[] attemptId = hash("attempt");
        final ExternalDeliveryIdentityV1 owner = ExternalDeliveryIdentityV1.publishAttempt(attemptId);
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, kafkaResource());
            CanonicalProtobuf.uint32(output, 2, 3);
            CanonicalProtobuf.uint64(output, 3, 17);
            CanonicalProtobuf.uint64(output, 5, 1_002);
            CanonicalProtobuf.bytes(output, 6, owner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, hash("prepared"));
            CanonicalProtobuf.bytes(output, 8, hash("response"));
        });

        final PublishEvidenceV1 evidence = PublishEvidenceV1.create(
                PublishEvidenceKindV1.KAFKA_PRODUCE_ACK,
                EvidenceVerificationStatusV1.VERIFIED_PUBLISHED, branch);
        final PublishEvidenceV1 decoded = PublishEvidenceV1.decode(evidence.canonicalBytes());

        assertArrayEquals(evidence.evidenceId(), decoded.evidenceId());
        assertEquals(PublishEvidenceKindV1.KAFKA_PRODUCE_ACK, decoded.evidenceKind());
        decoded.requireBusinessMutation(attemptId, true);
    }

    @Test
    void adapterNonSubmissionIsAClosedNotPublishedBranch() {
        final byte[] attemptId = hash("not-published-attempt");
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, nestedMarker());
            CanonicalProtobuf.bytes(output, 2, ExternalDeliveryIdentityV1.publishAttempt(attemptId)
                    .canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, hash("prepared"));
            CanonicalProtobuf.uint32(output, 4, 1);
            CanonicalProtobuf.bytes(output, 5, hash("request"));
            CanonicalProtobuf.uint32(output, 6, 1);
            CanonicalProtobuf.uint32(output, 7, StableCode.CAPABILITY_UNAVAILABLE.wireValue());
        });
        final PublishEvidenceV1 evidence = PublishEvidenceV1.create(PublishEvidenceKindV1.ADAPTER_NON_SUBMISSION,
                EvidenceVerificationStatusV1.VERIFIED_NOT_PUBLISHED, branch);
        PublishEvidenceV1.decode(evidence.canonicalBytes()).requireBusinessMutation(attemptId, false);
    }

    @Test
    void rejectsDigestStatusAndOwnerMismatches() {
        final byte[] attemptId = hash("mismatch-attempt");
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, kafkaResource());
            CanonicalProtobuf.uint32(output, 2, 0);
            CanonicalProtobuf.uint64(output, 3, 1);
            CanonicalProtobuf.uint64(output, 5, 1_000);
            CanonicalProtobuf.bytes(output, 6, ExternalDeliveryIdentityV1.publishAttempt(attemptId)
                    .canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, hash("prepared"));
            CanonicalProtobuf.bytes(output, 8, hash("response"));
        });
        final PublishEvidenceV1 evidence = PublishEvidenceV1.create(PublishEvidenceKindV1.KAFKA_PRODUCE_ACK,
                EvidenceVerificationStatusV1.VERIFIED_PUBLISHED, branch);
        final byte[] tampered = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, evidence.evidenceKind().wireValue());
            CanonicalProtobuf.uint32(output, 2, evidence.verificationStatus().wireValue());
            CanonicalProtobuf.bytes(output, 3, hash("wrong-id"));
            CanonicalProtobuf.bytes(output, evidence.branchField(), evidence.branch());
        });
        assertThrows(IllegalArgumentException.class, () -> PublishEvidenceV1.decode(tampered));
        assertThrows(IllegalArgumentException.class, () -> evidence.requireBusinessMutation(hash("other"), true));
        assertThrows(IllegalArgumentException.class, () -> PublishEvidenceV1.create(
                PublishEvidenceKindV1.KAFKA_PRODUCE_ACK,
                EvidenceVerificationStatusV1.VERIFIED_NOT_PUBLISHED, branch));
    }

    private static byte[] kafkaResource() {
        return BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1(
                "cluster-a", UUID.nameUUIDFromBytes(Bytes.utf8("topic")))).canonicalBytes();
    }

    private static byte[] nestedMarker() {
        return ProtocolTestFixtures.baselineKafkaChannel();
    }

    private static byte[] hash(final String value) {
        return Bytes.sha256(Bytes.utf8(value));
    }
}
