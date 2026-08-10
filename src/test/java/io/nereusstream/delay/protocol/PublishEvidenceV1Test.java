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

    @Test
    void rejectsKafkaEvidenceWithPulsarTargetResource() {
        final byte[] attemptId = hash("wrong-target-attempt");
        final byte[] pulsarResource = BrokerResourceIdentityV1.pulsar(new PulsarBrokerResourceIdentityV1(
                "cluster-a", hash("pulsar-resource"), "persistent://tenant/ns/topic", 1)).canonicalBytes();
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, pulsarResource);
            CanonicalProtobuf.uint32(output, 2, 0);
            CanonicalProtobuf.uint64(output, 3, 1);
            CanonicalProtobuf.uint64(output, 5, 1_000);
            CanonicalProtobuf.bytes(output, 6, ExternalDeliveryIdentityV1.publishAttempt(attemptId)
                    .canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, hash("prepared"));
            CanonicalProtobuf.bytes(output, 8, hash("response"));
        });

        assertThrows(IllegalArgumentException.class, () -> PublishEvidenceV1.create(
                PublishEvidenceKindV1.KAFKA_PRODUCE_ACK,
                EvidenceVerificationStatusV1.VERIFIED_PUBLISHED, branch));
    }

    @Test
    void certifiedPulsarHandoffBindsTargetPartitionAndPreparedHashToAdmission() {
        final BrokerResourceIdentityV1 target = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1("cluster", hash("pulsar-resource"),
                        "persistent://tenant/ns/topic", 1));
        final PublishAdmissionBodyTest.Fixture fixture = PublishAdmissionBodyTest.Fixture.createWithProfiles(
                new ShardId(RouteIncarnation.random(), 21),
                new ProfileRefV1(Bytes.utf8("destination"), 1, hash("destination-hash"),
                        ProfileKindV1.DESTINATION).canonicalBytes(),
                new ProfileRefV1(Bytes.utf8("capability"), 2, hash("capability-hash"),
                        ProfileKindV1.DELIVERY_CAPABILITY).canonicalBytes(),
                target, AdapterKindV1.PULSAR, 1_500, 0);
        final PublishAdmissionBody admission = PublishAdmissionBody.decode(fixture.body());
        final ChannelResourceIdentityV1 channel = ChannelResourceIdentityV1.decode(
                admission.channel().canonicalBytes());
        final PublishEvidenceV1 evidence = PublishEvidenceV1.create(PublishEvidenceKindV1.PULSAR_SEND_ACK,
                EvidenceVerificationStatusV1.VERIFIED_PUBLISHED,
                pulsarAckBranch(admission, channel.targetResource(), channel.physicalPartition(),
                        admission.preparedPublishHash()));

        evidence.requireBusinessMutation(admission.publishAttemptId(), true);
        evidence.requireCertifiedPulsarHandoffBinding(admission);

        final BrokerResourceIdentityV1 foreignTarget = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1("cluster", hash("foreign-resource"),
                        "persistent://tenant/ns/topic", 1));
        assertThrows(IllegalArgumentException.class, () -> PublishEvidenceV1.create(
                PublishEvidenceKindV1.PULSAR_SEND_ACK, EvidenceVerificationStatusV1.VERIFIED_PUBLISHED,
                pulsarAckBranch(admission, foreignTarget, channel.physicalPartition(),
                        admission.preparedPublishHash())).requireCertifiedPulsarHandoffBinding(admission));
        assertThrows(IllegalArgumentException.class, () -> PublishEvidenceV1.create(
                PublishEvidenceKindV1.PULSAR_SEND_ACK, EvidenceVerificationStatusV1.VERIFIED_PUBLISHED,
                pulsarAckBranch(admission, channel.targetResource(), channel.physicalPartition() + 1,
                        admission.preparedPublishHash())).requireCertifiedPulsarHandoffBinding(admission));
        assertThrows(IllegalArgumentException.class, () -> PublishEvidenceV1.create(
                PublishEvidenceKindV1.PULSAR_SEND_ACK, EvidenceVerificationStatusV1.VERIFIED_PUBLISHED,
                pulsarAckBranch(admission, channel.targetResource(), channel.physicalPartition(), hash("wrong")))
                .requireCertifiedPulsarHandoffBinding(admission));
    }

    @Test
    void operatorAttestationRequiresEvidenceVerifierProfile() {
        final byte[] attemptId = hash("operator-attempt");
        final byte[] validBranch = operatorBranch(attemptId, ProfileKindV1.EVIDENCE_VERIFIER);
        assertEquals(PublishEvidenceKindV1.OPERATOR_ATTESTATION,
                PublishEvidenceV1.create(PublishEvidenceKindV1.OPERATOR_ATTESTATION,
                        EvidenceVerificationStatusV1.VERIFIED_PUBLISHED, validBranch).evidenceKind());

        final byte[] wrongBranch = operatorBranch(attemptId, ProfileKindV1.OBJECT_STORE);
        assertThrows(IllegalArgumentException.class, () -> PublishEvidenceV1.create(
                PublishEvidenceKindV1.OPERATOR_ATTESTATION,
                EvidenceVerificationStatusV1.VERIFIED_PUBLISHED, wrongBranch));
    }

    private static byte[] operatorBranch(final byte[] attemptId, final ProfileKindV1 profileKind) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ProfileRefV1(Bytes.utf8("operator-profile"), 1,
                    hash("operator-profile-hash"), profileKind).canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, ExternalDeliveryIdentityV1.publishAttempt(attemptId)
                    .canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, hash("prepared"));
            CanonicalProtobuf.bytes(output, 4, kafkaResource());
            CanonicalProtobuf.uint32(output, 5, 0);
            CanonicalProtobuf.uint32(output, 6, EvidenceVerificationStatusV1.VERIFIED_PUBLISHED.wireValue());
            CanonicalProtobuf.int64(output, 7, 100);
            CanonicalProtobuf.int64(output, 8, 200);
            CanonicalProtobuf.bytes(output, 9, hash("payload"));
            CanonicalProtobuf.uint32(output, 10, 1);
            CanonicalProtobuf.bytes(output, 11, new byte[64]);
        });
    }

    private static byte[] pulsarAckBranch(final PublishAdmissionBody admission,
                                          final BrokerResourceIdentityV1 target,
                                          final long partition,
                                          final byte[] preparedHash) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, target.canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, partition);
            CanonicalProtobuf.uint64(output, 3, 17);
            CanonicalProtobuf.uint64(output, 4, 23);
            CanonicalProtobuf.uint32(output, 5, 0);
            CanonicalProtobuf.uint64(output, 6, 2_001);
            CanonicalProtobuf.bytes(output, 7, hash("producer"));
            CanonicalProtobuf.uint64(output, 8, 42);
            CanonicalProtobuf.bytes(output, 9,
                    ExternalDeliveryIdentityV1.publishAttempt(admission.publishAttemptId()).canonicalBytes());
            CanonicalProtobuf.bytes(output, 10, preparedHash);
            CanonicalProtobuf.bytes(output, 11, hash("response"));
        });
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
