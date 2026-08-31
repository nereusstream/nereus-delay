package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PublishEvidenceTest {
    @Test
    void kafkaAckEnvelopeDerivesAndChecksEvidenceId() {
        final byte[] attemptId = hash("attempt");
        final ExternalDeliveryIdentity owner = ExternalDeliveryIdentity.publishAttempt(attemptId);
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, kafkaResource());
            CanonicalProtobuf.uint32(output, 2, 3);
            CanonicalProtobuf.uint64(output, 3, 17);
            CanonicalProtobuf.uint64(output, 5, 1_002);
            CanonicalProtobuf.bytes(output, 6, owner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, hash("prepared"));
            CanonicalProtobuf.bytes(output, 8, hash("response"));
        });

        final PublishEvidence evidence = PublishEvidence.create(
                PublishEvidenceKind.KAFKA_PRODUCE_ACK, EvidenceVerificationStatus.VERIFIED_PUBLISHED, branch);
        final PublishEvidence decoded = PublishEvidence.decode(evidence.canonicalBytes());

        assertArrayEquals(evidence.evidenceId(), decoded.evidenceId());
        assertEquals(PublishEvidenceKind.KAFKA_PRODUCE_ACK, decoded.evidenceKind());
        decoded.requireBusinessMutation(attemptId, true);
    }

    @Test
    void adapterNonSubmissionIsAClosedNotPublishedBranch() {
        final byte[] attemptId = hash("not-published-attempt");
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, nestedMarker());
            CanonicalProtobuf.bytes(
                    output,
                    2,
                    ExternalDeliveryIdentity.publishAttempt(attemptId).canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, hash("prepared"));
            CanonicalProtobuf.uint32(output, 4, 1);
            CanonicalProtobuf.bytes(output, 5, hash("request"));
            CanonicalProtobuf.uint32(output, 6, 1);
            CanonicalProtobuf.uint32(output, 7, StableCode.CAPABILITY_UNAVAILABLE.wireValue());
        });
        final PublishEvidence evidence = PublishEvidence.create(
                PublishEvidenceKind.ADAPTER_NON_SUBMISSION, EvidenceVerificationStatus.VERIFIED_NOT_PUBLISHED, branch);
        PublishEvidence.decode(evidence.canonicalBytes()).requireBusinessMutation(attemptId, false);
    }

    @Test
    void rejectsDigestStatusAndOwnerMismatches() {
        final byte[] attemptId = hash("mismatch-attempt");
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, kafkaResource());
            CanonicalProtobuf.uint32(output, 2, 0);
            CanonicalProtobuf.uint64(output, 3, 1);
            CanonicalProtobuf.uint64(output, 5, 1_000);
            CanonicalProtobuf.bytes(
                    output,
                    6,
                    ExternalDeliveryIdentity.publishAttempt(attemptId).canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, hash("prepared"));
            CanonicalProtobuf.bytes(output, 8, hash("response"));
        });
        final PublishEvidence evidence = PublishEvidence.create(
                PublishEvidenceKind.KAFKA_PRODUCE_ACK, EvidenceVerificationStatus.VERIFIED_PUBLISHED, branch);
        final byte[] tampered = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, evidence.evidenceKind().wireValue());
            CanonicalProtobuf.uint32(output, 2, evidence.verificationStatus().wireValue());
            CanonicalProtobuf.bytes(output, 3, hash("wrong-id"));
            CanonicalProtobuf.bytes(output, evidence.branchField(), evidence.branch());
        });
        assertThrows(IllegalArgumentException.class, () -> PublishEvidence.decode(tampered));
        assertThrows(IllegalArgumentException.class, () -> evidence.requireBusinessMutation(hash("other"), true));
        assertThrows(
                IllegalArgumentException.class,
                () -> PublishEvidence.create(
                        PublishEvidenceKind.KAFKA_PRODUCE_ACK,
                        EvidenceVerificationStatus.VERIFIED_NOT_PUBLISHED,
                        branch));
    }

    @Test
    void rejectsKafkaEvidenceWithPulsarTargetResource() {
        final byte[] attemptId = hash("wrong-target-attempt");
        final byte[] pulsarResource = BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                        "cluster-a", hash("pulsar-resource"), "persistent://tenant/ns/topic", 1))
                .canonicalBytes();
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, pulsarResource);
            CanonicalProtobuf.uint32(output, 2, 0);
            CanonicalProtobuf.uint64(output, 3, 1);
            CanonicalProtobuf.uint64(output, 5, 1_000);
            CanonicalProtobuf.bytes(
                    output,
                    6,
                    ExternalDeliveryIdentity.publishAttempt(attemptId).canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, hash("prepared"));
            CanonicalProtobuf.bytes(output, 8, hash("response"));
        });

        assertThrows(
                IllegalArgumentException.class,
                () -> PublishEvidence.create(
                        PublishEvidenceKind.KAFKA_PRODUCE_ACK, EvidenceVerificationStatus.VERIFIED_PUBLISHED, branch));
    }

    @Test
    void certifiedPulsarHandoffBindsTargetPartitionAndPreparedHashToAdmission() {
        final BrokerResourceIdentity target = BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                "cluster", hash("pulsar-resource"), "persistent://tenant/ns/topic", 1));
        final PublishAdmissionBodyTest.Fixture fixture = PublishAdmissionBodyTest.Fixture.createWithProfiles(
                new ShardId(RouteIncarnation.random(), 21),
                new ProfileRef(Bytes.utf8("destination"), 1, hash("destination-hash"), ProfileKind.DESTINATION)
                        .canonicalBytes(),
                new ProfileRef(Bytes.utf8("capability"), 2, hash("capability-hash"), ProfileKind.DELIVERY_CAPABILITY)
                        .canonicalBytes(),
                target,
                AdapterKind.PULSAR,
                1_500,
                0);
        final PublishAdmissionBody admission = PublishAdmissionBody.decode(fixture.body());
        final ChannelResourceIdentity channel =
                ChannelResourceIdentity.decode(admission.channel().canonicalBytes());
        final PublishEvidence evidence = PublishEvidence.create(
                PublishEvidenceKind.PULSAR_SEND_ACK,
                EvidenceVerificationStatus.VERIFIED_PUBLISHED,
                pulsarAckBranch(
                        admission,
                        channel.targetResource(),
                        channel.physicalPartition(),
                        admission.preparedPublishHash()));

        evidence.requireBusinessMutation(admission.publishAttemptId(), true);
        evidence.requireCertifiedPulsarHandoffBinding(admission);

        final BrokerResourceIdentity foreignTarget = BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                "cluster", hash("foreign-resource"), "persistent://tenant/ns/topic", 1));
        assertThrows(IllegalArgumentException.class, () -> PublishEvidence.create(
                        PublishEvidenceKind.PULSAR_SEND_ACK,
                        EvidenceVerificationStatus.VERIFIED_PUBLISHED,
                        pulsarAckBranch(
                                admission, foreignTarget, channel.physicalPartition(), admission.preparedPublishHash()))
                .requireCertifiedPulsarHandoffBinding(admission));
        assertThrows(IllegalArgumentException.class, () -> PublishEvidence.create(
                        PublishEvidenceKind.PULSAR_SEND_ACK,
                        EvidenceVerificationStatus.VERIFIED_PUBLISHED,
                        pulsarAckBranch(
                                admission,
                                channel.targetResource(),
                                channel.physicalPartition() + 1,
                                admission.preparedPublishHash()))
                .requireCertifiedPulsarHandoffBinding(admission));
        assertThrows(IllegalArgumentException.class, () -> PublishEvidence.create(
                        PublishEvidenceKind.PULSAR_SEND_ACK,
                        EvidenceVerificationStatus.VERIFIED_PUBLISHED,
                        pulsarAckBranch(
                                admission, channel.targetResource(), channel.physicalPartition(), hash("wrong")))
                .requireCertifiedPulsarHandoffBinding(admission));
    }

    @Test
    void certifiedPulsarHandoffAcceptsGenerationTwoAckFieldLayout() {
        final BrokerResourceIdentity target = BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                "cluster", hash("pulsar-generation-two-resource"), "persistent://tenant/ns/topic", 1));
        final PublishAdmissionBodyTest.Fixture fixture = PublishAdmissionBodyTest.Fixture.createWithProfiles(
                new ShardId(RouteIncarnation.random(), 22),
                new ProfileRef(Bytes.utf8("destination"), 1, hash("destination-hash"), ProfileKind.DESTINATION)
                        .canonicalBytes(),
                new ProfileRef(Bytes.utf8("capability"), 2, hash("capability-hash"), ProfileKind.DELIVERY_CAPABILITY)
                        .canonicalBytes(),
                target,
                AdapterKind.PULSAR,
                1_500,
                0);
        final PublishAdmissionBody admission = PublishAdmissionBody.decode(fixture.body());
        final ChannelResourceIdentity channel =
                ChannelResourceIdentity.decode(admission.channel().canonicalBytes());
        final PublishEvidence evidence = PublishEvidence.create(
                PublishEvidenceKind.PULSAR_SEND_ACK,
                EvidenceVerificationStatus.VERIFIED_PUBLISHED,
                pulsarAckGenerationTwoBranch(
                        admission,
                        channel.targetResource(),
                        channel.physicalPartition(),
                        admission.preparedPublishHash()));

        evidence.requireBusinessMutation(admission.publishAttemptId(), true);
        evidence.requireCertifiedPulsarHandoffBinding(admission);
    }

    @Test
    void operatorAttestationRequiresEvidenceVerifierProfile() {
        final byte[] attemptId = hash("operator-attempt");
        final byte[] validBranch = operatorBranch(attemptId, ProfileKind.EVIDENCE_VERIFIER);
        assertEquals(
                PublishEvidenceKind.OPERATOR_ATTESTATION,
                PublishEvidence.create(
                                PublishEvidenceKind.OPERATOR_ATTESTATION,
                                EvidenceVerificationStatus.VERIFIED_PUBLISHED,
                                validBranch)
                        .evidenceKind());

        final byte[] wrongBranch = operatorBranch(attemptId, ProfileKind.OBJECT_STORE);
        assertThrows(
                IllegalArgumentException.class,
                () -> PublishEvidence.create(
                        PublishEvidenceKind.OPERATOR_ATTESTATION,
                        EvidenceVerificationStatus.VERIFIED_PUBLISHED,
                        wrongBranch));
    }

    private static byte[] operatorBranch(final byte[] attemptId, final ProfileKind profileKind) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(
                    output,
                    1,
                    new ProfileRef(Bytes.utf8("operator-profile"), 1, hash("operator-profile-hash"), profileKind)
                            .canonicalBytes());
            CanonicalProtobuf.bytes(
                    output,
                    2,
                    ExternalDeliveryIdentity.publishAttempt(attemptId).canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, hash("prepared"));
            CanonicalProtobuf.bytes(output, 4, kafkaResource());
            CanonicalProtobuf.uint32(output, 5, 0);
            CanonicalProtobuf.uint32(output, 6, EvidenceVerificationStatus.VERIFIED_PUBLISHED.wireValue());
            CanonicalProtobuf.int64(output, 7, 100);
            CanonicalProtobuf.int64(output, 8, 200);
            CanonicalProtobuf.bytes(output, 9, hash("payload"));
            CanonicalProtobuf.uint32(output, 10, 1);
            CanonicalProtobuf.bytes(output, 11, new byte[64]);
        });
    }

    private static byte[] pulsarAckBranch(
            final PublishAdmissionBody admission,
            final BrokerResourceIdentity target,
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
            CanonicalProtobuf.bytes(
                    output,
                    9,
                    ExternalDeliveryIdentity.publishAttempt(admission.publishAttemptId())
                            .canonicalBytes());
            CanonicalProtobuf.bytes(output, 10, preparedHash);
            CanonicalProtobuf.bytes(output, 11, hash("response"));
        });
    }

    private static byte[] pulsarAckGenerationTwoBranch(
            final PublishAdmissionBody admission,
            final BrokerResourceIdentity target,
            final long partition,
            final byte[] preparedHash) {
        final byte[] producerHash = hash("generation-two-producer");
        final long sequenceId = 42;
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 2);
            CanonicalProtobuf.bytes(output, 2, target.canonicalBytes());
            CanonicalProtobuf.uint32(output, 3, partition);
            CanonicalProtobuf.uint64(output, 4, 17);
            CanonicalProtobuf.uint64(output, 5, 23);
            CanonicalProtobuf.uint32(output, 6, 0);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint64(output, 8, 2_001);
            CanonicalProtobuf.bytes(output, 9, producerHash);
            CanonicalProtobuf.uint32(output, 10, 22);
            CanonicalProtobuf.uint64(output, 11, 3);
            CanonicalProtobuf.uint64(output, 12, 4);
            CanonicalProtobuf.uint64(output, 13, sequenceId);
            CanonicalProtobuf.bytes(
                    output,
                    14,
                    ExternalDeliveryIdentity.publishAttempt(admission.publishAttemptId())
                            .canonicalBytes());
            CanonicalProtobuf.bytes(output, 15, preparedHash);
            CanonicalProtobuf.bytes(output, 16, hash("generation-two-template"));
            CanonicalProtobuf.bytes(output, 17, hash("generation-two-record"));
            CanonicalProtobuf.bytes(
                    output,
                    18,
                    PulsarSequenceAuthority.managedJournal(hash("generation-two-mapping"), sequenceId, producerHash)
                            .canonicalBytes());
            CanonicalProtobuf.bytes(output, 19, hash("generation-two-send"));
            CanonicalProtobuf.bytes(output, 20, hash("generation-two-response"));
            CanonicalProtobuf.bytes(output, 21, hash("generation-two-p1-source-lock"));
            CanonicalProtobuf.bytes(output, 22, hash("generation-two-artifact-set"));
        });
    }

    private static byte[] kafkaResource() {
        return BrokerResourceIdentity.kafka(
                        new KafkaBrokerResourceIdentity("cluster-a", UUID.nameUUIDFromBytes(Bytes.utf8("topic"))))
                .canonicalBytes();
    }

    private static byte[] nestedMarker() {
        return ProtocolTestFixtures.baselineKafkaChannel();
    }

    private static byte[] hash(final String value) {
        return Bytes.sha256(Bytes.utf8(value));
    }
}
