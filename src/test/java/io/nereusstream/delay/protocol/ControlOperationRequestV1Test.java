package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ControlOperationRequestV1Test {
    @Test
    void roundTripsEveryClosedOperationBranch() throws Exception {
        final ControlReasonV1 reason = new ControlReasonV1(ControlReasonKindV1.OPERATOR_REQUEST,
                bytes(32, 1), bytes(32, 2));
        final AcknowledgementSetV1 acknowledgements = new AcknowledgementSetV1(List.of(
                new AcknowledgementV1(AcknowledgementKindV1.POSSIBLE_DUPLICATE, bytes(32, 3), bytes(32, 4)),
                new AcknowledgementV1(AcknowledgementKindV1.POSSIBLE_DELIVERY, bytes(32, 5), bytes(32, 6)),
                new AcknowledgementV1(AcknowledgementKindV1.ORDER_LOSS, bytes(32, 7), bytes(32, 8))));
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(bytes(16, 9), 2, bytes(32, 10));
        final PublishQuotaGrantRequestV1 quota = new PublishQuotaGrantRequestV1(
                new QuotaGrantRefV1(bytes(32, 11), 1, new PublishAdmissionBody.ChargeVector(
                        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)),
                new QuotaTransferPlanRefV1(bytes(32, 12), bytes(32, 13), 3, bytes(32, 14)));
        final List<ControlOperationRequestV1> requests = List.of(
                ControlOperationRequestV1.stopNewSchedules(reason),
                ControlOperationRequestV1.pauseDestinationLane(reason),
                ControlOperationRequestV1.resumeDestinationLane(reason),
                ControlOperationRequestV1.closeDestinationLane(new CloseLaneRequestV1(reason,
                        ClosePolicyV1.V1_FREEZE_UNADMITTED_AND_PRESERVE_ADMITTED, true, acknowledgements)),
                ControlOperationRequestV1.breakOrdering(new BreakOrderingRequestV1(acknowledgements)),
                ControlOperationRequestV1.drainShard(new DrainShardRequestV1(reason, 500, true)),
                ControlOperationRequestV1.fenceShard(new FenceShardRequestV1(reason)),
                ControlOperationRequestV1.forceCheckpoint(new ForceCheckpointRequestV1(reason)),
                ControlOperationRequestV1.getCheckpointCatalog(),
                ControlOperationRequestV1.replayDeadLetter(new ReplayDeadLetterRequestV1(100, 200, retryPolicy,
                        true, acknowledgements)),
                ControlOperationRequestV1.resolveUncertain(new ResolveUncertainRequestV1(
                        UncertainResolutionKindV1.RETRY_ALLOW_POSSIBLE_DUPLICATE, null, true, false,
                        acknowledgements)),
                ControlOperationRequestV1.publishQuotaGrant(quota),
                profileRequests().get(0), profileRequests().get(1), profileRequests().get(2));

        for (ControlOperationRequestV1 request : requests) {
            assertEquals(request, ControlOperationRequestV1.decode(request.canonicalBytes()),
                    "branch " + request.kind());
        }
    }

    @Test
    void rejectsMismatchedBranchesAndClosedPresenceViolations() {
        final ControlReasonV1 reason = new ControlReasonV1(ControlReasonKindV1.INCIDENT, null, null);
        final LaneGateRequestV1 lane = new LaneGateRequestV1(reason);
        assertThrows(IllegalArgumentException.class, () -> new ControlOperationRequestV1(
                ControlOperationKindV1.STOP_NEW_SCHEDULES, lane));
        assertThrows(IllegalArgumentException.class, () -> new ControlOperationRequestV1(
                ControlOperationKindV1.PUBLISH_QUOTA_GRANT,
                new GetCheckpointCatalogRequestV1()));
        assertThrows(IllegalArgumentException.class, () -> new ResolveUncertainRequestV1(
                UncertainResolutionKindV1.RETRY_ALLOW_POSSIBLE_DUPLICATE, null, false, false,
                AcknowledgementSetV1.empty()));
        assertThrows(IllegalArgumentException.class, () -> new ResolveUncertainRequestV1(
                UncertainResolutionKindV1.TERMINALIZE_POSSIBLE_DELIVERY, null, true, true,
                new AcknowledgementSetV1(List.of(new AcknowledgementV1(
                        AcknowledgementKindV1.POSSIBLE_DELIVERY, bytes(32, 1), bytes(32, 2))))));

        final byte[] wrongOuterKind = CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 4,
                new StopNewSchedulesRequestV1(reason).canonicalBytes()));
        assertThrows(IllegalArgumentException.class, () -> ControlOperationRequestV1.decode(wrongOuterKind));
        final byte[] twoBranches = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new StopNewSchedulesRequestV1(reason).canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, lane.canonicalBytes());
        });
        assertThrows(IllegalArgumentException.class, () -> ControlOperationRequestV1.decode(twoBranches));
    }

    private static List<ControlOperationRequestV1> profileRequests() throws Exception {
        final ProfileSemanticEnvelopeV1 profile = destinationEnvelope();
        final ProfileRefV1 reference = profile.ref();
        final byte[] firstSecret = Bytes.utf8("provider://credential/v1");
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final CredentialEquivalenceAttestationV1 firstAttestation = attestation(reference, 1, firstSecret,
                keyPair);
        final CredentialBindingV1 binding = CredentialBindingV1.create(reference, 1, firstSecret, firstAttestation);
        final PublishDestinationProfileRequestV1 publish = new PublishDestinationProfileRequestV1(profile, binding);
        final DeprecateDestinationProfileRequestV1 deprecate = new DeprecateDestinationProfileRequestV1(reference,
                new ControlReasonV1(ControlReasonKindV1.POLICY_CHANGE, bytes(32, 20), null));
        final byte[] nextSecret = Bytes.utf8("provider://credential/v2");
        final RotateEquivalentSecretRequestV1 rotate = new RotateEquivalentSecretRequestV1(reference, 1, 2,
                nextSecret, Bytes.sha256(nextSecret), attestation(reference, 2, nextSecret, keyPair),
                binding.bindingDigest(), 4);
        return List.of(ControlOperationRequestV1.publishDestinationProfile(publish),
                ControlOperationRequestV1.deprecateDestinationProfile(deprecate),
                ControlOperationRequestV1.rotateEquivalentSecret(rotate));
    }

    private static ProfileSemanticEnvelopeV1 destinationEnvelope() {
        final ProfileRefV1 capability = new ProfileRefV1(Bytes.utf8("capability"), 1, bytes(32, 21),
                ProfileKindV1.DELIVERY_CAPABILITY);
        final DestinationProfileSemanticV1 body = new DestinationProfileSemanticV1(
                AdapterKindV1.KAFKA,
                BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("cluster", UUID.randomUUID())),
                2, TargetPartitionPolicyV1.EXPLICIT_OR_HASH, TargetPartitionHashInputV1.ORDERING_KEY,
                List.of(0), capability, 1, 0, 0, bytes(32, 22), 1_000, 128, 512, 1,
                Bytes.utf8("destination"), 0, 0, 1, bytes(32, 23));
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.DESTINATION, Bytes.utf8("destination"), 1, body);
    }

    private static CredentialEquivalenceAttestationV1 attestation(final ProfileRefV1 profile,
                                                                  final long generation,
                                                                  final byte[] secretReference,
                                                                  final KeyPair keyPair) {
        return CredentialEquivalenceAttestationV1.signed(profile, generation, Bytes.sha256(secretReference),
                bytes(32, 24), bytes(32, 25), 1, Bytes.utf8("verifier"), trustedTime(), 1_500, bytes(32, 26), 1,
                keyPair.getPrivate());
    }

    private static TrustedUtcIntervalEvidence trustedTime() {
        return new TrustedUtcIntervalEvidence(1_000, 1_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 2, 3,
                bytes(32, 27), 0, new byte[0]);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
