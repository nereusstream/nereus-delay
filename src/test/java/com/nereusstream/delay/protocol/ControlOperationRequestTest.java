package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ControlOperationRequestTest {
    @Test
    void roundTripsEveryClosedOperationBranch() throws Exception {
        final ControlReason reason = new ControlReason(ControlReasonKind.OPERATOR_REQUEST, bytes(32, 1), bytes(32, 2));
        final AcknowledgementSet acknowledgements = new AcknowledgementSet(List.of(
                new Acknowledgement(AcknowledgementKind.POSSIBLE_DUPLICATE, bytes(32, 3), bytes(32, 4)),
                new Acknowledgement(AcknowledgementKind.POSSIBLE_DELIVERY, bytes(32, 5), bytes(32, 6)),
                new Acknowledgement(AcknowledgementKind.ORDER_LOSS, bytes(32, 7), bytes(32, 8))));
        final RetryPolicyRef retryPolicy = new RetryPolicyRef(bytes(16, 9), 2, bytes(32, 10));
        final PublishQuotaGrantRequest quota = new PublishQuotaGrantRequest(
                new QuotaGrantRef(
                        bytes(32, 11),
                        1,
                        new PublishAdmissionBody.ChargeVector(
                                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)),
                new QuotaTransferPlanRef(bytes(32, 12), bytes(32, 13), 3, bytes(32, 14)));
        final List<ControlOperationRequest> requests = List.of(
                ControlOperationRequest.stopNewSchedules(reason),
                ControlOperationRequest.pauseDestinationLane(reason),
                ControlOperationRequest.resumeDestinationLane(reason),
                ControlOperationRequest.closeDestinationLane(new CloseLaneRequest(
                        reason, ClosePolicy._FREEZE_UNADMITTED_AND_PRESERVE_ADMITTED, true, acknowledgements)),
                ControlOperationRequest.breakOrdering(new BreakOrderingRequest(acknowledgements)),
                ControlOperationRequest.drainShard(new DrainShardRequest(reason, 500, true)),
                ControlOperationRequest.fenceShard(new FenceShardRequest(reason)),
                ControlOperationRequest.forceCheckpoint(new ForceCheckpointRequest(reason)),
                ControlOperationRequest.getCheckpointCatalog(),
                ControlOperationRequest.replayDeadLetter(
                        new ReplayDeadLetterRequest(100, 200, retryPolicy, true, acknowledgements)),
                ControlOperationRequest.resolveUncertain(new ResolveUncertainRequest(
                        UncertainResolutionKind.RETRY_ALLOW_POSSIBLE_DUPLICATE, null, true, false, acknowledgements)),
                ControlOperationRequest.publishQuotaGrant(quota),
                profileRequests().get(0),
                profileRequests().get(1),
                profileRequests().get(2));

        for (ControlOperationRequest request : requests) {
            assertEquals(request, ControlOperationRequest.decode(request.canonicalBytes()), "branch " + request.kind());
        }
    }

    @Test
    void rejectsMismatchedBranchesAndClosedPresenceViolations() {
        final ControlReason reason = new ControlReason(ControlReasonKind.INCIDENT, null, null);
        final LaneGateRequest lane = new LaneGateRequest(reason);
        assertThrows(
                IllegalArgumentException.class,
                () -> new ControlOperationRequest(ControlOperationKind.STOP_NEW_SCHEDULES, lane));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ControlOperationRequest(
                        ControlOperationKind.PUBLISH_QUOTA_GRANT, new GetCheckpointCatalogRequest()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ResolveUncertainRequest(
                        UncertainResolutionKind.RETRY_ALLOW_POSSIBLE_DUPLICATE,
                        null,
                        false,
                        false,
                        AcknowledgementSet.empty()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ResolveUncertainRequest(
                        UncertainResolutionKind.TERMINALIZE_POSSIBLE_DELIVERY,
                        null,
                        true,
                        true,
                        new AcknowledgementSet(List.of(new Acknowledgement(
                                AcknowledgementKind.POSSIBLE_DELIVERY, bytes(32, 1), bytes(32, 2))))));

        final byte[] wrongOuterKind = CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, 4, new StopNewSchedulesRequest(reason).canonicalBytes()));
        assertThrows(IllegalArgumentException.class, () -> ControlOperationRequest.decode(wrongOuterKind));
        final byte[] twoBranches = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new StopNewSchedulesRequest(reason).canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, lane.canonicalBytes());
        });
        assertThrows(IllegalArgumentException.class, () -> ControlOperationRequest.decode(twoBranches));
    }

    private static List<ControlOperationRequest> profileRequests() throws Exception {
        final ProfileSemanticEnvelope profile = destinationEnvelope();
        final ProfileRef reference = profile.ref();
        final byte[] firstSecret = Bytes.utf8("provider://credential/initial");
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final CredentialEquivalenceAttestation firstAttestation = attestation(reference, 1, firstSecret, keyPair);
        final CredentialBinding binding = CredentialBinding.create(reference, 1, firstSecret, firstAttestation);
        final PublishDestinationProfileRequest publish = new PublishDestinationProfileRequest(profile, binding);
        final DeprecateDestinationProfileRequest deprecate = new DeprecateDestinationProfileRequest(
                reference, new ControlReason(ControlReasonKind.POLICY_CHANGE, bytes(32, 20), null));
        final byte[] nextSecret = Bytes.utf8("provider://credential/current");
        final RotateEquivalentSecretRequest rotate = new RotateEquivalentSecretRequest(
                reference,
                1,
                2,
                nextSecret,
                Bytes.sha256(nextSecret),
                attestation(reference, 2, nextSecret, keyPair),
                binding.bindingDigest(),
                4);
        return List.of(
                ControlOperationRequest.publishDestinationProfile(publish),
                ControlOperationRequest.deprecateDestinationProfile(deprecate),
                ControlOperationRequest.rotateEquivalentSecret(rotate));
    }

    private static ProfileSemanticEnvelope destinationEnvelope() {
        final ProfileRef capability =
                new ProfileRef(Bytes.utf8("capability"), 1, bytes(32, 21), ProfileKind.DELIVERY_CAPABILITY);
        final DestinationProfileSemantic body = new DestinationProfileSemantic(
                AdapterKind.KAFKA,
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", UUID.randomUUID())),
                2,
                TargetPartitionPolicy.EXPLICIT_OR_HASH,
                TargetPartitionHashInput.ORDERING_KEY,
                List.of(0),
                capability,
                1,
                0,
                0,
                bytes(32, 22),
                1_000,
                128,
                512,
                1,
                Bytes.utf8("destination"),
                0,
                0,
                1,
                bytes(32, 23));
        return new ProfileSemanticEnvelope(ProfileKind.DESTINATION, Bytes.utf8("destination"), 1, body);
    }

    private static CredentialEquivalenceAttestation attestation(
            final ProfileRef profile, final long generation, final byte[] secretReference, final KeyPair keyPair) {
        return CredentialEquivalenceAttestation.signed(
                profile,
                generation,
                Bytes.sha256(secretReference),
                bytes(32, 24),
                bytes(32, 25),
                1,
                Bytes.utf8("verifier"),
                trustedTime(),
                1_500,
                bytes(32, 26),
                1,
                keyPair.getPrivate());
    }

    private static TrustedUtcIntervalEvidence trustedTime() {
        return new TrustedUtcIntervalEvidence(
                1_000,
                1_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("clock"),
                1,
                2,
                3,
                bytes(32, 27),
                0,
                new byte[0]);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
