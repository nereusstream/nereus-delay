package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.LargeScheduleIntent;
import io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import io.nereusstream.delay.protocol.ObjectStoreProviderKindV1;
import io.nereusstream.delay.protocol.OpaquePayloadUploadHandleV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PayloadAttestationOutcomeV1;
import io.nereusstream.delay.protocol.PayloadAttestationResponseV1;
import io.nereusstream.delay.protocol.PayloadCommitProofV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSet;
import io.nereusstream.delay.protocol.PayloadProofTrustSetSemanticV1;
import io.nereusstream.delay.protocol.PayloadProofVerifierKeyV1;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.PayloadUploadHandleOutcomeV1;
import io.nereusstream.delay.protocol.PayloadUploadHandleResponseV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.UploadHandleKindV1;
import io.nereusstream.delay.runtime.PayloadReservation;
import io.nereusstream.delay.runtime.PayloadReservationStatus;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryPayloadObjectStoreTest {
    @Test
    void issuesIdempotentHandleAndAttestsImmutablePayload() throws Exception {
        final KeyPair keyPair = keyPair();
        final PayloadProofTrustSetSemanticV1 trust = trustSet(keyPair, 9_000);
        final ProfileSemanticEnvelopeV1 profile = profile();
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        final InMemoryPayloadObjectStore store = new InMemoryPayloadObjectStore(profile,
                Bytes.sha256(Bytes.utf8("tenant")), trust, 7, keyPair.getPrivate());
        store.register(reservation);

        final var first = store.issueUploadHandle(reservation.reservationId(), UploadHandleKindV1.OPAQUE_SINGLE_PUT,
                1_000);
        assertEquals(PayloadUploadHandleOutcomeV1.ISSUED, first.outcome());
        final var second = store.issueUploadHandle(reservation.reservationId(), UploadHandleKindV1.OPAQUE_SINGLE_PUT,
                1_001);
        assertEquals(first, second);
        assertEquals(first, PayloadUploadHandleResponseV1.decode(first.canonicalBytes()));

        final var handle = first.issued();
        final var notReady = store.attest(handle, 1_002);
        assertEquals(PayloadAttestationOutcomeV1.OBJECT_NOT_READY_RETRYABLE, notReady.outcome());
        assertEquals(2_002L, notReady.error().retryAtEpochMs());

        store.upload(handle, Bytes.utf8("large"), 1_004);
        store.upload(handle, Bytes.utf8("large"), 1_005);
        final var attested = store.attest(handle, 1_006);
        assertEquals(PayloadAttestationOutcomeV1.ATTESTED, attested.outcome());
        final PayloadCommitProofV1 proof = attested.proof();
        assertTrue(PayloadProofTrustSet.fromSemantic(trust).verifies(proof, 1_006));
        assertEquals(attested, store.attest(handle, 1_007));
        assertEquals(attested, PayloadAttestationResponseV1.decode(attested.canonicalBytes()));
        assertArrayEquals(proof.canonicalBytes(), store.attest(handle, 1_008).proof().canonicalBytes());
    }

    @Test
    void negativeObservationTimeReturnsTypedIntegrityOutcome() throws Exception {
        final KeyPair keyPair = keyPair();
        final InMemoryPayloadObjectStore store = new InMemoryPayloadObjectStore(profile(),
                Bytes.sha256(Bytes.utf8("tenant")), trustSet(keyPair, 9_000), 7, keyPair.getPrivate());
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        store.register(reservation);
        final var handle = store.issueUploadHandle(reservation.reservationId(),
                UploadHandleKindV1.OPAQUE_SINGLE_PUT, 1_000).issued();

        assertEquals(PayloadUploadHandleOutcomeV1.INTEGRITY_ERROR,
                store.issueUploadHandle(reservation.reservationId(), UploadHandleKindV1.OPAQUE_SINGLE_PUT, -1)
                        .outcome());
        assertEquals(PayloadAttestationOutcomeV1.INTEGRITY_ERROR,
                store.attest(handle, -1).outcome());
    }

    @Test
    void rejectsPayloadDriftAndUnauthorizedOrExpiredHandles() throws Exception {
        final KeyPair keyPair = keyPair();
        final InMemoryPayloadObjectStore store = new InMemoryPayloadObjectStore(profile(),
                Bytes.sha256(Bytes.utf8("tenant")), trustSet(keyPair, 9_000), 7, keyPair.getPrivate());
        final PayloadReservation reservation = reservation(2_000, Bytes.utf8("large"));
        store.register(reservation);

        assertEquals(PayloadUploadHandleOutcomeV1.NOT_FOUND_OR_NOT_AUTHORIZED,
                store.issueUploadHandle(Bytes.sha256(Bytes.utf8("missing")), UploadHandleKindV1.OPAQUE_SINGLE_PUT,
                        1_000).outcome());
        assertEquals(PayloadUploadHandleOutcomeV1.INTEGRITY_ERROR,
                store.issueUploadHandle(reservation.reservationId(), UploadHandleKindV1.OPAQUE_MULTIPART,
                        1_000).outcome());

        final var handle = store.issueUploadHandle(reservation.reservationId(),
                UploadHandleKindV1.OPAQUE_SINGLE_PUT, 1_000).issued();
        assertThrows(IllegalArgumentException.class, () -> store.upload(handle, Bytes.utf8("wrong"), 1_001));
        store.upload(handle, Bytes.utf8("large"), 1_001);
        assertThrows(IllegalStateException.class, () -> store.upload(handle, Bytes.utf8("other"), 1_002));

        final var expired = store.issueUploadHandle(reservation.reservationId(),
                UploadHandleKindV1.OPAQUE_SINGLE_PUT, 2_001);
        assertEquals(PayloadUploadHandleOutcomeV1.RESERVATION_EXPIRED, expired.outcome());
        assertEquals(PayloadAttestationOutcomeV1.NOT_FOUND_OR_NOT_AUTHORIZED,
                store.attest(OpaquePayloadUploadHandleV1.create(Bytes.sha256(Bytes.utf8("other-reservation")),
                        profile().ref(), UploadHandleKindV1.OPAQUE_SINGLE_PUT, 2_000, Bytes.utf8("foreign")),
                        1_000).outcome());
        assertFalse(store.attest(handle, 2_001).outcome() == PayloadAttestationOutcomeV1.ATTESTED);
    }

    @Test
    void registrationIsExactAndDoesNotAcceptTrustSetDrift() throws Exception {
        final KeyPair keyPair = keyPair();
        final PayloadProofTrustSetSemanticV1 trust = trustSet(keyPair, 9_000);
        final InMemoryPayloadObjectStore store = new InMemoryPayloadObjectStore(profile(),
                Bytes.sha256(Bytes.utf8("tenant")), trust, 7, keyPair.getPrivate());
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        store.register(reservation);
        store.register(PayloadReservation.decode(reservation.encode()));

        final PayloadReservation drifted = new PayloadReservation(reservation.shardId(), reservation.reservationId(),
                reservation.commandId(), reservation.delayMessageId(), reservation.commandHash(), reservation.intent(),
                reservation.reservationExpiryEpochMs(), PayloadReservationStatus.RESERVED,
                reservation.stateVersion() + 1, reservation.sourcePosition(), null);
        assertThrows(IllegalStateException.class, () -> store.register(drifted));
    }

    @Test
    void boundsHandleLifetimeAndResignsAfterCapabilityExpiry() throws Exception {
        final KeyPair keyPair = keyPair();
        final PayloadProofTrustSetSemanticV1 trust = trustSet(keyPair, 9_000);
        final InMemoryPayloadObjectStore store = new InMemoryPayloadObjectStore(profile(),
                Bytes.sha256(Bytes.utf8("tenant")), trust, 7, 500, keyPair.getPrivate());
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        store.register(reservation);

        final var first = store.issueUploadHandle(reservation.reservationId(),
                UploadHandleKindV1.OPAQUE_SINGLE_PUT, 1_000).issued();
        assertEquals(1_500L, first.expiresAtEpochMs());
        assertEquals(first, store.issueUploadHandle(reservation.reservationId(),
                UploadHandleKindV1.OPAQUE_SINGLE_PUT, 1_200).issued());
        assertEquals(PayloadAttestationOutcomeV1.NOT_FOUND_OR_NOT_AUTHORIZED,
                store.attest(first, 1_501).outcome());

        final var second = store.issueUploadHandle(reservation.reservationId(),
                UploadHandleKindV1.OPAQUE_SINGLE_PUT, 1_501).issued();
        assertEquals(2_001L, second.expiresAtEpochMs());
        assertNotEquals(first, second);
        assertThrows(IllegalArgumentException.class, () -> store.upload(first, Bytes.utf8("large"), 1_501));
        store.upload(second, Bytes.utf8("large"), 1_502);
        assertEquals(PayloadAttestationOutcomeV1.ATTESTED, store.attest(second, 1_503).outcome());
    }

    @Test
    void receiptBindsObjectIdentityAndSourceReservationState() throws Exception {
        final KeyPair keyPair = keyPair();
        final PayloadProofTrustSetSemanticV1 trust = trustSet(keyPair, 9_000);
        final InMemoryPayloadObjectStore store = new InMemoryPayloadObjectStore(profile(),
                Bytes.sha256(Bytes.utf8("tenant")), trust, 7, 500, keyPair.getPrivate());
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        store.register(reservation);

        final var receipt = store.reservationReceipt(reservation);
        assertEquals(receipt, PayloadReservationReceiptV1.decodeFrame(receipt.frame()));
        final var handle = store.issueUploadHandle(receipt, UploadHandleKindV1.OPAQUE_SINGLE_PUT,
                1_000).issued();
        store.upload(receipt, handle, Bytes.utf8("large"), 1_001);
        assertEquals(PayloadAttestationOutcomeV1.ATTESTED,
                store.attest(receipt, handle, 1_002).outcome());

        final var drifted = PayloadReservationReceiptV1.create(receipt.reservationId(), receipt.delayMessageId(),
                receipt.shardId(), receipt.appliedSourcePosition(), receipt.stateVersion(), receipt.objectStoreProfile(),
                receipt.container(), receipt.objectKey(), receipt.expectedLength() + 1, receipt.payloadSha256(),
                receipt.reservationExpiryEpochMs(), receipt.trustSet());
        assertEquals(PayloadUploadHandleOutcomeV1.NOT_FOUND_OR_NOT_AUTHORIZED,
                store.issueUploadHandle(drifted, UploadHandleKindV1.OPAQUE_SINGLE_PUT, 1_003).outcome());
        assertThrows(IllegalArgumentException.class,
                () -> store.upload(drifted, handle, Bytes.utf8("large"), 1_003));
        assertEquals(PayloadAttestationOutcomeV1.NOT_FOUND_OR_NOT_AUTHORIZED,
                store.attest(drifted, handle, 1_003).outcome());
    }

    private static ProfileSemanticEnvelopeV1 profile() {
        final ObjectStoreProfileSemanticV1 body = new ObjectStoreProfileSemanticV1(ObjectStoreProviderKindV1.S3,
                digest("endpoint"), digest("credential-scope"), 1, true, true, true, true,
                digest("encryption"), 1_024, ObjectStoreProfileSemanticV1.SINGLE_PUT, 1, digest("lifecycle"));
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.OBJECT_STORE, Bytes.utf8("object-store"), 1, body);
    }

    private static PayloadProofTrustSetSemanticV1 trustSet(final KeyPair keyPair, final long notAfter) {
        return new PayloadProofTrustSetSemanticV1(9,
                List.of(PayloadProofVerifierKeyV1.fromPublicKey(7, keyPair.getPublic(), 0, notAfter)));
    }

    private static PayloadReservation reservation(final long expiry, final byte[] payload) {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final CommandId commandId = CommandId.random(shard);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final LargeScheduleIntent intent = new LargeScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("lane")), 2_000, 4_000, OrderingMode.BEST_EFFORT,
                payload.length, Bytes.sha256(payload), 1_000, 9);
        final byte[] sourcePosition = new KafkaSourcePosition(shard, "embedded", UUID.nameUUIDFromBytes(
                Bytes.utf8("payload-source")), 1, null, 1_000).canonicalBytes();
        return new PayloadReservation(shard, Bytes.sha256(Bytes.utf8("reservation"), commandId.bytes()), commandId,
                messageId, Bytes.sha256(Bytes.utf8("command")), intent, expiry, PayloadReservationStatus.RESERVED,
                1, sourcePosition, null);
    }

    private static KeyPair keyPair() throws Exception {
        return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    }

    private static byte[] digest(final String value) {
        return Bytes.sha256(Bytes.utf8(value));
    }
}
