package com.nereusstream.delay.adapter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.LargeScheduleIntent;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemantic;
import com.nereusstream.delay.protocol.ObjectStoreProviderKind;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandle;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PayloadAttestationOutcome;
import com.nereusstream.delay.protocol.PayloadAttestationResponse;
import com.nereusstream.delay.protocol.PayloadProofTrustSet;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import com.nereusstream.delay.protocol.PayloadProofVerifierKey;
import com.nereusstream.delay.protocol.PayloadReference;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PayloadUploadHandleOutcome;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponse;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.UploadHandleKind;
import com.nereusstream.delay.runtime.PayloadReservation;
import com.nereusstream.delay.runtime.PayloadReservationStatus;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class InMemoryPayloadObjectStoreTest {
    @Test
    void issuesIdempotentHandleAndAttestsImmutablePayload() throws Exception {
        final KeyPair keyPair = keyPair();
        final PayloadProofTrustSetSemantic trust = trustSet(keyPair, 9_000);
        final ProfileSemanticEnvelope profile = profile();
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        final InMemoryPayloadObjectStore store = new InMemoryPayloadObjectStore(
                profile, Bytes.sha256(Bytes.utf8("tenant")), trust, 7, keyPair.getPrivate());
        store.register(reservation);

        final var first =
                store.issueUploadHandle(reservation.reservationId(), UploadHandleKind.OPAQUE_SINGLE_PUT, 1_000);
        assertEquals(PayloadUploadHandleOutcome.ISSUED, first.outcome());
        final var second =
                store.issueUploadHandle(reservation.reservationId(), UploadHandleKind.OPAQUE_SINGLE_PUT, 1_001);
        assertEquals(first, second);
        assertEquals(first, PayloadUploadHandleResponse.decode(first.canonicalBytes()));

        final var handle = first.issued();
        final var notReady = store.attest(handle, 1_002);
        assertEquals(PayloadAttestationOutcome.OBJECT_NOT_READY_RETRYABLE, notReady.outcome());
        assertEquals(2_002L, notReady.error().retryAtEpochMs());

        store.upload(handle, Bytes.utf8("large"), 1_004);
        store.upload(handle, Bytes.utf8("large"), 1_005);
        final var attested = store.attest(handle, 1_006);
        assertEquals(PayloadAttestationOutcome.ATTESTED, attested.outcome());
        final CanonicalPayloadCommitProof proof = attested.proof();
        assertTrue(PayloadProofTrustSet.fromSemantic(trust).verifies(proof, 1_006));
        assertEquals(attested, store.attest(handle, 1_007));
        assertEquals(attested, PayloadAttestationResponse.decode(attested.canonicalBytes()));
        assertArrayEquals(
                proof.canonicalBytes(), store.attest(handle, 1_008).proof().canonicalBytes());
    }

    @Test
    void negativeObservationTimeReturnsTypedIntegrityOutcome() throws Exception {
        final KeyPair keyPair = keyPair();
        final InMemoryPayloadObjectStore store = new InMemoryPayloadObjectStore(
                profile(), Bytes.sha256(Bytes.utf8("tenant")), trustSet(keyPair, 9_000), 7, keyPair.getPrivate());
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        store.register(reservation);
        final var handle = store.issueUploadHandle(
                        reservation.reservationId(), UploadHandleKind.OPAQUE_SINGLE_PUT, 1_000)
                .issued();

        assertEquals(
                PayloadUploadHandleOutcome.INTEGRITY_ERROR,
                store.issueUploadHandle(reservation.reservationId(), UploadHandleKind.OPAQUE_SINGLE_PUT, -1)
                        .outcome());
        assertEquals(
                PayloadAttestationOutcome.INTEGRITY_ERROR,
                store.attest(handle, -1).outcome());
    }

    @Test
    void rejectsPayloadDriftAndUnauthorizedOrExpiredHandles() throws Exception {
        final KeyPair keyPair = keyPair();
        final InMemoryPayloadObjectStore store = new InMemoryPayloadObjectStore(
                profile(), Bytes.sha256(Bytes.utf8("tenant")), trustSet(keyPair, 9_000), 7, keyPair.getPrivate());
        final PayloadReservation reservation = reservation(2_000, Bytes.utf8("large"));
        store.register(reservation);

        assertEquals(
                PayloadUploadHandleOutcome.NOT_FOUND_OR_NOT_AUTHORIZED,
                store.issueUploadHandle(Bytes.sha256(Bytes.utf8("missing")), UploadHandleKind.OPAQUE_SINGLE_PUT, 1_000)
                        .outcome());
        assertEquals(
                PayloadUploadHandleOutcome.INTEGRITY_ERROR,
                store.issueUploadHandle(reservation.reservationId(), UploadHandleKind.OPAQUE_MULTIPART, 1_000)
                        .outcome());

        final var handle = store.issueUploadHandle(
                        reservation.reservationId(), UploadHandleKind.OPAQUE_SINGLE_PUT, 1_000)
                .issued();
        assertThrows(IllegalArgumentException.class, () -> store.upload(handle, Bytes.utf8("wrong"), 1_001));
        store.upload(handle, Bytes.utf8("large"), 1_001);
        assertThrows(IllegalStateException.class, () -> store.upload(handle, Bytes.utf8("other"), 1_002));

        final var expired =
                store.issueUploadHandle(reservation.reservationId(), UploadHandleKind.OPAQUE_SINGLE_PUT, 2_001);
        assertEquals(PayloadUploadHandleOutcome.RESERVATION_EXPIRED, expired.outcome());
        assertEquals(
                PayloadAttestationOutcome.NOT_FOUND_OR_NOT_AUTHORIZED,
                store.attest(
                                OpaquePayloadUploadHandle.create(
                                        Bytes.sha256(Bytes.utf8("other-reservation")),
                                        profile().ref(),
                                        UploadHandleKind.OPAQUE_SINGLE_PUT,
                                        2_000,
                                        Bytes.utf8("foreign")),
                                1_000)
                        .outcome());
        assertFalse(store.attest(handle, 2_001).outcome() == PayloadAttestationOutcome.ATTESTED);
    }

    @Test
    void registrationIsExactAndDoesNotAcceptTrustSetDrift() throws Exception {
        final KeyPair keyPair = keyPair();
        final PayloadProofTrustSetSemantic trust = trustSet(keyPair, 9_000);
        final InMemoryPayloadObjectStore store = new InMemoryPayloadObjectStore(
                profile(), Bytes.sha256(Bytes.utf8("tenant")), trust, 7, keyPair.getPrivate());
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        store.register(reservation);
        store.register(PayloadReservation.decode(reservation.encode()));

        final PayloadReservation drifted = new PayloadReservation(
                reservation.shardId(),
                reservation.reservationId(),
                reservation.commandId(),
                reservation.delayMessageId(),
                reservation.commandHash(),
                reservation.intent(),
                reservation.reservationExpiryEpochMs(),
                PayloadReservationStatus.RESERVED,
                reservation.stateVersion() + 1,
                reservation.sourcePosition(),
                null);
        assertThrows(IllegalStateException.class, () -> store.register(drifted));
    }

    @Test
    void registryRegistrationRequiresTheExactPinnedTrustSetSemantic() throws Exception {
        final KeyPair adapterKey = keyPair();
        final KeyPair foreignKey = keyPair();
        final PayloadProofTrustSetSemantic adapterTrust = trustSet(adapterKey, 9_000);
        final PayloadProofTrustSetSemantic foreignTrust = trustSet(foreignKey, 9_000);
        final InMemoryPayloadObjectStore store = new InMemoryPayloadObjectStore(
                profile(), Bytes.sha256(Bytes.utf8("tenant")), adapterTrust, 7, adapterKey.getPrivate());
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));

        assertEquals(adapterTrust.version(), foreignTrust.version());
        assertNotEquals(adapterTrust.ref(), foreignTrust.ref());
        assertThrows(
                IllegalArgumentException.class,
                () -> store.register(reservation, foreignTrust.ref(), profile().ref()));
        final ProfileRef foreignProfile = new ProfileRef(
                Bytes.utf8("foreign-object-store"), 1, digest("foreign-object-store"), ProfileKind.OBJECT_STORE);
        assertThrows(
                IllegalArgumentException.class, () -> store.register(reservation, adapterTrust.ref(), foreignProfile));
        assertEquals(
                PayloadUploadHandleOutcome.NOT_FOUND_OR_NOT_AUTHORIZED,
                store.issueUploadHandle(reservation.reservationId(), UploadHandleKind.OPAQUE_SINGLE_PUT, 1_000)
                        .outcome());

        store.register(reservation, adapterTrust.ref(), profile().ref());
        assertEquals(adapterTrust.ref(), store.reservationReceipt(reservation).trustSet());
    }

    @Test
    void receiptAnchorSurvivesSourceOrderedReservationLifecycleTransitions() throws Exception {
        final KeyPair keyPair = keyPair();
        final PayloadProofTrustSetSemantic trust = trustSet(keyPair, 9_000);
        final InMemoryPayloadObjectStore store = new InMemoryPayloadObjectStore(
                profile(), Bytes.sha256(Bytes.utf8("tenant")), trust, 7, keyPair.getPrivate());
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        store.register(reservation);
        final PayloadReservationReceipt receipt = store.reservationReceipt(reservation);

        final PayloadReservation logicallyExpired = new PayloadReservation(
                reservation.shardId(),
                reservation.reservationId(),
                reservation.commandId(),
                reservation.delayMessageId(),
                reservation.commandHash(),
                reservation.intent(),
                reservation.reservationExpiryEpochMs(),
                PayloadReservationStatus.EXPIRED,
                reservation.stateVersion(),
                reservation.sourcePosition(),
                null);
        store.register(logicallyExpired);
        assertEquals(
                PayloadUploadHandleOutcome.RESERVATION_EXPIRED,
                store.issueUploadHandle(receipt, UploadHandleKind.OPAQUE_SINGLE_PUT, 1_000)
                        .outcome());

        final PayloadReservation cancellationReservation = reservation(5_000, Bytes.utf8("large"));
        final InMemoryPayloadObjectStore cancellationStore = new InMemoryPayloadObjectStore(
                profile(), Bytes.sha256(Bytes.utf8("tenant")), trust, 7, keyPair.getPrivate());
        cancellationStore.register(cancellationReservation);
        final PayloadReservationReceipt cancellationReceipt =
                cancellationStore.reservationReceipt(cancellationReservation);
        final byte[] cancellationPosition = new KafkaSourcePosition(
                        cancellationReservation.shardId(),
                        "embedded",
                        UUID.nameUUIDFromBytes(Bytes.utf8("payload-cancel-source")),
                        2,
                        null,
                        1_100)
                .canonicalBytes();
        final PayloadReservation abandoned = new PayloadReservation(
                cancellationReservation.shardId(),
                cancellationReservation.reservationId(),
                cancellationReservation.commandId(),
                cancellationReservation.delayMessageId(),
                cancellationReservation.commandHash(),
                cancellationReservation.intent(),
                cancellationReservation.reservationExpiryEpochMs(),
                PayloadReservationStatus.ABANDONED,
                cancellationReservation.stateVersion() + 1,
                cancellationPosition,
                null);
        cancellationStore.register(abandoned);
        assertEquals(
                PayloadUploadHandleOutcome.RESERVATION_ABANDONED,
                cancellationStore
                        .issueUploadHandle(cancellationReceipt, UploadHandleKind.OPAQUE_SINGLE_PUT, 1_100)
                        .outcome());
    }

    @Test
    void committedTransitionClosesReceiptAndFencesForeignObjectIdentity() throws Exception {
        final KeyPair keyPair = keyPair();
        final PayloadProofTrustSetSemantic trust = trustSet(keyPair, 9_000);
        final ProfileSemanticEnvelope profile = profile();
        final InMemoryPayloadObjectStore store = new InMemoryPayloadObjectStore(
                profile, Bytes.sha256(Bytes.utf8("tenant")), trust, 7, keyPair.getPrivate());
        final byte[] payload = Bytes.utf8("large");
        final PayloadReservation reservation = reservation(5_000, payload);
        store.register(reservation);
        final PayloadReservationReceipt receipt = store.reservationReceipt(reservation);
        final byte[] container =
                Bytes.concat(Bytes.utf8("nereus-delay-local/"), Bytes.utf8(Bytes.hex(profile.profileId())));
        final byte[] objectKey =
                Bytes.concat(Bytes.utf8("reservation/"), Bytes.utf8(Bytes.hex(reservation.reservationId())));
        final PayloadReference committedPayload = new PayloadReference(
                profile.semanticHash(),
                container,
                objectKey,
                Bytes.concat(Bytes.utf8("sha256-"), Bytes.utf8(Bytes.hex(Bytes.sha256(payload)))),
                Bytes.sha256(payload),
                payload.length,
                Bytes.sha256(payload));
        final byte[] commitPosition = new KafkaSourcePosition(
                        reservation.shardId(),
                        "embedded",
                        UUID.nameUUIDFromBytes(Bytes.utf8("payload-commit-source")),
                        2,
                        null,
                        1_100)
                .canonicalBytes();
        final PayloadReservation committed = reservation.withLifecycle(
                PayloadReservationStatus.COMMITTED, reservation.stateVersion() + 1, commitPosition, committedPayload);
        store.register(committed);
        assertEquals(
                PayloadUploadHandleOutcome.RESERVATION_CLOSED,
                store.issueUploadHandle(receipt, UploadHandleKind.OPAQUE_SINGLE_PUT, 1_100)
                        .outcome());

        final InMemoryPayloadObjectStore reopened = new InMemoryPayloadObjectStore(
                profile, Bytes.sha256(Bytes.utf8("tenant")), trust, 7, keyPair.getPrivate());
        final PayloadReservation reopenedReservation = PayloadReservation.decode(committed.encode());
        reopened.register(reopenedReservation);
        assertEquals(receipt, reopened.reservationReceipt(reopenedReservation));
        assertEquals(
                PayloadUploadHandleOutcome.RESERVATION_CLOSED,
                reopened.issueUploadHandle(receipt, UploadHandleKind.OPAQUE_SINGLE_PUT, 1_100)
                        .outcome());

        final PayloadReference foreignPayload = new PayloadReference(
                profile.semanticHash(),
                Bytes.utf8("foreign"),
                objectKey,
                committedPayload.immutableObjectVersion(),
                committedPayload.etag(),
                payload.length,
                Bytes.sha256(payload));
        final PayloadReservation foreignCommit = new PayloadReservation(
                reservation.shardId(),
                reservation.reservationId(),
                reservation.commandId(),
                reservation.delayMessageId(),
                reservation.commandHash(),
                reservation.intent(),
                reservation.reservationExpiryEpochMs(),
                PayloadReservationStatus.COMMITTED,
                reservation.stateVersion() + 1,
                commitPosition,
                foreignPayload);
        final InMemoryPayloadObjectStore foreignStore = new InMemoryPayloadObjectStore(
                profile, Bytes.sha256(Bytes.utf8("tenant")), trust, 7, keyPair.getPrivate());
        foreignStore.register(reservation);
        assertThrows(IllegalStateException.class, () -> foreignStore.register(foreignCommit));
    }

    @Test
    void boundsHandleLifetimeAndResignsAfterCapabilityExpiry() throws Exception {
        final KeyPair keyPair = keyPair();
        final PayloadProofTrustSetSemantic trust = trustSet(keyPair, 9_000);
        final InMemoryPayloadObjectStore store = new InMemoryPayloadObjectStore(
                profile(), Bytes.sha256(Bytes.utf8("tenant")), trust, 7, 500, keyPair.getPrivate());
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        store.register(reservation);

        final var first = store.issueUploadHandle(
                        reservation.reservationId(), UploadHandleKind.OPAQUE_SINGLE_PUT, 1_000)
                .issued();
        assertEquals(1_500L, first.expiresAtEpochMs());
        assertEquals(
                first,
                store.issueUploadHandle(reservation.reservationId(), UploadHandleKind.OPAQUE_SINGLE_PUT, 1_200)
                        .issued());
        assertEquals(
                PayloadAttestationOutcome.NOT_FOUND_OR_NOT_AUTHORIZED,
                store.attest(first, 1_501).outcome());

        final var second = store.issueUploadHandle(
                        reservation.reservationId(), UploadHandleKind.OPAQUE_SINGLE_PUT, 1_501)
                .issued();
        assertEquals(2_001L, second.expiresAtEpochMs());
        assertNotEquals(first, second);
        assertThrows(IllegalArgumentException.class, () -> store.upload(first, Bytes.utf8("large"), 1_501));
        store.upload(second, Bytes.utf8("large"), 1_502);
        assertEquals(
                PayloadAttestationOutcome.ATTESTED, store.attest(second, 1_503).outcome());
    }

    @Test
    void receiptBindsObjectIdentityAndSourceReservationState() throws Exception {
        final KeyPair keyPair = keyPair();
        final PayloadProofTrustSetSemantic trust = trustSet(keyPair, 9_000);
        final InMemoryPayloadObjectStore store = new InMemoryPayloadObjectStore(
                profile(), Bytes.sha256(Bytes.utf8("tenant")), trust, 7, 500, keyPair.getPrivate());
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        store.register(reservation);

        final var receipt = store.reservationReceipt(reservation);
        assertEquals(receipt, PayloadReservationReceipt.decodeFrame(receipt.frame()));
        final var handle = store.issueUploadHandle(receipt, UploadHandleKind.OPAQUE_SINGLE_PUT, 1_000)
                .issued();
        store.upload(receipt, handle, Bytes.utf8("large"), 1_001);
        assertEquals(
                PayloadAttestationOutcome.ATTESTED,
                store.attest(receipt, handle, 1_002).outcome());

        final var drifted = PayloadReservationReceipt.create(
                receipt.reservationId(),
                receipt.delayMessageId(),
                receipt.shardId(),
                receipt.appliedSourcePosition(),
                receipt.stateVersion(),
                receipt.objectStoreProfile(),
                receipt.container(),
                receipt.objectKey(),
                receipt.expectedLength() + 1,
                receipt.payloadSha256(),
                receipt.reservationExpiryEpochMs(),
                receipt.trustSet());
        assertEquals(
                PayloadUploadHandleOutcome.NOT_FOUND_OR_NOT_AUTHORIZED,
                store.issueUploadHandle(drifted, UploadHandleKind.OPAQUE_SINGLE_PUT, 1_003)
                        .outcome());
        assertThrows(IllegalArgumentException.class, () -> store.upload(drifted, handle, Bytes.utf8("large"), 1_003));
        assertEquals(
                PayloadAttestationOutcome.NOT_FOUND_OR_NOT_AUTHORIZED,
                store.attest(drifted, handle, 1_003).outcome());
    }

    private static ProfileSemanticEnvelope profile() {
        final ObjectStoreProfileSemantic body = new ObjectStoreProfileSemantic(
                ObjectStoreProviderKind.S3,
                digest("endpoint"),
                digest("credential-scope"),
                1,
                true,
                true,
                true,
                true,
                digest("encryption"),
                1_024,
                ObjectStoreProfileSemantic.SINGLE_PUT,
                1,
                digest("lifecycle"));
        return new ProfileSemanticEnvelope(ProfileKind.OBJECT_STORE, Bytes.utf8("object-store"), 1, body);
    }

    private static PayloadProofTrustSetSemantic trustSet(final KeyPair keyPair, final long notAfter) {
        return new PayloadProofTrustSetSemantic(
                9, List.of(PayloadProofVerifierKey.fromPublicKey(7, keyPair.getPublic(), 0, notAfter)));
    }

    private static PayloadReservation reservation(final long expiry, final byte[] payload) {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final CommandId commandId = CommandId.random(shard);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final LargeScheduleIntent intent = new LargeScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("lane")),
                2_000,
                4_000,
                OrderingMode.BEST_EFFORT,
                payload.length,
                Bytes.sha256(payload),
                1_000,
                9);
        final byte[] sourcePosition = new KafkaSourcePosition(
                        shard, "embedded", UUID.nameUUIDFromBytes(Bytes.utf8("payload-source")), 1, null, 1_000)
                .canonicalBytes();
        return new PayloadReservation(
                shard,
                Bytes.sha256(Bytes.utf8("reservation"), commandId.bytes()),
                commandId,
                messageId,
                Bytes.sha256(Bytes.utf8("command")),
                intent,
                expiry,
                PayloadReservationStatus.RESERVED,
                1,
                sourcePosition,
                null);
    }

    private static KeyPair keyPair() throws Exception {
        return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    }

    private static byte[] digest(final String value) {
        return Bytes.sha256(Bytes.utf8(value));
    }
}
