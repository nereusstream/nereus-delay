package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.LargeScheduleIntent;
import io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import io.nereusstream.delay.protocol.ObjectStoreProviderKindV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PayloadAttestationOutcomeV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetSemanticV1;
import io.nereusstream.delay.protocol.PayloadProofVerifierKeyV1;
import io.nereusstream.delay.protocol.PayloadUploadHandleOutcomeV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.UploadHandleKindV1;
import io.nereusstream.delay.runtime.PayloadReservation;
import io.nereusstream.delay.runtime.PayloadReservationStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FilesystemPayloadObjectStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void payloadSurvivesAdapterRestartAndProofBytesRemainStable() throws Exception {
        final KeyPair keyPair = keyPair();
        final PayloadProofTrustSetSemanticV1 trust = trustSet(keyPair, 9_000);
        final ProfileSemanticEnvelopeV1 profile = profile();
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        final byte[] tenant = Bytes.sha256(Bytes.utf8("tenant"));

        final FilesystemPayloadObjectStore first = new FilesystemPayloadObjectStore(tempDir, profile, tenant,
                trust, 7, keyPair.getPrivate());
        first.register(reservation, trust.ref(), profile.ref());
        final var handle = first.issueUploadHandle(reservation.reservationId(), UploadHandleKindV1.OPAQUE_SINGLE_PUT,
                1_000).issued();
        first.upload(handle, Bytes.utf8("large"), 1_001);
        final var firstAttestation = first.attest(handle, 1_002);
        assertEquals(PayloadAttestationOutcomeV1.ATTESTED, firstAttestation.outcome());

        final FilesystemPayloadObjectStore reopened = new FilesystemPayloadObjectStore(tempDir, profile, tenant,
                trust, 7, keyPair.getPrivate());
        reopened.register(PayloadReservation.decode(reservation.encode()), trust.ref(), profile.ref());
        final var reopenedHandle = reopened.issueUploadHandle(reservation.reservationId(),
                UploadHandleKindV1.OPAQUE_SINGLE_PUT, 1_000).issued();
        assertEquals(handle, reopenedHandle);
        final var reopenedAttestation = reopened.attest(reopenedHandle, 1_003);
        assertEquals(PayloadAttestationOutcomeV1.ATTESTED, reopenedAttestation.outcome());
        assertArrayEquals(firstAttestation.proof().canonicalBytes(), reopenedAttestation.proof().canonicalBytes());
    }

    @Test
    void registryRegistrationRequiresExactPrepareAuthoritiesBeforeHandleOrFileState() throws Exception {
        final KeyPair adapterKey = keyPair();
        final KeyPair foreignKey = keyPair();
        final PayloadProofTrustSetSemanticV1 adapterTrust = trustSet(adapterKey, 9_000);
        final PayloadProofTrustSetSemanticV1 foreignTrust = trustSet(foreignKey, 9_000);
        final ProfileSemanticEnvelopeV1 profile = profile();
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        final FilesystemPayloadObjectStore store = new FilesystemPayloadObjectStore(tempDir, profile,
                Bytes.sha256(Bytes.utf8("tenant")), adapterTrust, 7, adapterKey.getPrivate());

        assertEquals(adapterTrust.version(), foreignTrust.version());
        assertNotEquals(adapterTrust.ref(), foreignTrust.ref());
        assertThrows(IllegalArgumentException.class,
                () -> store.register(reservation, foreignTrust.ref(), profile.ref()));
        final ProfileRefV1 foreignProfile = new ProfileRefV1(Bytes.utf8("foreign-object-store"), 1,
                profile.ref().semanticHash(), ProfileKindV1.OBJECT_STORE);
        assertThrows(IllegalArgumentException.class,
                () -> store.register(reservation, adapterTrust.ref(), foreignProfile));
        assertEquals(PayloadUploadHandleOutcomeV1.NOT_FOUND_OR_NOT_AUTHORIZED,
                store.issueUploadHandle(reservation.reservationId(), UploadHandleKindV1.OPAQUE_SINGLE_PUT,
                        1_000).outcome());
        try (var files = Files.walk(tempDir.resolve("objects"))) {
            assertEquals(0, files.filter(Files::isRegularFile).count());
        }

        store.register(reservation, adapterTrust.ref(), profile.ref());
        assertEquals(adapterTrust.ref(), store.reservationReceipt(reservation).trustSet());
    }

    @Test
    void immutableConflictAndCorruptionFailClosed() throws Exception {
        final KeyPair keyPair = keyPair();
        final ProfileSemanticEnvelopeV1 profile = profile();
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        final FilesystemPayloadObjectStore store = new FilesystemPayloadObjectStore(tempDir, profile,
                Bytes.sha256(Bytes.utf8("tenant")), trustSet(keyPair, 9_000), 7, keyPair.getPrivate());
        store.register(reservation);
        final var handle = store.issueUploadHandle(reservation.reservationId(), UploadHandleKindV1.OPAQUE_SINGLE_PUT,
                1_000).issued();
        store.upload(handle, Bytes.utf8("large"), 1_001);
        assertThrows(IllegalStateException.class, () -> store.upload(handle, Bytes.utf8("other"), 1_002));

        final String identity = Bytes.hex(reservation.reservationId());
        final Path object = tempDir.resolve("objects").resolve(identity.substring(0, 2))
                .resolve(identity.substring(2, 4)).resolve(identity + ".payload");
        Files.write(object, Bytes.utf8("corrupt"));
        assertEquals(PayloadAttestationOutcomeV1.OBJECT_IDENTITY_CONFLICT,
                store.attest(handle, 1_003).outcome());
    }

    @Test
    void rejectsSymlinkedObjectRoot() throws Exception {
        final Path outside = tempDir.resolve("outside");
        Files.createDirectories(outside);
        final Path root = tempDir.resolve("payload-link");
        try {
            Files.createSymbolicLink(root, outside);
        } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
            return;
        }
        final KeyPair keyPair = keyPair();
        assertThrows(IllegalArgumentException.class, () -> new FilesystemPayloadObjectStore(root, profile(),
                Bytes.sha256(Bytes.utf8("tenant")), trustSet(keyPair, 9_000), 7, keyPair.getPrivate()));
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
