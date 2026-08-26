package com.nereusstream.delay.adapter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.LargeScheduleIntent;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemantic;
import com.nereusstream.delay.protocol.ObjectStoreProviderKind;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PayloadAttestationOutcome;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import com.nereusstream.delay.protocol.PayloadProofVerifierKey;
import com.nereusstream.delay.protocol.PayloadUploadHandleOutcome;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.UploadHandleKind;
import com.nereusstream.delay.runtime.PayloadReservation;
import com.nereusstream.delay.runtime.PayloadReservationStatus;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FilesystemPayloadObjectStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void payloadSurvivesAdapterRestartAndProofBytesRemainStable() throws Exception {
        final KeyPair keyPair = keyPair();
        final PayloadProofTrustSetSemantic trust = trustSet(keyPair, 9_000);
        final ProfileSemanticEnvelope profile = profile();
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        final byte[] tenant = Bytes.sha256(Bytes.utf8("tenant"));

        final FilesystemPayloadObjectStore first =
                new FilesystemPayloadObjectStore(tempDir, profile, tenant, trust, 7, keyPair.getPrivate());
        first.register(reservation, trust.ref(), profile.ref());
        final var handle = first.issueUploadHandle(
                        reservation.reservationId(), UploadHandleKind.OPAQUE_SINGLE_PUT, 1_000)
                .issued();
        first.upload(handle, Bytes.utf8("large"), 1_001);
        final var firstAttestation = first.attest(handle, 1_002);
        assertEquals(PayloadAttestationOutcome.ATTESTED, firstAttestation.outcome());

        final FilesystemPayloadObjectStore reopened =
                new FilesystemPayloadObjectStore(tempDir, profile, tenant, trust, 7, keyPair.getPrivate());
        reopened.register(PayloadReservation.decode(reservation.encode()), trust.ref(), profile.ref());
        final var reopenedHandle = reopened.issueUploadHandle(
                        reservation.reservationId(), UploadHandleKind.OPAQUE_SINGLE_PUT, 1_000)
                .issued();
        assertEquals(handle, reopenedHandle);
        final var reopenedAttestation = reopened.attest(reopenedHandle, 1_003);
        assertEquals(PayloadAttestationOutcome.ATTESTED, reopenedAttestation.outcome());
        assertArrayEquals(
                firstAttestation.proof().canonicalBytes(),
                reopenedAttestation.proof().canonicalBytes());
    }

    @Test
    void registryRegistrationRequiresExactPrepareAuthoritiesBeforeHandleOrFileState() throws Exception {
        final KeyPair adapterKey = keyPair();
        final KeyPair foreignKey = keyPair();
        final PayloadProofTrustSetSemantic adapterTrust = trustSet(adapterKey, 9_000);
        final PayloadProofTrustSetSemantic foreignTrust = trustSet(foreignKey, 9_000);
        final ProfileSemanticEnvelope profile = profile();
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        final FilesystemPayloadObjectStore store = new FilesystemPayloadObjectStore(
                tempDir, profile, Bytes.sha256(Bytes.utf8("tenant")), adapterTrust, 7, adapterKey.getPrivate());

        assertEquals(adapterTrust.version(), foreignTrust.version());
        assertNotEquals(adapterTrust.ref(), foreignTrust.ref());
        assertThrows(
                IllegalArgumentException.class, () -> store.register(reservation, foreignTrust.ref(), profile.ref()));
        final ProfileRef foreignProfile = new ProfileRef(
                Bytes.utf8("foreign-object-store"), 1, profile.ref().semanticHash(), ProfileKind.OBJECT_STORE);
        assertThrows(
                IllegalArgumentException.class, () -> store.register(reservation, adapterTrust.ref(), foreignProfile));
        assertEquals(
                PayloadUploadHandleOutcome.NOT_FOUND_OR_NOT_AUTHORIZED,
                store.issueUploadHandle(reservation.reservationId(), UploadHandleKind.OPAQUE_SINGLE_PUT, 1_000)
                        .outcome());
        try (var files = Files.walk(tempDir.resolve("objects"))) {
            assertEquals(0, files.filter(Files::isRegularFile).count());
        }

        store.register(reservation, adapterTrust.ref(), profile.ref());
        assertEquals(adapterTrust.ref(), store.reservationReceipt(reservation).trustSet());
    }

    @Test
    void immutableConflictAndCorruptionFailClosed() throws Exception {
        final KeyPair keyPair = keyPair();
        final ProfileSemanticEnvelope profile = profile();
        final PayloadReservation reservation = reservation(5_000, Bytes.utf8("large"));
        final FilesystemPayloadObjectStore store = new FilesystemPayloadObjectStore(
                tempDir,
                profile,
                Bytes.sha256(Bytes.utf8("tenant")),
                trustSet(keyPair, 9_000),
                7,
                keyPair.getPrivate());
        store.register(reservation);
        final var handle = store.issueUploadHandle(
                        reservation.reservationId(), UploadHandleKind.OPAQUE_SINGLE_PUT, 1_000)
                .issued();
        store.upload(handle, Bytes.utf8("large"), 1_001);
        assertThrows(IllegalStateException.class, () -> store.upload(handle, Bytes.utf8("other"), 1_002));

        final String identity = Bytes.hex(reservation.reservationId());
        final Path object = tempDir.resolve("objects")
                .resolve(identity.substring(0, 2))
                .resolve(identity.substring(2, 4))
                .resolve(identity + ".payload");
        Files.write(object, Bytes.utf8("corrupt"));
        assertEquals(
                PayloadAttestationOutcome.OBJECT_IDENTITY_CONFLICT,
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
        assertThrows(
                IllegalArgumentException.class,
                () -> new FilesystemPayloadObjectStore(
                        root,
                        profile(),
                        Bytes.sha256(Bytes.utf8("tenant")),
                        trustSet(keyPair, 9_000),
                        7,
                        keyPair.getPrivate()));
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
