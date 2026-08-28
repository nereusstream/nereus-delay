package com.nereusstream.delay.assessment;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.google.gson.JsonParser;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PersistentStagingEvidenceTest {
    @TempDir
    Path tempDir;

    @Test
    void signedEnvelopeIsSelfContainedAndTamperFailsClosed() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] payload = "{\"status\":\"PASS\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        final Path evidence = tempDir.resolve("evidence.json");
        PersistentStagingEvidence.writeSignedNew(evidence, payload, keys.getPrivate(), keys.getPublic(), 7);

        final PersistentStagingEvidence.Verified verified = PersistentStagingEvidence.readVerified(evidence);
        assertArrayEquals(payload, verified.payload());
        assertEquals(7, verified.keyGeneration());
        assertEquals(
                "PASS",
                JsonParser.parseString(new String(verified.payload()))
                        .getAsJsonObject()
                        .get("status")
                        .getAsString());

        final byte[] tampered = Files.readAllBytes(evidence);
        tampered[tampered.length - 2] = (byte) (tampered[tampered.length - 2] ^ 1);
        Files.delete(evidence);
        Files.write(evidence, tampered);
        assertThrows(Exception.class, () -> PersistentStagingEvidence.readVerified(evidence));
    }

    @Test
    void evidenceWriterNeverOverwritesAnExistingReceipt() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final Path evidence = tempDir.resolve("evidence.json");
        final byte[] payload = new byte[] {1, 2, 3};
        PersistentStagingEvidence.writeSignedNew(evidence, payload, keys.getPrivate(), keys.getPublic(), 1);
        final byte[] first = Files.readAllBytes(evidence);
        assertThrows(
                java.nio.file.FileAlreadyExistsException.class,
                () -> PersistentStagingEvidence.writeSignedNew(
                        evidence, new byte[] {4}, keys.getPrivate(), keys.getPublic(), 1));
        assertTrueSame(first, Files.readAllBytes(evidence));
    }

    private static void assertTrueSame(final byte[] expected, final byte[] actual) {
        assertArrayEquals(expected, actual);
        assertEquals(Arrays.hashCode(expected), Arrays.hashCode(actual));
    }
}
