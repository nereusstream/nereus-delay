package io.nereusstream.delay.runtime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CredentialBindingProtectionV1;
import io.nereusstream.delay.protocol.CredentialBindingV1;
import io.nereusstream.delay.protocol.CredentialEquivalenceAttestationV1;
import io.nereusstream.delay.protocol.CredentialUseKindV1;
import io.nereusstream.delay.protocol.CredentialUseLeaseV1;
import io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import io.nereusstream.delay.protocol.ObjectStoreProviderKindV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.RotateEquivalentSecretRequestV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.store.CheckpointManifestLimits;
import io.nereusstream.delay.store.OxiaObjectStoreCredentialLeaseActivator;
import io.nereusstream.delay.store.S3CompatibleCheckpointObjectStoreAdapter;
import io.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Two-JVM real-Oxia proof for credential binding rotation and old-lease protection. */
@Tag("real-service")
class CredentialBindingDurableChaosTest {
    private static final String ARTIFACT_ENV = "NEREUS_DELAY_CREDENTIAL_CHAOS_ARTIFACT_DIR";
    private static final String PHASE_ENV = "NEREUS_DELAY_CREDENTIAL_CHAOS_PHASE";
    private static final String PREFIX_ENV = "NEREUS_DELAY_CREDENTIAL_CHAOS_PREFIX";
    private static final String OXIA_ENV = "NEREUS_DELAY_OXIA_ENDPOINT";
    private static final String SCHEMA = "nereus-delay-chaos-durable-state-dump-v1";
    private static final String CELL = "credential-binding-drift";
    private static final String FAULT = "CREDENTIAL_BINDING_ROTATION";
    private static final String BEFORE_PHASE = "BEFORE_FRESH_PROCESS_RECOVERY";
    private static final String AFTER_PHASE = "RECOVERED_AFTER_FRESH_PROCESS";
    private static final long MAX_LEASE_TTL_MS = 10_000;
    private static final long MAX_ATTESTATION_AGE_MS = 300_000;
    private static final long ATTESTATION_VALIDITY_MS = 60_000;
    private static final long OLD_LEASE_TTL_MS = 6_000;
    private static final long NEW_LEASE_TTL_MS = 6_000;
    private static final URI ENDPOINT = URI.create("http://127.0.0.1:1");
    private static final String REGION = "us-east-1";
    private static final String BUCKET = "credential-drift-checkpoints";
    private static final String ACCESS_KEY = "credential-drift-access";
    private static final String SECRET_KEY = "credential-drift-secret";
    private static final CheckpointManifestLimits LIMITS = new CheckpointManifestLimits(
            10, 1 << 20, 1 << 20, 1_024, 1 << 20, 10, 1_024);
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    @Test
    void rotatesProtectedCredentialBindingAcrossFreshProcess() throws Exception {
        final String phase = required(PHASE_ENV);
        final Path artifact = Path.of(required(ARTIFACT_ENV));
        Files.createDirectories(artifact);
        switch (phase) {
            case "before" -> writeBefore(artifact);
            case "after" -> writeAfter(artifact);
            default -> throw new IllegalArgumentException("unsupported credential chaos phase: " + phase);
        }
    }

    private static void writeBefore(final Path artifact) throws Exception {
        final String oxiaEndpoint = required(OXIA_ENV);
        final String prefix = required(PREFIX_ENV);
        final long now = System.currentTimeMillis();
        final Fixture fixture = fixture(now);
        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = connect(oxiaEndpoint, prefix)) {
            final OxiaSyncProfileCatalogBackend authority = authority(client, prefix, fixture.verifier());
            final var firstHead = authority.publish(fixture.profile(), fixture.binding(1));
            assertEquals(1, firstHead.secretGeneration());
            assertEquals(1, firstHead.headRevision());

            final long oldLeaseUntil = now + OLD_LEASE_TTL_MS;
            final S3CompatibleCheckpointObjectStoreAdapter oldAdapter = activate(authority,
                    fixture.profile(), fixture.fingerprint(1), now, oldLeaseUntil);
            assertNotNull(oldAdapter);
            final CredentialBindingProtectionV1 oldProtection = authority.resolveProtection(
                    fixture.profile().ref(), 1);
            assertNotNull(oldProtection);
            assertTrue(oldProtection.objectStoreLeaseProtectionUntilEpochMs() >= oldLeaseUntil);

            final CredentialBindingV1 nextBinding = fixture.binding(2);
            final CredentialBindingV1 firstBinding = fixture.binding(1);
            final RotateEquivalentSecretRequestV1 rotation = new RotateEquivalentSecretRequestV1(
                    fixture.profile().ref(), 1, 2, nextBinding.secretReference(),
                    nextBinding.secretReferenceSha256(), nextBinding.equivalenceAttestation(),
                    firstBinding.bindingDigest(), 1);
            final var rotatedHead = authority.rotate(rotation);
            assertEquals(2, rotatedHead.secretGeneration());
            assertEquals(2, rotatedHead.headRevision());
            final CredentialBindingProtectionV1 rotatedOldProtection = authority.resolveProtection(
                    fixture.profile().ref(), 1);
            assertNotNull(rotatedOldProtection);
            assertTrue(rotatedOldProtection.objectStoreLeaseProtectionUntilEpochMs() >= oldLeaseUntil);

            writeAtomically(artifact.resolve("manifest.json"), manifest(prefix, fixture));
            writeAtomically(artifact.resolve("before.json"), dump(BEFORE_PHASE, prefix,
                    ProcessHandle.current().pid(), fixture, rotatedHead.secretGeneration(),
                    rotatedHead.headRevision(), oldLeaseUntil,
                    rotatedOldProtection.objectStoreLeaseProtectionUntilEpochMs(),
                    rotatedOldProtection.protectionDigest(), false, 1, 0,
                    "HEAD_ROTATED_OLD_LEASE_PROTECTION_RETAINED"));
        }
    }

    private static void writeAfter(final Path artifact) throws Exception {
        final JsonObject manifest = read(artifact.resolve("manifest.json"));
        final JsonObject before = read(artifact.resolve("before.json"));
        final String oxiaEndpoint = required(OXIA_ENV);
        final String prefix = required(PREFIX_ENV);
        assertEquals(SCHEMA, before.get("schema").getAsString());
        assertEquals(BEFORE_PHASE, before.get("phase").getAsString());
        assertEquals(FAULT, before.get("fault").getAsString());
        assertEquals(prefix, manifest.get("key_prefix").getAsString());
        assertEquals(prefix, before.get("key_prefix").getAsString());
        final ProfileSemanticEnvelopeV1 profile = ProfileSemanticEnvelopeV1.decode(decode(
                manifest.get("profile_bytes").getAsString()));
        final PublicKey verifier = KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(
                decode(manifest.get("verifier_public_key").getAsString())));
        final byte[] fingerprintV1 = decode(manifest.get("fingerprint_v1").getAsString());
        final byte[] fingerprintV2 = decode(manifest.get("fingerprint_v2").getAsString());
        final long now = System.currentTimeMillis();
        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = connect(oxiaEndpoint, prefix)) {
            final OxiaSyncProfileCatalogBackend authority = authority(client, prefix, verifier);
            final var head = authority.resolveHead(profile.ref());
            final CredentialBindingV1 current = authority.resolveBinding(profile.ref(), head.secretGeneration());
            final CredentialBindingV1 old = authority.resolveBinding(profile.ref(), 1);
            final CredentialBindingProtectionV1 oldProtection = authority.resolveProtection(profile.ref(), 1);
            assertNotNull(head);
            assertNotNull(current);
            assertNotNull(old);
            assertNotNull(oldProtection);
            assertEquals(2, head.secretGeneration());
            assertEquals(2, head.headRevision());
            assertEquals(2, current.secretGeneration());
            assertEquals(1, old.secretGeneration());
            assertTrue(oldProtection.objectStoreLeaseProtectionUntilEpochMs()
                    >= before.get("old_lease_valid_until").getAsLong());

            final OxiaObjectStoreCredentialLeaseActivator wrongMaterialActivator = new
                    OxiaObjectStoreCredentialLeaseActivator(authority,
                    (resolvedProfile, resolvedBinding) -> new OxiaObjectStoreCredentialLeaseActivator
                            .ObjectStoreCredentialMaterial(ACCESS_KEY, SECRET_KEY, null, fingerprintV1),
                    MAX_LEASE_TTL_MS, MAX_ATTESTATION_AGE_MS);
            assertThrows(IllegalStateException.class, () -> activate(wrongMaterialActivator, profile,
                    now, now + NEW_LEASE_TTL_MS));

            final OxiaObjectStoreCredentialLeaseActivator rightMaterialActivator = new
                    OxiaObjectStoreCredentialLeaseActivator(authority,
                    (resolvedProfile, resolvedBinding) -> new OxiaObjectStoreCredentialLeaseActivator
                            .ObjectStoreCredentialMaterial(ACCESS_KEY, SECRET_KEY, null, fingerprintV2),
                    MAX_LEASE_TTL_MS, MAX_ATTESTATION_AGE_MS);
            final S3CompatibleCheckpointObjectStoreAdapter newAdapter = activate(rightMaterialActivator, profile,
                    now, now + NEW_LEASE_TTL_MS);
            assertNotNull(newAdapter);
            final CredentialBindingProtectionV1 newProtection = authority.resolveProtection(profile.ref(), 2);
            assertNotNull(newProtection);
            assertTrue(newProtection.objectStoreLeaseProtectionUntilEpochMs() >= now + NEW_LEASE_TTL_MS);
            assertNotEquals(before.get("process_pid").getAsLong(), ProcessHandle.current().pid());

            writeAtomically(artifact.resolve("after.json"), dump(AFTER_PHASE, prefix,
                    ProcessHandle.current().pid(), profile, head.secretGeneration(), head.headRevision(),
                    before.get("old_lease_valid_until").getAsLong(),
                    oldProtection.objectStoreLeaseProtectionUntilEpochMs(), oldProtection.protectionDigest(),
                    true, head.secretGeneration(),
                    newProtection.objectStoreLeaseProtectionUntilEpochMs(),
                    "FRESH_PROCESS_REOPENED_HEAD_AND_REJECTED_STALE_FINGERPRINT_BEFORE_PROVIDER_OWNERSHIP"));
        }
    }

    private static S3CompatibleCheckpointObjectStoreAdapter activate(
            final OxiaSyncProfileCatalogBackend authority, final ProfileSemanticEnvelopeV1 profile,
            final byte[] fingerprint, final long now, final long validUntil) {
        final OxiaObjectStoreCredentialLeaseActivator activator = new OxiaObjectStoreCredentialLeaseActivator(
                authority, (resolvedProfile, resolvedBinding) -> new OxiaObjectStoreCredentialLeaseActivator
                        .ObjectStoreCredentialMaterial(ACCESS_KEY, SECRET_KEY, null, fingerprint),
                MAX_LEASE_TTL_MS, MAX_ATTESTATION_AGE_MS);
        return activate(activator, profile, now, validUntil);
    }

    private static S3CompatibleCheckpointObjectStoreAdapter activate(
            final OxiaObjectStoreCredentialLeaseActivator activator, final ProfileSemanticEnvelopeV1 profile,
            final long now, final long validUntil) {
        return activator.activateS3Compatible(new OxiaObjectStoreCredentialLeaseActivator.ActivationRequest(
                profile.ref(), ENDPOINT, REGION, BUCKET, bytes(32, 40), evidence(now), validUntil, LIMITS,
                HttpClient.newHttpClient(), Clock.fixed(Instant.ofEpochMilli(now), ZoneOffset.UTC),
                Duration.ofSeconds(2)));
    }

    private static OxiaSyncOwnerLeaseBackend.ClientHandle connect(final String endpoint, final String prefix)
            throws Exception {
        return OxiaSyncOwnerLeaseBackend.connect(endpoint, "default", "credential-chaos-" + UUID.randomUUID(),
                Duration.ofSeconds(15), prefix + "/client");
    }

    private static OxiaSyncProfileCatalogBackend authority(
            final OxiaSyncOwnerLeaseBackend.ClientHandle client, final String prefix, final PublicKey verifier) {
        return new OxiaSyncProfileCatalogBackend(client, prefix + "/catalog", MAX_LEASE_TTL_MS,
                MAX_ATTESTATION_AGE_MS, CredentialAttestationTrustSet.single(1, Bytes.utf8("credential-chaos"),
                1, verifier, 0, System.currentTimeMillis() + MAX_ATTESTATION_AGE_MS));
    }

    private static Fixture fixture(final long now) throws Exception {
        final URI endpoint = ENDPOINT;
        final ObjectStoreProfileSemanticV1 semantic = new ObjectStoreProfileSemanticV1(
                ObjectStoreProviderKindV1.S3_COMPATIBLE,
                S3CompatibleCheckpointObjectStoreAdapter.endpointConfigDigest(endpoint, REGION, BUCKET),
                S3CompatibleCheckpointObjectStoreAdapter.credentialAuthorizationScopeDigest(
                        ACCESS_KEY, REGION, BUCKET),
                1, true, true, true, true, bytes(32, 1), 1 << 20,
                ObjectStoreProfileSemanticV1.SINGLE_PUT, 1, bytes(32, 2));
        final ProfileSemanticEnvelopeV1 profile = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.OBJECT_STORE, Bytes.utf8("credential-drift-profile"), 1, semantic);
        return new Fixture(profile, KeyPairGenerator.getInstance("Ed25519").generateKeyPair(), now);
    }

    private static CredentialBindingV1 binding(final Fixture fixture, final long generation) {
        final byte[] reference = Bytes.utf8("secret://credential-drift/v" + generation);
        final CredentialEquivalenceAttestationV1 attestation = CredentialEquivalenceAttestationV1.signed(
                fixture.profile().ref(), generation, Bytes.sha256(reference),
                ((ObjectStoreProfileSemanticV1) fixture.profile().body()).credentialAuthorizationScopeDigest(),
                fixture.fingerprint(generation), 1, Bytes.utf8("credential-chaos"),
                evidence(fixture.now() - 1_000), fixture.now() + ATTESTATION_VALIDITY_MS,
                bytes(32, (int) (50 + generation)), 1, fixture.keyPair().getPrivate());
        return CredentialBindingV1.create(fixture.profile().ref(), generation, reference, attestation);
    }

    private static JsonObject manifest(final String prefix, final Fixture fixture) {
        final JsonObject result = new JsonObject();
        result.addProperty("schema", SCHEMA);
        result.addProperty("cell", CELL);
        result.addProperty("fault", FAULT);
        result.addProperty("key_prefix", prefix);
        result.addProperty("profile_bytes", encode(fixture.profile().canonicalBytes()));
        result.addProperty("verifier_public_key", encode(fixture.keyPair().getPublic().getEncoded()));
        result.addProperty("fingerprint_v1", encode(fixture.fingerprint(1)));
        result.addProperty("fingerprint_v2", encode(fixture.fingerprint(2)));
        result.addProperty("profile_sha256", Bytes.hex(Bytes.sha256(fixture.profile().canonicalBytes())));
        return result;
    }

    private static JsonObject dump(final String phase, final String prefix, final long pid, final Object profileOrFixture,
                                   final long headGeneration, final long headRevision, final long oldLeaseUntil,
                                   final long oldProtectionUntil, final byte[] oldProtectionDigest,
                                   final boolean driftRejected, final long freshLeaseGeneration,
                                   final long freshProtectionUntil, final String recoveryAction) {
        final ProfileSemanticEnvelopeV1 profile = profileOrFixture instanceof Fixture fixture
                ? fixture.profile() : (ProfileSemanticEnvelopeV1) profileOrFixture;
        final JsonObject result = new JsonObject();
        result.addProperty("schema", SCHEMA);
        result.addProperty("cell", CELL);
        result.addProperty("phase", phase);
        result.addProperty("fault", FAULT);
        result.addProperty("dump_forced", true);
        result.addProperty("durable_oxia_read", true);
        result.addProperty("process_pid", pid);
        result.addProperty("key_prefix", prefix);
        result.addProperty("profile_sha256", Bytes.hex(Bytes.sha256(profile.canonicalBytes())));
        result.addProperty("head_generation", headGeneration);
        result.addProperty("head_revision", headRevision);
        result.addProperty("old_lease_generation", 1);
        result.addProperty("old_lease_valid_until", oldLeaseUntil);
        result.addProperty("old_protection_until", oldProtectionUntil);
        result.addProperty("old_protection_digest", Bytes.hex(oldProtectionDigest));
        result.addProperty("stale_fingerprint_rejected", driftRejected);
        result.addProperty("fresh_lease_generation", freshLeaseGeneration);
        result.addProperty("fresh_protection_until", freshProtectionUntil);
        result.addProperty("recovery_action", recoveryAction);
        return result;
    }

    private static JsonObject read(final Path path) throws IOException {
        return JsonParser.parseString(Files.readString(path, StandardCharsets.UTF_8)).getAsJsonObject();
    }

    private static void writeAtomically(final Path target, final JsonObject value) throws IOException {
        final Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
        final byte[] bytes = (GSON.toJson(value) + "\n").getBytes(StandardCharsets.UTF_8);
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(bytes));
            channel.force(true);
        }
        Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        try (FileChannel directory = FileChannel.open(target.getParent(), StandardOpenOption.READ)) {
            directory.force(true);
        }
    }

    private static String required(final String name) {
        final String value = System.getenv(name);
        Assumptions.assumeTrue(value != null && !value.isBlank(), name + " is not configured");
        return value;
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest) {
        return new TrustedUtcIntervalEvidence(earliest, earliest + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, (int) earliest), 1, 1, 1,
                bytes(32, (int) earliest + 1), 0, null);
    }

    private static String encode(final byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static byte[] decode(final String encoded) {
        return Base64.getDecoder().decode(encoded);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private record Fixture(ProfileSemanticEnvelopeV1 profile, KeyPair keyPair, long now) {
        private Fixture {
            Objects.requireNonNull(profile, "profile");
            Objects.requireNonNull(keyPair, "keyPair");
        }

        private CredentialBindingV1 binding(final long generation) {
            return CredentialBindingDurableChaosTest.binding(this, generation);
        }

        private byte[] fingerprint(final long generation) {
            return bytes(32, generation == 1 ? 5 : 15);
        }

        private PublicKey verifier() {
            return keyPair.getPublic();
        }
    }
}
