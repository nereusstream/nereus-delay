package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.CredentialUseLease;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.CredentialAttestationTrustSet;
import com.nereusstream.delay.runtime.ProfileCatalog;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.CertPathValidator;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/** Bounded immutable-version mTLS material loading; the original verifier still owns Broker authorization. */
public final class LocalBrokerMtlsResolver {
    public static final String VERIFIER_ID = "nereus-delay-local-broker-mtls";
    public static final int VERIFIER_VERSION = 1;
    private static final Pattern REFERENCE = Pattern.compile("nd-local-mtls-v1://([0-9a-f]{32})/([0-9a-f]{32})");
    private final Path root;
    private final Limits limits;
    private final byte[] serverTrustPem;
    private final List<X509Certificate> clientRoots;
    private final byte[] policyDigest;
    private final EndpointPolicy endpoints;
    private final Map<String, Version> versions = new HashMap<>();
    private final Map<String, FileImage> privateEvidence = new HashMap<>();
    private long retainedBytes;

    public record Limits(int maximumFileBytes, int maximumTotalBytes, int maximumCertificates,
            int maximumVersions, long maximumRetainedBytes) {
        public Limits {
            if (maximumFileBytes <= 0 || maximumFileBytes >= Integer.MAX_VALUE
                    || maximumTotalBytes < maximumFileBytes || maximumTotalBytes >= Integer.MAX_VALUE
                    || maximumCertificates <= 0 || maximumCertificates > 32
                    || maximumVersions <= 0 || maximumVersions >= Integer.MAX_VALUE
                    || maximumRetainedBytes < maximumTotalBytes || maximumRetainedBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("local mTLS resolver requires finite material limits");
            }
        }
    }

    /** Exact deployment endpoints; no resolver lookup, URL rewrite or hostname-verification fallback. */
    public record EndpointPolicy(Set<String> kafkaBootstrapServers, Set<String> pulsarServiceUrls) {
        public EndpointPolicy {
            kafkaBootstrapServers = Set.copyOf(kafkaBootstrapServers); pulsarServiceUrls = Set.copyOf(pulsarServiceUrls);
            if (kafkaBootstrapServers.size() + pulsarServiceUrls.size() == 0
                    || kafkaBootstrapServers.size() + pulsarServiceUrls.size() > 128) { throw unavailable(); }
            for (var endpoint : kafkaBootstrapServers) { requireEndpoint(endpoint); }
            for (var endpoint : pulsarServiceUrls) {
                requireEndpoint(endpoint);
                if (!endpoint.startsWith("pulsar+ssl://")) { throw unavailable(); }
            }
        }
        private static void requireEndpoint(String endpoint) {
            if (endpoint.isBlank() || !endpoint.equals(endpoint.trim()) || endpoint.length() > 4096
                    || endpoint.chars().anyMatch(value -> value < 33 || value > 126)) { throw unavailable(); }
        }
        private byte[] digest() {
            return Bytes.sha256(CanonicalProtobuf.message(out -> {
                for (var endpoint : kafkaBootstrapServers.stream().sorted().toList()) { CanonicalProtobuf.bytes(out, 1, Bytes.utf8(endpoint)); }
                for (var endpoint : pulsarServiceUrls.stream().sorted().toList()) { CanonicalProtobuf.bytes(out, 2, Bytes.utf8(endpoint)); }
            }));
        }
    }

    /** Loads the closed deployment schema before reading and verifying actual trust files. */
    public static LocalBrokerMtlsResolver load(Path configuration) throws IOException {
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(configuration);
        final Path root = f.absolutePath("credentialRoot"), clientTrust = f.absolutePath("clientTrustFile"),
                serverTrust = f.absolutePath("serverTrustFile");
        final var limits = new Limits(f.integer("maximumFileBytes"), f.integer("maximumTotalBytes"),
                f.integer("maximumCertificates"), f.integer("maximumVersions"), f.number("maximumRetainedBytes"));
        final int kafkaCount = f.integer("endpoints.kafkaCount"), pulsarCount = f.integer("endpoints.pulsarCount");
        if ((long) kafkaCount + pulsarCount < 1 || (long) kafkaCount + pulsarCount > 128) {
            throw new IllegalArgumentException("mTLS endpoint inventory requires one finite explicit deployment set");
        }
        final var kafka = new java.util.HashSet<String>();
        final var pulsar = new java.util.HashSet<String>();
        for (int i = 0; i < kafkaCount; i++) {
            if (!kafka.add(f.text("endpoints.kafka." + i))) throw new IllegalArgumentException("duplicate Kafka TLS endpoint");
        }
        for (int i = 0; i < pulsarCount; i++) {
            if (!pulsar.add(f.text("endpoints.pulsar." + i))) throw new IllegalArgumentException("duplicate Pulsar TLS endpoint");
        }
        final var endpoints = new EndpointPolicy(kafka, pulsar);
        f.requireComplete();
        return new LocalBrokerMtlsResolver(root, clientTrust, serverTrust, limits, endpoints);
    }

    /** Trust files are deployment inputs, separate from the client fingerprint; they are loaded once without symlinks. */
    public LocalBrokerMtlsResolver(Path credentialRoot, Path clientTrustFile, Path serverTrustFile,
            Limits limits, EndpointPolicy endpoints) {
        this.root = Objects.requireNonNull(credentialRoot, "controlled credential root").toAbsolutePath().normalize();
        this.limits = Objects.requireNonNull(limits, "limits");
        this.endpoints = Objects.requireNonNull(endpoints, "fixed TLS endpoint policy");
        try {
            final var clientTrust = readAbsolute(clientTrustFile, limits.maximumFileBytes());
            final var serverTrust = readAbsolute(serverTrustFile, limits.maximumFileBytes());
            clientRoots = certificates(clientTrust.bytes(), limits.maximumCertificates());
            certificates(serverTrust.bytes(), limits.maximumCertificates());
            if (clientTrust.bytes().length + (long) serverTrust.bytes().length > limits.maximumTotalBytes()) { throw unavailable(); }
            for (var cert : clientRoots) { if (cert.getBasicConstraints() < 0) { throw unavailable(); } }
            serverTrustPem = serverTrust.bytes();
            policyDigest = Bytes.sha256(Bytes.utf8("nereus-delay-local-mtls-offline-pkix-v1\0"),
                    clientTrust.bytes(), serverTrustPem, endpoints.digest());
            retainedBytes = clientTrust.bytes().length + (long) serverTrustPem.length;
        } catch (Exception failure) { throw unavailable(); }
    }

    public byte[] tlsConfigurationDigest() { return Bytes.copy(policyDigest); }

    /** Resolve only at control/startup boundaries on the caller's I/O owner; SEND checks this local receipt. */
    public Receipt resolve(TargetChannelIdentity channel, CanonicalTargetPartition physical, ProfileCatalog profiles,
            CredentialAttestationTrustSet trust, Supplier<TrustedUtcIntervalEvidence> time) {
        try {
            final var binding = TargetDestinationCredentialChecks.requireBinding(channel, physical, profiles, trust);
            final var lease = channel.credentialLease();
            final var attestation = binding.equivalenceAttestation();
            if (attestation.verifierVersion() != VERIFIER_VERSION
                    || !Arrays.equals(attestation.verifierId(), Bytes.utf8(VERIFIER_ID))) { throw unavailable(); }
            requireProtection(binding, lease, profiles);
            final var referenceBytes = binding.secretReference();
            for (byte value : referenceBytes) { if (value < 0) { throw unavailable(); } }
            final var reference = REFERENCE.matcher(new String(referenceBytes, StandardCharsets.US_ASCII));
            if (!reference.matches() || reference.group(1).equals("0".repeat(32))
                    || reference.group(2).equals("0".repeat(32))) { throw unavailable(); }
            final String id = reference.group(1), version = reference.group(2);
            final FileImage certs, key, evidence;
            final List<Object> directoryKeys;
            try (var directory = Directory.open(root.resolve(id).resolve(version))) {
                directoryKeys = directory.identities();
                certs = directory.read("client-cert-chain.pem", limits.maximumFileBytes());
                key = directory.read("client-key.pk8.pem", limits.maximumFileBytes());
                evidence = directory.read("verifier-evidence.bin", limits.maximumFileBytes());
            }
            final long bytes = Math.addExact(Math.addExact(certs.bytes().length, key.bytes().length), evidence.bytes().length);
            if (bytes > limits.maximumTotalBytes()) { throw unavailable(); }
            final var chain = certificates(certs.bytes(), limits.maximumCertificates());
            final var leaf = chain.getFirst();
            final byte[] fingerprint = Bytes.sha256(leaf.getEncoded());
            if (!Bytes.constantTimeEquals(fingerprint, attestation.resolvedCredentialFingerprintDigest())
                    || !Bytes.constantTimeEquals(Bytes.sha256(evidence.bytes()), attestation.verificationEvidenceSha256())) {
                throw unavailable();
            }
            requireEvidence(evidence.bytes(), id, version, binding, fingerprint, chain, key.bytes());
            requireCertificates(chain, lease);
            requirePrivateKey(key.bytes(), leaf);
            final var after = Objects.requireNonNull(time.get(), "trusted credential interval");
            lease.requireActiveAt(after);
            final var confirmed = TargetDestinationCredentialChecks.requireBinding(channel, physical, profiles, trust);
            if (!confirmed.equals(binding)) { throw unavailable(); }
            requireProtection(binding, lease, profiles);
            final var loaded = new Version(certs, key, fingerprint,
                    Math.addExact(certs.bytes().length, key.bytes().length), directoryKeys);
            final var snapshot = retain(id + '/' + version, loaded, Map.of("destination-evidence", evidence));
            return new Receipt(binding, lease, snapshot, serverTrustPem, policyDigest, physical, endpoints,
                    retainedProofs(id + '/' + version, List.of("destination-evidence")));
        } catch (Exception failure) { throw unavailable(); }
    }

    /** Independent Source authorization: no synthetic Destination Profile, channel, Owner or Source position. */
    public SourceReceipt resolveSource(Path authorizationFile, byte[] signedRoute,
            SourceTransportCredentialAuthorization.Trust trust, Supplier<TrustedUtcIntervalEvidence> time) {
        try {
            if (signedRoute.length == 0 || signedRoute.length > limits.maximumTotalBytes()) { throw unavailable(); }
            final var input = readAbsolute(authorizationFile, Math.min(limits.maximumFileBytes(),
                    SourceTransportCredentialAuthorization.MAX_BYTES));
            final var authorization = SourceTransportCredentialAuthorization.decode(input.bytes());
            trust.verify(authorization, signedRoute, time.get());
            if (!Arrays.equals(policyDigest, authorization.policyDigest())
                    || !Arrays.equals(Bytes.sha256(authorization.reference()), authorization.referenceDigest())) { throw unavailable(); }
            final var reference = REFERENCE.matcher(new String(authorization.reference(), StandardCharsets.US_ASCII));
            if (!reference.matches() || reference.group(1).equals("0".repeat(32))
                    || reference.group(2).equals("0".repeat(32))) { throw unavailable(); }
            final String id = reference.group(1), version = reference.group(2);
            final FileImage certs, key, evidence;
            final List<Object> directories;
            final String evidenceName = "source-verifier-evidence-" + Bytes.hex(authorization.scopeImageDigest()) + ".bin";
            try (var directory = Directory.open(root.resolve(id).resolve(version))) {
                directories = directory.identities();
                certs = directory.read("client-cert-chain.pem", limits.maximumFileBytes());
                key = directory.read("client-key.pk8.pem", limits.maximumFileBytes());
                evidence = directory.read(evidenceName, limits.maximumFileBytes());
            }
            final long total = Math.addExact(input.bytes().length, Math.addExact(evidence.bytes().length,
                    Math.addExact(certs.bytes().length, key.bytes().length)));
            if (total > limits.maximumTotalBytes()) { throw unavailable(); }
            final var chain = certificates(certs.bytes(), limits.maximumCertificates());
            final var fingerprint = Bytes.sha256(chain.getFirst().getEncoded());
            if (!Arrays.equals(fingerprint, authorization.fingerprint())
                    || !Arrays.equals(Bytes.sha256(evidence.bytes()), authorization.evidenceDigest())) { throw unavailable(); }
            requireSourceEvidence(evidence.bytes(), id, version, authorization, chain, key.bytes());
            requireCertificates(chain, authorization.notBeforeEpochMs(), authorization.notAfterEpochMs());
            requirePrivateKey(key.bytes(), chain.getFirst());
            trust.verify(authorization, signedRoute, time.get());
            final var loaded = new Version(certs, key, fingerprint, Math.addExact(certs.bytes().length, key.bytes().length), directories);
            final String scope = Bytes.hex(authorization.identityDigest());
            final var snapshot = retain(id + '/' + version, loaded,
                    Map.of("source-authorization/" + scope, input, "source-evidence/" + scope, evidence));
            return new SourceReceipt(authorization, trust, snapshot, serverTrustPem, policyDigest, endpoints,
                    retainedProofs(id + '/' + version, List.of("source-authorization/" + scope, "source-evidence/" + scope)));
        } catch (Exception failure) { throw unavailable(); }
    }

    /** Dedicated Target recovery input. Call only on the original pre-reserved, bounded startup I/O owner. */
    public TargetRecoveryReceipt resolveTargetRecovery(Path authorizationFile,
            com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping,
            TargetRecoveryCredentialAuthorization.Trust trust, Supplier<TrustedUtcIntervalEvidence> time,
            TargetDestinationSdkBudget.Client originalSdkBudget, Runnable ownedIoGuard) {
        // This is captured-input capacity, in addition to the actual producer/reader envelope supplied by its owner.
        final long[] minimum = new long[com.nereusstream.delay.protocol.CapacityDimension.COUNT];
        minimum[com.nereusstream.delay.protocol.CapacityDimension.BUFFERED_BYTES.wireValue() - 1] =
                Math.addExact((long) limits.maximumTotalBytes(), serverTrustPem.length);
        minimum[com.nereusstream.delay.protocol.CapacityDimension.OPEN_FILES.wireValue() - 1] = 66;
        Objects.requireNonNull(originalSdkBudget, "pre-reserved original recovery SDK generation")
                .requireCovers(new com.nereusstream.delay.protocol.CapacityVector(minimum));
        Objects.requireNonNull(ownedIoGuard, "original bounded I/O operation").run();
        originalSdkBudget.beginConstruction();
        TargetRecoveryCredentialAuthorization.Hold hold = null;
        try {
            final var input = readAbsolute(authorizationFile,
                    Math.min(limits.maximumFileBytes(), TargetRecoveryCredentialAuthorization.MAX_BYTES));
            final var authorization = TargetRecoveryCredentialAuthorization.decode(input.bytes());
            authorization.requireMapping(mapping);
            trust.verify(authorization, time.get());
            if (!Arrays.equals(policyDigest, authorization.policyDigest())) throw unavailable();
            final var reference = REFERENCE.matcher(new String(authorization.reference(), StandardCharsets.US_ASCII));
            if (!reference.matches() || reference.group(1).equals("0".repeat(32))
                    || reference.group(2).equals("0".repeat(32))) throw unavailable();
            final String id = reference.group(1), version = reference.group(2);
            hold = trust.acquire(authorization, time.get());
            trust.requireHeld(authorization, hold, time.get());
            ownedIoGuard.run();
            final FileImage certs, key, evidence;
            final List<Object> directories;
            final String evidenceName = "target-recovery-verifier-evidence-" + Bytes.hex(authorization.scopeImageDigest()) + ".bin";
            long remaining = limits.maximumTotalBytes() - (long) input.bytes().length;
            try (var directory = Directory.open(root.resolve(id).resolve(version))) {
                directories = directory.identities();
                certs = directory.read("client-cert-chain.pem", recoveryReadLimit(remaining));
                remaining -= certs.bytes().length;
                key = directory.read("client-key.pk8.pem", recoveryReadLimit(remaining));
                remaining -= key.bytes().length;
                evidence = directory.read(evidenceName, recoveryReadLimit(remaining));
            }
            final var chain = certificates(certs.bytes(), limits.maximumCertificates());
            final var fingerprint = Bytes.sha256(chain.getFirst().getEncoded());
            if (!Arrays.equals(fingerprint, authorization.fingerprint())
                    || !Arrays.equals(Bytes.sha256(evidence.bytes()), authorization.evidenceDigest())) throw unavailable();
            requireTargetRecoveryEvidence(evidence.bytes(), id, version, authorization, chain, key.bytes());
            requireCertificates(chain, authorization.notBeforeEpochMs(), authorization.notAfterEpochMs());
            requirePrivateKey(key.bytes(), chain.getFirst());
            trust.verify(authorization, time.get());
            trust.requireHeld(authorization, hold, time.get());
            ownedIoGuard.run();
            final var loaded = new Version(certs, key, fingerprint,
                    Math.addExact(certs.bytes().length, key.bytes().length), directories);
            final String scope = Bytes.hex(authorization.digest());
            final var snapshot = retain(id + '/' + version, loaded,
                    Map.of("target-recovery-authorization/" + scope, input, "target-recovery-evidence/" + scope, evidence));
            return new TargetRecoveryReceipt(authorization, trust, hold, originalSdkBudget, snapshot, serverTrustPem, endpoints,
                    retainedProofs(id + '/' + version,
                            List.of("target-recovery-authorization/" + scope, "target-recovery-evidence/" + scope)));
        } catch (Exception | Error failure) {
            final var retained = hold;
            final Runnable cleanup = () -> {
                if (retained != null) retained.close();
                originalSdkBudget.closed();
            };
            try { cleanup.run(); }
            catch (RuntimeException | Error cleanupFailure) { throw new TargetSdkCleanupFailure(unavailable(), cleanup); }
            if (failure instanceof Error fatal) throw fatal;
            throw unavailable();
        }
    }

    private int recoveryReadLimit(long remaining) {
        if (remaining <= 0) throw unavailable();
        return (int) Math.min(remaining, limits.maximumFileBytes());
    }

    private static void requireTargetRecoveryEvidence(byte[] bytes, String id, String version,
            TargetRecoveryCredentialAuthorization a, List<X509Certificate> chain, byte[] privateKey) throws Exception {
        final var fields = QueryCodecSupport.read(bytes, "private Target recovery mTLS evidence");
        QueryCodecSupport.requireNumbers(fields,
                new int[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}, "private Target recovery mTLS evidence");
        final var chainBytes = new ByteArrayOutputStream();
        for (var cert : chain) chainBytes.write(Bytes.lp32(cert.getEncoded()));
        final byte[] probe = QueryCodecSupport.bytes(fields.get(15), 16);
        final var canonical = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, Bytes.hexToBytes(id));
            CanonicalProtobuf.bytes(out, 3, Bytes.hexToBytes(version));
            CanonicalProtobuf.bytes(out, 4, a.bindingId());
            CanonicalProtobuf.uint64Bits(out, 5, a.credentialGeneration());
            CanonicalProtobuf.bytes(out, 6, a.referenceDigest());
            CanonicalProtobuf.bytes(out, 7, a.fingerprint());
            CanonicalProtobuf.bytes(out, 8, a.principalHash());
            CanonicalProtobuf.bytes(out, 9, Bytes.sha256(chainBytes.toByteArray()));
            CanonicalProtobuf.bytes(out, 10, Bytes.sha256(chain.getFirst().getPublicKey().getEncoded()));
            CanonicalProtobuf.bytes(out, 11, Bytes.sha256(privateKey));
            CanonicalProtobuf.bytes(out, 12, a.policyDigest());
            CanonicalProtobuf.bytes(out, 13, a.scopeImageDigest());
            CanonicalProtobuf.uint32(out, 14, TargetRecoveryCredentialAuthorization.OPERATIONS);
            CanonicalProtobuf.bytes(out, 15, Bytes.sha256(probe));
            CanonicalProtobuf.bytes(out, 16, probe);
        });
        if (probe.length == 0 || !Arrays.equals(bytes, canonical)) throw unavailable();
    }

    /** Same-version material and every retained private proof remain immutable; all bytes are charged once. */
    private synchronized Version retain(String identity, Version loaded, Map<String, FileImage> proofs) {
        final var previous = versions.get(identity);
        if (previous != null && !previous.same(loaded)) { throw unavailable(); }
        long addition = previous == null ? loaded.bytes() : 0;
        int newProofs = 0;
        for (var entry : proofs.entrySet()) {
            final var old = privateEvidence.get(evidenceKey(identity, entry.getKey()));
            if (old != null && !old.same(entry.getValue())) { throw unavailable(); }
            if (old == null) { addition = Math.addExact(addition, entry.getValue().bytes().length); newProofs++; }
        }
        if (previous == null && versions.size() >= limits.maximumVersions()
                || privateEvidence.size() + (long) newProofs > 4L * limits.maximumVersions()
                || addition > limits.maximumRetainedBytes() - retainedBytes) { throw unavailable(); }
        if (previous == null) { versions.put(identity, loaded); }
        for (var entry : proofs.entrySet()) { privateEvidence.putIfAbsent(evidenceKey(identity, entry.getKey()), entry.getValue()); }
        retainedBytes += addition;
        return previous == null ? loaded : previous;
    }
    private static String evidenceKey(String material, String proof) {
        return proof.startsWith("source-authorization/") ? proof : material + '/' + proof;
    }
    private synchronized List<FileImage> retainedProofs(String material, List<String> proofs) {
        return proofs.stream().map(proof -> Objects.requireNonNull(privateEvidence.get(evidenceKey(material, proof)))).toList();
    }

    private static void requireSourceEvidence(byte[] bytes, String id, String version,
            SourceTransportCredentialAuthorization authorization, List<X509Certificate> chain, byte[] key) throws Exception {
        final var fields = QueryCodecSupport.read(bytes, "private Source mTLS evidence");
        QueryCodecSupport.requireNumbers(fields, new int[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14}, "private Source mTLS evidence");
        final var chainBytes = new ByteArrayOutputStream();
        for (var cert : chain) { chainBytes.write(Bytes.lp32(cert.getEncoded())); }
        final byte[] probe = QueryCodecSupport.bytes(fields.get(13), 14);
        final var canonical = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, Bytes.hexToBytes(id));
            CanonicalProtobuf.bytes(out, 3, Bytes.hexToBytes(version));
            CanonicalProtobuf.bytes(out, 4, authorization.bindingId());
            CanonicalProtobuf.uint64Bits(out, 5, authorization.credentialGeneration());
            CanonicalProtobuf.bytes(out, 6, authorization.referenceDigest());
            CanonicalProtobuf.bytes(out, 7, authorization.fingerprint());
            CanonicalProtobuf.bytes(out, 8, Bytes.sha256(chainBytes.toByteArray()));
            CanonicalProtobuf.bytes(out, 9, Bytes.sha256(chain.getFirst().getPublicKey().getEncoded()));
            CanonicalProtobuf.bytes(out, 10, Bytes.sha256(key));
            CanonicalProtobuf.bytes(out, 11, authorization.policyDigest());
            CanonicalProtobuf.bytes(out, 12, authorization.scopeImageDigest());
            CanonicalProtobuf.bytes(out, 13, Bytes.sha256(probe));
            CanonicalProtobuf.bytes(out, 14, probe);
        });
        if (probe.length == 0 || !Arrays.equals(bytes, canonical)) { throw unavailable(); }
    }

    private static void requireProtection(CredentialBinding binding, CredentialUseLease lease, ProfileCatalog profiles) {
        final var protection = Objects.requireNonNull(profiles.resolveProtection(binding.profile(), binding.secretGeneration()));
        if (!protection.profile().equals(binding.profile()) || protection.secretGeneration() != binding.secretGeneration()
                || !Bytes.constantTimeEquals(protection.bindingDigest(), binding.bindingDigest())
                || Long.compareUnsigned(protection.protectionRevision(), lease.protectionRevision()) < 0
                || protection.managedChannelProtectionUntilEpochMs() < lease.validUntilEpochMs()) { throw unavailable(); }
    }

    private static void requireEvidence(byte[] bytes, String id, String version, CredentialBinding binding,
            byte[] fingerprint, List<X509Certificate> chain, byte[] privateKeyPem) throws Exception {
        final var fields = QueryCodecSupport.read(bytes, "local mTLS verifier evidence");
        QueryCodecSupport.requireNumbers(fields, new int[] {1,2,3,4,5,6,7,8,9,10,11,12,13}, "local mTLS verifier evidence");
        final var chainBytes = new ByteArrayOutputStream();
        for (var cert : chain) { chainBytes.write(Bytes.lp32(cert.getEncoded())); }
        final byte[] probe = QueryCodecSupport.bytes(fields.get(11), 12);
        final byte[] canonical = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, Bytes.hexToBytes(id));
            CanonicalProtobuf.bytes(out, 3, Bytes.hexToBytes(version));
            CanonicalProtobuf.bytes(out, 4, binding.profile().canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 5, binding.secretGeneration());
            CanonicalProtobuf.bytes(out, 6, binding.secretReferenceSha256());
            CanonicalProtobuf.bytes(out, 7, binding.equivalenceAttestation().authorizationScopeDigest());
            CanonicalProtobuf.bytes(out, 8, fingerprint);
            CanonicalProtobuf.bytes(out, 9, Bytes.sha256(chainBytes.toByteArray()));
            CanonicalProtobuf.bytes(out, 10, Bytes.sha256(chain.getFirst().getPublicKey().getEncoded()));
            CanonicalProtobuf.bytes(out, 11, Bytes.sha256(probe));
            CanonicalProtobuf.bytes(out, 12, probe);
            CanonicalProtobuf.bytes(out, 13, Bytes.sha256(privateKeyPem));
        });
        if (probe.length == 0 || !Arrays.equals(bytes, canonical)) { throw unavailable(); }
    }

    private void requireCertificates(List<X509Certificate> chain, CredentialUseLease lease) throws Exception {
        requireCertificates(chain, lease.issuedAt().earliestEpochMs(), lease.validUntilEpochMs());
    }
    private void requireCertificates(List<X509Certificate> chain, long earliest, long until) throws Exception {
        for (var cert : chain) {
            cert.checkValidity(new Date(earliest));
            cert.checkValidity(new Date(until));
        }
        final var leaf = chain.getFirst();
        final var usage = leaf.getKeyUsage();
        if (leaf.getBasicConstraints() >= 0 || usage == null || !usage[0]
                || leaf.getExtendedKeyUsage() == null || !leaf.getExtendedKeyUsage().contains("1.3.6.1.5.5.7.3.2")) {
            throw unavailable();
        }
        if (leaf.getPublicKey() instanceof RSAPublicKey rsa) {
            if (rsa.getModulus().bitLength() < 2048) { throw unavailable(); }
        } else if (leaf.getPublicKey() instanceof ECPublicKey ec) {
            if (ec.getParams().getOrder().bitLength() < 256) { throw unavailable(); }
        } else { throw unavailable(); }
        final var anchors = new java.util.HashSet<TrustAnchor>();
        for (var rootCert : clientRoots) { anchors.add(new TrustAnchor(rootCert, null)); }
        final var path = new ArrayList<>(chain);
        if (clientRoots.contains(path.getLast())) { path.removeLast(); }
        final var params = new PKIXParameters(anchors);
        // Deliberate offline deployment policy: no network revocation fetch during material resolution.
        params.setRevocationEnabled(false); params.setDate(new Date(earliest));
        final var validated = (java.security.cert.PKIXCertPathValidatorResult) CertPathValidator.getInstance("PKIX")
                .validate(CertificateFactory.getInstance("X.509").generateCertPath(path), params);
        validated.getTrustAnchor().getTrustedCert().checkValidity(new Date(earliest));
        validated.getTrustAnchor().getTrustedCert().checkValidity(new Date(until));
    }

    private static void requirePrivateKey(byte[] pem, X509Certificate leaf) throws Exception {
        final var text = new String(pem, StandardCharsets.US_ASCII).trim();
        final String begin = "-----BEGIN PRIVATE KEY-----", end = "-----END PRIVATE KEY-----";
        if (!text.startsWith(begin) || !text.endsWith(end)) { throw unavailable(); }
        final var body = text.substring(begin.length(), text.length() - end.length()).replaceAll("\\s", "");
        final var der = Base64.getDecoder().decode(body);
        final String algorithm = leaf.getPublicKey().getAlgorithm();
        final PrivateKey key = KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(der));
        final String signing = algorithm.equals("RSA") ? "SHA256withRSA" : "SHA256withECDSA";
        final byte[] challenge = new byte[32]; new SecureRandom().nextBytes(challenge);
        final var sign = Signature.getInstance(signing); sign.initSign(key); sign.update(challenge);
        final var verify = Signature.getInstance(signing); verify.initVerify(leaf); verify.update(challenge);
        if (!verify.verify(sign.sign())) { throw unavailable(); }
    }

    private static List<X509Certificate> certificates(byte[] pem, int maximum) throws Exception {
        for (byte value : pem) { if (value < 0) { throw unavailable(); } }
        final var text = new String(pem, StandardCharsets.US_ASCII);
        final var blocks = Pattern.compile("-----BEGIN CERTIFICATE-----\\s*([A-Za-z0-9+/=\\r\\n]+)\\s*-----END CERTIFICATE-----").matcher(text);
        final var result = new ArrayList<X509Certificate>();
        int end = 0;
        while (blocks.find()) {
            if (!text.substring(end, blocks.start()).isBlank() && end != blocks.start()
                    || result.size() >= maximum) { throw unavailable(); }
            final byte[] der = Base64.getDecoder().decode(blocks.group(1).replaceAll("\\s", ""));
            final var cert = (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(new ByteArrayInputStream(der));
            if (!Arrays.equals(der, cert.getEncoded())) { throw unavailable(); }
            result.add(cert); end = blocks.end();
        }
        if (result.isEmpty() || !text.substring(end).isBlank() && end != text.length()) { throw unavailable(); }
        return List.copyOf(result);
    }

    /** No private reference/path/material is attached to the public diagnostic. */
    private static IllegalStateException unavailable() { return new IllegalStateException("local mTLS activation unavailable"); }

    /** Privately created proof of the parsed snapshot, not Broker ACL or Source/SEND authorization. */
    public static final class Receipt {
        private final CredentialBinding binding;
        private final CredentialUseLease lease;
        private final Version version;
        private final byte[] serverTrust, policy;
        private final CanonicalTargetPartition physical;
        private final EndpointPolicy endpoints;
        private final List<FileImage> proofs;
        private Receipt(CredentialBinding binding, CredentialUseLease lease, Version version,
                byte[] serverTrust, byte[] policy, CanonicalTargetPartition physical, EndpointPolicy endpoints, List<FileImage> proofs) {
            this.binding = binding; this.lease = lease; this.version = version;
            this.serverTrust = serverTrust; this.policy = policy; this.physical = physical;
            this.endpoints = endpoints;
            this.proofs = proofs;
        }
        public void requireBinding(CredentialBinding original) {
            if (!binding.equals(original)) { throw unavailable(); }
        }
        public void requireChannel(TargetChannelIdentity channel, TrustedUtcIntervalEvidence time) {
            if (!Arrays.equals(channel.credentialLease().canonicalBytes(), lease.canonicalBytes())
                    || !channel.context().target().equals(physical.id())) { throw unavailable(); }
            lease.requireActiveAt(time);
        }
        public boolean sameMaterial(Receipt other) {
            return other != null && binding.equals(other.binding) && version.same(other.version)
                    && Arrays.equals(policy, other.policy);
        }
        public void requireActive(TrustedUtcIntervalEvidence time) { lease.requireActiveAt(time); }
        public void requirePulsarEndpoint(String endpoint) {
            if (!endpoints.pulsarServiceUrls().contains(endpoint)) { throw unavailable(); }
        }
        public void requireKafkaBootstrap(Object endpoint) {
            final String configured = endpoint instanceof List<?> values
                    ? String.join(",", values.stream().map(value -> (String) value).toList()) : (String) endpoint;
            if (!endpoints.kafkaBootstrapServers().contains(configured)) { throw unavailable(); }
        }
        public byte[] certificateChainPem() { return Bytes.copy(version.certs().bytes()); }
        public byte[] privateKeyPem() { return Bytes.copy(version.key().bytes()); }
        public byte[] serverTrustPem() { return Bytes.copy(serverTrust); }
        public long retainedBytes() { return version.bytes() + serverTrust.length + proofs.stream().mapToLong(proof -> proof.bytes().length).sum(); }
        @Override public String toString() { return "ResolvedBrokerMtlsReceipt"; }
    }

    /** Actual recovery-only material; the native owner keeps this hold until its original SDK and late I/O exit. */
    public static final class TargetRecoveryReceipt implements AutoCloseable {
        private final TargetRecoveryCredentialAuthorization authorization;
        private final TargetRecoveryCredentialAuthorization.Trust trust;
        private final TargetRecoveryCredentialAuthorization.Hold hold;
        private final TargetDestinationSdkBudget.Client budget;
        private final Version version;
        private final byte[] serverTrust;
        private final EndpointPolicy endpoints;
        private final List<FileImage> proofs;
        private boolean stopping, closed;
        private TargetRecoveryReceipt(TargetRecoveryCredentialAuthorization authorization,
                TargetRecoveryCredentialAuthorization.Trust trust, TargetRecoveryCredentialAuthorization.Hold hold,
                TargetDestinationSdkBudget.Client budget,
                Version version, byte[] serverTrust, EndpointPolicy endpoints, List<FileImage> proofs) {
            this.authorization = authorization; this.trust = trust; this.hold = hold; this.budget = budget; this.version = version;
            this.serverTrust = serverTrust; this.endpoints = endpoints; this.proofs = proofs;
        }
        public TargetRecoveryCredentialAuthorization authorization() { return authorization; }
        public synchronized void requireCurrent(com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping,
                TrustedUtcIntervalEvidence time) {
            if (stopping || budget.released()) throw unavailable();
            authorization.requireMapping(mapping); trust.requireHeld(authorization, hold, time);
        }
        public void requireKafkaBootstrap(Object endpoint) {
            final String configured = endpoint instanceof List<?> values
                    ? String.join(",", values.stream().map(value -> (String) value).toList()) : (String) endpoint;
            if (!endpoints.kafkaBootstrapServers().contains(configured)) throw unavailable();
        }
        public byte[] certificateChainPem() { return Bytes.copy(version.certs().bytes()); }
        public byte[] privateKeyPem() { return Bytes.copy(version.key().bytes()); }
        public byte[] serverTrustPem() { return Bytes.copy(serverTrust); }
        /** Public references omit private material paths, raw probe requests and every private-key byte. */
        public byte[] publicEvidence() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, 1);
                CanonicalProtobuf.bytes(out, 2, authorization.digest());
                CanonicalProtobuf.bytes(out, 3, authorization.scopeImageDigest());
                CanonicalProtobuf.bytes(out, 4, authorization.evidenceDigest());
                CanonicalProtobuf.bytes(out, 5, authorization.bindingId());
                CanonicalProtobuf.uint64Bits(out, 6, authorization.credentialGeneration());
                CanonicalProtobuf.bytes(out, 7, authorization.referenceDigest());
                CanonicalProtobuf.bytes(out, 8, authorization.fingerprint());
                CanonicalProtobuf.bytes(out, 9, authorization.principalHash());
                CanonicalProtobuf.bytes(out, 10, authorization.policyDigest());
            });
        }
        public long retainedBytes() {
            return version.bytes() + serverTrust.length + proofs.stream().mapToLong(p -> p.bytes().length).sum();
        }
        public synchronized boolean closeCompleted() { return closed; }
        /** Caller must first prove actual native/late-I/O termination; expiry alone never releases protection. */
        @Override public synchronized void close() {
            stopping = true;
            if (!closed) { hold.close(); budget.closed(); closed = true; }
        }
        @Override public String toString() { return "ResolvedTargetRecoveryBrokerMtlsReceipt"; }
    }

    /** Actual Source material, separately authenticated from every Destination permission. */
    public static final class SourceReceipt {
        private final SourceTransportCredentialAuthorization authorization;
        private final SourceTransportCredentialAuthorization.Trust trust;
        private final Version version;
        private final byte[] serverTrust, policy;
        private final EndpointPolicy endpoints;
        private final List<FileImage> proofs;
        private SourceReceipt(SourceTransportCredentialAuthorization authorization, SourceTransportCredentialAuthorization.Trust trust,
                Version version, byte[] serverTrust, byte[] policy, EndpointPolicy endpoints, List<FileImage> proofs) {
            this.authorization = authorization; this.trust = trust; this.version = version;
            this.serverTrust = serverTrust; this.policy = policy; this.endpoints = endpoints;
            this.proofs = proofs;
        }
        public SourceTransportCredentialAuthorization authorization() { return authorization; }
        public void requireActive(TrustedUtcIntervalEvidence time) { trust.requireActive(); authorization.requireTime(time); }
        public void requireOperation(SourceTransportCredentialAuthorization.Operation operation, TrustedUtcIntervalEvidence time) {
            requireActive(time); authorization.requireOperation(operation);
        }
        public Runnable guardRead(SourceTransportCredentialAuthorization.Operation role,
                com.nereusstream.delay.protocol.SourcePosition position, String topic, String name,
                Supplier<TrustedUtcIntervalEvidence> time, Runnable heldOwnerAndRetention) {
            if (role != SourceTransportCredentialAuthorization.Operation.HISTORY
                    && role != SourceTransportCredentialAuthorization.Operation.OBSERVATION
                    && role != SourceTransportCredentialAuthorization.Operation.RECOVERY) { throw unavailable(); }
            authorization.requirePosition(position, topic); authorization.requireName(role, name);
            Objects.requireNonNull(heldOwnerAndRetention, "original reader Owner/retention protection");
            return () -> {
                requireOperation(role, time.get()); requireOperation(SourceTransportCredentialAuthorization.Operation.LOOKUP, time.get());
                heldOwnerAndRetention.run();
                requireOperation(role, time.get());
            };
        }
        /** Recovery begins before an active Worker exists; the original accepted assignment/Owner/Pin guard is mandatory. */
        public Runnable guardRecovery(com.nereusstream.delay.protocol.ShardId shard, String topic, String name,
                Supplier<TrustedUtcIntervalEvidence> time, Runnable heldAssignmentOwnerAndRetention) {
            if (!authorization.shard().equals(shard) || !authorization.physicalTopic().equals(topic)) { throw unavailable(); }
            authorization.requireName(SourceTransportCredentialAuthorization.Operation.RECOVERY, name);
            Objects.requireNonNull(heldAssignmentOwnerAndRetention, "original recovery assignment/Owner/retention protection");
            return () -> {
                requireOperation(SourceTransportCredentialAuthorization.Operation.RECOVERY, time.get());
                requireOperation(SourceTransportCredentialAuthorization.Operation.LOOKUP, time.get());
                heldAssignmentOwnerAndRetention.run();
                requireOperation(SourceTransportCredentialAuthorization.Operation.RECOVERY, time.get());
            };
        }
        public void requireKafkaBootstrap(Object endpoint) {
            final String configured = endpoint instanceof List<?> values
                    ? String.join(",", values.stream().map(value -> (String) value).toList()) : (String) endpoint;
            if (!endpoints.kafkaBootstrapServers().contains(configured)) { throw unavailable(); }
        }
        public void requirePulsarEndpoint(String endpoint) { if (!endpoints.pulsarServiceUrls().contains(endpoint)) { throw unavailable(); } }
        public boolean sameMaterial(Receipt destination) {
            return destination != null && version.same(destination.version) && Arrays.equals(policy, destination.policy);
        }
        public boolean sameMaterial(SourceReceipt source) {
            return source != null && version.same(source.version) && Arrays.equals(policy, source.policy);
        }
        public byte[] certificateChainPem() { return Bytes.copy(version.certs().bytes()); }
        public byte[] privateKeyPem() { return Bytes.copy(version.key().bytes()); }
        public byte[] serverTrustPem() { return Bytes.copy(serverTrust); }
        @Override public String toString() { return "ResolvedSourceBrokerMtlsReceipt"; }
    }

    private record FileImage(byte[] bytes, Object fileKey, long size, java.nio.file.attribute.FileTime modified) {
        private boolean same(FileImage other) {
            return fileKey.equals(other.fileKey) && size == other.size && modified.equals(other.modified)
                    && Arrays.equals(bytes, other.bytes);
        }
        @Override public String toString() { return "PrivateMtlsFileImage"; }
    }
    private record Version(FileImage certs, FileImage key, byte[] fingerprint, long bytes,
            List<Object> directories) {
        private boolean same(Version other) {
            return certs.same(other.certs) && key.same(other.key)
                    && Arrays.equals(fingerprint, other.fingerprint) && directories.equals(other.directories);
        }
        @Override public String toString() { return "PrivateMtlsVersion"; }
    }

    private static FileImage readAbsolute(Path path, int bound) throws Exception {
        final var exact = path.toAbsolutePath().normalize();
        try (var directory = Directory.open(exact.getParent())) { return directory.read(exact.getFileName().toString(), bound); }
    }
    private static final class Directory implements java.io.Closeable {
        private final List<SecureDirectoryStream<Path>> opened;
        private Directory(List<SecureDirectoryStream<Path>> opened) { this.opened = opened; }
        private static Directory open(Path absolute) throws Exception {
            final var opened = new ArrayList<SecureDirectoryStream<Path>>();
            try {
                final DirectoryStream<Path> initial = Files.newDirectoryStream(absolute.getRoot());
                if (!(initial instanceof SecureDirectoryStream<Path> secure)) { initial.close(); throw unavailable(); }
                opened.add(secure);
                if (absolute.getNameCount() > 64) { throw unavailable(); }
                for (var part : absolute) { opened.add(opened.getLast().newDirectoryStream(part, LinkOption.NOFOLLOW_LINKS)); }
                return new Directory(opened);
            } catch (Exception failure) {
                for (var stream : opened.reversed()) { try { stream.close(); } catch (Exception ignored) { /* original failure */ } }
                throw failure;
            }
        }
        private FileImage read(String name, int bound) throws Exception {
            final var directory = opened.getLast(); final var relative = Path.of(name);
            final var attributes = directory.getFileAttributeView(relative, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
            final BasicFileAttributes before = attributes.readAttributes();
            if (!before.isRegularFile() || before.fileKey() == null || before.size() <= 0 || before.size() > bound) { throw unavailable(); }
            final var bytes = new ByteArrayOutputStream(Math.toIntExact(before.size()));
            try (SeekableByteChannel channel = directory.newByteChannel(relative,
                    Set.of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS))) {
                final var buffer = ByteBuffer.allocate(Math.min(bound, 8192));
                int count;
                while ((count = channel.read(buffer)) != -1) {
                    if (count == 0) { throw unavailable(); }
                    buffer.flip();
                    if (buffer.remaining() > bound - bytes.size()) { throw unavailable(); }
                    bytes.write(buffer.array(), 0, buffer.remaining()); buffer.clear();
                }
            }
            final var after = attributes.readAttributes();
            if (!before.fileKey().equals(after.fileKey()) || before.size() != after.size()
                    || !before.lastModifiedTime().equals(after.lastModifiedTime()) || bytes.size() != before.size()) { throw unavailable(); }
            return new FileImage(bytes.toByteArray(), before.fileKey(), before.size(), before.lastModifiedTime());
        }
        private List<Object> identities() throws IOException {
            final var identities = new ArrayList<Object>();
            for (var directory : opened) {
                identities.add(Objects.requireNonNull(directory.getFileAttributeView(BasicFileAttributeView.class)
                        .readAttributes().fileKey(), "controlled directory identity"));
            }
            return List.copyOf(identities);
        }
        @Override public void close() throws IOException {
            IOException failure = null;
            for (var stream : opened.reversed()) {
                try { stream.close(); } catch (IOException caught) { if (failure == null) { failure = caught; } else { failure.addSuppressed(caught); } }
            }
            if (failure != null) { throw failure; }
        }
    }
}
