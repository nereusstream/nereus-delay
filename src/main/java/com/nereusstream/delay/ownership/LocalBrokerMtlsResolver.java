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
            final var loaded = new Version(certs, key, evidence, fingerprint, bytes, directoryKeys);
            final Version snapshot;
            synchronized (this) {
                final var previous = versions.get(id + '/' + version);
                if (previous != null) {
                    if (!previous.same(loaded)) { throw unavailable(); }
                    snapshot = previous;
                } else {
                    if (versions.size() >= limits.maximumVersions()
                            || bytes > limits.maximumRetainedBytes() - retainedBytes) { throw unavailable(); }
                    versions.put(id + '/' + version, loaded); retainedBytes += bytes; snapshot = loaded;
                }
            }
            return new Receipt(binding, lease, snapshot, serverTrustPem, policyDigest, physical, endpoints);
        } catch (Exception failure) { throw unavailable(); }
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
        for (var cert : chain) {
            cert.checkValidity(new Date(lease.issuedAt().earliestEpochMs()));
            cert.checkValidity(new Date(lease.validUntilEpochMs()));
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
        params.setRevocationEnabled(false); params.setDate(new Date(lease.issuedAt().latestEpochMs()));
        final var validated = (java.security.cert.PKIXCertPathValidatorResult) CertPathValidator.getInstance("PKIX")
                .validate(CertificateFactory.getInstance("X.509").generateCertPath(path), params);
        validated.getTrustAnchor().getTrustedCert().checkValidity(new Date(lease.issuedAt().earliestEpochMs()));
        validated.getTrustAnchor().getTrustedCert().checkValidity(new Date(lease.validUntilEpochMs()));
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
        private Receipt(CredentialBinding binding, CredentialUseLease lease, Version version,
                byte[] serverTrust, byte[] policy, CanonicalTargetPartition physical, EndpointPolicy endpoints) {
            this.binding = binding; this.lease = lease; this.version = version;
            this.serverTrust = serverTrust; this.policy = policy; this.physical = physical;
            this.endpoints = endpoints;
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
        public long retainedBytes() { return version.bytes() + serverTrust.length; }
        @Override public String toString() { return "ResolvedBrokerMtlsReceipt"; }
    }

    private record FileImage(byte[] bytes, Object fileKey, long size, java.nio.file.attribute.FileTime modified) {
        private boolean same(FileImage other) {
            return fileKey.equals(other.fileKey) && size == other.size && modified.equals(other.modified)
                    && Arrays.equals(bytes, other.bytes);
        }
        @Override public String toString() { return "PrivateMtlsFileImage"; }
    }
    private record Version(FileImage certs, FileImage key, FileImage evidence, byte[] fingerprint, long bytes,
            List<Object> directories) {
        private boolean same(Version other) {
            return certs.same(other.certs) && key.same(other.key) && evidence.same(other.evidence)
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
