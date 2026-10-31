package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.KafkaReceiptSlotResource;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetKafkaReceiptMapping;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.CredentialAttestationTrustSet;
import java.nio.charset.StandardCharsets;
import java.security.Signature;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Signed private deployment input for original K2 fencing/read recovery, never SEND or a Source command. */
public final class TargetRecoveryCredentialAuthorization {
    public static final int MAX_BYTES = 32 * 1024;
    /** Closed version-1 purpose: guarded lookup, original transactional.id fencing and receipt read only. */
    public static final int OPERATIONS = 7;

    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-recovery-credential\0");
    private static final byte[] SIGNATURE_DOMAIN = Bytes.utf8("nereus-delay-target-recovery-credential-signature\0");
    private final List<CanonicalProtobuf.Reader.Field> fields;
    private final byte[] encoded, digest;
    private final TargetChannelIdentity channel;
    private final CanonicalTargetPartition target;
    private final KafkaReceiptSlotResource receipt;
    private final TrustedUtcIntervalEvidence verifiedAt;

    private TargetRecoveryCredentialAuthorization(byte[] bytes) {
        if (bytes.length == 0 || bytes.length > MAX_BYTES) {
            throw unavailable();
        }
        fields = QueryCodecSupport.read(bytes, "private Target recovery authorization");
        final int[] numbers = new int[31];
        for (int i = 0; i < numbers.length; i++) {
            numbers[i] = i + 1;
        }
        QueryCodecSupport.requireNumbers(fields, numbers, "private Target recovery authorization");
        if (number(1) != 1
                || number(4) == 0
                || number(17) == 0
                || number(26) != OPERATIONS
                || number(28) == 0
                || number(29) == 0) {
            throw unavailable();
        }
        for (int n : new int[] {2, 3, 5, 6, 16, 19, 20, 21, 22, 25, 30}) {
            Bytes.requireLength(value(n), 32, "Target recovery digest");
            nonzero(value(n));
        }
        if (value(15).length != 0 && value(15).length != 32) {
            throw unavailable();
        }
        if (value(15).length != 0) {
            nonzero(value(15));
        }
        channel = TargetChannelIdentity.decode(value(7));
        target = CanonicalTargetPartition.decode(value(8));
        receipt = KafkaReceiptSlotResource.decode(value(10));
        final var c = channel.context();
        if (c.kind() != ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT
                || target.resource().kind() != BrokerResourceIdentity.Kind.KAFKA
                || !target.id().equals(c.target())
                || !Arrays.equals(c.controlScopeRef(), value(6))
                || !Arrays.equals(c.producerIdentity(), value(14))
                || !receipt.routeIncarnation().equals(c.sourceShard().routeIncarnation())
                || receipt.shardPartition() != c.sourceShard().partition()
                || !receipt.authenticatedClusterId()
                        .equals(target.resource().kafka().authenticatedClusterId())) {
            throw unavailable();
        }
        topic(9);
        topic(11);
        text(27, 128);
        if (value(18).length == 0 || value(18).length > 128 || !Arrays.equals(Bytes.sha256(value(18)), value(19))) {
            throw unavailable();
        }
        final long slots = number(12), partition = number(13);
        if (slots < 1
                || slots > Integer.MAX_VALUE
                || partition > Integer.MAX_VALUE
                || receipt.receiptLaneSlot() < 0
                || receipt.receiptLaneSlot() >= slots
                || Math.addExact(
                                Math.multiplyExact(Integer.toUnsignedLong(receipt.shardPartition()), slots),
                                receipt.receiptLaneSlot())
                        != partition) {
            throw unavailable();
        }
        verifiedAt = TrustedUtcIntervalEvidence.decode(value(23));
        if (number(24) <= verifiedAt.latestEpochMs()) {
            throw unavailable();
        }
        digest = Bytes.sha256(DOMAIN, canonical(29));
        if (!Arrays.equals(digest, value(30)) || value(31).length != 64 || !Arrays.equals(bytes, canonical(31))) {
            throw unavailable();
        }
        encoded = Bytes.copy(bytes);
    }

    /** Decoding validates only shape; activation requires explicit recovery issuer trust and a live material hold. */
    public static TargetRecoveryCredentialAuthorization decode(byte[] bytes) {
        try {
            return new TargetRecoveryCredentialAuthorization(bytes);
        } catch (RuntimeException failure) {
            throw unavailable();
        }
    }

    private byte[] canonical(int through) {
        return CanonicalProtobuf.message(out -> {
            for (int n = 1; n <= through; n++) {
                if (Set.of(1, 12, 13, 26, 28, 29).contains(n)) {
                    CanonicalProtobuf.uint32Bits(out, n, QueryCodecSupport.uint32(fields.get(n - 1), n));
                } else if (n == 4 || n == 17) {
                    CanonicalProtobuf.uint64Bits(out, n, number(n));
                } else if (n == 24) {
                    CanonicalProtobuf.int64(out, n, number(n));
                } else {
                    CanonicalProtobuf.bytes(out, n, value(n));
                }
            }
        });
    }

    private byte[] value(int n) {
        return QueryCodecSupport.bytes(fields.get(n - 1), n);
    }

    private long number(int n) {
        return QueryCodecSupport.uint64Bits(fields.get(n - 1), n);
    }

    private String text(int n, int maximum) {
        final byte[] bytes = value(n);
        final String text = new String(bytes, StandardCharsets.UTF_8);
        if (bytes.length > maximum
                || text.isBlank()
                || !Arrays.equals(bytes, text.getBytes(StandardCharsets.UTF_8))
                || !Normalizer.isNormalized(text, Normalizer.Form.NFC)
                || text.indexOf('\0') >= 0
                || !text.equals(text.trim())) {
            throw unavailable();
        }
        return text;
    }

    private String topic(int n) {
        final String name = text(n, 249);
        if (!name.matches("[a-zA-Z0-9._-]{1,249}") || name.equals(".") || name.equals("..")) {
            throw unavailable();
        }
        return name;
    }

    private static void nonzero(byte[] bytes) {
        for (byte b : bytes) {
            if (b != 0) {
                return;
            }
        }
        throw unavailable();
    }

    public byte[] canonicalBytes() {
        return Bytes.copy(encoded);
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] scopeImageDigest() {
        return Bytes.sha256(canonical(24));
    }

    public byte[] reference() {
        return Bytes.copy(value(18));
    }

    public byte[] referenceDigest() {
        return Bytes.copy(value(19));
    }

    public byte[] bindingId() {
        return Bytes.copy(value(16));
    }

    public long credentialGeneration() {
        return number(17);
    }

    public byte[] fingerprint() {
        return Bytes.copy(value(20));
    }

    public byte[] principalHash() {
        return Bytes.copy(value(21));
    }

    public byte[] policyDigest() {
        return Bytes.copy(value(22));
    }

    public byte[] evidenceDigest() {
        return Bytes.copy(value(25));
    }

    String historyScope() {
        return Bytes.hex(value(2)) + ":" + Bytes.hex(value(3)) + ":" + Long.toUnsignedString(number(4)) + ":"
                + Bytes.hex(value(5));
    }

    public long notBeforeEpochMs() {
        return verifiedAt.earliestEpochMs();
    }

    public long notAfterEpochMs() {
        return number(24);
    }

    public void requireTime(TrustedUtcIntervalEvidence time) {
        if (time.earliestEpochMs() < verifiedAt.latestEpochMs() || time.latestEpochMs() >= notAfterEpochMs()) {
            throw unavailable();
        }
    }

    /**
     * Equality is a constraint on a later private runtime receipt, never proof of pending Source/Owner/retention
     * rights.
     */
    public void requireMapping(TargetKafkaReceiptMapping mapping) {
        final var admitted = Objects.requireNonNull(mapping).admission();
        final var binding = admitted.kafkaReceipt();
        channel.requireExactFrozenIdentity(admitted.channel());
        if (!Arrays.equals(
                        target.canonicalBytes(),
                        admitted.publication().physical().canonicalBytes())
                || !topic(9).equals(binding.targetPhysicalTopic())
                || !receipt.equals(binding.receiptResource())
                || !topic(11).equals(binding.receiptPhysicalTopic())
                || number(12) != binding.slotsPerShard()
                || number(13) != binding.receiptPartition()
                || value(15).length != 0 && !Arrays.equals(value(15), Bytes.sha256(mapping.canonicalBytes()))) {
            throw unavailable();
        }
    }
    /**
     * Actual Source/Owner providers supply these values; a separately valid deployment authorization cannot cross
     * realms.
     */
    public void requireRuntimeScope(OwnerIdentity currentOwner, byte[] actualTenantScope) {
        if (!Arrays.equals(value(2), Objects.requireNonNull(currentOwner).deploymentId())
                || !Arrays.equals(value(5), actualTenantScope)) {
            throw unavailable();
        }
    }

    @Override
    public String toString() {
        return "PrivateTargetRecoveryAuthorization";
    }

    private static IllegalStateException unavailable() {
        return new IllegalStateException("Target recovery credential unavailable");
    }

    /** Explicit TARGET_RECOVERY purpose allowlist over the original verifier trust set, never all its keys. */
    public record Issuer(String verifierId, int verifierVersion, int signingKeyVersion) {
        public Issuer {
            if (verifierId == null
                    || !verifierId.matches("[A-Za-z0-9._-]{1,128}")
                    || verifierVersion == 0
                    || signingKeyVersion == 0) {
                throw unavailable();
            }
        }
    }

    /** Actual deployment material authority acquires an authenticated, retained version on the owning I/O path. */
    @FunctionalInterface
    public interface MaterialAuthority {
        Hold acquire(TargetRecoveryCredentialAuthorization authorization);
    }
    /**
     * Owns actual current permission/probe and immutable-version retirement protection, independent of the SEND lease.
     * Retirement protection stays held until explicit close, including after authorization expires; expiry only stops
     * use.
     * The deployment authority owns partial-acquisition cleanup if acquire cannot return this original handle.
     */
    public interface Hold extends AutoCloseable {
        byte[] authorizationDigest();

        byte[] evidenceDigest();

        byte[] principalHash();

        long retainedUntilEpochMs();
        /**
         * Revalidate actual authenticated recovery permission, revocation and the original still-held material version.
         */
        void requireCurrent();
        /** Retry the same original close after failure; successful close must be idempotent. */
        @Override
        void close();
    }

    public static final class Trust {
        private final byte[] deployment, configuration, tenant;
        private final long generation;
        private final CredentialAttestationTrustSet keys;
        private final Set<Issuer> issuers;
        private final MaterialAuthority authority;
        private volatile boolean active = true;

        public Trust(
                byte[] deployment,
                byte[] configuration,
                long generation,
                byte[] tenantScope,
                CredentialAttestationTrustSet keys,
                Set<Issuer> recoveryIssuers,
                MaterialAuthority authority) {
            for (byte[] b : List.of(deployment, configuration, tenantScope)) {
                Bytes.requireLength(b, 32, "recovery deployment scope");
                nonzero(b);
            }
            if (generation == 0 || recoveryIssuers.isEmpty() || recoveryIssuers.size() > 1024) {
                throw unavailable();
            }
            this.deployment = Bytes.copy(deployment);
            this.configuration = Bytes.copy(configuration);
            this.tenant = Bytes.copy(tenantScope);
            this.generation = generation;
            this.keys = Objects.requireNonNull(keys);
            issuers = Set.copyOf(recoveryIssuers);
            this.authority = Objects.requireNonNull(authority, "actual recovery material/permission authority");
        }

        public void invalidate() {
            active = false;
        }

        String historyScope() {
            return Bytes.hex(deployment) + ":" + Bytes.hex(configuration) + ":" + Long.toUnsignedString(generation)
                    + ":" + Bytes.hex(tenant);
        }

        void verify(TargetRecoveryCredentialAuthorization a, TrustedUtcIntervalEvidence time) throws Exception {
            if (!active) {
                throw unavailable();
            }
            verifyHistorical(a, time);
            if (!active) {
                throw unavailable();
            }
        }
        /**
         * Original observation-time signature/scope check; today's invalidation never rewrites historical
         * authorization.
         */
        public void verifyHistorical(TargetRecoveryCredentialAuthorization a, TrustedUtcIntervalEvidence time)
                throws Exception {
            a.requireTime(time);
            final var issuer = new Issuer(a.text(27, 128), (int) a.number(28), (int) a.number(29));
            if (!issuers.contains(issuer)
                    || !Arrays.equals(deployment, a.value(2))
                    || !Arrays.equals(configuration, a.value(3))
                    || generation != a.number(4)
                    || !Arrays.equals(tenant, a.value(5))) {
                throw unavailable();
            }
            final var key = keys.keys().stream()
                    .filter(k -> k.verifierVersion() == issuer.verifierVersion()
                            && k.signingKeyVersion() == issuer.signingKeyVersion()
                            && Arrays.equals(k.verifierId(), Bytes.utf8(issuer.verifierId())))
                    .findFirst()
                    .orElseThrow(TargetRecoveryCredentialAuthorization::unavailable);
            if (a.notBeforeEpochMs() < key.verifyNotBeforeEpochMs()
                    || a.notAfterEpochMs() > key.verifyNotAfterEpochMs()) {
                throw unavailable();
            }
            final var signature = Signature.getInstance("Ed25519");
            signature.initVerify(key.publicKey());
            signature.update(Bytes.sha256(SIGNATURE_DOMAIN, a.digest, Bytes.u32beBits(issuer.signingKeyVersion())));
            if (!signature.verify(a.value(31))) {
                throw unavailable();
            }
        }
        /**
         * Exact public approval selected at the observation; no material authority, paths or private bytes transfer.
         */
        byte[] historicalPublicInputs(TargetRecoveryCredentialAuthorization a, TrustedUtcIntervalEvidence time)
                throws Exception {
            verifyHistorical(a, time);
            final var key = keys.keys().stream()
                    .filter(k -> k.verifierVersion() == (int) a.number(28)
                            && k.signingKeyVersion() == (int) a.number(29)
                            && Arrays.equals(k.verifierId(), Bytes.utf8(a.text(27, 128))))
                    .findFirst()
                    .orElseThrow(TargetRecoveryCredentialAuthorization::unavailable);
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, 1);
                CanonicalProtobuf.bytes(out, 2, deployment);
                CanonicalProtobuf.bytes(out, 3, configuration);
                CanonicalProtobuf.uint64Bits(out, 4, generation);
                CanonicalProtobuf.bytes(out, 5, tenant);
                CanonicalProtobuf.bytes(out, 6, key.verifierId());
                CanonicalProtobuf.uint32Bits(out, 7, key.verifierVersion());
                CanonicalProtobuf.uint32Bits(out, 8, key.signingKeyVersion());
                CanonicalProtobuf.bytes(out, 9, key.publicKey().getEncoded());
                CanonicalProtobuf.uint64(out, 10, key.verifyNotBeforeEpochMs());
                CanonicalProtobuf.uint64(out, 11, key.verifyNotAfterEpochMs());
            });
        }

        Hold acquire(TargetRecoveryCredentialAuthorization a, TrustedUtcIntervalEvidence time) throws Exception {
            verify(a, time);
            return Objects.requireNonNull(authority.acquire(a), "actual retained recovery material");
        }

        void requireHeld(TargetRecoveryCredentialAuthorization a, Hold hold, TrustedUtcIntervalEvidence time) {
            if (!active) {
                throw unavailable();
            }
            a.requireTime(time);
            hold.requireCurrent();
            if (!Arrays.equals(a.digest, hold.authorizationDigest())
                    || !Arrays.equals(a.evidenceDigest(), hold.evidenceDigest())
                    || !Arrays.equals(a.principalHash(), hold.principalHash())
                    || hold.retainedUntilEpochMs() < a.notAfterEpochMs()
                    || !active) {
                throw unavailable();
            }
        }
    }
}
