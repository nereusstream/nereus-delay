package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ControlRole;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.Limits;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;

/**
 * Independent signed current policy for Catalog metadata initialization; ordinary management grants cannot supply it.
 */
public final class TargetCatalogRootAuthority {
    public static final int MAX_POLICY_BYTES = 8192;
    private static final byte[] POLICY_DOMAIN = Bytes.utf8("nereus-delay-catalog-root-current-policy-v1\0");
    private static final byte[] SIGN_DOMAIN = Bytes.utf8("nereus-delay-catalog-root-current-policy-signature-v1\0");

    public enum Purpose {
        INITIALIZE_FRESH_CATALOG_ROOT,
        ADOPT_EXISTING_CATALOG_ROOT_GUARD
    }

    /** Data only. The Catalog reconstructs every storage/deployment/capacity field from its original SDK and Root. */
    public record Scope(
            byte[] storageNamespace,
            String prefix,
            String rootKey,
            String guardKey,
            byte[] deploymentRoot,
            Limits limits,
            byte[] allocation,
            byte[] nonce) {
        public Scope {
            storageNamespace = fixed(storageNamespace, 32);
            deploymentRoot = fixed(deploymentRoot, 32);
            allocation = fixed(allocation, 32);
            nonce = fixed(nonce, 32);
            Objects.requireNonNull(limits);
            if (Arrays.equals(allocation, new byte[32]) || Arrays.equals(nonce, new byte[32])) {
                throw new IllegalArgumentException("unassigned original Catalog allocation/nonce");
            }
            Objects.requireNonNull(prefix);
            Objects.requireNonNull(rootKey);
            Objects.requireNonNull(guardKey);
            if (prefix.isBlank()
                    || prefix.endsWith("/")
                    || prefix.indexOf('\0') >= 0
                    || Bytes.utf8(prefix).length > 1024
                    || !java.text.Normalizer.isNormalized(prefix, java.text.Normalizer.Form.NFC)
                    || !rootKey.equals(prefix + "/target-kafka-receipt-storage")
                    || !guardKey.equals(prefix + "/target-kafka-receipt-storage-guard")) {
                throw new IllegalArgumentException("Catalog allocation changes its exact canonical root/guard keys");
            }
        }

        @Override
        public byte[] storageNamespace() {
            return Bytes.copy(storageNamespace);
        }

        @Override
        public byte[] deploymentRoot() {
            return Bytes.copy(deploymentRoot);
        }

        @Override
        public byte[] allocation() {
            return Bytes.copy(allocation);
        }

        @Override
        public byte[] nonce() {
            return Bytes.copy(nonce);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(o -> {
                CanonicalProtobuf.uint32(o, 1, 1);
                CanonicalProtobuf.bytes(o, 2, storageNamespace);
                CanonicalProtobuf.bytes(o, 3, Bytes.utf8(prefix));
                CanonicalProtobuf.bytes(o, 4, Bytes.utf8(rootKey));
                CanonicalProtobuf.bytes(o, 5, Bytes.utf8(guardKey));
                CanonicalProtobuf.bytes(o, 6, deploymentRoot);
                CanonicalProtobuf.uint64(o, 7, limits.physicalBytes());
                CanonicalProtobuf.uint32(o, 8, limits.physicalRecords());
                CanonicalProtobuf.uint32(o, 9, limits.maximumPublications());
                CanonicalProtobuf.uint32(o, 10, limits.maximumTopics());
                CanonicalProtobuf.uint32(o, 11, limits.maximumPreparedReferences());
                CanonicalProtobuf.bytes(o, 12, limits.capacitySourceDigest());
                CanonicalProtobuf.bytes(o, 13, allocation);
                CanonicalProtobuf.bytes(o, 14, nonce);
            });
        }

        public static Scope decode(byte[] bytes) {
            final var f = fields(bytes, 14);
            version(f);
            final var value = new Scope(
                    QueryCodecSupport.fixed(f.get(1), 2, 32),
                    text(f.get(2), 3),
                    text(f.get(3), 4),
                    text(f.get(4), 5),
                    QueryCodecSupport.fixed(f.get(5), 6, 32),
                    new Limits(
                            QueryCodecSupport.uint(f.get(6), 7),
                            QueryCodecSupport.uint32(f.get(7), 8),
                            QueryCodecSupport.uint32(f.get(8), 9),
                            QueryCodecSupport.uint32(f.get(9), 10),
                            QueryCodecSupport.uint32(f.get(10), 11),
                            QueryCodecSupport.fixed(f.get(11), 12, 32)),
                    QueryCodecSupport.fixed(f.get(12), 13, 32),
                    QueryCodecSupport.fixed(f.get(13), 14, 32));
            QueryCodecSupport.requireCanonical(bytes, value.canonicalBytes(), "Catalog root scope");
            return value;
        }
    }

    /** A complete signed policy, not a bearer token or freshness certificate. No public key is carried by it. */
    public record Policy(
            Purpose purpose,
            Scope scope,
            AuthorIdentity issuer,
            int keyVersion,
            long policyVersion,
            byte[] actor,
            byte[] roles,
            byte[] actorScope,
            byte[] signature) {
        public Policy {
            Objects.requireNonNull(purpose);
            Objects.requireNonNull(scope);
            Objects.requireNonNull(issuer);
            actor = fixed(actor, 32);
            roles = fixed(roles, 32);
            actorScope = fixed(actorScope, 32);
            signature = fixed(signature, 64);
            if (keyVersion <= 0 || policyVersion == 0) {
                throw new IllegalArgumentException("unassigned Catalog purpose policy");
            }
        }

        @Override
        public byte[] actor() {
            return Bytes.copy(actor);
        }

        @Override
        public byte[] roles() {
            return Bytes.copy(roles);
        }

        @Override
        public byte[] actorScope() {
            return Bytes.copy(actorScope);
        }

        @Override
        public byte[] signature() {
            return Bytes.copy(signature);
        }

        private byte[] body() {
            return CanonicalProtobuf.message(o -> {
                CanonicalProtobuf.uint32(o, 1, 1);
                CanonicalProtobuf.uint32(o, 2, purpose.ordinal() + 1);
                CanonicalProtobuf.bytes(o, 3, scope.canonicalBytes());
                CanonicalProtobuf.bytes(o, 4, issuer.canonicalBytes());
                CanonicalProtobuf.uint32(o, 5, keyVersion);
                CanonicalProtobuf.uint64Bits(o, 6, policyVersion);
                CanonicalProtobuf.bytes(o, 7, actor);
                CanonicalProtobuf.bytes(o, 8, roles);
                CanonicalProtobuf.bytes(o, 9, actorScope);
            });
        }

        public byte[] digest() {
            return Bytes.sha256(POLICY_DOMAIN, body());
        }

        public byte[] signingBytes() {
            return Bytes.sha256(SIGN_DOMAIN, digest());
        }

        public byte[] canonicalBytes() {
            final byte[] value = CanonicalProtobuf.message(o -> {
                CanonicalProtobuf.bytes(o, 1, body());
                CanonicalProtobuf.bytes(o, 2, signature);
            });
            if (value.length > MAX_POLICY_BYTES) {
                throw new IllegalArgumentException("Catalog root policy exceeds its finite bound");
            }
            return value;
        }

        public static Policy decode(byte[] bytes) {
            final var outer = fields(bytes, 2);
            final var f = fields(QueryCodecSupport.bytes(outer.get(0), 1), 9);
            version(f);
            final int purpose = QueryCodecSupport.uint32(f.get(1), 2);
            if (purpose < 1 || purpose > Purpose.values().length) {
                throw new IllegalArgumentException("unknown Catalog root purpose");
            }
            final var value = new Policy(
                    Purpose.values()[purpose - 1],
                    Scope.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                    AuthorIdentity.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                    QueryCodecSupport.uint32(f.get(4), 5),
                    QueryCodecSupport.uint64Bits(f.get(5), 6),
                    QueryCodecSupport.fixed(f.get(6), 7, 32),
                    QueryCodecSupport.fixed(f.get(7), 8, 32),
                    QueryCodecSupport.fixed(f.get(8), 9, 32),
                    QueryCodecSupport.fixed(outer.get(1), 2, 64));
            QueryCodecSupport.requireCanonical(bytes, value.canonicalBytes(), "Catalog root policy");
            return value;
        }
    }

    /** Root-owned purpose allowlist. Policy material cannot add an issuer, key, purpose or deployment pin. */
    public record Pin(Purpose purpose, byte[] deploymentRoot, AuthorIdentity issuer, int keyVersion, PublicKey key) {
        public Pin {
            Objects.requireNonNull(purpose);
            deploymentRoot = fixed(deploymentRoot, 32);
            Objects.requireNonNull(issuer);
            Objects.requireNonNull(key);
            if (keyVersion <= 0 || key.getEncoded() == null || key.getEncoded().length != 44) {
                throw new IllegalArgumentException("Catalog root pin requires an independent Ed25519 key/version");
            }
        }

        @Override
        public byte[] deploymentRoot() {
            return Bytes.copy(deploymentRoot);
        }
    }

    private final EnumMap<Purpose, Pin> pins = new EnumMap<>(Purpose.class);

    public TargetCatalogRootAuthority(List<Pin> pins) {
        if (pins.isEmpty() || pins.size() > Purpose.values().length) {
            throw new IllegalArgumentException("finite explicit Catalog purpose pins required");
        }
        for (var pin : pins) {
            if (this.pins.putIfAbsent(pin.purpose(), pin) != null) {
                throw new IllegalArgumentException("duplicate Catalog root purpose pin");
            }
        }
    }

    public void requireHistory(Policy policy, Scope actual) {
        final var pin = pins.get(policy.purpose());
        if (pin == null
                || pin.keyVersion() != policy.keyVersion()
                || !Arrays.equals(pin.issuer().canonicalBytes(), policy.issuer().canonicalBytes())
                || !Arrays.equals(pin.deploymentRoot(), actual.deploymentRoot())
                || !Arrays.equals(actual.canonicalBytes(), policy.scope().canonicalBytes())) {
            throw new IllegalStateException(
                    "Catalog root policy differs from its independent purpose/issuer/deployment/allocation pins");
        }
        try {
            final var verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(pin.key());
            verifier.update(policy.signingBytes());
            if (!verifier.verify(policy.signature())) {
                throw new IllegalStateException("Catalog root purpose signature is invalid");
            }
            policy.canonicalBytes();
        } catch (GeneralSecurityException invalid) {
            throw new IllegalStateException("invalid Catalog root purpose key", invalid);
        }
    }
    /** The authenticator is the actual live view of this exact signed root policy, not the ordinary topic policy. */
    public Permit acquire(Policy policy, Scope actual, TargetReceiptCurrentManagement.Authenticator authenticator) {
        requireHistory(policy, actual);
        final var permit = new Permit(this, policy, Objects.requireNonNull(authenticator.authenticate()));
        permit.requireCurrent(actual);
        return permit;
    }

    public static final class Permit {
        private final TargetCatalogRootAuthority authority;
        private final Policy policy;
        private final TargetReceiptCurrentManagement.Snapshot snapshot;

        private Permit(
                TargetCatalogRootAuthority authority, Policy policy, TargetReceiptCurrentManagement.Snapshot snapshot) {
            this.authority = authority;
            this.policy = policy;
            this.snapshot = snapshot;
        }

        public Policy policy() {
            return policy;
        }

        public void requireAuthority(TargetCatalogRootAuthority expected) {
            if (authority != expected) {
                throw new IllegalArgumentException("Catalog purpose permit belongs to another original authority");
            }
        }

        public void requireCurrent(Scope actual) {
            authority.requireHistory(policy, actual);
            snapshot.requireCurrent();
            final var actor = Objects.requireNonNull(snapshot.context());
            if (snapshot.policyVersion() != policy.policyVersion()
                    || !Arrays.equals(snapshot.policyDigest(), policy.digest())
                    || !actor.roleSet().contains(ControlRole.PLATFORM_OPERATOR)
                    || !Arrays.equals(actor.actorIdHash(), policy.actor())
                    || !Arrays.equals(actor.roleSet().digest(), policy.roles())
                    || !Arrays.equals(actor.tenantResourceScopeHash(), policy.actorScope())) {
                throw new IllegalStateException(
                        "Catalog purpose identity/current signed policy is stale or unauthorized");
            }
            snapshot.requireCurrent();
        }
    }

    /**
     * Actual external allocation authority, whose records and one-time responsibility outlive this Root/guard.
     * Its own bounded transports/resources remain with that authority. A static grant/file or two absent keys
     * cannot implement this contract. Creation is forbidden after a Root birth has been recorded, including
     * after Root/guard loss; recordInitialRoot must durably and idempotently bind the first actual birth only.
     */
    public interface FreshAllocation {
        void requireNeverOrdinarilyUsed(Scope scope);

        void requireInitialCreation(Scope scope);

        void recordInitialRoot(InitialRoot actual);
    }
    /** Opaque actual Catalog observation; the external authority must retain its exact scope/version/digest. */
    public static final class InitialRoot {
        private final Scope scope;
        private final long version;
        private final byte[] digest;

        private InitialRoot(Scope scope, long version, byte[] digest) {
            this.scope = scope;
            this.version = version;
            this.digest = digest;
        }

        public Scope scope() {
            return scope;
        }

        public long version() {
            return version;
        }

        public byte[] digest() {
            return Bytes.copy(digest);
        }
    }
    /** Only a Catalog-owned observation can create the external birth input; no caller-supplied version receipt. */
    public static InitialRoot observedInitialRoot(
            com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.InitialRootObservation actual) {
        Objects.requireNonNull(actual);
        return new InitialRoot(actual.scope(), actual.version(), actual.digest());
    }

    public static long maximumInputBytes() {
        return 4L * MAX_POLICY_BYTES;
    }

    private static byte[] fixed(byte[] bytes, int size) {
        Bytes.requireLength(bytes, size, "Catalog root authority field");
        return Bytes.copy(bytes);
    }

    private static String text(CanonicalProtobuf.Reader.Field field, int number) {
        return new String(QueryCodecSupport.bytes(field, number), java.nio.charset.StandardCharsets.UTF_8);
    }

    private static List<CanonicalProtobuf.Reader.Field> fields(byte[] bytes, int count) {
        if (bytes.length == 0 || bytes.length > MAX_POLICY_BYTES) {
            throw new IllegalArgumentException("invalid Catalog root authority image size");
        }
        final var f = QueryCodecSupport.read(bytes, "Catalog root authority");
        final int[] numbers = new int[count];
        for (int i = 0; i < count; i++) {
            numbers[i] = i + 1;
        }
        QueryCodecSupport.requireNumbers(f, numbers, "Catalog root authority");
        return f;
    }

    private static void version(List<CanonicalProtobuf.Reader.Field> fields) {
        if (QueryCodecSupport.uint32(fields.get(0), 1) != 1) {
            throw new IllegalArgumentException("unknown Catalog authority schema");
        }
    }
}
