package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Consumes complete deployment history; it neither creates history nor authorizes a payload operation. */
public final class TargetEvidenceObjectLifecycleAuthority {
    public static final int MAX_HISTORY_BYTES = 32 * 1024;
    private static final byte[] PURPOSE = Bytes.utf8("QUALIFY_EVIDENCE_OBJECT_LIFECYCLE");
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-evidence-object-lifecycle-history-v1\0");
    private static final byte[] SIGN_DOMAIN = Bytes.utf8("nereus-delay-evidence-object-lifecycle-history-signature-v1\0");

    /**
     * Closed, currently supported protocols. A complete history must name every admitted path, including old ones.
     * Rule 1 forbids overwrite, delete/range-delete, marker GC, version reuse, reset and snapshot rewind.
     * Rule 2 requires original Root/Native refs or actual Catalog Pin before raw reads, complete closure and
     * permanent rejection of new dependencies, and an authenticated full copy before the raw dependency ends.
     * Rules 3/4 never read or retain the raw evidence blob. Unknown/raw audit/export protocols are unsupported.
     */
    public enum Rule {
        CONDITIONAL_ORIGINAL_VALUE_WRITER_V1,
        ROOT_NATIVE_CHECKPOINT_REFERENCED_READER_V1,
        NATIVE_ONLY_RESERVATION_QUERY_V1,
        NATIVE_OR_AUTHENTICATED_COPY_EXPORT_V1
    }

    /** Public proof data. The baseline and issuer/key are independently pinned by the original deployment root. */
    public record History(TargetCatalogRootAuthority.Scope scope, long firstEpoch, long firstVersion,
            long lastEpoch, long lastVersion, byte[] completeBaseline, byte[] evidenceDigest,
            Set<Rule> protocols, AuthorIdentity issuer, int keyVersion, byte[] signature) {
        public History {
            Objects.requireNonNull(scope); Objects.requireNonNull(issuer); protocols = Set.copyOf(protocols);
            completeBaseline = nonzero(completeBaseline); evidenceDigest = nonzero(evidenceDigest);
            signature = fixed(signature, 64);
            if (firstEpoch == 0 || Long.compareUnsigned(lastEpoch, firstEpoch) < 0 || firstVersion < 0
                    || lastVersion < firstVersion || keyVersion <= 0 || issuer.kind() != AuthorIdentity.Kind.CONTROL
                    || !protocols.contains(Rule.CONDITIONAL_ORIGINAL_VALUE_WRITER_V1)
                    || !protocols.contains(Rule.ROOT_NATIVE_CHECKPOINT_REFERENCED_READER_V1))
                throw new IllegalArgumentException("incomplete or unsupported evidence lifecycle history");
        }
        @Override public byte[] completeBaseline() { return Bytes.copy(completeBaseline); }
        @Override public byte[] evidenceDigest() { return Bytes.copy(evidenceDigest); }
        @Override public byte[] signature() { return Bytes.copy(signature); }
        private byte[] body() {
            return CanonicalProtobuf.message(o -> {
                CanonicalProtobuf.uint32(o, 1, 1); CanonicalProtobuf.bytes(o, 2, PURPOSE);
                CanonicalProtobuf.bytes(o, 3, scope.canonicalBytes());
                CanonicalProtobuf.uint64Bits(o, 4, firstEpoch); CanonicalProtobuf.uint64(o, 5, firstVersion);
                CanonicalProtobuf.uint64Bits(o, 6, lastEpoch); CanonicalProtobuf.uint64(o, 7, lastVersion);
                CanonicalProtobuf.bytes(o, 8, completeBaseline); CanonicalProtobuf.bytes(o, 9, evidenceDigest);
                for (var rule : Rule.values()) if (protocols.contains(rule)) CanonicalProtobuf.uint32(o, 10, rule.ordinal() + 1);
                CanonicalProtobuf.bytes(o, 11, issuer.canonicalBytes()); CanonicalProtobuf.uint32(o, 12, keyVersion);
            });
        }
        public byte[] digest() { return Bytes.sha256(DOMAIN, body()); }
        public byte[] signingBytes() { return Bytes.sha256(SIGN_DOMAIN, digest()); }
        public byte[] canonicalBytes() {
            final byte[] bytes = CanonicalProtobuf.message(o -> {
                CanonicalProtobuf.bytes(o, 1, body()); CanonicalProtobuf.bytes(o, 2, signature);
            });
            if (bytes.length > MAX_HISTORY_BYTES) throw new IllegalArgumentException("lifecycle history exceeds its finite bound");
            return bytes;
        }
        public static History decode(byte[] bytes) {
            final var outer = fields(bytes); QueryCodecSupport.requireNumbers(outer, new int[]{1, 2}, "lifecycle history");
            final var f = fields(QueryCodecSupport.bytes(outer.getFirst(), 1));
            if (f.size() < 12 || f.size() > 11 + Rule.values().length)
                throw new IllegalArgumentException("invalid lifecycle protocol history fields");
            for (int i = 0; i < 9; i++) if (f.get(i).number() != i + 1)
                throw new IllegalArgumentException("missing lifecycle history field");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1
                    || !Arrays.equals(QueryCodecSupport.bytes(f.get(1), 2), PURPOSE))
                throw new IllegalArgumentException("wrong lifecycle history schema or independent purpose");
            final var rules = EnumSet.noneOf(Rule.class); int n = 9;
            while (n < f.size() && f.get(n).number() == 10) {
                final int rule = QueryCodecSupport.uint32(f.get(n++), 10);
                if (rule < 1 || rule > Rule.values().length || !rules.add(Rule.values()[rule - 1]))
                    throw new IllegalArgumentException("unknown or duplicate historical evidence protocol");
            }
            if (n + 2 != f.size()) throw new IllegalArgumentException("incomplete lifecycle history issuer");
            final var history = new History(TargetCatalogRootAuthority.Scope.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                    QueryCodecSupport.uint64Bits(f.get(3), 4), QueryCodecSupport.uint(f.get(4), 5),
                    QueryCodecSupport.uint64Bits(f.get(5), 6), QueryCodecSupport.uint(f.get(6), 7),
                    QueryCodecSupport.fixed(f.get(7), 8, 32), QueryCodecSupport.fixed(f.get(8), 9, 32), rules,
                    AuthorIdentity.decode(QueryCodecSupport.bytes(f.get(n), 11)), QueryCodecSupport.uint32(f.get(n + 1), 12),
                    QueryCodecSupport.fixed(outer.get(1), 2, 64));
            QueryCodecSupport.requireCanonical(bytes, history.canonicalBytes(), "lifecycle history"); return history;
        }
    }

    /** The independently trusted complete starting point cannot be selected by the signed material itself. */
    public record Pin(TargetCatalogRootAuthority.Scope scope, long firstEpoch, long firstVersion,
            byte[] completeBaseline, AuthorIdentity issuer, int keyVersion, PublicKey key) {
        public Pin {
            Objects.requireNonNull(scope); Objects.requireNonNull(issuer); Objects.requireNonNull(key);
            completeBaseline = nonzero(completeBaseline);
            if (firstEpoch == 0 || firstVersion < 0 || keyVersion <= 0 || issuer.kind() != AuthorIdentity.Kind.CONTROL
                    || key.getEncoded() == null || key.getEncoded().length != 44)
                throw new IllegalArgumentException("independent complete lifecycle baseline and Ed25519 key required");
            try { key = java.security.KeyFactory.getInstance("Ed25519").generatePublic(
                    new java.security.spec.X509EncodedKeySpec(key.getEncoded())); }
            catch (GeneralSecurityException invalid) { throw new IllegalArgumentException("invalid lifecycle public key", invalid); }
        }
        @Override public byte[] completeBaseline() { return Bytes.copy(completeBaseline); }
    }

    /** Prepared paths only; actual bounded bytes are read under the original Root's reserved input window. */
    public record Inputs(Path historyFile, TargetEvidenceObjectLifecycleAuthority authority,
            TargetReceiptCurrentManagement.Authenticator authenticator,
            java.util.function.Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads) {
        public Inputs {
            Objects.requireNonNull(historyFile); Objects.requireNonNull(authority); Objects.requireNonNull(authenticator); Objects.requireNonNull(reads);
            if (!historyFile.isAbsolute() || !historyFile.normalize().equals(historyFile)
                    || Bytes.utf8(historyFile.toString()).length > 4096)
                throw new IllegalArgumentException("absolute canonical bounded lifecycle history path required");
        }
        public long maximumRetainedBytes() { return 8L * MAX_HISTORY_BYTES + 16 * 1024; }
        public Verified load(TargetCatalogRootAuthority.Scope actual, Runnable current) throws IOException {
            current.run(); final byte[] bytes = TargetWorkerRuntimeConfigLoader.loadBoundedImage(historyFile, MAX_HISTORY_BYTES);
            current.run(); final var verified = authority.verify(History.decode(bytes), actual); current.run(); return verified;
        }
    }

    /**
     * Actual original authenticator/control-plane admission hold, not a snapshot callback or a lease created here.
     * Until close succeeds it prevents admission of any protocol outside this exact complete qualified history.
     * Acquisition failures and pre-handoff transports/resources remain with that original authority.
     */
    public interface HeldAdmission extends TargetStoreBackend.CommitGuard {
        TargetCatalogRootAuthority.Scope scope();
        long epoch();
        long historyVersion();
        byte[] historyDigest();
    }

    private final Pin pin;
    public TargetEvidenceObjectLifecycleAuthority(Pin pin) { this.pin = Objects.requireNonNull(pin); }
    private Verified verify(History history, TargetCatalogRootAuthority.Scope actual) {
        if (!Arrays.equals(pin.scope().canonicalBytes(), actual.canonicalBytes())
                || !Arrays.equals(history.scope().canonicalBytes(), actual.canonicalBytes())
                || history.firstEpoch() != pin.firstEpoch() || history.firstVersion() != pin.firstVersion()
                || !Arrays.equals(history.completeBaseline(), pin.completeBaseline())
                || !Arrays.equals(history.issuer().canonicalBytes(), pin.issuer().canonicalBytes()) || history.keyVersion() != pin.keyVersion())
            throw new IllegalStateException("lifecycle history changes original allocation, complete baseline, purpose issuer or scope");
        try {
            final var verifier = Signature.getInstance("Ed25519"); verifier.initVerify(pin.key()); verifier.update(history.signingBytes());
            if (!verifier.verify(history.signature())) throw new IllegalStateException("invalid complete lifecycle history signature");
        } catch (GeneralSecurityException invalid) { throw new IllegalStateException("lifecycle history authority is unavailable", invalid); }
        return new Verified(this, history);
    }
    public static final class Verified {
        private final TargetEvidenceObjectLifecycleAuthority authority;
        private final History history;
        private Verified(TargetEvidenceObjectLifecycleAuthority authority, History history) { this.authority = authority; this.history = history; }
        public History history() { return history; }
        public void requireAuthority(TargetEvidenceObjectLifecycleAuthority expected) {
            if (authority != expected) throw new IllegalArgumentException("lifecycle history belongs to another original authority");
        }
        /** Retain the returned actual handle before requireCurrent: failed validation never loses close responsibility. */
        public AdmissionProtection acquire(TargetReceiptCurrentManagement.Authenticator authenticator) {
            return new AdmissionProtection(this, Objects.requireNonNull(authenticator.protectEvidenceObjectLifecycle(this), "actual lifecycle admission hold"));
        }
    }
    public static final class AdmissionProtection implements AutoCloseable {
        private final Verified verified;
        private final HeldAdmission actual;
        private boolean closed;
        private AdmissionProtection(Verified verified, HeldAdmission actual) { this.verified = verified; this.actual = actual; }
        public Verified qualifiedHistory() { requireCurrent(); return verified; }
        public void requireCurrent() {
            if (closed) throw new IllegalStateException("actual lifecycle admission protection is closed");
            actual.requireCurrent(); final var h = verified.history;
            if (!Arrays.equals(h.scope().canonicalBytes(), actual.scope().canonicalBytes()) || actual.epoch() != h.lastEpoch()
                    || actual.historyVersion() != h.lastVersion() || !Arrays.equals(actual.historyDigest(), h.digest()))
                throw new IllegalStateException("actual admission epoch is not protected by the complete qualified lifecycle history");
            actual.requireCurrent();
        }
        @Override public void close() { if (!closed) { actual.close(); closed = true; } }
    }

    private static byte[] fixed(byte[] bytes, int n) { Bytes.requireLength(bytes, n, "lifecycle authority field"); return Bytes.copy(bytes); }
    private static byte[] nonzero(byte[] bytes) {
        final byte[] value = fixed(bytes, 32); if (Arrays.equals(value, new byte[32])) throw new IllegalArgumentException("unassigned complete history evidence"); return value;
    }
    private static List<CanonicalProtobuf.Reader.Field> fields(byte[] bytes) {
        if (bytes.length == 0 || bytes.length > MAX_HISTORY_BYTES) throw new IllegalArgumentException("invalid bounded lifecycle history");
        return QueryCodecSupport.read(bytes, "lifecycle history");
    }
}
