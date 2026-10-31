package com.nereusstream.delay.protocol;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/** Signed exact initial Target reader/profile input; issuer trust and immutable-version protection remain external. */
public final class TargetControlSnapshotInput {
    public static final int MAX_CANONICAL_BYTES = 1 << 20;
    private static final byte[] HASH_DOMAIN = Bytes.utf8("nereus-delay-target-control-snapshot-input\0");
    private static final byte[] SIGN_DOMAIN = Bytes.utf8("nereus-delay-target-control-snapshot-signature\0");
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final byte[] grantDigest;
    private final SourcePosition source;
    private final byte[] envelopeDigest;
    private final List<ProtocolTuple> tuples;
    private final List<ProfileRef> profiles;
    private final int issuerKeyVersion;
    private final byte[] digest;
    private final byte[] signature;

    private TargetControlSnapshotInput(TargetQuotaScope scope, byte[] lineage, byte[] grantDigest,
            SourcePosition source,
            byte[] envelopeDigest, List<ProtocolTuple> tuples, List<ProfileRef> profiles, int issuerKeyVersion,
            byte[] signature) {
        this.scope = Objects.requireNonNull(scope, "scope");
        if (scope.target() != null || !scope.shard().equals(TargetSourcePosition.requireBounded(source).shardId())
                || issuerKeyVersion == 0) {
            throw new IllegalArgumentException("Target control snapshot requires an exact Shard/Source/key scope");
        }
        TargetCompatibilityCodec.assigned(lineage, 16, "lineage");
        TargetCompatibilityCodec.assigned(grantDigest, 32, "initialGrantDigest");
        TargetCompatibilityCodec.assigned(envelopeDigest, 32, "initialGrantEnvelopeDigest");
        this.lineage = Bytes.copy(lineage);
        this.grantDigest = Bytes.copy(grantDigest);
        this.source = source;
        this.envelopeDigest = Bytes.copy(envelopeDigest);
        this.tuples = sorted(tuples, ProtocolTuple::canonicalBytes, 32);
        if (this.tuples.isEmpty()) { throw new IllegalArgumentException("Target reader tuple set is empty"); }
        this.profiles = sorted(profiles, ProfileRef::canonicalBytes, 4096);
        final var identities = new java.util.HashSet<String>();
        for (final var profile : profiles) {
            if (!identities.add(Bytes.hex(profile.profileId()) + '/' + Long.toUnsignedString(profile.version()))) {
                throw new IllegalArgumentException("duplicate immutable Target control Profile version");
            }
        }
        this.issuerKeyVersion = issuerKeyVersion;
        digest = Bytes.sha256(HASH_DOMAIN, fields());
        if (signature != null) { Bytes.requireLength(signature, 64, "snapshot signature"); }
        this.signature = signature == null ? null : Bytes.copy(signature);
    }

    /** Issues one bounded authorization while retaining this exact immutable original META10 input. */
    public TargetProtocolTupleAuthorization authorizeTuple(TargetInitialControlSnapshot initial,TargetProtocolActivationDescriptor descriptor,PrivateKey originalIssuer){
        if(!Arrays.equals(canonicalBytes(),initial.input().canonicalBytes()))throw new IllegalArgumentException("tuple authorization changed original signed Root input");
        return TargetProtocolTupleAuthorization.signed(initial,descriptor,originalIssuer);
    }

    public static TargetControlSnapshotInput signed(TargetQuotaScope scope, byte[] lineage, byte[] initialGrantDigest,
            SourcePosition initialSource, byte[] initialEnvelopeDigest, List<ProtocolTuple> tuples,
            List<ProfileRef> profiles, int issuerKeyVersion, PrivateKey key) {
        final var draft = new TargetControlSnapshotInput(scope, lineage, initialGrantDigest, initialSource,
                initialEnvelopeDigest, tuples, profiles, issuerKeyVersion, null);
        try {
            final var signer = Signature.getInstance("Ed25519");
            signer.initSign(key);
            signer.update(Bytes.concat(SIGN_DOMAIN, draft.digest));
            return new TargetControlSnapshotInput(scope, lineage, initialGrantDigest, initialSource,
                    initialEnvelopeDigest, tuples, profiles, issuerKeyVersion, signer.sign());
        } catch (GeneralSecurityException failure) {
            throw new IllegalArgumentException("cannot sign Target control snapshot", failure);
        }
    }

    public boolean verifySignature(PublicKey key) {
        try {
            final var verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(key);
            verifier.update(Bytes.concat(SIGN_DOMAIN, digest));
            return verifier.verify(signature);
        } catch (GeneralSecurityException failure) {
            throw new IllegalArgumentException("cannot verify Target control snapshot", failure);
        }
    }

    public void requireInitial(TargetQuotaScope expectedScope, byte[] expectedLineage, TargetQuotaGrant grant,
            SourcePosition expectedSource, byte[] expectedEnvelope) {
        if (!scope.equals(expectedScope) || !Arrays.equals(lineage, expectedLineage)
                || !Arrays.equals(grantDigest, grant.digest())
                || !Arrays.equals(source.canonicalBytes(), expectedSource.canonicalBytes())
                || !Arrays.equals(envelopeDigest, expectedEnvelope)) {
            throw new IllegalArgumentException("Target control snapshot differs from its exact initial grant/Source");
        }
    }

    public TargetQuotaScope scope() { return scope; }
    public byte[] lineage() { return Bytes.copy(lineage); }
    public SourcePosition source() { return source; }
    public byte[] envelopeDigest() { return Bytes.copy(envelopeDigest); }
    public List<ProtocolTuple> protocolTuples() { return tuples; }
    public List<ProfileRef> profiles() { return profiles; }
    public int issuerKeyVersion() { return issuerKeyVersion; }
    public byte[] snapshotDigest() { return Bytes.copy(digest); }

    public byte[] canonicalBytes() {
        if (signature == null) { throw new IllegalStateException("Target control snapshot has no signature"); }
        final byte[] encoded = CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 10, digest);
            CanonicalProtobuf.bytes(out, 11, signature);
        });
        if (encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target control input too large");
        }
        return encoded;
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, lineage);
            CanonicalProtobuf.bytes(out, 4, grantDigest);
            CanonicalProtobuf.bytes(out, 5, source.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, envelopeDigest);
            for (final var tuple : tuples) { CanonicalProtobuf.bytes(out, 7, tuple.canonicalBytes()); }
            for (final var profile : profiles) { CanonicalProtobuf.bytes(out, 8, profile.canonicalBytes()); }
            CanonicalProtobuf.uint32Bits(out, 9, issuerKeyVersion);
        });
    }

    public static TargetControlSnapshotInput decode(byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 4140, true,
                "Target control snapshot input");
        if (fields.size() < 10 || QueryCodecSupport.uint32(fields.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unsupported Target control snapshot input version");
        }
        final var tuples = new ArrayList<ProtocolTuple>();
        final var profiles = new ArrayList<ProfileRef>();
        int index = 6;
        while (index < fields.size() && fields.get(index).number() == 7) {
            tuples.add(ProtocolTuple.decode(QueryCodecSupport.bytes(fields.get(index++), 7)));
        }
        while (index < fields.size() && fields.get(index).number() == 8) {
            profiles.add(ProfileRef.decode(QueryCodecSupport.bytes(fields.get(index++), 8)));
        }
        if (index + 3 != fields.size()) {
            throw new IllegalArgumentException("unknown Target control snapshot fields");
        }
        final var value = new TargetControlSnapshotInput(
                TargetQuotaScope.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                QueryCodecSupport.fixed(fields.get(2), 3, 16), QueryCodecSupport.fixed(fields.get(3), 4, 32),
                SourcePositionCodec.decode(QueryCodecSupport.bytes(fields.get(4), 5)),
                QueryCodecSupport.fixed(fields.get(5), 6, 32), tuples, profiles,
                QueryCodecSupport.uint32Bits(fields.get(index), 9),
                QueryCodecSupport.fixed(fields.get(index + 2), 11, 64));
        if (!Arrays.equals(value.digest, QueryCodecSupport.fixed(fields.get(index + 1), 10, 32))) {
            throw new IllegalArgumentException("Target control snapshot input digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target control snapshot input");
        return value;
    }

    private static <T> List<T> sorted(List<T> values, java.util.function.Function<T, byte[]> identity, int limit) {
        if (values.size() > limit) { throw new IllegalArgumentException("Target control input list exceeds limit"); }
        final var result = new TreeMap<String, T>();
        for (final var value : values) {
            if (result.put(Bytes.hex(identity.apply(value)), value) != null) {
                throw new IllegalArgumentException("duplicate Target control snapshot entry");
            }
        }
        return List.copyOf(result.values());
    }
}
