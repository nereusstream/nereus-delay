package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Objects;

/** Exact immutable object identity, not a closure snapshot, Source adoption proof or deletion permission. */
public record TargetExternalEvidenceObjectResource(Subtype subtype, byte[] catalogNamespaceId, byte[] deploymentId,
        ShardId shard, byte[] tenantScope, byte[] recoveryLineage, String exactKey, byte[] objectDigest,
        byte[] valueDigest, long canonicalValueBytes, byte[] immutableVersion, Provenance provenance) {
    public static final int SCHEMA_VERSION = 1;
    public static final int MAX_VERSION_BYTES = 512;
    private static final int MAX_PROVENANCE_BYTES = Math.max(TargetExternalEvidenceOwner.MAX_CANONICAL_BYTES + 128,
            TargetRecoveryEvidenceOwner.MAX_CANONICAL_BYTES + 16);
    public static final int MAX_CANONICAL_BYTES = MAX_PROVENANCE_BYTES + TargetRecoveryProofRef.MAX_KEY_BYTES + 2048;

    public enum Subtype {
        CONTROL_PROTECTION(1), CONTROL_ANCHOR(2), RECOVERY_PROOF(3);
        private final int wire;
        Subtype(int wire) { this.wire = wire; }
        public int wireValue() { return wire; }
        static Subtype decode(int wire) {
            for (var value : values()) if (value.wire == wire) return value;
            throw invalid();
        }
    }

    public sealed interface Provenance permits Protection, Anchor, Recovery, Staging {
        byte[] canonicalBytes();
    }

    /** The immutable native owner already contains original CONTROL digest/Source and first Source adoption. */
    public record Protection(TargetExternalEvidenceOwner owner) implements Provenance {
        public Protection {
            if (Objects.requireNonNull(owner).kind() != TargetKafkaReceiptControlArtifact.PROTECTION) throw invalid();
        }
        @Override public byte[] canonicalBytes() { return branch(1, owner.canonicalBytes()); }
    }

    /** Bounded immutable creator facts supplement the original owner; actual Native mapping/ref joins remain mandatory. */
    public record Anchor(TargetExternalEvidenceOwner owner, byte[] mappingId, byte[] admissionHash) implements Provenance {
        public Anchor {
            if (Objects.requireNonNull(owner).kind() != TargetKafkaReceiptControlArtifact.ANCHOR) throw invalid();
            mappingId = assigned(mappingId); admissionHash = assigned(admissionHash);
        }
        @Override public byte[] mappingId() { return Bytes.copy(mappingId); }
        @Override public byte[] admissionHash() { return Bytes.copy(admissionHash); }
        public byte[] admissionId() {
            return SystemMutation.computeSystemMutationId(owner.primaryOwner().identity().shard(),
                    SystemMutationType.TARGET_PUBLISH_ADMISSION, owner.creatorAttemptId(), admissionHash);
        }
        @Override public byte[] canonicalBytes() {
            return branch(2, CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, owner.canonicalBytes()); CanonicalProtobuf.bytes(out, 2, mappingId);
                CanonicalProtobuf.bytes(out, 3, admissionHash);
            }));
        }
    }

    /** NV49 already binds mapping/evidence/Attempt, original Admission and adopting Outcome ID/hash/Source. */
    public record Recovery(TargetRecoveryEvidenceOwner owner) implements Provenance {
        public Recovery { Objects.requireNonNull(owner); }
        @Override public byte[] canonicalBytes() { return branch(3, owner.canonicalBytes()); }
    }

    /** No adoption/Target logical owner. The origin digest excludes mutable phase, prepared-use and reference state. */
    public record Staging(byte[] publicationIntentId, byte[] publicationOriginDigest, byte[] deploymentPhysicalOwner,
            int authorizationKind, byte[] originalAuthorizationDigest, TrustedUtcIntervalEvidence createdAt) implements Provenance {
        public static final int CONTROL_AUTHORIZATION = 1;
        public static final int RECOVERY_PROOF_AUTHORIZATION = 2;
        public Staging {
            publicationIntentId = assigned(publicationIntentId); publicationOriginDigest = assigned(publicationOriginDigest);
            deploymentPhysicalOwner = assigned(deploymentPhysicalOwner); originalAuthorizationDigest = assigned(originalAuthorizationDigest);
            if (authorizationKind != CONTROL_AUTHORIZATION && authorizationKind != RECOVERY_PROOF_AUTHORIZATION) throw invalid();
            Objects.requireNonNull(createdAt);
            if (createdAt.canonicalBytes().length > 1024) throw invalid();
        }
        @Override public byte[] publicationIntentId() { return Bytes.copy(publicationIntentId); }
        @Override public byte[] publicationOriginDigest() { return Bytes.copy(publicationOriginDigest); }
        @Override public byte[] deploymentPhysicalOwner() { return Bytes.copy(deploymentPhysicalOwner); }
        @Override public byte[] originalAuthorizationDigest() { return Bytes.copy(originalAuthorizationDigest); }
        @Override public byte[] canonicalBytes() {
            return branch(4, CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, publicationIntentId); CanonicalProtobuf.bytes(out, 2, publicationOriginDigest);
                CanonicalProtobuf.bytes(out, 3, deploymentPhysicalOwner); CanonicalProtobuf.uint32(out, 4, authorizationKind);
                CanonicalProtobuf.bytes(out, 5, originalAuthorizationDigest); CanonicalProtobuf.bytes(out, 6, createdAt.canonicalBytes());
            }));
        }
    }

    public TargetExternalEvidenceObjectResource {
        Objects.requireNonNull(subtype); Objects.requireNonNull(shard); Objects.requireNonNull(provenance);
        catalogNamespaceId = assigned(catalogNamespaceId); deploymentId = assigned(deploymentId);
        tenantScope = assigned(tenantScope);
        recoveryLineage = TargetCompatibilityCodec.assigned(recoveryLineage, 16, "external object recovery lineage");
        objectDigest = assigned(objectDigest); valueDigest = assigned(valueDigest);
        Objects.requireNonNull(exactKey);
        if (exactKey.isBlank() || !exactKey.equals(exactKey.trim()) || exactKey.indexOf('\0') >= 0
                || Bytes.utf8(exactKey).length > TargetRecoveryProofRef.MAX_KEY_BYTES
                || !Normalizer.isNormalized(exactKey, Normalizer.Form.NFC)
                || !exactKey.equals(new String(Bytes.utf8(exactKey), StandardCharsets.UTF_8))
                || canonicalValueBytes < 1 || canonicalValueBytes > (subtype == Subtype.RECOVERY_PROOF
                        ? TargetRecoveryProofRef.MAX_PROOF_BYTES : TargetKafkaReceiptControlArtifact.MAX_CANONICAL_BYTES)
                || immutableVersion == null || immutableVersion.length > MAX_VERSION_BYTES
                || provenance.canonicalBytes().length > MAX_PROVENANCE_BYTES) throw invalid();
        immutableVersion = Bytes.copy(immutableVersion);
        TargetQuotaIncarnation owner;
        if (provenance instanceof Staging original) {
            if (original.authorizationKind() != (subtype == Subtype.RECOVERY_PROOF
                    ? Staging.RECOVERY_PROOF_AUTHORIZATION : Staging.CONTROL_AUTHORIZATION)) throw invalid();
            owner = null;
        } else if (provenance instanceof Recovery original) {
            final var actual = original.owner(); final var ref = actual.reference();
            if (subtype != Subtype.RECOVERY_PROOF || !exactKey.equals(ref.exactKey())
                    || canonicalValueBytes != ref.canonicalValueBytes()) throw invalid();
            same(objectDigest, ref.proofDigest()); same(valueDigest, ref.valueDigest()); owner = actual.primaryOwner();
        } else {
            TargetExternalEvidenceOwner actual;
            if (provenance instanceof Protection original && subtype == Subtype.CONTROL_PROTECTION) actual = original.owner();
            else if (provenance instanceof Anchor original && subtype == Subtype.CONTROL_ANCHOR) actual = original.owner();
            else throw invalid();
            if (!exactKey.equals(actual.exactKey()) || canonicalValueBytes != actual.canonicalValueBytes()) throw invalid();
            same(objectDigest, actual.artifactDigest()); same(valueDigest, actual.valueDigest()); owner = actual.primaryOwner();
        }
        if (owner != null) {
            if (!shard.equals(owner.identity().shard())) throw invalid();
            same(tenantScope, owner.tenantScope()); same(recoveryLineage, owner.recoveryLineage());
        }
    }

    @Override public byte[] catalogNamespaceId() { return Bytes.copy(catalogNamespaceId); }
    @Override public byte[] deploymentId() { return Bytes.copy(deploymentId); }
    @Override public byte[] tenantScope() { return Bytes.copy(tenantScope); }
    @Override public byte[] recoveryLineage() { return Bytes.copy(recoveryLineage); }
    @Override public byte[] objectDigest() { return Bytes.copy(objectDigest); }
    @Override public byte[] valueDigest() { return Bytes.copy(valueDigest); }
    @Override public byte[] immutableVersion() { return Bytes.copy(immutableVersion); }
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, SCHEMA_VERSION); CanonicalProtobuf.uint32(out, 2, subtype.wireValue());
            CanonicalProtobuf.bytes(out, 3, catalogNamespaceId); CanonicalProtobuf.bytes(out, 4, deploymentId);
            CanonicalProtobuf.bytes(out, 5, new ShardSubject(shard).canonicalBytes()); CanonicalProtobuf.bytes(out, 6, tenantScope);
            CanonicalProtobuf.bytes(out, 7, recoveryLineage); CanonicalProtobuf.bytes(out, 8, Bytes.utf8(exactKey));
            CanonicalProtobuf.bytes(out, 9, objectDigest); CanonicalProtobuf.bytes(out, 10, valueDigest);
            CanonicalProtobuf.uint64(out, 11, canonicalValueBytes); CanonicalProtobuf.bytes(out, 12, immutableVersion);
            CanonicalProtobuf.bytes(out, 13, provenance.canonicalBytes());
        });
    }
    public ResourceRetireIntentBody.ExactResourceIdentity exactIdentity() {
        return ResourceRetireIntentBody.decodeResourceIdentity(ResourceKind.TARGET_EXTERNAL_EVIDENCE_OBJECT,
                branch(ResourceKind.TARGET_EXTERNAL_EVIDENCE_OBJECT.wireValue(), canonicalBytes()));
    }
    public static TargetExternalEvidenceObjectResource decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 13, false, "Target external object identity");
        QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,4,5,6,7,8,9,10,11,12,13}, "Target external object identity");
        if (QueryCodecSupport.uint(f.get(0), 1) != SCHEMA_VERSION) throw invalid();
        final var resource = new TargetExternalEvidenceObjectResource(Subtype.decode(QueryCodecSupport.uint32(f.get(1), 2)),
                QueryCodecSupport.fixed(f.get(2), 3, 32), QueryCodecSupport.fixed(f.get(3), 4, 32),
                ShardSubject.decode(QueryCodecSupport.nested(f.get(4), 5)).shardId(), QueryCodecSupport.fixed(f.get(5), 6, 32),
                QueryCodecSupport.fixed(f.get(6), 7, 16), new String(QueryCodecSupport.bytes(f.get(7), 8), StandardCharsets.UTF_8),
                QueryCodecSupport.fixed(f.get(8), 9, 32), QueryCodecSupport.fixed(f.get(9), 10, 32), QueryCodecSupport.uint(f.get(10), 11),
                QueryCodecSupport.bytes(f.get(11), 12), decodeProvenance(QueryCodecSupport.nested(f.get(12), 13)));
        QueryCodecSupport.requireCanonical(encoded, resource.canonicalBytes(), "Target external object identity"); return resource;
    }
    private static Provenance decodeProvenance(byte[] encoded) {
        final var outer = TargetCompatibilityCodec.read(encoded, MAX_PROVENANCE_BYTES, 1, false, "external object provenance");
        if (outer.size() != 1) throw invalid();
        final var field = outer.getFirst(); final byte[] value = QueryCodecSupport.nested(field, field.number());
        final Provenance origin;
        switch (field.number()) {
            case 1 -> origin = new Protection(TargetExternalEvidenceOwner.decode(value));
            case 2 -> {
                final var f = TargetCompatibilityCodec.read(value, MAX_PROVENANCE_BYTES, 3, false, "anchor creator");
                QueryCodecSupport.requireNumbers(f, new int[] {1,2,3}, "anchor creator");
                origin = new Anchor(TargetExternalEvidenceOwner.decode(QueryCodecSupport.nested(f.get(0), 1)),
                        QueryCodecSupport.fixed(f.get(1), 2, 32), QueryCodecSupport.fixed(f.get(2), 3, 32));
            }
            case 3 -> origin = new Recovery(TargetRecoveryEvidenceOwner.decode(value));
            case 4 -> {
                final var f = TargetCompatibilityCodec.read(value, 1536, 6, false, "unadopted staging origin");
                QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,4,5,6}, "unadopted staging origin");
                origin = new Staging(QueryCodecSupport.fixed(f.get(0), 1, 32), QueryCodecSupport.fixed(f.get(1), 2, 32),
                        QueryCodecSupport.fixed(f.get(2), 3, 32), QueryCodecSupport.uint32(f.get(3), 4), QueryCodecSupport.fixed(f.get(4), 5, 32),
                        TrustedUtcIntervalEvidence.decode(QueryCodecSupport.nested(f.get(5), 6)));
            }
            default -> throw invalid();
        }
        QueryCodecSupport.requireCanonical(encoded, origin.canonicalBytes(), "external object provenance"); return origin;
    }
    private static byte[] branch(int number, byte[] value) {
        return CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, number, value));
    }
    private static byte[] assigned(byte[] value) { return TargetCompatibilityCodec.assigned(value, 32, "external object origin identity"); }
    private static void same(byte[] expected, byte[] actual) { if (!Arrays.equals(expected, actual)) throw invalid(); }
    private static IllegalArgumentException invalid() { return new IllegalArgumentException("invalid Target external evidence object identity/provenance"); }
    @Override public boolean equals(Object other) { return other instanceof TargetExternalEvidenceObjectResource that && Arrays.equals(canonicalBytes(), that.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(canonicalBytes()); }
}
