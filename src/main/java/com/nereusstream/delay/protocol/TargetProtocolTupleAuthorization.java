package com.nereusstream.delay.protocol;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.Objects;

/** Original Root issuer grants one exact reader descriptor/tuple and base, without replacing immutable Root history. */
public record TargetProtocolTupleAuthorization(
        TargetQuotaIdentity root,
        byte[] initialSnapshotDigest,
        byte[] initialInputDigest,
        byte[] initialGrantDigest,
        TargetQuotaMutation initialSource,
        TargetQuotaScope scope,
        byte[] lineage,
        byte[] descriptorDigest,
        ProtocolTuple tuple,
        ProtocolTuple baseTuple,
        int issuerKeyVersion,
        byte[] signature,
        byte[] migrationRootDigest,
        byte[] migrationPlanDigest,
        TargetQuotaMutation migrationActivation,
        byte[] migrationManagementDigest) {
    public static final int MAX_BYTES = TargetQuotaIdentity.MAX_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + TargetQuotaScope.MAX_CANONICAL_BYTES
            + 512;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-root-tuple-authorization-v1\0");
    private static final byte[] MIGRATION_DOMAIN =
            Bytes.utf8("nereus-delay-target-migration-root-tuple-authorization-v1\0");
    private static final byte[] SIGN = Bytes.utf8("nereus-delay-target-root-tuple-authorization-signature-v1\0");

    public TargetProtocolTupleAuthorization(
            TargetQuotaIdentity root,
            byte[] snapshot,
            byte[] input,
            byte[] grant,
            TargetQuotaMutation source,
            TargetQuotaScope scope,
            byte[] lineage,
            byte[] descriptor,
            ProtocolTuple tuple,
            ProtocolTuple base,
            int issuer,
            byte[] signature) {
        this(
                root,
                snapshot,
                input,
                grant,
                source,
                scope,
                lineage,
                descriptor,
                tuple,
                base,
                issuer,
                signature,
                null,
                null,
                null,
                null);
    }

    public TargetProtocolTupleAuthorization {
        Objects.requireNonNull(root);
        Objects.requireNonNull(scope);
        Objects.requireNonNull(tuple);
        Objects.requireNonNull(baseTuple);
        lineage = TargetCompatibilityCodec.assigned(lineage, 16, "original Root lineage");
        descriptorDigest = TargetCompatibilityCodec.assigned(descriptorDigest, 32, "exact reader descriptor");
        if (migrationRootDigest == null) {
            initialSnapshotDigest =
                    TargetCompatibilityCodec.assigned(initialSnapshotDigest, 32, "immutable initial META10 digest");
            initialInputDigest =
                    TargetCompatibilityCodec.assigned(initialInputDigest, 32, "original signed input digest");
            initialGrantDigest =
                    TargetCompatibilityCodec.assigned(initialGrantDigest, 32, "original Shard grant digest");
            Objects.requireNonNull(initialSource).requireSourceApplied();
            if (migrationPlanDigest != null || migrationActivation != null || migrationManagementDigest != null) {
                throw new IllegalArgumentException("mixed Root authority origins");
            }
        } else {
            if (initialSnapshotDigest != null
                    || initialInputDigest != null
                    || initialGrantDigest != null
                    || initialSource != null) {
                throw new IllegalArgumentException("migration Root cannot fabricate META10 or initial grant");
            }
            migrationRootDigest =
                    TargetCompatibilityCodec.assigned(migrationRootDigest, 32, "actual original migration Root");
            migrationPlanDigest = TargetCompatibilityCodec.assigned(migrationPlanDigest, 32, "original migration plan");
            migrationManagementDigest = TargetCompatibilityCodec.assigned(
                    migrationManagementDigest, 32, "original migration management issuer anchor");
            Objects.requireNonNull(migrationActivation).requireSourceApplied();
        }
        final var at = migrationActivation == null ? initialSource : migrationActivation;
        if (root.kind() != TargetQuotaIdentity.Kind.SHARD
                || scope.target() != null
                || !scope.shard().equals(root.shard())
                || !scope.shard().equals(at.source().shardId())
                || issuerKeyVersion == 0) {
            throw new IllegalArgumentException("Root tuple authorization changed original Source/Root scope");
        }
        if (signature != null) {
            Bytes.requireLength(signature, 64, "Root issuer signature");
            signature = Bytes.copy(signature);
        }
    }

    private static byte[] copy(byte[] value) {
        return value == null ? null : Bytes.copy(value);
    }

    @Override
    public byte[] initialSnapshotDigest() {
        return copy(initialSnapshotDigest);
    }

    @Override
    public byte[] initialInputDigest() {
        return copy(initialInputDigest);
    }

    @Override
    public byte[] initialGrantDigest() {
        return copy(initialGrantDigest);
    }

    @Override
    public byte[] lineage() {
        return Bytes.copy(lineage);
    }

    @Override
    public byte[] descriptorDigest() {
        return Bytes.copy(descriptorDigest);
    }

    @Override
    public byte[] signature() {
        return copy(signature);
    }

    @Override
    public byte[] migrationRootDigest() {
        return copy(migrationRootDigest);
    }

    @Override
    public byte[] migrationPlanDigest() {
        return copy(migrationPlanDigest);
    }

    @Override
    public byte[] migrationManagementDigest() {
        return copy(migrationManagementDigest);
    }

    public boolean migrated() {
        return migrationRootDigest != null;
    }

    public static TargetProtocolTupleAuthorization signed(
            TargetInitialControlSnapshot initial,
            TargetProtocolActivationDescriptor descriptor,
            PrivateKey originalIssuer) {
        requireNew(descriptor);
        return new TargetProtocolTupleAuthorization(
                        initial.root(),
                        initial.snapshotDigest(),
                        initial.input().snapshotDigest(),
                        initial.initialGrant().grant().digest(),
                        initial.mutation(),
                        initial.input().scope(),
                        initial.input().lineage(),
                        descriptor.digest(),
                        descriptor.tuple(),
                        descriptor.base().systemMutationTuple(),
                        initial.input().issuerKeyVersion(),
                        null)
                .sign(originalIssuer);
    }
    /** Original management snapshot issuer is independently pinned to the actual A/material authority, never inferred from a Source signer. */
    public static TargetProtocolTupleAuthorization signedForMigration(
            TargetMigrationRoot original,
            TargetProtocolActivationDescriptor descriptor,
            int originalManagementIssuerVersion,
            PrivateKey originalManagementIssuer) {
        if (!(descriptor.extendedRootAuthorization()
                || descriptor.schemaVersion() == 10
                || descriptor.schemaVersion() == 11)) {
            throw new IllegalArgumentException(
                    "migration Root extension permits only the explicitly registered reader/delete prerequisites");
        }
        if (!original.activated()) {
            throw new IllegalArgumentException("migration reader extension requires actual original A");
        }
        final var draft = new TargetProtocolTupleAuthorization(
                original.owner().identity(),
                null,
                null,
                null,
                null,
                original.plan().scope(),
                original.plan().newLineage(),
                descriptor.digest(),
                descriptor.tuple(),
                descriptor.base().systemMutationTuple(),
                originalManagementIssuerVersion,
                null,
                original.digest(),
                original.plan().digest(),
                original.activatedAt(),
                original.plan().managementAuthorityDigest());
        draft.requireMigration(original, descriptor);
        return draft.sign(originalManagementIssuer);
    }

    private static void requireNew(TargetProtocolActivationDescriptor descriptor) {
        if (!descriptor.extendedRootAuthorization()) {
            throw new IllegalArgumentException("old descriptor retains immutable original whitelist");
        }
    }

    private TargetProtocolTupleAuthorization sign(PrivateKey issuer) {
        try {
            final var signer = Signature.getInstance("Ed25519");
            signer.initSign(issuer);
            signer.update(Bytes.concat(SIGN, digest()));
            return new TargetProtocolTupleAuthorization(
                    root,
                    initialSnapshotDigest,
                    initialInputDigest,
                    initialGrantDigest,
                    initialSource,
                    scope,
                    lineage,
                    descriptorDigest,
                    tuple,
                    baseTuple,
                    issuerKeyVersion,
                    signer.sign(),
                    migrationRootDigest,
                    migrationPlanDigest,
                    migrationActivation,
                    migrationManagementDigest);
        } catch (GeneralSecurityException failure) {
            throw new IllegalArgumentException("cannot sign original Root tuple authorization", failure);
        }
    }

    private void requireDescriptor(TargetProtocolActivationDescriptor descriptor) {
        if (!(descriptor.extendedRootAuthorization()
                || migrated() && (descriptor.schemaVersion() == 10 || descriptor.schemaVersion() == 11))) {
            throw new IllegalArgumentException("descriptor is not an explicitly permitted Root extension");
        }
        if (!Arrays.equals(descriptorDigest, descriptor.digest())
                || !tuple.equals(descriptor.tuple())
                || !baseTuple.equals(descriptor.base().systemMutationTuple())) {
            throw new IllegalArgumentException("Root extension changed exact descriptor/tuple/base");
        }
    }

    public void require(TargetInitialControlSnapshot initial, TargetProtocolActivationDescriptor descriptor) {
        requireDescriptor(descriptor);
        if (migrated()
                || !root.equals(initial.root())
                || !scope.equals(initial.input().scope())
                || !Arrays.equals(lineage, initial.input().lineage())
                || !Arrays.equals(initialSnapshotDigest, initial.snapshotDigest())
                || !Arrays.equals(initialInputDigest, initial.input().snapshotDigest())
                || !Arrays.equals(
                        initialGrantDigest, initial.initialGrant().grant().digest())
                || !initialSource.equals(initial.mutation())
                || issuerKeyVersion != initial.input().issuerKeyVersion()) {
            throw new IllegalArgumentException(
                    "Root issuer extension changed exact original META10/grant/Source/envelope");
        }
    }

    public void requireMigration(TargetMigrationRoot original, TargetProtocolActivationDescriptor descriptor) {
        requireDescriptor(descriptor);
        if (descriptor.supportsParentControlCredit()
                && (original.rootIssuerAuthorization() == null
                        || original.rootIssuerAuthorization().schemaVersion() != 2)) {
            throw new IllegalArgumentException(
                    "Parent reader extension requires its original signed migration Root issuer policy");
        }
        if (original.rootIssuerAuthorization() != null) {
            original.rootIssuerAuthorization().requireExtension(this);
        }
        if (!migrated()
                || !original.activated()
                || !root.equals(original.owner().identity())
                || !scope.equals(original.plan().scope())
                || !Arrays.equals(lineage, original.plan().newLineage())
                || !Arrays.equals(migrationRootDigest, original.digest())
                || !Arrays.equals(migrationPlanDigest, original.plan().digest())
                || !migrationActivation.equals(original.activatedAt())
                || !Arrays.equals(migrationManagementDigest, original.plan().managementAuthorityDigest())) {
            throw new IllegalArgumentException(
                    "reader extension changed actual original migration A/management issuer anchor");
        }
    }

    public boolean verifySignature(PublicKey originalIssuer) {
        if (signature == null) {
            return false;
        }
        try {
            final var verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(originalIssuer);
            verifier.update(Bytes.concat(SIGN, digest()));
            return verifier.verify(signature);
        } catch (GeneralSecurityException failure) {
            throw new IllegalArgumentException("cannot verify original Root tuple authorization", failure);
        }
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, migrated() ? 2 : 1);
            CanonicalProtobuf.bytes(out, 2, root.canonicalBytes());
            if (!migrated()) {
                CanonicalProtobuf.bytes(out, 3, initialSnapshotDigest);
                CanonicalProtobuf.bytes(out, 4, initialInputDigest);
                CanonicalProtobuf.bytes(out, 5, initialGrantDigest);
                CanonicalProtobuf.bytes(out, 6, initialSource.canonicalBytes());
            }
            CanonicalProtobuf.bytes(out, 7, scope.canonicalBytes());
            CanonicalProtobuf.bytes(out, 8, lineage);
            CanonicalProtobuf.bytes(out, 9, descriptorDigest);
            CanonicalProtobuf.bytes(out, 10, tuple.canonicalBytes());
            CanonicalProtobuf.bytes(out, 11, baseTuple.canonicalBytes());
            CanonicalProtobuf.uint32Bits(out, 12, issuerKeyVersion);
            if (migrated()) {
                CanonicalProtobuf.bytes(out, 15, migrationRootDigest);
                CanonicalProtobuf.bytes(out, 16, migrationPlanDigest);
                CanonicalProtobuf.bytes(out, 17, migrationActivation.canonicalBytes());
                CanonicalProtobuf.bytes(out, 18, migrationManagementDigest);
            }
        });
    }

    public byte[] digest() {
        return Bytes.sha256(migrated() ? MIGRATION_DOMAIN : DOMAIN, fields());
    }

    public byte[] canonicalBytes() {
        if (signature == null) {
            throw new IllegalStateException("Root tuple authorization is unsigned");
        }
        return Bytes.concat(fields(), CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, migrated() ? 19 : 13, digest());
            CanonicalProtobuf.bytes(out, migrated() ? 20 : 14, signature);
        }));
    }

    public static TargetProtocolTupleAuthorization decode(byte[] bytes) {
        final var f = TargetCompatibilityCodec.read(bytes, MAX_BYTES, 14, false, "original Root tuple authorization");
        final boolean migrated = QueryCodecSupport.uint32(f.get(0), 1) == 2;
        QueryCodecSupport.requireNumbers(
                f,
                migrated
                        ? new int[] {1, 2, 7, 8, 9, 10, 11, 12, 15, 16, 17, 18, 19, 20}
                        : new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
                "original Root tuple authorization");
        if (!migrated && QueryCodecSupport.uint32(f.get(0), 1) != 1) {
            throw new IllegalArgumentException("unknown Root tuple authorization schema");
        }
        final var value = migrated
                ? new TargetProtocolTupleAuthorization(
                        TargetQuotaIdentity.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                        null,
                        null,
                        null,
                        null,
                        TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(2), 7)),
                        QueryCodecSupport.fixed(f.get(3), 8, 16),
                        QueryCodecSupport.fixed(f.get(4), 9, 32),
                        ProtocolTuple.decode(QueryCodecSupport.bytes(f.get(5), 10)),
                        ProtocolTuple.decode(QueryCodecSupport.bytes(f.get(6), 11)),
                        QueryCodecSupport.uint32Bits(f.get(7), 12),
                        QueryCodecSupport.fixed(f.get(13), 20, 64),
                        QueryCodecSupport.fixed(f.get(8), 15, 32),
                        QueryCodecSupport.fixed(f.get(9), 16, 32),
                        TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(10), 17)),
                        QueryCodecSupport.fixed(f.get(11), 18, 32))
                : new TargetProtocolTupleAuthorization(
                        TargetQuotaIdentity.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                        QueryCodecSupport.fixed(f.get(2), 3, 32),
                        QueryCodecSupport.fixed(f.get(3), 4, 32),
                        QueryCodecSupport.fixed(f.get(4), 5, 32),
                        TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(5), 6)),
                        TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(6), 7)),
                        QueryCodecSupport.fixed(f.get(7), 8, 16),
                        QueryCodecSupport.fixed(f.get(8), 9, 32),
                        ProtocolTuple.decode(QueryCodecSupport.bytes(f.get(9), 10)),
                        ProtocolTuple.decode(QueryCodecSupport.bytes(f.get(10), 11)),
                        QueryCodecSupport.uint32Bits(f.get(11), 12),
                        QueryCodecSupport.fixed(f.get(13), 14, 64));
        if (!Arrays.equals(value.digest(), QueryCodecSupport.fixed(f.get(12), migrated ? 19 : 13, 32))) {
            throw new IllegalArgumentException("Root tuple authorization digest changed");
        }
        QueryCodecSupport.requireCanonical(bytes, value.canonicalBytes(), "Root tuple authorization");
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetProtocolTupleAuthorization value
                && Arrays.equals(canonicalBytes(), value.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest());
    }
}
