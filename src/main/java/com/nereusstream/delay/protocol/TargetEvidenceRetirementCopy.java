package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Complete public bytes for a retiring object. This codec does not approve the supplied historical keys. */
public record TargetEvidenceRetirementCopy(TargetExternalEvidenceObjectResource resource, byte[] signedObject,
        byte[] publicInputs, byte[] closureImage) {
    public static final int MAX_PUBLIC_INPUT_BYTES = 2 * 1024 * 1024;
    public static final int MAX_CANONICAL_BYTES = TargetEvidenceRetirementContext.MAX_COPY_BYTES;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-evidence-retirement-copy\0");
    private static final byte[] CLOSURE_DOMAIN = Bytes.utf8("nereus-delay-target-evidence-reference-closure\0");
    public TargetEvidenceRetirementCopy {
        Objects.requireNonNull(resource); Objects.requireNonNull(signedObject);
        Objects.requireNonNull(publicInputs); Objects.requireNonNull(closureImage);
        if (signedObject.length != resource.canonicalValueBytes() || publicInputs.length == 0
                || publicInputs.length > MAX_PUBLIC_INPUT_BYTES || closureImage.length == 0
                || closureImage.length > TargetEvidenceRetirementContext.MAX_CLOSURE_BYTES
                || !Arrays.equals(Bytes.sha256(signedObject), resource.valueDigest()))
            throw new IllegalArgumentException("retirement copy lost exact signed object/public input bound");
        signedObject = Bytes.copy(signedObject); publicInputs = Bytes.copy(publicInputs); closureImage = Bytes.copy(closureImage);
        requireObject(resource, signedObject, publicInputs); requireClosure(resource, closureImage);
        if (estimatedBytes(resource, signedObject.length, publicInputs.length, closureImage.length) > MAX_CANONICAL_BYTES)
            throw new IllegalArgumentException("retirement copy exceeds finite manifest bound");
    }
    public static long estimatedBytes(TargetExternalEvidenceObjectResource resource,long objectBytes,long inputBytes,long closureBytes) {
        return Math.addExact(256L + resource.exactIdentity().canonicalBytes().length,
                Math.addExact(objectBytes, Math.addExact(inputBytes, closureBytes)));
    }
    /** Complete bound without copying any raw value; used before Source snapshot encoding. */
    public long estimatedBytes() { return estimatedBytes(resource, signedObject.length, publicInputs.length, closureImage.length); }
    @Override public byte[] signedObject() { return Bytes.copy(signedObject); }
    @Override public byte[] publicInputs() { return Bytes.copy(publicInputs); }
    @Override public byte[] closureImage() { return Bytes.copy(closureImage); }
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, resource.exactIdentity().canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, signedObject); CanonicalProtobuf.bytes(out, 4, publicInputs);
            CanonicalProtobuf.bytes(out, 5, closureImage);
        });
    }
    public byte[] digest() { return Bytes.sha256(DOMAIN, canonicalBytes()); }
    public void requireContext(TargetEvidenceRetirementContext context) {
        context.requireResource(resource);
        final var closure = closureFields(closureImage);
        if (context.closureRevision() != QueryCodecSupport.uint64Bits(closure.get(11), 12)
                || context.closureBytes() != closureImage.length
                || !Arrays.equals(context.closureDigest(), Bytes.sha256(CLOSURE_DOMAIN, closureImage))
                || !Arrays.equals(context.preparedUsesDigest(), QueryCodecSupport.fixed(closure.get(9), 10, 32))
                || context.uses().size() != QueryCodecSupport.uint32(closure.get(10), 11)
                || context.copyManifestBytes() != canonicalBytes().length
                || !Arrays.equals(context.copyManifestDigest(), digest()))
            throw new IllegalArgumentException("retirement context differs from complete actual copy/closure/use set");
    }
    public static TargetEvidenceRetirementCopy decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 5, false, "retirement public copy");
        QueryCodecSupport.requireNumbers(f, new int[]{1,2,3,4,5}, "retirement public copy");
        if (QueryCodecSupport.uint(f.get(0), 1) != 1) throw new IllegalArgumentException("unknown retirement copy schema");
        final var copy = new TargetEvidenceRetirementCopy(TargetEvidenceRetireIntentBody.decodeResource(QueryCodecSupport.nested(f.get(1), 2)),
                QueryCodecSupport.bytes(f.get(2), 3), QueryCodecSupport.bytes(f.get(3), 4), QueryCodecSupport.bytes(f.get(4), 5));
        QueryCodecSupport.requireCanonical(encoded, copy.canonicalBytes(), "retirement public copy"); return copy;
    }
    private static java.util.List<CanonicalProtobuf.Reader.Field> closureFields(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, TargetEvidenceRetirementContext.MAX_CLOSURE_BYTES,
                4096 + 18, true, "retirement exact closure");
        if (f.size() < 18) throw new IllegalArgumentException("incomplete exact closure");
        QueryCodecSupport.requireNumbers(f.subList(0, 15), new int[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15}, "closure prefix");
        int n = 15; String previous = null;
        while (n < f.size() && f.get(n).number() == 16) {
            final var entry = TargetCompatibilityCodec.read(QueryCodecSupport.nested(f.get(n++), 16), 40, 2, false, "closure candidate");
            QueryCodecSupport.requireNumbers(entry, new int[]{1,2}, "closure candidate");
            final int kind = QueryCodecSupport.uint32(entry.get(0), 1);
            final String order = kind + ":" + Bytes.hex(QueryCodecSupport.fixed(entry.get(1), 2, 32));
            if (kind < 1 || kind > 3 || previous != null && previous.compareTo(order) >= 0)
                throw new IllegalArgumentException("closure candidates are not unique/sorted");
            previous = order;
        }
        QueryCodecSupport.requireNumbers(f.subList(n, f.size()), new int[]{17,18,19}, "closure suffix");
        final int phase=QueryCodecSupport.uint32(f.get(13),14);
        final byte[] adopted=QueryCodecSupport.bytes(f.get(14),15);
        QueryCodecSupport.fixed(f.get(7),8,32);QueryCodecSupport.fixed(f.get(8),9,32);QueryCodecSupport.fixed(f.get(9),10,32);
        QueryCodecSupport.fixed(f.get(n),17,32);QueryCodecSupport.fixed(f.get(n+1),18,32);QueryCodecSupport.fixed(f.get(n+2),19,32);
        final byte[] inventory=CanonicalProtobuf.message(out->{for(int i=15;i<f.size()-3;i++)
            CanonicalProtobuf.bytes(out,1,QueryCodecSupport.nested(f.get(i),16));});
        final byte[] inventoryDigest=Bytes.sha256(Bytes.utf8("nereus-delay-target-evidence-closure-inventory\0"),inventory);
        if (QueryCodecSupport.uint(f.get(0), 1) != 1 || QueryCodecSupport.uint64Bits(f.get(11), 12) == 0
                ||QueryCodecSupport.uint32(f.get(10),11)>TargetEvidenceRetirementContext.MAX_USES||phase<1||phase>4
                ||adopted.length!=0&&adopted.length!=32||!Arrays.equals(inventoryDigest,QueryCodecSupport.fixed(f.get(n),17,32)))
            throw new IllegalArgumentException("invalid original closure schema/revision");
        return f;
    }
    private static void requireClosure(TargetExternalEvidenceObjectResource resource, byte[] encoded) {
        final var f = closureFields(encoded);
        if (QueryCodecSupport.uint32(f.get(1), 2) != resource.subtype().wireValue()
                || !Arrays.equals(QueryCodecSupport.bytes(f.get(2), 3), Bytes.utf8(resource.exactKey()))
                || !Arrays.equals(QueryCodecSupport.fixed(f.get(3), 4, 32), resource.objectDigest())
                || !Arrays.equals(QueryCodecSupport.fixed(f.get(4), 5, 32), resource.valueDigest())
                || QueryCodecSupport.uint(f.get(5), 6) != resource.canonicalValueBytes()
                || !Arrays.equals(Bytes.u64beBits(QueryCodecSupport.uint64Bits(f.get(6), 7)), resource.immutableVersion())
                || !Arrays.equals(QueryCodecSupport.fixed(f.get(12), 13, 32), resource.catalogNamespaceId()))
            throw new IllegalArgumentException("copy closure changed the exact Catalog object");
    }
    private static void requireObject(TargetExternalEvidenceObjectResource resource,byte[] raw,byte[] inputs) {
        if (resource.subtype() == TargetExternalEvidenceObjectResource.Subtype.RECOVERY_PROOF) {
            final var proof = TargetRecoveryProof.decode(raw); final var ref = proof.reference(resource.exactKey());
            if (!Arrays.equals(ref.proofDigest(), resource.objectDigest())
                    ||!resource.shard().equals(proof.mapping().admission().shard())
                    ||!Arrays.equals(resource.tenantScope(),proof.tenantScope())
                    ||!Arrays.equals(resource.recoveryLineage(),proof.frozenOwner().recoveryLineage()))
                throw new IllegalArgumentException("copy changed proof digest/Source/tenant/lineage");
            if (resource.provenance() instanceof TargetExternalEvidenceObjectResource.Recovery origin) origin.owner().requireProof(proof);
            final var f = TargetCompatibilityCodec.read(inputs, MAX_PUBLIC_INPUT_BYTES, 12, false, "recovery public history");
            QueryCodecSupport.requireNumbers(f, new int[]{1,2,3,4,5,6,7,8,9,10,11,12}, "recovery public history");
            if (QueryCodecSupport.uint(f.get(0), 1) != 1
                    || !Arrays.equals(QueryCodecSupport.fixed(f.get(1), 2, 32), proof.observer().deploymentId())
                    || !Arrays.equals(QueryCodecSupport.bytes(f.get(4), 5), proof.issuer().canonicalBytes())
                    || QueryCodecSupport.uint32Bits(f.get(5), 6) != proof.signingKeyVersion()
                    || !Arrays.equals(QueryCodecSupport.bytes(f.get(9), 10), Bytes.utf8(proof.sourceCommit()))
                    || !Arrays.equals(QueryCodecSupport.fixed(f.get(10), 11, 32), proof.sdkJar()))
                throw new IllegalArgumentException("copy changed original recovery issuer/SDK scope");
            QueryCodecSupport.fixed(f.get(6), 7, 44);
            final var material = TargetCompatibilityCodec.read(QueryCodecSupport.nested(f.get(11), 12), 2048, 11, false, "recovery material approval");
            QueryCodecSupport.requireNumbers(material, new int[]{1,2,3,4,5,6,7,8,9,10,11}, "recovery material approval");
            if (QueryCodecSupport.uint(material.get(0), 1) != 1) throw new IllegalArgumentException("unknown recovery material history schema");
            QueryCodecSupport.fixed(material.get(8), 9, 44);
            final var originalMaterial=TargetCompatibilityCodec.read(proof.publicMaterial(),TargetRecoveryProofRef.MAX_PROOF_BYTES,
                    7,false,"original archived public material");
            QueryCodecSupport.requireNumbers(originalMaterial,new int[]{1,2,3,4,5,6,7},"original archived public material");
            final var authorization=TargetCompatibilityCodec.read(QueryCodecSupport.nested(originalMaterial.get(1),2),32*1024,
                    31,false,"original recovery authorization");
            final int[] numbers=new int[31];for(int i=0;i<31;i++)numbers[i]=i+1;
            QueryCodecSupport.requireNumbers(authorization,numbers,"original recovery authorization");
            final long width=QueryCodecSupport.uint(f.get(3),4),before=QueryCodecSupport.uint(f.get(7),8),after=QueryCodecSupport.uint(f.get(8),9);
            if(width<1||width>60000||after<=before||proof.observedAt().earliestEpochMs()<before||proof.observedAt().latestEpochMs()>=after
                    ||!Arrays.equals(QueryCodecSupport.fixed(material.get(1),2,32),QueryCodecSupport.fixed(authorization.get(1),2,32))
                    ||!Arrays.equals(QueryCodecSupport.fixed(material.get(2),3,32),QueryCodecSupport.fixed(authorization.get(2),3,32))
                    ||QueryCodecSupport.uint64Bits(material.get(3),4)!=QueryCodecSupport.uint64Bits(authorization.get(3),4)
                    ||!Arrays.equals(QueryCodecSupport.fixed(material.get(4),5,32),proof.tenantScope())
                    ||!Arrays.equals(QueryCodecSupport.fixed(material.get(4),5,32),QueryCodecSupport.fixed(authorization.get(4),5,32))
                    ||!Arrays.equals(QueryCodecSupport.bytes(material.get(5),6),QueryCodecSupport.bytes(authorization.get(26),27))
                    ||QueryCodecSupport.uint32Bits(material.get(6),7)!=QueryCodecSupport.uint32Bits(authorization.get(27),28)
                    ||QueryCodecSupport.uint32Bits(material.get(7),8)!=QueryCodecSupport.uint32Bits(authorization.get(28),29)
                    ||QueryCodecSupport.uint(material.get(10),11)<=QueryCodecSupport.uint(material.get(9),10))
                throw new IllegalArgumentException("copy changed original proof/material approval window/scope");
            proof.observedAt().requireWidthAtMost(width);
            final String prefix = new String(QueryCodecSupport.bytes(f.get(2), 3), StandardCharsets.UTF_8);
            if (!resource.exactKey().equals(prefix + "/target-kafka-recovery-proof/" + Bytes.hex(proof.digest())))
                throw new IllegalArgumentException("copy changed original recovery namespace prefix");
        } else {
            final var artifact = TargetKafkaReceiptControlArtifact.decode(raw);
            if (artifact.kind() != resource.subtype().wireValue() || !Arrays.equals(artifact.digest(), resource.objectDigest()))
                throw new IllegalArgumentException("copy changed original CONTROL object");
            final TargetExternalEvidenceOwner owner=resource.provenance() instanceof TargetExternalEvidenceObjectResource.Protection p
                    ?p.owner():resource.provenance() instanceof TargetExternalEvidenceObjectResource.Anchor a?a.owner():null;
            if(owner!=null&&(!Arrays.equals(owner.originalControlDigest(),Bytes.sha256(artifact.control().canonicalBytes()))
                    ||!Arrays.equals(owner.originalControlSource().canonicalBytes(),artifact.authorizedAt().canonicalBytes())))
                throw new IllegalArgumentException("copy changed original CONTROL Source provenance");
            final var f = TargetCompatibilityCodec.read(inputs, MAX_PUBLIC_INPUT_BYTES, 8, false, "CONTROL public history");
            QueryCodecSupport.requireNumbers(f, new int[]{1,2,3,4,5,6,7,8}, "CONTROL public history");
            if (QueryCodecSupport.uint(f.get(0), 1) != 1) throw new IllegalArgumentException("unknown CONTROL public history schema");
            QueryCodecSupport.fixed(f.get(6), 7, 44);
            final var protection = TargetKafkaReceiptControlArtifact.decode(QueryCodecSupport.nested(f.get(7), 8));
            if (protection.kind() != TargetKafkaReceiptControlArtifact.PROTECTION
                    || artifact.kind() == TargetKafkaReceiptControlArtifact.PROTECTION
                    && !Arrays.equals(artifact.canonicalBytes(), protection.canonicalBytes())
                    || artifact.kind() == TargetKafkaReceiptControlArtifact.ANCHOR
                    && !Arrays.equals(artifact.protectionDigest(), protection.digest()))
                throw new IllegalArgumentException("CONTROL copy lost original protection");
        }
    }
}
