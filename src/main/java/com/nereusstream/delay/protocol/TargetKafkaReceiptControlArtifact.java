package com.nereusstream.delay.protocol;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.Objects;

/** Dedicated signed artifacts in the original CONTROL trust domain; neither is physical SEND authority. */
public final class TargetKafkaReceiptControlArtifact {
    public static final int PROTECTION = 1;
    public static final int ANCHOR = 2;
    public static final int MANAGEMENT = 3;
    public static final int MAX_PREPARED_BYTES = 128 * 1024;
    public static final int MAX_ANCHOR_BYTES = 4096;
    public static final int MAX_CANONICAL_BYTES = TargetKafkaReceiptProtection.MAX_CANONICAL_BYTES
            + MAX_PREPARED_BYTES + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 256;
    private final int kind;
    private final byte[] image;
    private final byte[] protectionDigest;
    private final PreparedControlOperation control;
    private final SourcePosition authorizedAt;
    private final byte[] digest;
    private final byte[] signature;

    private TargetKafkaReceiptControlArtifact(int kind, byte[] image, byte[] protectionDigest,
            PreparedControlOperation control, SourcePosition authorizedAt, byte[] signature) {
        this.kind = kind;
        this.image = Bytes.copy(Objects.requireNonNull(image, "actual image"));
        this.control = Objects.requireNonNull(control, "original registered CONTROL operation");
        this.authorizedAt = Objects.requireNonNull(authorizedAt, "original authorization Source");
        this.protectionDigest = Bytes.copy(Objects.requireNonNull(protectionDigest, "protection digest"));
        this.signature = Bytes.copy(signature);
        TargetSourcePosition.requireBounded(authorizedAt);
        if (control.canonicalBytes().length > MAX_PREPARED_BYTES || signature.length != 64
                || (control.kind() != ControlOperationKind.CREATE_TARGET_CHANNEL
                        && control.kind() != ControlOperationKind.RENEW_TARGET_CHANNEL)
                || !(control.request().branch() instanceof TargetChannelControlRequest request)
                || request.channel().context().kind() != ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT
                || !request.channel().context().sourceShard().equals(authorizedAt.shardId())) {
            throw new IllegalArgumentException("receipt artifact changes original CONTROL channel/Source or signature bounds");
        }
        if (kind == PROTECTION) {
            TargetKafkaReceiptProtection.decode(image).requireChannel(request.channel());
            if (protectionDigest.length != 0) { throw new IllegalArgumentException("protection artifact cannot recursively reference itself"); }
        } else if (kind == ANCHOR) {
            TargetCompatibilityCodec.assigned(protectionDigest, 32, "original protection digest");
            anchorFields(image);
        } else if (kind == MANAGEMENT) {
            final var managed = TargetKafkaReceiptManagementImage.decode(image);
            if (protectionDigest.length != 0 || !Arrays.equals(request.channel().context().controlScopeRef(), managed.controlScopeRef())) {
                throw new IllegalArgumentException("topic management changes original CONTROL scope");
            }
        } else { throw new IllegalArgumentException("unknown receipt CONTROL artifact kind"); }
        if (kind == MANAGEMENT && unsignedBytes().length + 104 > TargetKafkaReceiptProtection.MAX_MANAGEMENT_BYTES) {
            throw new IllegalArgumentException("signed deployment authorization exceeds retained management image bound");
        }
        digest = kind == ANCHOR ? Bytes.sha256(Bytes.utf8("nereus-delay-target-receipt-anchor\0"), image)
                : Bytes.sha256(Bytes.utf8(kind == MANAGEMENT ? "nereus-delay-target-kafka-receipt-management\0"
                        : "nereus-delay-target-kafka-receipt-protection\0"), unsignedBytes());
    }

    public static TargetKafkaReceiptControlArtifact signProtection(TargetKafkaReceiptProtection protection,
            PreparedControlOperation control, SourcePosition authorizedAt, PrivateKey key) {
        return sign(PROTECTION, protection.canonicalBytes(), new byte[0], control, authorizedAt, key);
    }

    /** Caller must supply the actual private sampler's original bytes after authenticated protected sampling. */
    public static TargetKafkaReceiptControlArtifact signAnchor(byte[] actualObservation, byte[] protectionDigest,
            PreparedControlOperation control, SourcePosition authorizedAt, PrivateKey key) {
        return sign(ANCHOR, actualObservation, protectionDigest, control, authorizedAt, key);
    }
    public static TargetKafkaReceiptControlArtifact signManagement(TargetKafkaReceiptManagementImage image,
            PreparedControlOperation control, SourcePosition authorizedAt, PrivateKey key) {
        return sign(MANAGEMENT, image.canonicalBytes(), new byte[0], control, authorizedAt, key);
    }

    private static TargetKafkaReceiptControlArtifact sign(int kind, byte[] image, byte[] protectionDigest,
            PreparedControlOperation control, SourcePosition at, PrivateKey key) {
        final var unsigned = new TargetKafkaReceiptControlArtifact(kind, image, protectionDigest, control, at, new byte[64]);
        try {
            final var signer = Signature.getInstance("Ed25519");
            signer.initSign(Objects.requireNonNull(key, "original CONTROL signing key"));
            signer.update(unsigned.signatureInput());
            return new TargetKafkaReceiptControlArtifact(kind, image, protectionDigest, control, at, signer.sign());
        } catch (GeneralSecurityException failure) { throw new IllegalArgumentException("cannot sign receipt CONTROL artifact", failure); }
    }

    public boolean verifySignature(PublicKey key) {
        try {
            if (!control.verifySignature(key)) { return false; }
            final var verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(key); verifier.update(signatureInput()); return verifier.verify(signature);
        } catch (GeneralSecurityException failure) { throw new IllegalArgumentException("cannot verify receipt CONTROL artifact", failure); }
    }

    public int kind() { return kind; }
    public byte[] image() { return Bytes.copy(image); }
    public byte[] digest() { return Bytes.copy(digest); }
    public byte[] protectionDigest() { return Bytes.copy(protectionDigest); }
    public PreparedControlOperation control() { return control; }
    public SourcePosition authorizedAt() { return authorizedAt; }
    public TargetKafkaReceiptProtection protection() {
        if (kind != PROTECTION) { throw new IllegalStateException("anchor is not a protection image"); }
        return TargetKafkaReceiptProtection.decode(image);
    }

    public void requireAnchorBinding(TargetKafkaReceiptControlArtifact protectedBy, TargetKafkaReceiptBinding binding) {
        if (kind != ANCHOR || protectedBy.kind != PROTECTION
                || !Arrays.equals(protectedBy.digest, protectionDigest)
                || !Arrays.equals(binding.scanObservationDigest(), digest)
                || !Arrays.equals(binding.retentionProtectionDigest(), protectionDigest)
                || !Arrays.equals(control.canonicalBytes(), protectedBy.control.canonicalBytes())
                || !Arrays.equals(authorizedAt.canonicalBytes(), protectedBy.authorizedAt.canonicalBytes())) {
            throw new IllegalArgumentException("anchor differs from its original CONTROL/protection/Admission references");
        }
        final var p = protectedBy.protection();
        final var f = anchorFields(image);
        if (!Arrays.equals(QueryCodecSupport.nested(f.get(1), 2), p.resource().canonicalBytes())
                || !Arrays.equals(QueryCodecSupport.bytes(f.get(2), 3), Bytes.utf8(p.physicalTopic()))
                || QueryCodecSupport.uint(f.get(3), 4) != p.physicalPartition()
                || QueryCodecSupport.uint64Bits(f.get(4), 5) != binding.scanStartOffset()
                || !Arrays.equals(QueryCodecSupport.fixed(f.get(5), 6, 32), protectionDigest)
                || !p.resource().equals(binding.receiptResource()) || !p.physicalTopic().equals(binding.receiptPhysicalTopic())
                || p.physicalPartition() != binding.receiptPartition() || p.slotsPerShard() != binding.slotsPerShard()) {
            throw new IllegalArgumentException("anchor resource/geometry/start differs from its durable protection and Admission");
        }
    }

    private byte[] unsignedBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.uint32(out, 2, kind);
            CanonicalProtobuf.bytes(out, 3, image);
            CanonicalProtobuf.bytes(out, 4, protectionDigest);
            CanonicalProtobuf.bytes(out, 5, control.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, authorizedAt.canonicalBytes());
        });
    }
    private byte[] signatureInput() {
        return Bytes.concat(Bytes.utf8(kind == PROTECTION
                ? "nereus-delay-target-kafka-receipt-protection-signature\0"
                : kind == ANCHOR ? "nereus-delay-target-kafka-receipt-anchor-signature\0"
                : "nereus-delay-target-kafka-receipt-management-signature\0"),
                Bytes.lp32(unsignedBytes()), Bytes.u32be(control.signingKeyVersion()));
    }
    public byte[] canonicalBytes() {
        return Bytes.concat(unsignedBytes(), CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 7, digest); CanonicalProtobuf.bytes(out, 8, signature);
        }));
    }
    public static TargetKafkaReceiptControlArtifact decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 8, false, "receipt CONTROL artifact");
        QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,4,5,6,7,8}, "receipt CONTROL artifact");
        if (QueryCodecSupport.uint(f.getFirst(), 1) != 1) { throw new IllegalArgumentException("unknown receipt artifact schema"); }
        final byte[] prepared = QueryCodecSupport.nested(f.get(4), 5);
        if (prepared.length > MAX_PREPARED_BYTES) { throw new IllegalArgumentException("receipt CONTROL operation exceeds bound"); }
        final var result = new TargetKafkaReceiptControlArtifact(QueryCodecSupport.uint32(f.get(1), 2),
                QueryCodecSupport.bytes(f.get(2), 3), QueryCodecSupport.bytes(f.get(3), 4),
                PreparedControlOperation.decode(prepared), SourcePositionCodec.decode(QueryCodecSupport.nested(f.get(5), 6)),
                QueryCodecSupport.fixed(f.get(7), 8, 64));
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(f.get(6), 7, 32))) { throw new IllegalArgumentException("receipt artifact digest changed"); }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "receipt CONTROL artifact");
        return result;
    }

    private static java.util.List<CanonicalProtobuf.Reader.Field> anchorFields(byte[] image) {
        final var f = TargetCompatibilityCodec.read(image, MAX_ANCHOR_BYTES, 14, false, "receipt anchor observation");
        QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14}, "receipt anchor observation");
        if (QueryCodecSupport.uint(f.getFirst(), 1) != 1 || QueryCodecSupport.uint64Bits(f.get(4), 5) < 0
                || QueryCodecSupport.uint(f.get(8), 9) < 13
                || QueryCodecSupport.uint64Bits(f.get(12), 13) < 0
                || QueryCodecSupport.uint64Bits(f.get(12), 13) > QueryCodecSupport.uint64Bits(f.get(4), 5)) {
            throw new IllegalArgumentException("receipt anchor lacks its actual empty-end Fetch range");
        }
        KafkaReceiptSlotResource.decode(QueryCodecSupport.nested(f.get(1), 2));
        QueryCodecSupport.fixed(f.get(5), 6, 32); QueryCodecSupport.fixed(f.get(7), 8, 32);
        QueryCodecSupport.fixed(f.get(13), 14, 32);
        return f;
    }
}
