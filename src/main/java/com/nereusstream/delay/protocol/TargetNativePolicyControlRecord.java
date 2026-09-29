package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Immutable source-applied Target Native authority event retained by the Target Store. */
public final class TargetNativePolicyControlRecord {
    public static final int VALUE_TYPE = 42;
    public static final int MAX_CANONICAL_BYTES = TargetNativePolicyControlBody.MAX_CANONICAL_BYTES
            + TargetMembershipGrant.MAX_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + 512;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-native-control-record\0");

    private final TargetNativePolicyControlBody body;
    private final TargetQuotaMutation mutation;
    private final ControlAuthor author;
    private final TargetMembershipGrant grant;
    private final byte[] lineage;
    private final byte[] digest;

    public TargetNativePolicyControlRecord(
            final TargetNativePolicyControlBody body,
            final TargetQuotaMutation mutation,
            final ControlAuthor author,
            final TargetMembershipGrant grant,
            final byte[] lineage) {
        this.body = Objects.requireNonNull(body, "body");
        this.mutation = Objects.requireNonNull(mutation, "mutation");
        this.author = Objects.requireNonNull(author, "author");
        mutation.requireSourceApplied();
        TargetCompatibilityCodec.assigned(lineage, 16, "recoveryLineage");
        this.lineage = Bytes.copy(lineage);
        final var request = body.request();
        final boolean memberOperation = request.operationKind() == ControlOperationKind.APPROVE_TARGET_NATIVE_MEMBER
                || request.operationKind() == ControlOperationKind.CLOSE_TARGET_NATIVE_MEMBER;
        if (!body.shard().equals(mutation.source().shardId())
                || !body.shard().equals(request.scope().sourceShard())
                || memberOperation != (grant != null)
                || grant != null && !Arrays.equals(grant.digest(), request.grantDigest())) {
            throw new IllegalArgumentException("Target Native control record source/member projection mismatch");
        }
        this.grant = grant;
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public TargetNativePolicyControlBody body() {
        return body;
    }

    public TargetQuotaMutation mutation() {
        return mutation;
    }

    public ControlAuthor author() {
        return author;
    }

    public TargetMembershipGrant grant() {
        return grant;
    }

    public byte[] recoveryLineage() {
        return Bytes.copy(lineage);
    }

    public SourcePosition source() {
        return mutation.source();
    }

    public byte[] key() {
        final var request = body.request();
        return switch (request.operationKind()) {
            case INSTALL_TARGET_NATIVE_PUBLISHER_PERMISSION ->
                TargetKeyCodec.nativePublisher(request.scope().digest(), request.keyGeneration());
            case CLOSE_TARGET_NATIVE_PUBLISHER_PERMISSION ->
                TargetKeyCodec.nativePublisherClosure(request.scope().digest(), request.keyGeneration());
            case ACTIVATE_TARGET_NATIVE_POLICY ->
                TargetKeyCodec.nativeActivation(
                        request.scope().digest(), request.snapshot().generation());
            case APPROVE_TARGET_NATIVE_MEMBER ->
                TargetKeyCodec.nativeMember(request.scope().digest(), request.grantDigest());
            case CLOSE_TARGET_NATIVE_MEMBER ->
                TargetKeyCodec.nativeMemberClosure(request.scope().digest(), request.grantDigest());
            default -> throw new IllegalStateException("unknown Target Native control operation");
        };
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, body.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, mutation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, author.canonicalBytes());
            if (grant != null) {
                CanonicalProtobuf.bytes(out, 5, grant.canonicalBytes());
            }
            CanonicalProtobuf.bytes(out, 6, lineage);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 7, digest);
        });
    }

    public static TargetNativePolicyControlRecord decode(final byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(
                encoded, MAX_CANONICAL_BYTES, 7, false, "Target Native policy control record");
        if (fields.size() != 6 && fields.size() != 7) {
            throw new IllegalArgumentException("Target Native policy control record has an unexpected field count");
        }
        final boolean hasGrant = fields.size() == 7 && fields.get(4).number() == 5;
        QueryCodecSupport.requireNumbers(
                fields,
                hasGrant ? new int[] {1, 2, 3, 4, 5, 6, 7} : new int[] {1, 2, 3, 4, 6, 7},
                "Target Native policy control record");
        if (QueryCodecSupport.uint32(fields.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown Target Native policy control record version");
        }
        final var result = new TargetNativePolicyControlRecord(
                TargetNativePolicyControlBody.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(2), 3)),
                ControlAuthor.decode(QueryCodecSupport.bytes(fields.get(3), 4)),
                hasGrant ? TargetMembershipGrant.decode(QueryCodecSupport.bytes(fields.get(4), 5)) : null,
                QueryCodecSupport.fixed(fields.get(hasGrant ? 5 : 4), 6, 16));
        if (!Bytes.constantTimeEquals(result.digest, QueryCodecSupport.fixed(fields.getLast(), 7, 32))) {
            throw new IllegalArgumentException("Target Native policy control record digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target Native policy control record");
        return result;
    }

    public static TargetNativePolicyControlRecord decodeForStore(
            final byte[] key, final byte[] encoded, final ShardId shard, final byte[] lineage) {
        final var result = decode(encoded);
        if (!Arrays.equals(key, result.key())
                || !shard.equals(result.body.shard())
                || !Arrays.equals(lineage, result.lineage)) {
            throw new IllegalStateException("Target Native control Store key/Shard/lineage mismatch");
        }
        return result;
    }
}
