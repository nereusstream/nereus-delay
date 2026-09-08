package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Source-applied grant projection. Decoding is not a signature, registration, Route or capacity authority. */
public final class TargetQuotaGrantActivation {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 30;
    public static final int MAX_CANONICAL_BYTES = 2
            + 3
            + TargetQuotaGrantControlRequest.MAX_CANONICAL_BYTES
            + 76
            + 4
            + TargetQuotaMutation.MAX_CANONICAL_BYTES
            + 3 * 34
            + 4
            + TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-quota-grant-activation\0");
    private final TargetQuotaGrantControlRequest request;
    private final ControlRef controlRef;
    private final TargetQuotaMutation mutation;
    private final byte[] systemMutationId;
    private final byte[] systemMutationHash;
    private final TargetQuotaIncarnation allocation;
    private final byte[] digest;

    public TargetQuotaGrantActivation(
            final TargetQuotaGrantControlRequest request,
            final ControlRef controlRef,
            final TargetQuotaMutation mutation,
            final byte[] systemMutationId,
            final byte[] systemMutationHash,
            final TargetQuotaIncarnation allocation) {
        this.request = Objects.requireNonNull(request, "request");
        this.controlRef = Objects.requireNonNull(controlRef, "controlRef");
        this.mutation = Objects.requireNonNull(mutation, "mutation");
        request.requireControlRef(controlRef);
        if (!request.next().scope().shard().equals(mutation.source().shardId())
                || Long.compareUnsigned(request.next().version(), mutation.sequence()) > 0) {
            throw new IllegalArgumentException("quota grant activation source/sequence mismatch");
        }
        this.systemMutationId = TargetCompatibilityCodec.assigned(systemMutationId, 32, "systemMutationId");
        this.systemMutationHash = TargetCompatibilityCodec.assigned(systemMutationHash, 32, "systemMutationHash");
        if (!Arrays.equals(
                systemMutationId,
                SystemMutation.computeSystemMutationId(
                        mutation.source().shardId(),
                        SystemMutationType.APPLY_SHARD_CONTROL,
                        controlRef.logicalOperationIdentity(TargetQuotaGrantControlRequest.CONTROL_KIND),
                        systemMutationHash))) {
            throw new IllegalArgumentException("quota grant activation mutation ID mismatch");
        }
        this.allocation = allocation;
        if (allocation == null) {
            if (grant().scope().target() != null
                    && (!grant().limit().isZero()
                            || (request.prior() != null
                                    && !request.prior().limit().isZero()))) {
                throw new IllegalArgumentException(
                        "current or prior nonzero Target grant requires its immutable allocation origin");
            }
        } else {
            if (grant().scope().target() == null
                    || !grant().scope().equals(allocation.scope())
                    || allocation.draining()
                    || !Arrays.equals(
                            grant().accounting().canonicalBytes(),
                            allocation.accounting().canonicalBytes())) {
                throw new IllegalArgumentException("quota grant allocation scope/accounting/phase mismatch");
            }
            final int sequenceOrder =
                    Long.compareUnsigned(allocation.allocation().sequence(), mutation.sequence());
            final int sourceOrder = allocation.allocation().source().compareTo(mutation.source());
            if ((request.prior() == null && sequenceOrder != 0)
                    || (request.prior() != null && !request.prior().limit().isZero() && sequenceOrder == 0)
                    || sequenceOrder > 0
                    || Integer.signum(sequenceOrder) != Integer.signum(sourceOrder)
                    || (sequenceOrder == 0 && !allocation.allocation().equals(mutation))) {
                throw new IllegalArgumentException("quota grant allocation source/sequence mismatch");
            }
        }
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public TargetQuotaGrantControlRequest request() {
        return request;
    }

    public TargetQuotaGrant grant() {
        return request.next();
    }

    public ControlRef controlRef() {
        return controlRef;
    }

    public TargetQuotaMutation mutation() {
        return mutation;
    }

    public byte[] systemMutationId() {
        return Bytes.copy(systemMutationId);
    }

    public byte[] systemMutationHash() {
        return Bytes.copy(systemMutationHash);
    }

    /** Historical OPEN allocation proof; the actual descriptor still governs current ingress and retirement. */
    public TargetQuotaIncarnation allocation() {
        return allocation;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] key() {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG, TargetKeyCodec.KEY_FORMAT},
                grant().scope().keySuffix());
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, request.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, controlRef.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, mutation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, systemMutationId);
            CanonicalProtobuf.bytes(out, 6, systemMutationHash);
            if (allocation != null) {
                CanonicalProtobuf.bytes(out, 7, allocation.canonicalBytes());
            }
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 8, digest);
        });
    }

    public static TargetQuotaGrantActivation decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 8, false, "Target quota activation");
        QueryCodecSupport.requireNumbers(
                fields,
                fields.size() == 8 ? new int[] {1, 2, 3, 4, 5, 6, 7, 8} : new int[] {1, 2, 3, 4, 5, 6, 8},
                "Target quota activation");
        if (QueryCodecSupport.uint32(fields.getFirst(), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported quota activation version");
        }
        final byte[] ref = QueryCodecSupport.bytes(fields.get(2), 3);
        TargetCompatibilityCodec.read(ref, 74, 3, false, "quota grant activation ControlRef");
        final var result = new TargetQuotaGrantActivation(
                TargetQuotaGrantControlRequest.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                ControlRef.decode(ref),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(3), 4)),
                QueryCodecSupport.fixed(fields.get(4), 5, 32),
                QueryCodecSupport.fixed(fields.get(5), 6, 32),
                fields.size() == 8 ? TargetQuotaIncarnation.decode(QueryCodecSupport.bytes(fields.get(6), 7)) : null);
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(fields.getLast(), 8, 32))) {
            throw new IllegalArgumentException("quota grant activation digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target quota activation");
        return result;
    }

    public static TargetQuotaGrantActivation decodeForStore(
            final byte[] key, final byte[] encoded, final ShardId shard, final byte[] authenticatedTenantRoutingScope) {
        final var result = decode(encoded);
        result.grant().scope().requireRoute(shard, authenticatedTenantRoutingScope);
        if (!Arrays.equals(key, result.key())) {
            throw new IllegalArgumentException("quota grant activation key mismatch");
        }
        return result;
    }
}
