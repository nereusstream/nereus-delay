package com.nereusstream.delay.protocol;

import java.security.PrivateKey;
import java.util.Objects;

/**
 * Constructs a signed source mutation for one already prepared Control target.
 *
 * <p>The caller still supplies a canonical operation-specific body and a
 * trusted service author. This factory derives the type-specific logical
 * identity, signs the exact envelope, and runs the complete target binding
 * check before returning it.</p>
 */
public final class ControlSystemMutationFactory {
    private ControlSystemMutationFactory() {}

    public static SystemMutation sign(
            final PreparedControlOperation prepared,
            final ControlTargetRef target,
            final ShardId shardId,
            final long retryUntilEpochMs,
            final byte[] canonicalBody,
            final byte[] authorIdentity,
            final int signingKeyVersion,
            final PrivateKey signingKey) {
        Objects.requireNonNull(prepared, "prepared");
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(canonicalBody, "canonicalBody");
        Objects.requireNonNull(authorIdentity, "authorIdentity");
        Objects.requireNonNull(signingKey, "signingKey");
        final SystemMutationType type =
                ControlTargetMutationBinding.expectedMutationType(prepared.kind(), target.targetKind());
        final ControlRef controlRef =
                new ControlRef(prepared.operationId(), prepared.requestHash(), target.targetIndex());
        final byte[] logicalIdentity =
                switch (type) {
                    case APPLY_SHARD_CONTROL ->
                        controlRef.logicalOperationIdentity(
                                ApplyShardControlBody.decode(canonicalBody).controlKind());
                    case REPLAY_DEAD_LETTER, RESOLVE_UNCERTAIN -> controlRef.logicalOperationIdentity(type);
                    default -> throw new IllegalArgumentException("unsupported Control target mutation type");
                };
        final SystemMutation mutation = SystemMutation.signed(
                shardId,
                type,
                retryUntilEpochMs,
                logicalIdentity,
                canonicalBody,
                authorIdentity,
                signingKeyVersion,
                signingKey);
        prepared.validateTargetMutation(target, mutation);
        return mutation;
    }
}
