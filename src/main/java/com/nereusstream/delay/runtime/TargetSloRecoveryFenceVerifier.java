package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.Arrays;
import java.util.Objects;

/** Local actual-prefix facts only; independent original Fence/time authority and old-domain qualification remain required. */
public final class TargetSloRecoveryFenceVerifier {
    private TargetSloRecoveryFenceVerifier() {}

    public static TargetQuotaMutation requireApplied(TargetStoreBackend.Reader reader, TargetQuotaScope scope,
            byte[] lineage, SystemMutation exactFence, TargetQuotaMutation actualCut) {
        Objects.requireNonNull(reader); Objects.requireNonNull(scope); Objects.requireNonNull(exactFence);
        Objects.requireNonNull(actualCut); Bytes.requireLength(lineage, 16, "SLO recovery Fence lineage");
        reader.requireWithinElapsedBudget();
        if (exactFence.type() != SystemMutationType.TIME_FENCE) {
            throw new IllegalStateException("SLO recovery requires its exact original type-4 TIME_FENCE");
        }
        final var aggregate = reader.aggregate();
        if (scope.target() != null || !scope.shard().equals(reader.shardId())
                || !scope.shard().equals(exactFence.shardId()) || !scope.shard().equals(actualCut.source().shardId())
                || !actualCut.equals(aggregate.mutation()) || actualCut.sequence() != reader.sourceSequence()
                || reader.source() == null
                || !Arrays.equals(actualCut.source().canonicalBytes(), reader.source().canonicalBytes())) {
            throw new IllegalStateException("SLO recovery Fence cut is not the actual current Shard Source frontier");
        }
        final var body = TargetTimeFenceBody.decode(exactFence.canonicalBody());
        final var authorBytes = exactFence.authorIdentity();
        if (authorBytes.length > TargetTimeFenceVerifier.MAX_WRITER_ID_BYTES + 17) {
            throw new IllegalStateException("SLO recovery Fence author exceeds the original verifier bound");
        }
        final var author = AuthorIdentity.decode(authorBytes);
        if (!body.shard().equals(scope.shard()) || body.retryUntil() != exactFence.retryUntilEpochMs()
                || body.fenceKeyVersion() != exactFence.signingKeyVersion()
                || !Arrays.equals(body.proofId(), exactFence.logicalOperationIdentity())
                || author.kind() != AuthorIdentity.Kind.FENCE
                || author.first().length > TargetTimeFenceVerifier.MAX_WRITER_ID_BYTES
                || !Arrays.equals(author.canonicalBytes(), authorBytes)) {
            throw new IllegalStateException("SLO recovery Fence changed its exact body/key/proof/FENCE author binding");
        }
        final var identity = new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD, scope.shard(),
                aggregate.accountingIncarnation(), null, null);
        final var rootKey = identity.key(); rootKey[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var rootRaw = Objects.requireNonNull(reader.get(ColumnFamily.META, rootKey),
                "SLO recovery Fence lacks its actual Shard Root");
        final var root = TargetQuotaIncarnation.decodeForStore(rootKey,
                TargetValueEnvelope.decode(rootRaw, TargetQuotaIncarnation.VALUE_TYPE).payload(),
                scope.shard(), scope.tenantScope());
        if (!root.identity().equals(identity) || !Arrays.equals(root.recoveryLineage(), lineage)) {
            throw new IllegalStateException("SLO recovery Fence changed its actual Root incarnation/lineage");
        }
        root.latestMutation().requireAtOrBefore(actualCut);
        final var firstKey = Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                exactFence.systemMutationId());
        final var firstRaw = reader.get(ColumnFamily.DEDUPE, firstKey);
        if (firstRaw == null) {
            throw new IllegalStateException("exact SLO recovery Fence has not been observed in the actual applied prefix; retain UNKNOWN");
        }
        final var first = result(firstKey, firstRaw);
        final var at = first.mutation();
        at.requireSourceApplied(); at.requireAtOrBefore(actualCut); first.requireOwner(root);
        if (first.kind() != TargetResultRecord.Kind.SYSTEM || first.allocation() != null
                || !Arrays.equals(at.mutationDigest(), Bytes.sha256(exactFence.canonicalEnvelope()))
                || at.source().brokerPersistenceTimeEpochMs() > exactFence.retryUntilEpochMs()
                || !Arrays.equals(first.typedPayload(), SystemMutationResult.from(exactFence, ApplyStatus.APPLIED,
                        StableCode.OK, at.source().canonicalBytes()).encode())) {
            throw new IllegalStateException("exact SLO recovery Fence lacks its original APPLIED OK System first/stamp");
        }
        final var positionKey = Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                at.source().canonicalBytes());
        final var positionRaw = Objects.requireNonNull(reader.get(ColumnFamily.DEDUPE, positionKey),
                "exact SLO recovery Fence lacks its original physical Source POSITION");
        final var position = result(positionKey, positionRaw);
        position.requireOwner(root); position.requireFirst(first);
        if (position.kind() != TargetResultRecord.Kind.POSITION_SYSTEM || !position.mutation().equals(at)
                || position.allocation() != null) {
            throw new IllegalStateException("SLO recovery Fence POSITION changed its original Root/first/Source stamp");
        }
        if (reader.closedIngressDeadlineThrough() < body.closeThrough()) {
            throw new IllegalStateException("actual cumulative ingress Fence does not cover the exact applied Fence body");
        }
        reader.requireWithinElapsedBudget();
        return at;
    }

    private static TargetResultRecord result(byte[] key, byte[] raw) {
        final var payload = TargetValueEnvelope.decode(raw, TargetResultRecord.VALUE_TYPE).payload();
        final var record = TargetResultRecord.decode(payload);
        record.requireStored(key, TargetResultRecord.VALUE_TYPE, payload);
        return record;
    }
}
