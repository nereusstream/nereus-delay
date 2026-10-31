package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplayOutcome;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Privately minted from actual Target first/POS, for progressing only the isolated old shadow. */
public final class TargetMigrationTargetApplied {
    private final ShardStore.LegacyCheckpointReplayCopy old;
    private final TargetMigrationPlan plan;
    private final SourceReplayEntry entry;
    private final TargetMigrationShadowProgress progress;
    private final SourceReplayOutcome outcome;
    private final Runnable current;
    private final IngressFenceState fence;

    private TargetMigrationTargetApplied(ShardStore.LegacyCheckpointReplayCopy old, TargetMigrationPlan plan,
            SourceReplayEntry entry, TargetMigrationShadowProgress progress, SourceReplayOutcome outcome,
            Runnable current, IngressFenceState fence) {
        this.old = old; this.plan = plan; this.entry = entry; this.progress = progress; this.outcome = outcome;
        this.current = current; this.fence = fence;
    }

    /** Reads real Native results; a caller result, source watermark or hash alone cannot construct this proof. */
    public static TargetMigrationTargetApplied read(ShardStore.LegacyCheckpointReplayCopy old,
            ShardStore target, TargetMigrationPlan plan, SourceReplayEntry entry, BoundedReadBudget budget,
            Runnable originalDomainProtection) {
        Objects.requireNonNull(old); Objects.requireNonNull(target); Objects.requireNonNull(plan); Objects.requireNonNull(entry);
        final var guard = Objects.requireNonNull(originalDomainProtection);
        guard.run(); TargetMigrationStore.requireIdentity(target.metadata(), plan);
        final var prior = Objects.requireNonNull(old.store().appliedShardLogPosition());
        final var read = target.readWithBudget(budget, () -> {
            guard.run();
            final var root = TargetMigrationStore.decodeRoot(target.get(ColumnFamily.META, TargetMigrationRoot.keyFor(plan.scope().shard())));
            if (root == null || !root.activated() || !root.plan().equals(plan)
                    || target.appliedShardLogPosition().compareTo(entry.position()) < 0)
                throw new IllegalStateException("shadow progression lacks actual same-plan Target A/frontier");
            final byte[] positionKey = Bytes.concat(new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                    entry.position().canonicalBytes());
            final var position = result(target, positionKey);
            if (position == null || !Arrays.equals(position.mutation().source().canonicalBytes(), entry.position().canonicalBytes()))
                throw new IllegalStateException("actual Target physical Source result is unavailable");
            if (!Arrays.equals(position.recoveryLineage(), plan.newLineage())
                    || !Arrays.equals(position.tenantScope(), plan.scope().tenantScope()))
                throw new IllegalStateException("Target physical proof changed migration lineage/tenant");
            TargetResultRecord first;
            SourceReplayOutcome outcome;
            IngressFenceState fence = null;
            final TargetMigrationShadowProgress.LogicalProofKind kind;
            if (entry instanceof SourceReplayRecord command) {
                if (!Arrays.equals(position.mutation().mutationDigest(), command.command().canonicalFrameDigest()))
                    throw new IllegalStateException("Target physical Command changed its original frame");
                if (position.kind() == TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED) {
                    final var value = position.requireExpiredCommand(command.command());
                    outcome = SourceReplayOutcome.command(entry.position(), value); first = null;
                    kind = TargetMigrationShadowProgress.LogicalProofKind.EXPIRED_COMMAND_POSITION;
                } else {
                    if (position.kind() != TargetResultRecord.Kind.POSITION_COMMAND)
                        throw new IllegalStateException("Target Source proof has another physical result kind");
                    first = result(target, Bytes.concat(new byte[] {TargetKeyCodec.RESULT_COMMAND_TAG, TargetKeyCodec.KEY_FORMAT},
                            command.command().commandId().bytes()));
                    if (first == null || first.kind() != TargetResultRecord.Kind.COMMAND)
                        throw new IllegalStateException("Target physical Command lacks its retained first");
                    position.requireFirst(first);
                    final var value = CommandDedupeRecord.decode(first.typedPayload());
                    if (!value.protocolTuple().equals(command.command().protocolTuple())
                            || !Arrays.equals(value.commandHash(), command.command().commandHash()))
                        throw new IllegalStateException("Target first changed original Command hash/tuple");
                    final var original = value.result();
                    outcome = SourceReplayOutcome.command(entry.position(), new CommandResult(original.applyStatus(), original.stableCode(),
                            original.generation(), original.stateVersion(), original.messageStatus(), entry.position().canonicalBytes()));
                    kind = TargetMigrationShadowProgress.LogicalProofKind.FIRST_POSITION;
                }
            } else {
                final var input = ((SourceReplayMutation) entry).mutation();
                if (position.kind() != TargetResultRecord.Kind.POSITION_SYSTEM
                        || !Arrays.equals(position.mutation().mutationDigest(), Bytes.sha256(input.canonicalEnvelope())))
                    throw new IllegalStateException("Target physical System proof changed its exact input");
                first = result(target, Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, input.systemMutationId()));
                if (first == null || first.kind() != TargetResultRecord.Kind.SYSTEM)
                    throw new IllegalStateException("Target physical System proof lacks its actual first");
                position.requireFirst(first);
                final var original = SystemMutationResult.decode(first.typedPayload());
                if (!Arrays.equals(original.mutationId(), input.systemMutationId()) || !Arrays.equals(original.mutationHash(), input.mutationHash())
                        || original.mutationType() != input.type() || original.retryUntilEpochMs() != input.retryUntilEpochMs()
                        || !Arrays.equals(original.authorIdentity(), input.authorIdentity()))
                    throw new IllegalStateException("Target first changed original System identity/hash/author");
                outcome = SourceReplayOutcome.systemMutation(entry.position(), SystemMutationResult.from(input,
                        original.applyStatus(), original.stableCode(), entry.position().canonicalBytes()));
                if (input.type() == SystemMutationType.TIME_FENCE && original.applyStatus() == ApplyStatus.APPLIED) {
                    final var body = TargetTimeFenceBody.decode(input.canonicalBody());
                    fence = new IngressFenceState(body.closeThrough(), body.proofId());
                }
                kind = TargetMigrationShadowProgress.LogicalProofKind.FIRST_POSITION;
            }
            final byte[] frame = frame(entry);
            final TargetMigrationShadowProgress marker;
            if (Arrays.equals(prior.canonicalBytes(), entry.position().canonicalBytes())) {
                final byte[] raw = old.store().get(ColumnFamily.META, TargetMigrationShadowProgress.keyFor(plan.scope().shard()));
                if (raw == null) throw new IllegalStateException("same old-shadow position has no retained Target progression");
                marker = TargetMigrationShadowProgress.decode(TargetValueEnvelope.decode(raw, TargetMigrationShadowProgress.VALUE_TYPE).payload());
                marker.requirePlan(plan); marker.requireFrame(frame);
                if (!marker.appliedAt().equals(position.mutation()) || !Arrays.equals(marker.targetPositionDigest(), position.digest()))
                    throw new IllegalStateException("same old-shadow Source point changed its actual Target proof");
            } else marker = TargetMigrationShadowProgress.fromFrame(plan, frame, position.mutation(), prior, root.digest(),
                    first == null ? null : first.digest(), position.digest(), kind);
            guard.run();
            return new TargetMigrationTargetApplied(old, plan, entry, marker, outcome, guard, fence);
        });
        guard.run(); return target.withReadView(read.view(), read::value);
    }

    public TargetMigrationShadowProgress progress() { return progress; }
    public SourceReplayOutcome outcome() { return outcome; }
    public IngressFenceState fence() { return fence; }
    public void requireStore(ShardStore actual) {
        current.run();
        if (old.store() != actual || !Arrays.equals(actual.metadata().storeIncarnation(), plan.oldStore())
                || !Arrays.equals(actual.metadata().dbIdentity(), plan.oldDbIdentity()))
            throw new IllegalStateException("migration shadow proof belongs to another actual isolated old Store");
        progress.requirePlan(plan); progress.requireFrame(frame(entry));
    }
    public void requirePosition(ShardStore actual, SourcePosition source, long sequence) {
        requireStore(actual);
        if (Arrays.equals(source.canonicalBytes(), progress.appliedAt().source().canonicalBytes())) {
            if (sequence != progress.appliedAt().sequence()) throw new IllegalStateException("same shadow Source changed sequence");
        } else if (!Arrays.equals(source.canonicalBytes(), progress.prior().canonicalBytes())
                || TargetQuotaMutation.increment(sequence) != progress.appliedAt().sequence())
            throw new IllegalStateException("shadow Source progression is not the exact next actual Target step");
    }
    private static TargetResultRecord result(ShardStore store, byte[] key) {
        final byte[] raw = store.get(ColumnFamily.DEDUPE, key);
        if (raw == null) return null;
        final var value = TargetValueEnvelope.decode(raw, TargetResultRecord.VALUE_TYPE);
        final var record = TargetResultRecord.decode(value.payload()); record.requireStored(key, value.valueType(), value.payload()); return record;
    }
    private static byte[] frame(SourceReplayEntry entry) {
        return entry instanceof SourceReplayRecord command ? CommandCodec.encodeFrame(command.command()) : ((SourceReplayMutation) entry).mutation().encodeFrame();
    }
}
