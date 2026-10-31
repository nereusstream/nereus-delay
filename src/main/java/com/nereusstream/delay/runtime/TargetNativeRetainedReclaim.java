package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord;
import com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource;
import com.nereusstream.delay.protocol.TargetExternalEvidenceReference;
import com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact;
import com.nereusstream.delay.protocol.TargetKafkaReceiptMapping;
import com.nereusstream.delay.protocol.TargetNativeReclaimFacts;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetResourceReclaimBody;
import com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord;
import com.nereusstream.delay.protocol.TargetTimeFenceBody;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetRetirementLedgerCursor;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Native leaf deletion and one-time owned-object shares. No caller-provided allocation delta is accepted. */
public final class TargetNativeRetainedReclaim {
    private TargetNativeRetainedReclaim() {}

    public static SourceReplayMutation fence(TargetNativeReclaimFacts facts) {
        return new SourceReplayMutation(
                SystemMutation.decodeEnvelope(facts.fence().frame()),
                facts.fence().appliedAt().source(),
                null,
                null);
    }

    private static byte[] firstKey(byte[] id) {
        return Bytes.concat(new byte[] {TargetKeyCodec.RESULT_COMMAND_TAG, TargetKeyCodec.KEY_FORMAT}, id);
    }

    private static byte[] systemKey(byte[] id) {
        return Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, id);
    }

    private static byte[] positionKey(SourcePosition source) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT}, source.canonicalBytes());
    }

    private static byte[] value(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            ColumnFamily family,
            byte[] key,
            int type) {
        return TargetValueEnvelope.decode(
                        Objects.requireNonNull(
                                view.projected(family, key, overlay), "Native reclaim retained dependency"),
                        type)
                .payload();
    }

    private static TargetResultRecord result(
            TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay, byte[] key) {
        final var result = TargetResultRecord.decode(
                value(view, overlay, ColumnFamily.DEDUPE, key, TargetResultRecord.VALUE_TYPE));
        result.requireStored(key, TargetResultRecord.VALUE_TYPE, result.canonicalBytes());
        return result;
    }

    private static long closeThrough(TargetResourceReclaimBody body) {
        return TargetTimeFenceBody.decode(fence(Objects.requireNonNull(body.nativeFacts()))
                        .mutation()
                        .canonicalBody())
                .closeThrough();
    }

    private static void retainedWindow(TargetResourceReclaimBody body, TargetResultRecord record) {
        final var route = body.nativeFacts().originalRoute();
        final var id = new CommandId(record.logicalId());
        if (!id.routingId().shardId().equals(body.shard())) {
            throw new IllegalArgumentException("Native leaf logical Command belongs to another Shard");
        }
        final long deadline = Math.addExact(
                record.mutation().source().brokerPersistenceTimeEpochMs(), route.fullCommandResultRetentionMs());
        record.mutation().requireAtOrBefore(body.nativeFacts().fence().appliedAt());
        if (closeThrough(body) < deadline) {
            throw new IllegalStateException(
                    "original Source fence has not closed full result/physical query retention");
        }
    }

    private static void requireOriginalFirst(TargetResultRecord leaf, TargetResultRecord original) {
        leaf.requireFirst(original);
        if (leaf.kind() == TargetResultRecord.Kind.POSITION_COMMAND) {
            leaf.mutation().requireAfter(original.mutation());
        }
    }
    /** Full current Floor or complete live fold; each original historical capability is independently authenticated outside. */
    public static TargetQuotaMutation requireClosed(
            TargetCheckpointDependencies d, SystemMutation input, boolean allowApplied, Runnable current) {
        return requireClosed(d, TargetResourceReclaimBody.decode(input.canonicalBody()), input, allowApplied, current);
    }

    public static void requireDraftClosed(
            TargetCheckpointDependencies d, TargetResourceReclaimBody body, Runnable current) {
        requireClosed(d, body, null, false, current);
    }
    /** Same predicates over this repository's fully read retirement collector, using its already folded exact indexes. */
    public static TargetQuotaMutation requirePreparedClosed(
            TargetRetirementLedgerCursor ledger,
            TargetResourceReclaimBody body,
            SystemMutation input,
            boolean allowApplied,
            Runnable current) {
        final var d = ledger.dependenciesForClosure();
        if (body.incarnationReclaim()) {
            return TargetIncarnationReclaim.requirePreparedClosed(ledger, body, input, allowApplied, current);
        }
        final var facts = Objects.requireNonNull(body.nativeFacts());
        facts.requireFor(body);
        if (!body.nativeRetainedReclaim()
                || !d.root().metadata().shardId().equals(body.shard())
                || !Arrays.equals(d.root().root().tenantScope(), body.scope().tenantScope())
                || !Arrays.equals(d.root().root().recoveryLineage(), body.lineage())) {
            throw new IllegalArgumentException("Native reclaim changed original Source scope");
        }
        final var first = d.requireTimeFence(fence(facts));
        if (!first.mutation().equals(facts.fence().appliedAt())
                || d.ingressFence().closedThroughEpochMs() < closeThrough(body)) {
            throw new IllegalStateException("Native reclaim lost original APPLIED closure TimeFence");
        }
        final var candidate = input == null ? null : d.sourceHistory(input.systemMutationId());
        final var history =
                candidate != null && Arrays.equals(candidate.input().canonicalEnvelope(), input.canonicalEnvelope())
                        ? candidate
                        : null;
        final var decision = history == null ? null : d.resultRecord(systemKey(input.systemMutationId()));
        final boolean applied = decision != null && decision.appliedFirst();
        if (applied && !allowApplied) {
            throw new IllegalStateException("fresh Native preparation already has an APPLIED first");
        }
        final var at = applied ? history.decidedAt() : null;
        for (var operation : body.operations()) {
            current.run();
            if (operation instanceof TargetResourceReclaimBody.RetainedRecordDelete leaf) {
                // The physical complete ledger has an exact sample-reference index in its original Native view.
                ledger.requireUnreferenced(leaf.expected().key());
                retainedWindow(body, leaf.expected());
                final var actual = d.resultRecord(leaf.expected().key());
                if (applied) {
                    if (actual != null) {
                        throw new IllegalStateException("APPLIED Native leaf deletion retains its exact row");
                    }
                } else if (actual == null
                        || !Arrays.equals(
                                actual.canonicalBytes(), leaf.expected().canonicalBytes())) {
                    throw new IllegalStateException("Native leaf changed after selection");
                }
                if (leaf.expected().kind() == TargetResultRecord.Kind.RESULT
                        || leaf.expected().kind() == TargetResultRecord.Kind.POSITION_COMMAND) {
                    requireOriginalFirst(
                            leaf.expected(),
                            Objects.requireNonNull(
                                    d.resultRecord(firstKey(leaf.expected().logicalId())),
                                    "query deletion original COMMAND first"));
                }
                if (leaf.expected().kind().position()
                        && d.root()
                                        .source()
                                        .compareTo(leaf.expected().mutation().source())
                                <= 0) {
                    throw new IllegalStateException("current Source frontier POSITION is not a reclaimable leaf");
                }
            } else if (operation instanceof TargetResourceReclaimBody.RetainedAllocationRelease allocation) {
                final var actual = Objects.requireNonNull(
                                d.admission(allocation.expected().publishAttemptId()), "retained allocation Budget")
                        .attempt();
                if (!applied
                        && !Arrays.equals(
                                actual.canonicalBytes(), allocation.expected().canonicalBytes())) {
                    throw new IllegalStateException("retained allocation changed after selection");
                }
                if (applied
                        && !Arrays.equals(
                                actual.canonicalBytes(),
                                Objects.requireNonNull(
                                                ledger.projectedBudget(actual.publishAttemptId()),
                                                "complete retained allocation chain")
                                        .canonicalBytes())) {
                    throw new IllegalStateException("APPLIED retained allocation lost its exact Source successor");
                }
                requirePreparedWriterClosed(ledger, allocation.expected(), body, current);
                for (var receipt : allocation.confirmations()) {
                    current.run();
                    final var retained = Objects.requireNonNull(
                            d.evidenceConfirmation(receipt.key()), "actual confirmed owned-object deletion");
                    if (!Arrays.equals(retained.canonicalBytes(), receipt.canonicalBytes())) {
                        throw new IllegalStateException("allocation settlement replaced actual confirmation");
                    }
                    if (receipt.body().resource().provenance()
                            instanceof TargetExternalEvidenceObjectResource.Anchor anchor) {
                        requireAnchorOrigin(anchor, ledger.mapping(anchor.mappingId()), allocation.expected());
                    }
                    final var original = Objects.requireNonNull(
                            d.resultRecord(systemKey(receipt.confirmation().systemMutationId())),
                            "owned-object deletion first");
                    if (!original.appliedFirst() || !original.mutation().equals(receipt.appliedAt())) {
                        throw new IllegalStateException("allocation lacks actual APPLIED deletion");
                    }
                    d.requireSystemPosition(original);
                    final var settled = ledger.settlement(
                            allocation.expected().publishAttemptId(),
                            TargetResourceReclaimBody.RetainedAllocationRelease.shareIdentity(receipt));
                    if (settled != null && (at == null || !settled.equals(at))) {
                        throw new IllegalStateException(
                                "owned-object share was already settled by an earlier APPLIED Native reclaim");
                    }
                }
            } else {
                throw new IllegalArgumentException("Native reclaim group has a foreign operation");
            }
        }
        current.run();
        return at;
    }

    private static void requirePreparedWriterClosed(
            TargetRetirementLedgerCursor ledger,
            TargetQuotaAttemptBudget budget,
            TargetResourceReclaimBody body,
            Runnable current) {
        final var d = ledger.dependenciesForClosure();
        final var admission =
                Objects.requireNonNull(d.admission(budget.publishAttemptId()), "original retained admission");
        budget.resolvedAt().requireCoveredByFloor(body.floor());
        d.requireSystemPosition(admission.first());
        final var outcome = Objects.requireNonNull(
                ledger.resultAt(budget.resolvedAt()), "retained allocation definitive Source resolution");
        if (!outcome.appliedFirst()) {
            throw new IllegalStateException("retained allocation lost definitive Source resolution");
        }
        d.requireSystemPosition(outcome);
        if (closeThrough(body)
                < Math.max(
                        SystemMutationResult.decode(admission.first().typedPayload())
                                .retryUntilEpochMs(),
                        SystemMutationResult.decode(outcome.typedPayload()).retryUntilEpochMs())) {
            throw new IllegalStateException("original writer retry windows have not closed");
        }
        current.run();
    }

    private static TargetQuotaMutation requireClosed(
            TargetCheckpointDependencies d,
            TargetResourceReclaimBody body,
            SystemMutation input,
            boolean allowApplied,
            Runnable current) {
        if (body.incarnationReclaim()) {
            return TargetIncarnationReclaim.requireClosed(d, body, input, allowApplied, current);
        }
        final var facts = Objects.requireNonNull(body.nativeFacts());
        facts.requireFor(body);
        if (!body.nativeRetainedReclaim()
                || !d.root().metadata().shardId().equals(body.shard())
                || !Arrays.equals(d.root().root().tenantScope(), body.scope().tenantScope())
                || !Arrays.equals(d.root().root().recoveryLineage(), body.lineage())) {
            throw new IllegalArgumentException("Native reclaim changed original Source scope");
        }
        final var first = d.requireTimeFence(fence(facts));
        if (!first.mutation().equals(facts.fence().appliedAt())
                || d.ingressFence().closedThroughEpochMs() < closeThrough(body)) {
            throw new IllegalStateException("Native reclaim lost original APPLIED closure TimeFence");
        }
        final var history = input == null
                ? null
                : d.sourceHistories().stream()
                        .filter(h -> Arrays.equals(h.input().canonicalEnvelope(), input.canonicalEnvelope()))
                        .findFirst()
                        .orElse(null);
        final var decision = history == null ? null : d.resultRecord(systemKey(input.systemMutationId()));
        final boolean applied = decision != null && decision.appliedFirst();
        if (applied && !allowApplied) {
            throw new IllegalStateException("fresh Native preparation already has an APPLIED first");
        }
        final var at = applied ? history.decidedAt() : null;
        final var projected = applied
                ? projectedBudgets(d.sourceHistories(), id -> d.resultRecord(systemKey(id)))
                : Map.<String, TargetQuotaAttemptBudget>of();
        final var budgets = new HashMap<String, TargetQuotaAttemptBudget>();
        for (var a : d.admissions()) {
            budgets.put(Bytes.hex(a.attempt().publishAttemptId()), a.attempt());
        }
        final var confirmed = new HashMap<String, TargetEvidenceConfirmedRecord>();
        for (var c : d.evidenceConfirmations()) {
            confirmed.put(Bytes.hex(c.key()), c);
        }
        for (var operation : body.operations()) {
            current.run();
            if (operation instanceof TargetResourceReclaimBody.RetainedRecordDelete leaf) {
                TargetSloObservationSamples.requireUnreferenced(
                        d, leaf.expected().key());
                retainedWindow(body, leaf.expected());
                final var actual = d.resultRecord(leaf.expected().key());
                if (applied) {
                    if (actual != null) {
                        throw new IllegalStateException("APPLIED Native leaf deletion retains its exact row");
                    }
                } else if (actual == null
                        || !Arrays.equals(
                                actual.canonicalBytes(), leaf.expected().canonicalBytes())) {
                    throw new IllegalStateException("Native leaf changed after selection");
                }
                if (leaf.expected().kind() == TargetResultRecord.Kind.RESULT
                        || leaf.expected().kind() == TargetResultRecord.Kind.POSITION_COMMAND) {
                    final var original = Objects.requireNonNull(
                            d.resultRecord(firstKey(leaf.expected().logicalId())),
                            "query deletion must keep original COMMAND first");
                    requireOriginalFirst(leaf.expected(), original);
                }
                if (leaf.expected().kind().position()
                        && d.root()
                                        .source()
                                        .compareTo(leaf.expected().mutation().source())
                                <= 0) {
                    throw new IllegalStateException("current Source frontier POSITION is not a reclaimable leaf");
                }
            } else if (operation instanceof TargetResourceReclaimBody.RetainedAllocationRelease allocation) {
                final var actual = Objects.requireNonNull(
                        budgets.get(Bytes.hex(allocation.expected().publishAttemptId())), "retained allocation Budget");
                if (!applied
                        && !Arrays.equals(
                                actual.canonicalBytes(), allocation.expected().canonicalBytes())) {
                    throw new IllegalStateException("retained allocation changed after selection");
                }
                if (applied
                        && !Arrays.equals(
                                actual.canonicalBytes(),
                                Objects.requireNonNull(
                                                projected.get(Bytes.hex(actual.publishAttemptId())),
                                                "complete retained allocation chain")
                                        .canonicalBytes())) {
                    throw new IllegalStateException("APPLIED retained allocation lost its exact Source successor");
                }
                requireWriterClosed(d, allocation.expected(), body, current);
                for (var receipt : allocation.confirmations()) {
                    current.run();
                    final var retained = Objects.requireNonNull(
                            confirmed.get(Bytes.hex(receipt.key())), "actual confirmed owned-object deletion");
                    if (!Arrays.equals(retained.canonicalBytes(), receipt.canonicalBytes())) {
                        throw new IllegalStateException("allocation settlement replaced actual confirmation");
                    }
                    if (receipt.body().resource().provenance()
                            instanceof TargetExternalEvidenceObjectResource.Anchor anchor) {
                        final var mapping = d.kafkaMappings().stream()
                                .filter(m -> Arrays.equals(m.mappingId(), anchor.mappingId()))
                                .findFirst()
                                .orElseThrow();
                        requireAnchorOrigin(anchor, mapping, allocation.expected());
                    }
                    final var original = Objects.requireNonNull(
                            d.resultRecord(systemKey(receipt.confirmation().systemMutationId())),
                            "owned-object deletion first");
                    if (!original.appliedFirst() || !original.mutation().equals(receipt.appliedAt())) {
                        throw new IllegalStateException("allocation lacks actual APPLIED deletion");
                    }
                    d.requireSystemPosition(original);
                    requireUnsettled(d, allocation.expected(), receipt, at);
                }
            } else {
                throw new IllegalArgumentException("Native reclaim group has a foreign operation");
            }
        }
        current.run();
        return at;
    }
    /** Every surviving APPLIED edge has an exact before/after; later legal releases do not invalidate kind1 history. */
    public static Map<String, TargetQuotaAttemptBudget> projectedBudgets(
            List<TargetRetirementSourceHistoryRecord> histories,
            java.util.function.Function<byte[], TargetResultRecord> firsts) {
        final var ordered = new ArrayList<>(histories);
        ordered.sort((a, b) ->
                Long.compareUnsigned(a.decidedAt().sequence(), b.decidedAt().sequence()));
        final var latest = new HashMap<String, TargetQuotaAttemptBudget>();
        final var settled = new HashSet<String>();
        for (var history : ordered) {
            if (history.input().type() != SystemMutationType.TARGET_RESOURCE_RECLAIM) {
                continue;
            }
            final var first =
                    Objects.requireNonNull(firsts.apply(history.input().systemMutationId()), "actual reclaim first");
            if (!first.appliedFirst()) {
                continue;
            }
            final var body = TargetResourceReclaimBody.decode(history.input().canonicalBody());
            for (var operation : body.operations()) {
                final TargetQuotaAttemptBudget expected, next;
                if (operation instanceof TargetResourceReclaimBody.UnusedAttemptReserve unused) {
                    expected = unused.expected();
                    next = expected.unusedReserveProjection(body.floor(), history.decidedAt());
                } else if (operation instanceof TargetResourceReclaimBody.RetainedAllocationRelease allocation) {
                    expected = allocation.expected();
                    next = expected.retainedReleaseProjection(
                            allocation.remaining(), body.floor(), history.decidedAt());
                    for (var receipt : allocation.confirmations()) {
                        if (!settled.add(Bytes.hex(expected.publishAttemptId()) + ":"
                                + Bytes.hex(
                                        TargetResourceReclaimBody.RetainedAllocationRelease.shareIdentity(receipt)))) {
                            throw new IllegalStateException(
                                    "one owned-object share has multiple APPLIED allocation settlements");
                        }
                    }
                } else {
                    continue;
                }
                final String key = Bytes.hex(expected.publishAttemptId());
                final var prior = latest.put(key, next);
                if (prior != null && !Arrays.equals(prior.canonicalBytes(), expected.canonicalBytes())) {
                    throw new IllegalStateException("retained allocation history changed exact previous Budget");
                }
            }
        }
        return Map.copyOf(latest);
    }

    private static void requireWriterClosed(
            TargetCheckpointDependencies d,
            TargetQuotaAttemptBudget budget,
            TargetResourceReclaimBody body,
            Runnable current) {
        final var admission = d.admissions().stream()
                .filter(a -> Arrays.equals(a.attempt().publishAttemptId(), budget.publishAttemptId()))
                .findFirst()
                .orElseThrow();
        budget.resolvedAt().requireCoveredByFloor(body.floor());
        d.requireSystemPosition(admission.first());
        final var outcome = d.outcomeResults().stream()
                .filter(r -> r.mutation().equals(budget.resolvedAt()))
                .findFirst()
                .orElseThrow();
        if (!outcome.appliedFirst()) {
            throw new IllegalStateException("retained allocation lost definitive Source resolution");
        }
        d.requireSystemPosition(outcome);
        if (closeThrough(body)
                < Math.max(
                        SystemMutationResult.decode(admission.first().typedPayload())
                                .retryUntilEpochMs(),
                        SystemMutationResult.decode(outcome.typedPayload()).retryUntilEpochMs())) {
            throw new IllegalStateException("original writer retry windows have not closed");
        }
        current.run();
    }

    private static void requireUnsettled(
            TargetCheckpointDependencies d,
            TargetQuotaAttemptBudget budget,
            TargetEvidenceConfirmedRecord receipt,
            TargetQuotaMutation thisDecision) {
        for (var history : d.sourceHistories()) {
            if (history.input().type() != SystemMutationType.TARGET_RESOURCE_RECLAIM
                    || thisDecision != null && history.decidedAt().equals(thisDecision)) {
                continue;
            }
            final var prior = TargetResourceReclaimBody.decode(history.input().canonicalBody());
            if (!prior.nativeRetainedReclaim()) {
                continue;
            }
            final var first = d.resultRecord(systemKey(history.input().systemMutationId()));
            if (first == null || !first.appliedFirst()) {
                continue;
            }
            for (var operation : prior.operations()) {
                if (operation instanceof TargetResourceReclaimBody.RetainedAllocationRelease allocation
                        && Arrays.equals(allocation.expected().publishAttemptId(), budget.publishAttemptId())) {
                    for (var settled : allocation.confirmations()) {
                        if (Arrays.equals(
                                TargetResourceReclaimBody.RetainedAllocationRelease.shareIdentity(settled),
                                TargetResourceReclaimBody.RetainedAllocationRelease.shareIdentity(receipt))) {
                            throw new IllegalStateException(
                                    "owned-object share was already settled by an earlier APPLIED Native reclaim");
                        }
                    }
                }
            }
        }
    }
    /** Point checks are backed by the opaque complete Floor closure and this Source's independent actual admission loan. */
    public static void requireCurrent(TargetStoreBackend.Reader reader, SystemMutation input, Runnable current) {
        final var body = TargetResourceReclaimBody.decode(input.canonicalBody());
        final var facts = Objects.requireNonNull(body.nativeFacts());
        facts.requireFor(body);
        final var fence = fence(facts);
        final var first = result(reader, List.of(), systemKey(fence.mutation().systemMutationId()));
        if (!first.mutation().equals(facts.fence().appliedAt())
                || !Arrays.equals(
                        first.typedPayload(),
                        SystemMutationResult.from(
                                        fence.mutation(),
                                        ApplyStatus.APPLIED,
                                        StableCode.OK,
                                        fence.position().canonicalBytes())
                                .encode())) {
            throw new IllegalStateException("Native reclaim changed original APPLIED/OK TimeFence");
        }
        result(reader, List.of(), positionKey(fence.position())).requireFirst(first);
        if (reader.closedIngressDeadlineThrough() < closeThrough(body)) {
            throw new IllegalStateException("Native prefix lacks original closure fence");
        }
        if (!TargetResourceReclaimStore.matchesExpected(reader, body)) {
            throw new IllegalStateException("Native reclaim exact values changed after full closure");
        }
        if (body.incarnationReclaim()) {
            TargetIncarnationReclaim.requireCurrent(reader, input, current);
            return;
        }
        for (var operation : body.operations()) {
            current.run();
            if (operation instanceof TargetResourceReclaimBody.RetainedRecordDelete leaf) {
                TargetSloObservationSamples.requireUnreferenced(
                        reader, List.of(), leaf.expected().key());
                retainedWindow(body, leaf.expected());
                if (leaf.expected().kind() == TargetResultRecord.Kind.RESULT
                        || leaf.expected().kind() == TargetResultRecord.Kind.POSITION_COMMAND) {
                    requireOriginalFirst(
                            leaf.expected(),
                            result(reader, List.of(), firstKey(leaf.expected().logicalId())));
                }
                if (leaf.expected().kind().position()
                        && reader.source().compareTo(leaf.expected().mutation().source()) <= 0) {
                    throw new IllegalStateException("cannot delete current frontier POSITION");
                }
            } else if (operation instanceof TargetResourceReclaimBody.RetainedAllocationRelease allocation) {
                for (var receipt : allocation.confirmations()) {
                    requireConfirmation(reader, List.of(), receipt, allocation.expected());
                }
            }
        }
        current.run();
    }

    private static void requireConfirmation(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetEvidenceConfirmedRecord receipt,
            TargetQuotaAttemptBudget budget) {
        final var actual = TargetEvidenceConfirmedRecord.decodeForStore(
                receipt.key(),
                value(view, overlay, ColumnFamily.META, receipt.key(), TargetEvidenceConfirmedRecord.VALUE_TYPE),
                budget.locator().messageId().routingId().shardId(),
                budget.tenantScope(),
                budget.recoveryLineage());
        if (!Arrays.equals(actual.canonicalBytes(), receipt.canonicalBytes())) {
            throw new IllegalStateException("Native allocation confirmation changed");
        }
        final List<byte[]> absent;
        if (receipt.body().resource().provenance() instanceof TargetExternalEvidenceObjectResource.Recovery recovery) {
            final var owner = recovery.owner();
            final var ref = owner.referenceRecord();
            absent = List.of(owner.key(), ref.key(), ref.attemptKey());
        } else {
            final var anchor = (TargetExternalEvidenceObjectResource.Anchor)
                    receipt.body().resource().provenance();
            final byte[] mappingKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.KAFKA_RECEIPT_MAPPING_TAG, TargetKeyCodec.KEY_FORMAT},
                    anchor.mappingId());
            final var mapping = TargetKafkaReceiptMapping.decodeForStore(
                    mappingKey,
                    value(view, overlay, ColumnFamily.META, mappingKey, TargetKafkaReceiptMapping.VALUE_TYPE),
                    budget.primaryIdentity().shard());
            requireAnchorOrigin(anchor, mapping, budget);
            final var admission = result(view, overlay, systemKey(anchor.admissionId()));
            final var accepted = SystemMutationResult.decode(admission.typedPayload());
            if (!admission.appliedFirst()
                    || !admission.mutation().equals(mapping.mutation())
                    || accepted.mutationType() != SystemMutationType.TARGET_PUBLISH_ADMISSION
                    || accepted.stableCode() != StableCode.OK
                    || !Arrays.equals(accepted.mutationHash(), anchor.admissionHash())) {
                throw new IllegalStateException("anchor settlement lost its actual creator Admission first");
            }
            result(view, overlay, positionKey(mapping.mutation().source())).requireFirst(admission);
            final var ref = new TargetExternalEvidenceReference(
                    TargetKafkaReceiptControlArtifact.ANCHOR,
                    anchor.owner().artifactDigest(),
                    anchor.mappingId(),
                    budget.publishAttemptId(),
                    budget.primaryIdentity(),
                    budget.accounting(),
                    mapping.mutation(),
                    budget.recoveryLineage());
            absent = List.of(
                    anchor.owner().key(),
                    ref.key(),
                    TargetExternalEvidenceStore.referenceByAttemptKey(budget.publishAttemptId()));
        }
        for (var key : absent) {
            if (view.projected(ColumnFamily.META, key, overlay) != null) {
                throw new IllegalStateException(
                        "confirmed allocation share still has live Native ownership/references");
            }
        }
        final var first = result(view, overlay, systemKey(receipt.confirmation().systemMutationId()));
        if (!first.mutation().equals(receipt.appliedAt())
                || !Arrays.equals(
                        first.typedPayload(),
                        SystemMutationResult.from(
                                        receipt.confirmation(),
                                        ApplyStatus.APPLIED,
                                        StableCode.OK,
                                        receipt.appliedAt().source().canonicalBytes())
                                .encode())) {
            throw new IllegalStateException("allocation deletion lacks original Source first");
        }
        result(view, overlay, positionKey(receipt.appliedAt().source())).requireFirst(first);
    }

    private static void requireAnchorOrigin(
            TargetExternalEvidenceObjectResource.Anchor anchor,
            TargetKafkaReceiptMapping mapping,
            TargetQuotaAttemptBudget budget) {
        if (!Arrays.equals(mapping.admission().publishAttemptId(), budget.publishAttemptId())
                || !mapping.admission().locator().equals(budget.locator())
                || !mapping.admission().commitment().equals(budget.commitment())
                || mapping.admission().executionBytes() != budget.executionBytes()
                || !mapping.admission().allocated().covers(budget.allocated())
                || !mapping.mutation().equals(anchor.owner().createdAt())
                || !Arrays.equals(mapping.mutationHash(), anchor.admissionHash())
                || !Arrays.equals(mapping.mutationHash(), budget.admissionDigest())
                || !Arrays.equals(mapping.recoveryLineage(), budget.recoveryLineage())
                || !Arrays.equals(
                        mapping.admission().kafkaReceipt().scanObservationDigest(),
                        anchor.owner().artifactDigest())
                || !mapping.admission()
                        .channel()
                        .context()
                        .domain()
                        .equals(anchor.owner().originalDomain())
                || !mapping.admission().allocated().covers(anchor.owner().blobCharge())) {
            throw new IllegalStateException(
                    "anchor allocation lost its original unique creator Admission/mapping/charge");
        }
    }

    public static void requireAppliedDecision(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetResourceReclaimBody body,
            TargetQuotaMutation decision,
            TargetQuotaMutation frontier) {
        final var facts = Objects.requireNonNull(body.nativeFacts());
        facts.requireFor(body);
        final var original = fence(facts);
        final var first = result(view, overlay, systemKey(original.mutation().systemMutationId()));
        if (!first.mutation().equals(facts.fence().appliedAt())
                || !Arrays.equals(
                        first.typedPayload(),
                        SystemMutationResult.from(
                                        original.mutation(),
                                        ApplyStatus.APPLIED,
                                        StableCode.OK,
                                        original.position().canonicalBytes())
                                .encode())) {
            throw new IllegalStateException("Native history lost its original APPLIED closure fence");
        }
        result(view, overlay, positionKey(original.position())).requireFirst(first);
        decision.requireAtOrBefore(frontier);
        if (body.incarnationReclaim()) {
            TargetIncarnationReclaim.requireAppliedDecision(view, overlay, body, decision);
            return;
        }
        for (var operation : body.operations()) {
            if (operation instanceof TargetResourceReclaimBody.RetainedRecordDelete leaf) {
                TargetSloObservationSamples.requireUnreferenced(
                        view, overlay, leaf.expected().key());
                retainedWindow(body, leaf.expected());
                if (view.projected(ColumnFamily.DEDUPE, leaf.expected().key(), overlay) != null) {
                    throw new IllegalStateException("APPLIED Native record deletion retains its row");
                }
                if (leaf.expected().kind() == TargetResultRecord.Kind.RESULT
                        || leaf.expected().kind() == TargetResultRecord.Kind.POSITION_COMMAND) {
                    requireOriginalFirst(
                            leaf.expected(),
                            result(view, overlay, firstKey(leaf.expected().logicalId())));
                }
            } else if (operation instanceof TargetResourceReclaimBody.RetainedAllocationRelease allocation) {
                final var expected = allocation.expected();
                final var actual = TargetQuotaAttemptBudget.decodeForStore(
                        expected.key(),
                        value(view, overlay, ColumnFamily.META, expected.key(), TargetQuotaAttemptBudget.VALUE_TYPE),
                        body.shard());
                final var projected =
                        expected.retainedReleaseProjection(allocation.remaining(), body.floor(), decision);
                if (actual.mutation().equals(decision)) {
                    if (!Arrays.equals(actual.canonicalBytes(), projected.canonicalBytes())) {
                        throw new IllegalStateException("APPLIED allocation changed exact successor");
                    }
                } else {
                    actual.mutation().requireAfter(decision);
                    actual.mutation().requireAtOrBefore(frontier);
                    if (!actual.locator().equals(projected.locator())
                            || !actual.accounting().equals(projected.accounting())
                            || !actual.resolvedAt().equals(projected.resolvedAt())
                            || !projected.allocated().covers(actual.allocated())
                            || Long.compareUnsigned(actual.revision(), projected.revision()) <= 0) {
                        throw new IllegalStateException("later retained allocation lost original provenance");
                    }
                }
                for (var receipt : allocation.confirmations()) {
                    requireConfirmation(view, overlay, receipt, expected);
                }
            }
        }
    }
    /** Exact latest Source-backed allocation successor for incremental accounting and normal/cold point reads. */
    public static void requireAllocationSuccessor(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetResourceReclaimBody body,
            TargetQuotaAttemptBudget actual,
            TargetQuotaMutation decision) {
        final var operation = body.operations().stream()
                .filter(o -> o instanceof TargetResourceReclaimBody.RetainedAllocationRelease a
                        && Arrays.equals(a.expected().publishAttemptId(), actual.publishAttemptId()))
                .map(o -> (TargetResourceReclaimBody.RetainedAllocationRelease) o)
                .findFirst()
                .orElseThrow();
        if (!Arrays.equals(
                actual.canonicalBytes(),
                operation
                        .expected()
                        .retainedReleaseProjection(operation.remaining(), body.floor(), decision)
                        .canonicalBytes())) {
            throw new IllegalStateException(
                    "retained allocation successor changed original frozen fees/Source decision");
        }
        for (var receipt : operation.confirmations()) {
            requireConfirmation(view, overlay, receipt, operation.expected());
        }
    }
}
