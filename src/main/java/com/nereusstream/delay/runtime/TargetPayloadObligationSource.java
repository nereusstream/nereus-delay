package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.ownership.TargetPayloadSummaryLegacyPrefix;
import com.nereusstream.delay.store.*;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Exact Source-batch joins for the payload owner's qualified all-generation pending-attempt summary. */
public final class TargetPayloadObligationSource {
    private TargetPayloadObligationSource() {}

    private record Change(TargetQuotaAttemptBudget before, TargetQuotaAttemptBudget after) {}
    private record Ref(TargetMessageLocator locator, AttemptObligationRef value) {}
    private record Delta(TargetQuotaAttemptBudget admitted, TargetQuotaAttemptBudget pending,
            TargetQuotaAttemptBudget resolved) {}

    static TargetQuotaPayloadOwner owner(TargetStoreBackend.Reader reader, TargetQuotaScope scope,
            byte[] lineage, DelayMessageId id) {
        final byte[] key = ownerKey(id);
        final var owner = TargetQuotaPayloadOwner.decodeForStore(key, TargetValueEnvelope.decode(
                Objects.requireNonNull(reader.get(ColumnFamily.META, key), "original payload owner"),
                TargetQuotaPayloadOwner.VALUE_TYPE).payload(), scope.shard(), scope.tenantScope());
        if (!Arrays.equals(owner.recoveryLineage(), lineage)) {
            throw new IllegalStateException("payload obligation summary changes its original lineage");
        }
        owner.mutation().requireAtOrBefore(reader.aggregate().mutation());
        if (owner.summaryQualified()) owner.summaryMutation().requireAtOrBefore(reader.aggregate().mutation());
        return owner;
    }

    /** Count zero removes only the cross-generation attempt test; the current business/legacy gates remain. */
    static boolean canRetain(TargetStoreBackend.Reader reader, TargetMessageRecord current,
            TargetQuotaPayloadOwner owner, long nextCount) {
        owner.requireQualifiedSummary(); owner.requireMessagePayload(current);
        if (nextCount != 0 || owner.phase() != TargetQuotaPayloadOwner.Phase.ACTIVE
                || !current.runtime().terminal() || current.runtime().currentWorkKind() != CurrentSendWorkKind.NONE
                || !current.runtime().attemptObligations().isEmpty()) return false;
        final byte[] raw = reader.get(ColumnFamily.META, TargetMigrationCustodyGuard.keyFor(owner.messageId()));
        if (raw != null) {
            final var guard = TargetMigrationCustodyGuard.decode(TargetValueEnvelope.decode(raw,
                    TargetMigrationCustodyGuard.VALUE_TYPE).payload());
            if (!guard.messageId().equals(owner.messageId())) throw new IllegalStateException("legacy payload guard changed Message");
            if (!guard.settled()) return false;
        }
        return true;
    }

    /** Called inside the original immutable Source ReadView, before common accounting seals the complete batch. */
    public static void requireSource(TargetStoreBackend.Reader reader, List<TargetStoreBackend.Edit> edits,
            TargetQuotaScope scope, byte[] lineage, TargetQuotaMutation stamp,
            TargetPayloadObligationSummaryBootstrap.Qualification initialization,
            TargetPayloadSummaryLegacyPrefix.EventPermit legacyPrefix) {
        if (legacyPrefix != null) legacyPrefix.requireSource(reader, stamp);
        final Map<String, Change> budgets = new LinkedHashMap<>();
        final Map<String, TargetMessageLocator> generations = new LinkedHashMap<>();
        final Map<String, DelayMessageId> messages = new LinkedHashMap<>();
        final Map<String, TargetStoreBackend.Edit> ownerEdits = new LinkedHashMap<>();
        for (var edit : edits) {
            final byte[] key = edit.key();
            if (key.length < 2) continue;
            final int tag = Byte.toUnsignedInt(key[0]);
            if (edit.family() == ColumnFamily.META && tag == TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG) {
                requireActualBefore(reader, edit);
                final var before = budget(edit.before(), key, scope);
                final var after = budget(edit.after(), key, scope);
                final var selected = after == null ? before : after;
                Objects.requireNonNull(selected, "actual attempt edit");
                if (before != null && after != null) requireSameAttempt(before, after);
                if (!Arrays.equals(selected.tenantScope(), scope.tenantScope())
                        || !Arrays.equals(selected.recoveryLineage(), lineage)) throw new IllegalStateException("attempt changed original tenant/lineage");
                if (budgets.putIfAbsent(Bytes.hex(selected.publishAttemptId()), new Change(before, after)) != null)
                    throw new IllegalStateException("Source repeats one attempt Budget edit");
                addGeneration(generations, messages, selected.locator());
            } else if (edit.family() == ColumnFamily.ID && tag == TargetKeyCodec.MESSAGE_TAG) {
                requireActualBefore(reader, edit);
                for (byte[] raw : new byte[][] {edit.before(), edit.after()}) if (raw != null) {
                    addGeneration(generations, messages, message(raw, key, scope).locator());
                }
            } else if (edit.family() == ColumnFamily.TERMINAL && tag == TargetKeyCodec.TERMINAL_GENERATION_TAG) {
                requireActualBefore(reader, edit);
                for (byte[] raw : new byte[][] {edit.before(), edit.after()}) if (raw != null) {
                    final var terminal = terminal(raw, key, lineage);
                    addGeneration(generations, messages, terminal.locator());
                }
            } else if (edit.family() == ColumnFamily.META && tag == TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG) {
                requireActualBefore(reader, edit);
                if (ownerEdits.putIfAbsent(Bytes.hex(key), edit) != null)
                    throw new IllegalStateException("Source must emit one final payload-owner successor");
                final byte[] raw = edit.after() == null ? edit.before() : edit.after();
                final var owner = decodeOwner(raw, key, scope, lineage);
                messages.put(Bytes.hex(owner.messageId().bytes()), owner.messageId());
            }
        }
        final Map<String, Ref> beforeRefs = new LinkedHashMap<>(), afterRefs = new LinkedHashMap<>();
        for (var locator : generations.values()) {
            final byte[] key = TargetKeyCodec.message(locator.messageId());
            final var beforeMessage = nullableMessage(reader.get(ColumnFamily.ID, key), key, scope);
            final var afterMessage = nullableMessage(reader.projected(ColumnFamily.ID, key, edits), key, scope);
            final byte[] terminalKey = TargetTerminalGenerationRecord.key(locator);
            final var beforeTerminal = nullableTerminal(reader.get(ColumnFamily.TERMINAL, terminalKey), terminalKey, lineage);
            final var afterTerminal = nullableTerminal(reader.projected(ColumnFamily.TERMINAL, terminalKey, edits), terminalKey, lineage);
            collect(beforeRefs, locator, runtime(locator, beforeMessage, beforeTerminal));
            collect(afterRefs, locator, runtime(locator, afterMessage, afterTerminal));
        }
        final Map<String, Delta> deltas = new LinkedHashMap<>();
        final var ids = new LinkedHashSet<>(beforeRefs.keySet()); ids.addAll(afterRefs.keySet());
        final var witnessed = new LinkedHashSet<String>();
        for (String id : ids) {
            final var prior = beforeRefs.get(id); final var next = afterRefs.get(id);
            if (prior != null && next != null && prior.locator().equals(next.locator())
                    && Arrays.equals(prior.value().canonicalBytes(), next.value().canonicalBytes())) continue;
            final var change = Objects.requireNonNull(budgets.get(id), "changed runtime obligation has no same-batch Budget transition");
            witnessed.add(id);
            if (prior == null) {
                final var admitted = Objects.requireNonNull(change.after());
                if (change.before() != null || admitted.phase() != TargetQuotaAttemptBudget.Phase.ADMITTED
                        || next.value().ledgerState() != AttemptLedgerState.PUBLISHING
                        || !admitted.mutation().equals(stamp)) throw new IllegalStateException("only a true first Admission may add an obligation");
                requireRef(admitted, next);
                final var applied = requireApplied(edits, stamp, SystemMutationType.TARGET_PUBLISH_ADMISSION, admitted);
                requireAdmission(reader, edits, scope, stamp, admitted, next.value(), applied);
                addDelta(deltas, admitted.locator().messageId(), new Delta(admitted, null, null));
            } else if (next == null) {
                final var pending = Objects.requireNonNull(change.before()); final var resolved = Objects.requireNonNull(change.after());
                requireRef(pending, prior);
                if (!pending(pending) || resolved.phase() != TargetQuotaAttemptBudget.Phase.RESOLVED_AWAITING_FLOOR
                        || !stamp.equals(resolved.resolvedAt()) || !stamp.equals(resolved.mutation())
                        || !Arrays.equals(pending.resolve(EvidenceVerificationStatus.VERIFIED_PUBLISHED,
                                pending.allocated(), stamp).canonicalBytes(), resolved.canonicalBytes()))
                    throw new IllegalStateException("only first verified Source settlement may remove an obligation");
                requireApplied(edits, stamp, pending.phase() == TargetQuotaAttemptBudget.Phase.UNKNOWN
                        ? SystemMutationType.EVIDENCE_RESOLUTION : SystemMutationType.PUBLISH_OUTCOME, pending);
                addDelta(deltas, pending.locator().messageId(), new Delta(null, pending, resolved));
            } else {
                if (!prior.locator().equals(next.locator()) || change.before() == null || change.after() == null
                        || change.before().phase() != TargetQuotaAttemptBudget.Phase.ADMITTED
                        || change.after().phase() != TargetQuotaAttemptBudget.Phase.UNKNOWN
                        || !change.after().mutation().equals(stamp)
                        || prior.value().ledgerState() != AttemptLedgerState.PUBLISHING
                        || !Arrays.equals(prior.value().uncertain().canonicalBytes(), next.value().canonicalBytes())
                        || !Arrays.equals(change.before().unknown(change.before().allocated(), stamp).canonicalBytes(),
                                change.after().canonicalBytes()))
                    throw new IllegalStateException("Source changed an unresolved identity outside its exact UNKNOWN transition");
                requireRef(change.before(), prior); requireRef(change.after(), next);
                requireApplied(edits, stamp, SystemMutationType.PUBLISH_OUTCOME, change.before());
            }
        }
        for (var entry : budgets.entrySet()) {
            final var change = entry.getValue();
            final boolean born = change.before() == null && pending(change.after());
            final boolean settled = pending(change.before()) && !pending(change.after());
            final boolean unknown = change.before() != null && change.after() != null
                    && change.before().phase() == TargetQuotaAttemptBudget.Phase.ADMITTED
                    && change.after().phase() == TargetQuotaAttemptBudget.Phase.UNKNOWN;
            if ((born || settled || unknown) && !witnessed.contains(entry.getKey()))
                throw new IllegalStateException("Budget pending transition lacks its actual runtime/terminal change");
        }
        for (var id : messages.values()) {
            final byte[] key = ownerKey(id);
            final var before = nullableOwner(reader.get(ColumnFamily.META, key), key, scope, lineage);
            final var after = nullableOwner(reader.projected(ColumnFamily.META, key, edits), key, scope, lineage);
            if (before != null && before.summaryQualified()
                    && before.unresolvedCount() < visibleCount(beforeRefs, id))
                throw new IllegalStateException("qualified payload summary is below its actual visible pending identities");
            if (after != null && after.summaryQualified()
                    && after.unresolvedCount() < visibleCount(afterRefs, id))
                throw new IllegalStateException("qualified payload summary is below its projected pending identities");
            final var delta = deltas.get(Bytes.hex(id.bytes()));
            if (legacyPrefix != null && (before == null || !before.summaryQualified())) {
                legacyPrefix.requireLegacyOwner(reader, before, after, edits);
                continue;
            }
            if (delta != null) {
                if (before == null || after == null || !ownerEdits.containsKey(Bytes.hex(key)))
                    throw new IllegalStateException("pending attempt transition lacks one exact payload-owner successor");
                before.requireQualifiedSummary(); after.requireQualifiedSummary();
                final TargetQuotaPayloadOwner expected;
                if (delta.admitted() != null) expected = before.admitAttempt(delta.admitted(), stamp, (prior, next, floor) -> {});
                else {
                    final boolean retain = after.phase() == TargetQuotaPayloadOwner.Phase.RETAINED;
                    if (retain) {
                        final byte[] messageKey = TargetKeyCodec.message(id);
                        final var current = Objects.requireNonNull(nullableMessage(reader.projected(ColumnFamily.ID, messageKey, edits), messageKey, scope));
                        final byte[] terminalKey = TargetTerminalGenerationRecord.key(current.locator());
                        final var currentTerminal = Objects.requireNonNull(nullableTerminal(
                                reader.projected(ColumnFamily.TERMINAL, terminalKey, edits), terminalKey, lineage),
                                "actual current terminal generation");
                        currentTerminal.requireOwner(before);
                        runtime(current.locator(), current, currentTerminal);
                        if (!canRetain(reader, current, before, Math.subtractExact(before.unresolvedCount(), 1L)))
                            throw new IllegalStateException("zero attempts do not end current payload work/legacy responsibility");
                    }
                    expected = before.resolveAttempt(delta.pending(), delta.resolved(), retain, stamp, (prior, next, floor) -> {});
                }
                if (!Arrays.equals(expected.canonicalBytes(), after.canonicalBytes()))
                    throw new IllegalStateException("payload summary differs from its independently joined Source delta");
            } else if (after != null && after.summaryQualified() && (before == null || !before.summaryQualified())) {
                TargetPayloadObligationSummaryBootstrap.requireInitialization(reader, scope, lineage, before, after, stamp, edits, initialization);
            } else if (before != null && before.summaryQualified()) {
                if (after != null) {
                    after.requireQualifiedSummary(); before.requireSameSummaryOrigin(after);
                    if (before.unresolvedCount() != after.unresolvedCount()
                            || before.summaryRevision() != after.summaryRevision()
                            || !before.summaryMutation().equals(after.summaryMutation()))
                        throw new IllegalStateException("Source changed obligation summary without a pending identity transition");
                } else if (before.unresolvedCount() != 0) {
                    throw new IllegalStateException("Source deleted payload owner with unresolved obligations");
                }
            }
        }
    }

    private static SystemMutationResult requireApplied(List<TargetStoreBackend.Edit> edits, TargetQuotaMutation stamp,
            SystemMutationType expectedType, TargetQuotaAttemptBudget attempt) {
        int found = 0; SystemMutationResult matched = null;
        for (var edit : edits) if (edit.family() == ColumnFamily.DEDUPE && edit.before() == null && edit.after() != null
                && edit.key().length == 34 && edit.key()[0] == TargetKeyCodec.RESULT_SYSTEM_TAG) {
            final var first = TargetResultRecord.decode(TargetValueEnvelope.decode(edit.after(), TargetResultRecord.VALUE_TYPE).payload());
            if (!first.mutation().equals(stamp) || first.kind() != TargetResultRecord.Kind.SYSTEM) continue;
            final var result = SystemMutationResult.decode(first.typedPayload());
            final var author = AuthorIdentity.decode(result.authorIdentity());
            if (!Arrays.equals(first.key(), edit.key()) || !Arrays.equals(first.logicalId(), result.mutationId())
                    || author.kind() != (expectedType == SystemMutationType.EVIDENCE_RESOLUTION
                            ? AuthorIdentity.Kind.SERVICE : AuthorIdentity.Kind.OWNER))
                throw new IllegalStateException("obligation Source result changed its original key/author identity");
            if (result.applyStatus() != ApplyStatus.APPLIED || result.mutationType() != expectedType
                    || !Arrays.equals(result.appliedSourcePosition(), stamp.source().canonicalBytes())) continue;
            if (expectedType == SystemMutationType.TARGET_PUBLISH_ADMISSION
                    && (!Arrays.equals(result.mutationHash(), attempt.admissionDigest()) || result.stableCode() != StableCode.OK
                        || !Arrays.equals(result.mutationId(), SystemMutation.computeSystemMutationId(stamp.source().shardId(),
                                expectedType, attempt.publishAttemptId(), attempt.admissionDigest())))) continue;
            if (expectedType == SystemMutationType.PUBLISH_OUTCOME
                    && !Arrays.equals(result.mutationId(), SystemMutation.computeSystemMutationId(stamp.source().shardId(),
                            expectedType, attempt.publishAttemptId(), result.mutationHash()))) continue;
            found++; matched = result;
        }
        if (found != 1) throw new IllegalStateException("pending obligation transition lacks its exact first APPLIED Source result");
        return matched;
    }

    private static void requireAdmission(TargetStoreBackend.Reader reader, List<TargetStoreBackend.Edit> edits,
            TargetQuotaScope scope, TargetQuotaMutation stamp, TargetQuotaAttemptBudget admitted,
            AttemptObligationRef obligation, SystemMutationResult result) {
        final byte[] messageKey = TargetKeyCodec.message(admitted.locator().messageId());
        final var prior = Objects.requireNonNull(nullableMessage(reader.get(ColumnFamily.ID, messageKey), messageKey, scope));
        if (!prior.locator().equals(admitted.locator()) || prior.runtime().currentWorkKind() != CurrentSendWorkKind.CLAIMED)
            throw new IllegalStateException("new Admission lacks its actual reversible Claim generation");
        final byte[] claimKey = TargetClaimRecord.key(prior.runtime().claimId());
        final var claim = TargetClaimRecord.decode(TargetValueEnvelope.decode(
                Objects.requireNonNull(reader.get(ColumnFamily.INFLIGHT, claimKey)), TargetClaimRecord.VALUE_TYPE).payload());
        claim.requireStored(claimKey); claim.requireCurrent(prior);
        final var charge = TargetQuotaClaimCharge.decode(TargetValueEnvelope.decode(
                Objects.requireNonNull(reader.get(ColumnFamily.META, claim.chargeKey())), TargetQuotaClaimCharge.VALUE_TYPE).payload());
        claim.requireCharge(charge);
        final var author = AuthorIdentity.decode(result.authorIdentity());
        if (author.kind() != AuthorIdentity.Kind.OWNER || !claim.owner().equals(author.asOwnerIdentity())
                || obligation.ownerEpoch() != claim.owner().ownerEpoch()
                || admitted.executionBytes() != claim.executionBytes()
                || reader.projected(ColumnFamily.INFLIGHT, claimKey, edits) != null
                || reader.projected(ColumnFamily.META, claim.chargeKey(), edits) != null)
            throw new IllegalStateException("first Admission did not consume its exact original Claim/charge/Owner");
        stamp.requireStoreSuccessorOf(claim.creation());
        final var expected = claim.admitted(prior, obligation, Math.addExact(prior.runtime().admissionsUsed(), 1));
        final var actual = Objects.requireNonNull(nullableMessage(reader.projected(ColumnFamily.ID, messageKey, edits), messageKey, scope));
        if (!Arrays.equals(expected.canonicalBytes(), actual.canonicalBytes()))
            throw new IllegalStateException("first Admission runtime differs from its actual Claim-derived successor");
    }
    private static void requireRef(TargetQuotaAttemptBudget budget, Ref ref) {
        if (!budget.locator().equals(ref.locator()) || !Arrays.equals(budget.publishAttemptId(), ref.value().publishAttemptId())
                || ref.value().ledgerState() != (budget.phase() == TargetQuotaAttemptBudget.Phase.ADMITTED
                        ? AttemptLedgerState.PUBLISHING : AttemptLedgerState.UNCERTAIN))
            throw new IllegalStateException("runtime pending identity differs from its original Budget/locator/phase");
    }
    private static void requireSameAttempt(TargetQuotaAttemptBudget before, TargetQuotaAttemptBudget after) {
        if (!before.locator().equals(after.locator()) || !Arrays.equals(before.publishAttemptId(), after.publishAttemptId())
                || !Arrays.equals(before.admissionDigest(), after.admissionDigest()) || !before.primaryIdentity().equals(after.primaryIdentity())
                || !Arrays.equals(before.tenantScope(), after.tenantScope())
                || !before.accounting().equals(after.accounting()) || before.executionBytes() != after.executionBytes()
                || !before.commitment().equals(after.commitment()) || !Arrays.equals(before.recoveryLineage(), after.recoveryLineage()))
            throw new IllegalStateException("Source attempt transition changes its original identity/accounting");
    }
    private static long visibleCount(Map<String, Ref> refs, DelayMessageId id) {
        long count = 0;
        for (var ref : refs.values()) if (ref.locator().messageId().equals(id)) count = Math.incrementExact(count);
        return count;
    }
    private static void addDelta(Map<String, Delta> deltas, DelayMessageId id, Delta delta) {
        if (deltas.putIfAbsent(Bytes.hex(id.bytes()), delta) != null)
            throw new IllegalStateException("one Source operation cannot independently replace the same payload summary twice");
    }
    private static void addGeneration(Map<String, TargetMessageLocator> generations, Map<String, DelayMessageId> messages, TargetMessageLocator locator) {
        generations.putIfAbsent(Bytes.hex(locator.canonicalBytes()), locator);
        messages.putIfAbsent(Bytes.hex(locator.messageId().bytes()), locator.messageId());
    }
    private static void collect(Map<String, Ref> refs, TargetMessageLocator locator, TargetGenerationRuntimeIndex runtime) {
        if (runtime == null) return;
        for (var ref : runtime.attemptObligations()) if (refs.putIfAbsent(Bytes.hex(ref.publishAttemptId()), new Ref(locator, ref)) != null)
            throw new IllegalStateException("one unresolved identity occurs in multiple generations");
    }
    private static TargetGenerationRuntimeIndex runtime(TargetMessageLocator locator, TargetMessageRecord message, TargetTerminalGenerationRecord terminal) {
        final var current = message != null && message.locator().equals(locator) ? message.runtime() : null;
        if (terminal != null) {
            if (!terminal.locator().equals(locator) || current != null
                    && (terminal.stateVersion() != message.stateVersion()
                        || !Arrays.equals(current.canonicalBytes(), terminal.runtime().canonicalBytes())))
                throw new IllegalStateException("Message/Terminal mirrors disagree about pending identities");
            return terminal.runtime();
        }
        if (current != null && current.terminal()) throw new IllegalStateException("terminal Message lost its actual generation mirror");
        return current;
    }
    private static void requireActualBefore(TargetStoreBackend.Reader reader, TargetStoreBackend.Edit edit) {
        if (!Arrays.equals(edit.before(), reader.get(edit.family(), edit.key())))
            throw new IllegalStateException("payload obligation delta was not based on the actual Source ReadView");
    }
    private static boolean pending(TargetQuotaAttemptBudget value) {
        return value != null && (value.phase() == TargetQuotaAttemptBudget.Phase.ADMITTED || value.phase() == TargetQuotaAttemptBudget.Phase.UNKNOWN);
    }
    private static byte[] ownerKey(DelayMessageId id) { return Bytes.concat(new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, TargetKeyCodec.KEY_FORMAT}, id.bytes()); }
    private static TargetQuotaAttemptBudget budget(byte[] raw, byte[] key, TargetQuotaScope scope) { return raw == null ? null : TargetQuotaAttemptBudget.decodeForStore(key, TargetValueEnvelope.decode(raw, TargetQuotaAttemptBudget.VALUE_TYPE).payload(), scope.shard()); }
    private static TargetMessageRecord nullableMessage(byte[] raw, byte[] key, TargetQuotaScope scope) { return raw == null ? null : message(raw, key, scope); }
    private static TargetMessageRecord message(byte[] raw, byte[] key, TargetQuotaScope scope) { return TargetMessageRecord.decodeForStore(key, TargetValueEnvelope.decode(raw, TargetMessageRecord.VALUE_TYPE).payload(), scope.shard()); }
    private static TargetTerminalGenerationRecord nullableTerminal(byte[] raw, byte[] key, byte[] lineage) { return raw == null ? null : terminal(raw, key, lineage); }
    private static TargetTerminalGenerationRecord terminal(byte[] raw, byte[] key, byte[] lineage) {
        final var value = TargetTerminalGenerationRecord.decode(TargetValueEnvelope.decode(raw, TargetTerminalGenerationRecord.VALUE_TYPE).payload());
        if (!Arrays.equals(value.key(), key) || !Arrays.equals(value.recoveryLineage(), lineage)) throw new IllegalStateException("terminal obligation identity/lineage changed");
        return value;
    }
    private static TargetQuotaPayloadOwner nullableOwner(byte[] raw, byte[] key, TargetQuotaScope scope, byte[] lineage) { return raw == null ? null : decodeOwner(raw, key, scope, lineage); }
    private static TargetQuotaPayloadOwner decodeOwner(byte[] raw, byte[] key, TargetQuotaScope scope, byte[] lineage) {
        final var value = TargetQuotaPayloadOwner.decodeForStore(key, TargetValueEnvelope.decode(Objects.requireNonNull(raw), TargetQuotaPayloadOwner.VALUE_TYPE).payload(), scope.shard(), scope.tenantScope());
        if (!Arrays.equals(value.recoveryLineage(), lineage)) throw new IllegalStateException("payload obligation owner changed its original lineage");
        return value;
    }
}
