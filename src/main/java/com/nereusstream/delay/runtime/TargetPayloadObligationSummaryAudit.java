package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** Independent complete-cut reconstruction; it grants no Source initialization authority. */
public final class TargetPayloadObligationSummaryAudit {
    private TargetPayloadObligationSummaryAudit() {}

    private record Reference(TargetMessageLocator locator, AttemptObligationRef obligation) {}

    public static Map<String, Long> recompute(TargetCheckpointDependencies dependencies) {
        final var cursor = new RetirementCursor(dependencies, false);
        while (!cursor.advance(1024, () -> {})) {}
        return cursor.result();
    }

    public static void requireComplete(TargetCheckpointDependencies dependencies) {
        final var cursor = new RetirementCursor(dependencies);
        while (!cursor.advance(1024, () -> {})) {}
    }

    /** Complete same-view census, resumable without a terminal full scan or caller completeness flag. */
    public static final class RetirementCursor {
        private final TargetCheckpointDependencies dependencies;
        private final boolean verifySummaries;
        private final Map<String, TargetQuotaPayloadOwner> owners = new TreeMap<>();
        private final Map<TargetMessageLocator, byte[]> runtimes =
                new TreeMap<>(Comparator.comparing(value -> Bytes.hex(value.canonicalBytes())));
        private final Map<String, Reference> references = new TreeMap<>();
        private final Map<String, TargetResultRecord> byStamp = new TreeMap<>(), allFirsts = new TreeMap<>();
        private final Set<String> admissionFirsts = new TreeSet<>(), budgets = new TreeSet<>();
        private final Map<String, Long> counts = new TreeMap<>(),
                revisions = new TreeMap<>(),
                initialCounts = new TreeMap<>();
        private final Map<String, TargetQuotaMutation> latest = new TreeMap<>();
        private Iterator<TargetQuotaPayloadOwner> ownerRows;
        private Iterator<TargetCheckpointDependencies.MessageState> messages;
        private Iterator<TargetTerminalGenerationRecord> terminals;
        private Iterator<TargetResultRecord> results;
        private Iterator<TargetCheckpointDependencies.Admission> admissions;
        private Iterator<AttemptObligationRef> obligations;
        private TargetMessageLocator locator;
        private int phase;
        private boolean complete, failed;

        public RetirementCursor(TargetCheckpointDependencies dependencies) {
            this(dependencies, true);
        }

        private RetirementCursor(TargetCheckpointDependencies dependencies, boolean verifySummaries) {
            this.dependencies = Objects.requireNonNull(dependencies);
            this.verifySummaries = verifySummaries;
            ownerRows = dependencies.payloadOwnerRows().iterator();
        }

        public boolean advance(int maximumRows, Runnable current) {
            return advance(maximumRows, current, ignored -> {}, ignored -> {});
        }

        public boolean advance(
                int maximumRows,
                Runnable current,
                java.util.function.LongConsumer retained,
                java.util.function.LongConsumer copied) {
            if (failed || maximumRows < 1) {
                throw new IllegalStateException("invalid or failed payload summary cursor");
            }
            if (complete) {
                current.run();
                return true;
            }
            try {
                int used = 0;
                while (used < maximumRows && !complete) {
                    current.run();
                    if (obligations != null) {
                        if (!obligations.hasNext()) {
                            obligations = null;
                            continue;
                        }
                        final var obligation = obligations.next();
                        final String attempt = Bytes.hex(obligation.publishAttemptId());
                        if (references.putIfAbsent(attempt, new Reference(locator, obligation)) != null) {
                            throw new IllegalStateException("one unresolved attempt appears in different generations");
                        }
                        retained.accept(attempt.length());
                        used++;
                        continue;
                    }
                    switch (phase) {
                        case 0 -> {
                            if (!ownerRows.hasNext()) {
                                phase++;
                                messages = dependencies.messageStateRows().iterator();
                                continue;
                            }
                            final var owner = ownerRows.next();
                            final String key = id(owner.messageId());
                            if (owners.putIfAbsent(key, owner) != null) {
                                throw new IllegalStateException("payload summary repeats an owner identity");
                            }
                            counts.put(key, 0L);
                            retained.accept(key.length());
                        }
                        case 1 -> {
                            if (!messages.hasNext()) {
                                phase++;
                                terminals = dependencies.terminalGenerations().iterator();
                                continue;
                            }
                            final var message = messages.next();
                            beginRuntime(message.locator(), message.runtime(), retained, copied);
                        }
                        case 2 -> {
                            if (!terminals.hasNext()) {
                                phase++;
                                results = dependencies.resultRecordRows().iterator();
                                continue;
                            }
                            final var terminal = terminals.next();
                            beginRuntime(terminal.locator(), terminal.runtime(), retained, copied);
                        }
                        case 3 -> {
                            if (!results.hasNext()) {
                                phase++;
                                admissions = dependencies.admissions().iterator();
                                continue;
                            }
                            final var first = results.next();
                            if (first.kind() == TargetResultRecord.Kind.COMMAND
                                    || first.kind() == TargetResultRecord.Kind.SYSTEM) {
                                final byte[] stamp = first.mutation().canonicalBytes();
                                copied.accept(stamp.length);
                                final String token = Bytes.hex(stamp);
                                if (allFirsts.putIfAbsent(token, first) != null) {
                                    throw new IllegalStateException(
                                            "payload summary repeats an actual Source first stamp");
                                }
                                retained.accept(2L * stamp.length);
                                if (first.kind() == TargetResultRecord.Kind.SYSTEM) {
                                    byStamp.put(token, first);
                                    final var result = SystemMutationResult.decode(first.typedPayload());
                                    if (first.appliedFirst()
                                            && result.mutationType() == SystemMutationType.TARGET_PUBLISH_ADMISSION) {
                                        final byte[] key = first.key();
                                        copied.accept(key.length);
                                        final String identity = Bytes.hex(key);
                                        admissionFirsts.add(identity);
                                        retained.accept(identity.length());
                                    }
                                }
                            }
                        }
                        case 4 -> {
                            if (!admissions.hasNext()) {
                                if (!references.isEmpty() || !admissionFirsts.isEmpty()) {
                                    throw new IllegalStateException(
                                            "payload summary lacks a Budget for a runtime or APPLIED Admission");
                                }
                                if (!verifySummaries) {
                                    complete = true;
                                    continue;
                                }
                                phase++;
                                ownerRows = dependencies.payloadOwnerRows().iterator();
                                continue;
                            }
                            admission(admissions.next(), retained);
                        }
                        case 5 -> {
                            if (!ownerRows.hasNext()) {
                                phase++;
                                admissions = dependencies.admissions().iterator();
                                continue;
                            }
                            final var owner = ownerRows.next();
                            if (owner.summaryQualified()) {
                                final String key = id(owner.messageId());
                                owner.initializationSource()
                                        .requireAtOrBefore(
                                                dependencies.root().aggregate().mutation());
                                owner.summaryMutation()
                                        .requireAtOrBefore(
                                                dependencies.root().aggregate().mutation());
                                revisions.put(key, 1L);
                                latest.put(key, owner.initializationSource());
                                initialCounts.put(key, 0L);
                                retained.accept(key.length());
                            }
                        }
                        case 6 -> {
                            if (!admissions.hasNext()) {
                                phase++;
                                ownerRows = dependencies.payloadOwnerRows().iterator();
                                continue;
                            }
                            event(admissions.next());
                        }
                        case 7 -> {
                            if (!ownerRows.hasNext()) {
                                complete = true;
                                continue;
                            }
                            final var owner = ownerRows.next();
                            if (owner.summaryQualified()) {
                                final String key = id(owner.messageId());
                                if (owner.unresolvedCount() != counts.get(key)
                                        || owner.summaryRevision() != revisions.get(key)
                                        || !owner.summaryMutation().equals(latest.get(key))
                                        || owner.summaryOrigin() != TargetQuotaPayloadOwner.SummaryOrigin.BOOTSTRAP
                                                && initialCounts.get(key) != 0) {
                                    throw new IllegalStateException(
                                            "Native32 summary differs from the complete independent runtime/Budget/Source fold");
                                }
                                TargetPayloadObligationSummaryBootstrap.requireColdInitialization(
                                        dependencies,
                                        owner,
                                        initialCounts.get(key),
                                        allFirsts.get(Bytes.hex(
                                                owner.initializationSource().canonicalBytes())));
                            }
                        }
                        default -> throw new IllegalStateException("unknown payload summary phase");
                    }
                    used++;
                }
                current.run();
                return complete;
            } catch (RuntimeException | Error failure) {
                failed = true;
                throw failure;
            }
        }

        public Map<String, Long> result() {
            if (!complete || failed) {
                throw new IllegalStateException("payload summary census is incomplete");
            }
            return Collections.unmodifiableMap(counts);
        }

        private void beginRuntime(
                TargetMessageLocator value,
                TargetGenerationRuntimeIndex runtime,
                java.util.function.LongConsumer retained,
                java.util.function.LongConsumer copied) {
            final var owner = Objects.requireNonNull(
                    owners.get(id(value.messageId())), "generation runtime has no permanent payload owner");
            runtime.requireMessageProjection(value);
            if (!owner.primaryIdentity().target().equals(value.target())) {
                throw new IllegalStateException("historical generation changed its payload Target");
            }
            final byte[] digest = runtime.runtimeDigest();
            copied.accept(digest.length);
            final var prior = runtimes.putIfAbsent(value, digest);
            if (prior != null) {
                if (!Arrays.equals(prior, digest)) {
                    throw new IllegalStateException("current/terminal runtime mirrors disagree");
                }
                return;
            }
            retained.accept(digest.length);
            locator = value;
            obligations = runtime.attemptObligations().iterator();
        }

        private void admission(
                TargetCheckpointDependencies.Admission admission, java.util.function.LongConsumer retained) {
            final var budget = admission.attempt();
            final String attempt = Bytes.hex(budget.publishAttemptId());
            if (!budgets.add(attempt)) {
                throw new IllegalStateException("payload summary repeats an attempt Budget");
            }
            retained.accept(attempt.length());
            final var owner = Objects.requireNonNull(
                    owners.get(id(budget.locator().messageId())), "attempt Budget has no permanent payload owner");
            if (!owner.primaryIdentity().equals(budget.primaryIdentity())
                    || !owner.accounting().equals(budget.accounting())
                    || !Arrays.equals(owner.tenantScope(), budget.tenantScope())
                    || !Arrays.equals(owner.recoveryLineage(), budget.recoveryLineage())) {
                throw new IllegalStateException("attempt Budget changed its exact payload owner");
            }
            final var first = admission.first();
            final var result = SystemMutationResult.decode(first.typedPayload());
            final byte[] expected = SystemMutation.computeSystemMutationId(
                    dependencies.root().metadata().shardId(),
                    SystemMutationType.TARGET_PUBLISH_ADMISSION,
                    budget.publishAttemptId(),
                    budget.admissionDigest());
            if (!first.appliedFirst()
                    || result.stableCode() != StableCode.OK
                    || result.mutationType() != SystemMutationType.TARGET_PUBLISH_ADMISSION
                    || !Arrays.equals(result.mutationId(), expected)
                    || !Arrays.equals(result.mutationHash(), budget.admissionDigest())
                    || !admissionFirsts.remove(Bytes.hex(first.key()))) {
                throw new IllegalStateException("payload summary lacks the exact original Admission first");
            }
            dependencies.requireSystemPosition(first);
            first.mutation().requireAtOrBefore(budget.mutation());
            final var reference = references.remove(attempt);
            if (budget.phase() == TargetQuotaAttemptBudget.Phase.ADMITTED
                    || budget.phase() == TargetQuotaAttemptBudget.Phase.UNKNOWN) {
                if (reference == null
                        || !reference.locator().equals(budget.locator())
                        || budget.resolvedAt() != null
                        || reference.obligation().ledgerState()
                                != (budget.phase() == TargetQuotaAttemptBudget.Phase.ADMITTED
                                        ? AttemptLedgerState.PUBLISHING
                                        : AttemptLedgerState.UNCERTAIN)) {
                    throw new IllegalStateException("payload summary open Budget/runtime disagree");
                }
                if (budget.phase() == TargetQuotaAttemptBudget.Phase.ADMITTED) {
                    if (!budget.mutation().equals(first.mutation())) {
                        throw new IllegalStateException("ADMITTED Budget changed its first Source");
                    }
                } else {
                    requireResolution(dependencies, byStamp, budget, budget.mutation());
                }
                counts.compute(id(owner.messageId()), (key, count) -> Math.incrementExact(count));
            } else {
                if (reference != null || budget.resolvedAt() == null) {
                    throw new IllegalStateException("resolved Budget retains an unresolved runtime identity");
                }
                budget.resolvedAt().requireAfter(first.mutation());
                requireResolution(dependencies, byStamp, budget, budget.resolvedAt());
            }
        }

        private void event(TargetCheckpointDependencies.Admission admission) {
            final String key = id(admission.attempt().locator().messageId());
            final var owner =
                    dependencies.payloadOwner(admission.attempt().locator().messageId());
            if (owner == null || !owner.summaryQualified()) {
                return;
            }
            final var initialized = owner.initializationSource();
            final var born = admission.first().mutation();
            final var resolved = admission.attempt().resolvedAt();
            if (Long.compareUnsigned(born.sequence(), initialized.sequence()) > 0) {
                born.requireAfter(initialized);
                TargetPayloadObligationSummaryAudit.event(revisions, latest, key, born);
            } else if (resolved == null || Long.compareUnsigned(resolved.sequence(), initialized.sequence()) > 0) {
                initialCounts.compute(key, (ignored, value) -> Math.incrementExact(value));
            }
            if (resolved != null && Long.compareUnsigned(resolved.sequence(), initialized.sequence()) > 0) {
                resolved.requireAfter(initialized);
                TargetPayloadObligationSummaryAudit.event(revisions, latest, key, resolved);
            }
        }
    }

    private static void event(
            Map<String, Long> revisions, Map<String, TargetQuotaMutation> latest, String key, TargetQuotaMutation at) {
        revisions.compute(key, (ignored, value) -> Math.incrementExact(value));
        if (Long.compareUnsigned(at.sequence(), latest.get(key).sequence()) > 0) {
            latest.put(key, at);
        }
    }

    /** Full original signed Source images are correlated by the enclosing cold authentication path. */
    public static void requireOutcomeInputs(
            TargetCheckpointDependencies dependencies, List<TargetCheckpointDependencies.OutcomeImage> inputs) {
        final var originals = new HashMap<String, TargetCheckpointDependencies.OutcomeImage>();
        for (var input : inputs) {
            final String stamp = Bytes.hex(input.source().canonicalBytes());
            if (originals.putIfAbsent(stamp, input) != null) {
                throw new IllegalStateException("payload summary repeats an original Outcome Source position");
            }
        }
        for (var admission : dependencies.admissions()) {
            final var budget = admission.attempt();
            final var at = budget.resolvedAt() == null
                    ? budget.phase() == TargetQuotaAttemptBudget.Phase.UNKNOWN ? budget.mutation() : null
                    : budget.resolvedAt();
            if (at == null) {
                continue;
            }
            final var original = Objects.requireNonNull(
                    originals.get(Bytes.hex(at.source().canonicalBytes())),
                    "payload summary lacks its original Outcome input");
            final var input = original.image();
            final var body = input.type() == SystemMutationType.PUBLISH_OUTCOME
                    ? PublishOutcomeBody.decode(input.canonicalBody())
                    : PublishOutcomeBody.decodeEvidenceResolution(input.canonicalBody());
            if (!Arrays.equals(body.publishAttemptId(), budget.publishAttemptId())
                    || !Arrays.equals(Bytes.sha256(input.canonicalEnvelope()), at.mutationDigest())
                    || (budget.resolvedAt() == null) != (body.sideEffect() == 3)) {
                throw new IllegalStateException(
                        "payload summary changed its first actual unresolved/definitive Outcome");
            }
        }
    }

    private static void requireResolution(
            TargetCheckpointDependencies dependencies,
            Map<String, TargetResultRecord> byStamp,
            TargetQuotaAttemptBudget budget,
            TargetQuotaMutation at) {
        final var first = Objects.requireNonNull(
                byStamp.get(Bytes.hex(at.canonicalBytes())),
                "payload summary lacks the original resolving Source first");
        final var result = SystemMutationResult.decode(first.typedPayload());
        if (!first.appliedFirst()
                || result.mutationType() != SystemMutationType.PUBLISH_OUTCOME
                        && result.mutationType() != SystemMutationType.EVIDENCE_RESOLUTION
                || !Arrays.equals(
                        result.mutationId(),
                        SystemMutation.computeSystemMutationId(
                                dependencies.root().metadata().shardId(),
                                result.mutationType(),
                                budget.publishAttemptId(),
                                result.mutationHash()))) {
            throw new IllegalStateException("payload summary resolving first changed its exact attempt/Source");
        }
        dependencies.requireSystemPosition(first);
    }

    private static String id(DelayMessageId message) {
        return Bytes.hex(message.bytes());
    }
}
