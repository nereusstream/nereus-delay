package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.SloAuthoritativeStartFactory;
import com.nereusstream.delay.protocol.SloObjectiveName;
import com.nereusstream.delay.protocol.SloObservationOutbox;
import com.nereusstream.delay.protocol.SloPath;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSloCustodyRecord;
import com.nereusstream.delay.protocol.TargetSloObservationControlBody;
import com.nereusstream.delay.protocol.TargetSloObservationEventBody;
import com.nereusstream.delay.protocol.TargetSloObservationEventRecord;
import com.nereusstream.delay.protocol.TargetSloObservationGeneration;
import com.nereusstream.delay.protocol.TargetSloObservationPolicy;
import com.nereusstream.delay.protocol.TargetSloObservationSample;
import com.nereusstream.delay.protocol.TargetSloObservationState;
import com.nereusstream.delay.protocol.TargetSloWriterEpoch;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.LongConsumer;

/** Independent retained mode-history completeness; this never certifies SLO sample coverage. */
public final class TargetSloObservationHistoryAudit {
    private TargetSloObservationHistoryAudit() {}

    /**
     * Continuation of the original retirement predicates over the caller's frozen private inventory.
     * One step handles one inventory row, one declaration, or one ordered history edge. No inventory
     * is copied by construction and no last turn drains an unbounded collection. The byte callbacks
     * independently debit retained material and copies produced by this fold (retaining a copy does
     * not itself copy it again); map/object overhead and codec-internal working space
     * still belong to the original physical reservation, not to a claimed JVM heap measurement.
     */
    public static final class RetirementCursor {
        private enum Phase {
            SHAPE,
            WRITERS,
            GENERATIONS,
            DECLARATIONS,
            GENERATION_CHAIN,
            HEAD,
            CUSTODY_INDEX,
            SAMPLES,
            EVENTS,
            CUSTODY_HISTORY,
            EVENT_FIRSTS,
            EVENT_FIRST_END,
            FOLD_START,
            FOLD_EDGE,
            FOLD_END,
            BUSINESS_FIRSTS,
            BUSINESS_FIRST_END,
            COUNTS,
            ELIGIBILITY_MESSAGES,
            DONE
        }

        private final List<TargetSloObservationState> states;
        private final List<TargetSloObservationGeneration> generations;
        private final TargetQuotaScope scope;
        private final byte[] lineage;
        private final TargetQuotaMutation frontier;
        private final List<TargetSloObservationSample> samples;
        private final List<TargetResultRecord> firsts;
        private final List<TargetSloObservationEventRecord> events;
        private final List<TargetSloCustodyRecord> custodies;
        private final List<TargetSloWriterEpoch> writers;
        private final List<TargetMessageRecord> eligibilityMessages;
        private final HashMap<String, TargetSloWriterEpoch> writerRows = new HashMap<>();
        private final TreeMap<Long, TargetSloObservationGeneration> ordered = new TreeMap<>(Long::compareUnsigned);
        private final TreeMap<SourcePosition, TargetSloObservationGeneration> controls = new TreeMap<>();
        private final HashMap<Long, TargetSloObservationGeneration> byGeneration = new HashMap<>();
        private final HashMap<Long, long[]> counts = new HashMap<>();
        private final HashMap<Long, TargetQuotaMutation> latest = new HashMap<>();
        private final HashMap<Long, TargetSloObservationPolicy.Declaration> commandDeclarations = new HashMap<>();
        private final HashMap<Long, TargetSloObservationPolicy.Declaration> dueDeclarations = new HashMap<>();
        private final HashMap<String, TargetSloObservationSample> bySample = new HashMap<>();
        private final HashMap<String, TargetSloObservationSample> pendingSamples = new HashMap<>();
        private final HashMap<String, TargetSloObservationSample> dueFirsts = new HashMap<>();
        private final HashMap<String, TargetSloObservationSample> eligibilityFirsts = new HashMap<>();
        private final HashMap<String, TargetSloObservationSample> births = new HashMap<>();
        private final HashMap<String, TargetSloCustodyRecord> custodyByKey = new HashMap<>();
        private final HashMap<String, TreeMap<Long, HistoryEntry>> histories = new HashMap<>();
        private final HashMap<String, HistoryEntry> pendingEvents = new HashMap<>();
        private Phase phase = Phase.SHAPE;
        private Iterator<TargetSloWriterEpoch> writerIterator;
        private Iterator<TargetSloObservationGeneration> generationIterator;
        private Iterator<TargetSloObservationPolicy.Declaration> declarationIterator;
        private Iterator<TargetSloCustodyRecord> custodyIterator;
        private Iterator<TargetSloObservationSample> sampleIterator;
        private Iterator<TargetSloObservationEventRecord> eventIterator;
        private Iterator<TargetResultRecord> firstIterator;
        private Iterator<TargetMessageRecord> messageIterator;
        private Iterator<HistoryEntry> historyIterator;
        private TargetSloObservationGeneration selectedGeneration, previousGeneration;
        private long expectedGeneration = 1;
        private TargetSloObservationSample selectedSample, version;
        private SloObservationOutbox folded;
        private TargetQuotaMutation foldedAt;
        private HashMap<String, TargetSloObservationSample> versions;
        private String selectedSampleId;
        private boolean advancing, failed;
        private LongConsumer retained, copied;

        public RetirementCursor(
                List<TargetSloObservationState> states,
                List<TargetSloObservationGeneration> generations,
                TargetQuotaScope scope,
                byte[] lineage,
                TargetQuotaMutation frontier,
                List<TargetSloObservationSample> samples,
                List<TargetResultRecord> firsts,
                List<TargetSloObservationEventRecord> events,
                List<TargetSloCustodyRecord> custodies,
                List<TargetSloWriterEpoch> writers,
                List<TargetMessageRecord> eligibilityMessages) {
            this.states = Objects.requireNonNull(states);
            this.generations = Objects.requireNonNull(generations);
            this.scope = Objects.requireNonNull(scope);
            this.lineage = Objects.requireNonNull(lineage);
            this.frontier = Objects.requireNonNull(frontier);
            this.samples = Objects.requireNonNull(samples);
            this.firsts = Objects.requireNonNull(firsts);
            this.events = Objects.requireNonNull(events);
            this.custodies = Objects.requireNonNull(custodies);
            this.writers = Objects.requireNonNull(writers);
            this.eligibilityMessages = Objects.requireNonNull(eligibilityMessages);
        }

        public boolean advance(int maxRows, Runnable current, LongConsumer retained, LongConsumer copied) {
            if (maxRows <= 0) {
                throw new IllegalArgumentException("positive SLO history turn required");
            }
            Objects.requireNonNull(current);
            Objects.requireNonNull(retained);
            Objects.requireNonNull(copied);
            if (advancing || failed) {
                throw new IllegalStateException("SLO history cursor is advancing or failed");
            }
            advancing = true;
            this.retained = retained;
            this.copied = copied;
            try {
                current.run();
                for (int step = 0; step < maxRows && phase != Phase.DONE; step++) {
                    current.run();
                    step();
                    current.run();
                }
                return phase == Phase.DONE;
            } catch (RuntimeException | Error failure) {
                failed = true;
                throw failure;
            } finally {
                this.retained = null;
                this.copied = null;
                advancing = false;
            }
        }

        private void step() {
            switch (phase) {
                case SHAPE -> {
                    if (!(states.isEmpty() && generations.isEmpty()) && (states.size() != 1 || generations.isEmpty())) {
                        throw new IllegalStateException(
                                "Target SLO history lacks its sole current head or generations");
                    }
                    writerIterator = writers.iterator();
                    phase = Phase.WRITERS;
                }
                case WRITERS -> {
                    if (!writerIterator.hasNext()) {
                        generationIterator = generations.iterator();
                        phase = Phase.GENERATIONS;
                        return;
                    }
                    final var writer = writerIterator.next();
                    final var id = key(writer.key());
                    if (writerRows.putIfAbsent(id, writer) != null) {
                        throw new IllegalStateException("duplicate SLO writer epoch in actual checkpoint history");
                    }
                    keep(id);
                }
                case GENERATIONS -> {
                    if (!generationIterator.hasNext()) {
                        generationIterator = ordered.values().iterator();
                        phase = Phase.GENERATION_CHAIN;
                        return;
                    }
                    final var generation = generationIterator.next();
                    final var policy = generation.policy();
                    if (!scope.equals(policy.scope())
                            || !Arrays.equals(lineage, copy(policy.lineage()))
                            || ordered.putIfAbsent(generation.generation(), generation) != null) {
                        throw new IllegalStateException(
                                "Target SLO retained generations changed scope or repeat an ordinal");
                    }
                    generation.mutation().requireAtOrBefore(frontier);
                    policy.requireSource(frontier.source());
                    if (Long.compareUnsigned(
                                    generation.generation(), policy.limits().maximumConcurrentGenerations())
                            > 0) {
                        throw new IllegalStateException(
                                "Target SLO generation exceeded its original retained-generation window");
                    }
                    requirePolicyWriter(generation, writerRows);
                    controls.put(generation.enable().appliedAt().source(), generation);
                    byGeneration.put(generation.generation(), generation);
                    retained.accept(7L * Long.BYTES);
                    counts.put(generation.generation(), new long[7]);
                    latest.put(generation.generation(), generation.control().appliedAt());
                    selectedGeneration = generation;
                    declarationIterator = policy.declarations().iterator();
                    phase = Phase.DECLARATIONS;
                }
                case DECLARATIONS -> {
                    if (!declarationIterator.hasNext()) {
                        selectedGeneration = null;
                        declarationIterator = null;
                        phase = Phase.GENERATIONS;
                        return;
                    }
                    final var declaration = declarationIterator.next();
                    final long generation = selectedGeneration.generation();
                    if (declaration.objective().name() == SloObjectiveName.COMMAND_APPLIED_LATENCY) {
                        commandDeclarations.putIfAbsent(generation, declaration);
                    }
                    if (declaration.objective().name() == SloObjectiveName.DUE_ADMISSION_LAG
                            && declaration.path() == SloPath.ORDINARY_MANAGED) {
                        dueDeclarations.putIfAbsent(generation, declaration);
                    }
                }
                case GENERATION_CHAIN -> {
                    if (!generationIterator.hasNext()) {
                        phase = Phase.HEAD;
                        return;
                    }
                    final var generation = generationIterator.next();
                    if (generation.generation() != expectedGeneration) {
                        throw new IllegalStateException("Target SLO retained generation history has a gap");
                    }
                    if (previousGeneration != null) {
                        if (previousGeneration.active()
                                || !Arrays.equals(
                                        copy(TargetSloObservationState.from(previousGeneration)
                                                .digest()),
                                        copy(generation
                                                .enable()
                                                .body()
                                                .request()
                                                .expectedPriorDigest()))) {
                            throw new IllegalStateException("Target SLO enable lost its complete stopped predecessor");
                        }
                        generation
                                .enable()
                                .appliedAt()
                                .requireAfter(previousGeneration.control().appliedAt());
                    }
                    previousGeneration = generation;
                    expectedGeneration = TargetQuotaMutation.increment(expectedGeneration);
                }
                case HEAD -> {
                    if (!states.isEmpty()) {
                        final var state = states.getFirst();
                        if (!Arrays.equals(
                                copy(state.canonicalBytes()),
                                copy(TargetSloObservationState.from(Objects.requireNonNull(previousGeneration))
                                        .canonicalBytes()))) {
                            throw new IllegalStateException("Target SLO state lost exact original Control head");
                        }
                        state.mutation().requireAtOrBefore(frontier);
                    }
                    custodyIterator = custodies.iterator();
                    phase = Phase.CUSTODY_INDEX;
                }
                case CUSTODY_INDEX -> {
                    if (!custodyIterator.hasNext()) {
                        sampleIterator = samples.iterator();
                        phase = Phase.SAMPLES;
                        return;
                    }
                    final var custody = custodyIterator.next();
                    final var id = key(custody.key());
                    if (custodyByKey.putIfAbsent(id, custody) != null) {
                        throw new IllegalStateException("duplicate SLO custody row");
                    }
                    keep(id);
                }
                case SAMPLES -> {
                    if (!sampleIterator.hasNext()) {
                        eventIterator = events.iterator();
                        phase = Phase.EVENTS;
                        return;
                    }
                    indexSample(sampleIterator.next());
                }
                case EVENTS -> {
                    if (!eventIterator.hasNext()) {
                        custodyIterator = custodies.iterator();
                        phase = Phase.CUSTODY_HISTORY;
                        return;
                    }
                    final var event = eventIterator.next();
                    indexHistory(new HistoryEntry(event.input(), event.appliedAt(), event, null));
                    addHistoryUsage(event.body().generation(), stored(event), event.appliedAt());
                }
                case CUSTODY_HISTORY -> {
                    if (!custodyIterator.hasNext()) {
                        firstIterator = firsts.iterator();
                        phase = Phase.EVENT_FIRSTS;
                        return;
                    }
                    final var custody = custodyIterator.next();
                    indexHistory(new HistoryEntry(custody.input(), custody.appliedAt(), null, custody));
                    addHistoryUsage(custody.sample().generation(), stored(custody), custody.appliedAt());
                }
                case EVENT_FIRSTS -> {
                    if (!firstIterator.hasNext()) {
                        phase = Phase.EVENT_FIRST_END;
                        return;
                    }
                    eventFirst(firstIterator.next());
                }
                case EVENT_FIRST_END -> {
                    if (!pendingEvents.isEmpty()) {
                        throw new IllegalStateException("SLO event lost original first result");
                    }
                    sampleIterator = samples.iterator();
                    phase = Phase.FOLD_START;
                }
                case FOLD_START -> {
                    if (!sampleIterator.hasNext()) {
                        firstIterator = firsts.iterator();
                        phase = Phase.BUSINESS_FIRSTS;
                        return;
                    }
                    selectedSample = sampleIterator.next();
                    folded = SloObservationOutbox.open(
                            selectedSample.observation().start());
                    keepDigest();
                    foldedAt = selectedSample.first().mutation();
                    version = version(selectedSample, folded, foldedAt);
                    versions = new HashMap<>();
                    final var digest = key(version.digest());
                    versions.put(digest, version);
                    keep(digest);
                    selectedSampleId = key(selectedSample.observation().sampleId());
                    final var history = histories.get(selectedSampleId);
                    historyIterator = history == null ? null : history.values().iterator();
                    phase = Phase.FOLD_EDGE;
                }
                case FOLD_EDGE -> {
                    if (historyIterator == null || !historyIterator.hasNext()) {
                        phase = Phase.FOLD_END;
                        return;
                    }
                    fold(historyIterator.next());
                }
                case FOLD_END -> {
                    if (!foldedAt.equals(selectedSample.mutation())
                            || !Arrays.equals(copy(version.canonicalBytes()), copy(selectedSample.canonicalBytes()))) {
                        throw new IllegalStateException("SLO sample lost complete original Source event history");
                    }
                    histories.remove(selectedSampleId);
                    selectedSampleId = null;
                    selectedSample = null;
                    version = null;
                    folded = null;
                    foldedAt = null;
                    versions = null;
                    historyIterator = null;
                    phase = Phase.FOLD_START;
                }
                case BUSINESS_FIRSTS -> {
                    if (!firstIterator.hasNext()) {
                        phase = Phase.BUSINESS_FIRST_END;
                        return;
                    }
                    businessFirst(firstIterator.next());
                }
                case BUSINESS_FIRST_END -> {
                    if (!pendingSamples.isEmpty() || !dueFirsts.isEmpty() || !eligibilityFirsts.isEmpty()) {
                        throw new IllegalStateException("Target SLO sample has no eligible original business first");
                    }
                    generationIterator = generations.iterator();
                    phase = Phase.COUNTS;
                }
                case COUNTS -> {
                    if (!generationIterator.hasNext()) {
                        messageIterator = eligibilityMessages.iterator();
                        phase = Phase.ELIGIBILITY_MESSAGES;
                        return;
                    }
                    final var generation = generationIterator.next();
                    final var usage = counts.get(generation.generation());
                    final var reservation = generation.counts().reservation() == null
                            ? null
                            : new TargetSloObservationGeneration.Counts.Reservation(usage[5], usage[6]);
                    final var expected = new TargetSloObservationGeneration.Counts(
                            usage[0], usage[0], usage[1], usage[4], usage[2], usage[3], reservation);
                    if (!expected.equals(generation.counts())
                            || !latest.get(generation.generation()).equals(generation.mutation())) {
                        throw new IllegalStateException(
                                ("Target SLO generation disagrees with complete actual/reserved "
                                        + "sample/event/custody fold"));
                    }
                }
                case ELIGIBILITY_MESSAGES -> {
                    if (!messageIterator.hasNext()) {
                        phase = Phase.DONE;
                        return;
                    }
                    eligibilityMessage(messageIterator.next());
                }
                case DONE -> {}
            }
        }

        private void indexSample(TargetSloObservationSample sample) {
            final var generation = Objects.requireNonNull(
                    byGeneration.get(sample.generation()), "Target SLO sample lost original generation");
            final var usage = counts.get(sample.generation());
            final var id = key(sample.key());
            if (pendingSamples.putIfAbsent(id, sample) != null) {
                throw new IllegalStateException("duplicate Target SLO sample identity");
            }
            keep(id);
            final var sampleId = key(sample.observation().sampleId());
            if (bySample.put(sampleId, sample) == null) {
                keep(sampleId);
            }
            if (sample.admission() != null) {
                final var first = key(sample.first().key());
                if (dueFirsts.putIfAbsent(first, sample) != null) {
                    throw new IllegalStateException("Admission event was bound more than once");
                }
                keep(first);
            }
            if (sample.eligibility() != null) {
                final var first = key(sample.first().key());
                if (eligibilityFirsts.putIfAbsent(first, sample) != null) {
                    throw new IllegalStateException("Message birth eligibility was bound more than once");
                }
                keep(first);
                final var token = birth(sample.eligibility().locator());
                if (births.putIfAbsent(token, sample) != null) {
                    throw new IllegalStateException("duplicate ordinary eligibility birth");
                }
                keep(token);
            }
            usage[0] = Math.addExact(usage[0], 1);
            usage[1] = Math.addExact(usage[1], stored(sample));
            final var custody = custodyByKey.get(
                    key(TargetSloCustodyRecord.key(copy(sample.observation().sampleId()), copy(sample.digest()))));
            final boolean currentCustodied = custody != null;
            if (currentCustodied) {
                if (!Arrays.equals(copy(custody.sample().canonicalBytes()), copy(sample.canonicalBytes()))) {
                    throw new IllegalStateException("SLO custody changed exact current sample bytes");
                }
                usage[4] = Math.addExact(usage[4], 1);
            }
            if (generation.counts().reservation() != null) {
                final var reservation =
                        TargetSloObservationReservations.forSample(generation, sample, currentCustodied);
                usage[5] = Math.addExact(usage[5], reservation.records());
                usage[6] = Math.addExact(usage[6], reservation.bytes());
            } else if (currentCustodied) {
                throw new IllegalStateException("legacy SLO generation cannot pretend to have reserved custody");
            }
            latest(latest, sample.generation(), sample.mutation());
        }

        private void indexHistory(HistoryEntry row) {
            final var body =
                    TargetSloObservationEventBody.decode(copy(row.input().canonicalBody()));
            final var sampleId = key(
                    row.finalEvent() != null
                            ? body.sampleId()
                            : body.exportedSample().observation().sampleId());
            final var sample = Objects.requireNonNull(bySample.get(sampleId), "orphaned SLO Source event/custody");
            final var generation =
                    Objects.requireNonNull(byGeneration.get(sample.generation()), "SLO event original generation");
            requireEventWriter(generation, body, row.input(), row.appliedAt(), writerRows);
            generation.policy().requireSource(row.appliedAt().source());
            row.appliedAt().requireAfter(sample.first().mutation());
            if (row.finalEvent() != null) {
                final var event = row.finalEvent();
                if (body.generation() != sample.generation()
                        || !Arrays.equals(copy(body.bindingDigest()), copy(sample.bindingDigest()))) {
                    throw new IllegalStateException("SLO Final changed original sample binding");
                }
                event.appliedAt().requireAtOrBefore(sample.mutation());
                if (body.evidence() != null) {
                    body.evidence().requireObservation(sample, event.incoming());
                }
            } else if (body.exportedSample().generation() != sample.generation()
                    || !Arrays.equals(copy(body.exportedSample().bindingDigest()), copy(sample.bindingDigest()))) {
                throw new IllegalStateException("SLO custody changed original sample binding");
            }
            final var inputId = key(row.input().systemMutationId());
            if (pendingEvents.putIfAbsent(inputId, row) != null) {
                throw new IllegalStateException("duplicate SLO Source history edge");
            }
            keep(inputId);
            var history = histories.get(sampleId);
            if (history == null) {
                history = new TreeMap<>(Long::compareUnsigned);
                histories.put(sampleId, history);
                keep(sampleId);
            }
            if (history.putIfAbsent(row.appliedAt().sequence(), row) != null) {
                throw new IllegalStateException("duplicate SLO Source history edge");
            }
        }

        private void addHistoryUsage(long generation, long bytes, TargetQuotaMutation at) {
            final var usage = Objects.requireNonNull(counts.get(generation), "SLO event lost original generation");
            usage[2] = Math.addExact(usage[2], 1);
            usage[3] = Math.addExact(usage[3], bytes);
            latest(latest, generation, at);
        }

        private void eventFirst(TargetResultRecord first) {
            if (first.kind() != TargetResultRecord.Kind.SYSTEM) {
                return;
            }
            final var payload = copy(first.typedPayload());
            final var result = SystemMutationResult.decode(payload);
            if (result.mutationType() != SystemMutationType.TARGET_SLO_OBSERVATION) {
                return;
            }
            final var row = pendingEvents.remove(key(result.mutationId()));
            if (first.appliedFirst()) {
                if (row == null
                        || !first.mutation().equals(row.appliedAt())
                        || !Arrays.equals(
                                payload,
                                copy(SystemMutationResult.from(
                                                row.input(),
                                                ApplyStatus.APPLIED,
                                                StableCode.OK,
                                                copy(row.appliedAt().source().canonicalBytes()))
                                        .encode()))) {
                    throw new IllegalStateException(
                            "APPLIED SLO Source first lost its exact immutable Final/custody event");
                }
            } else if (row != null) {
                throw new IllegalStateException("rejected SLO event has an applied history edge");
            }
        }

        private void fold(HistoryEntry row) {
            if (row.finalEvent() != null) {
                final var event = row.finalEvent();
                if (!Arrays.equals(copy(folded.recordDigest()), copy(event.beforeDigest()))) {
                    throw new IllegalStateException("SLO Source merge history has a missing predecessor");
                }
                event.appliedAt().requireAfter(foldedAt);
                event.requireIncomingFor(version);
                final boolean merging = folded.finalObservation() != null;
                folded = folded.mergeFinal(
                        event.incoming(),
                        selectedSample.declaration().objective().direction());
                keepDigest();
                // SloSampleFinal.merge owns sample/start/evidence hashes and its content digest.
                if (merging) {
                    retained.accept(4L * 32);
                    copied.accept(4L * 32);
                }
                if (!Arrays.equals(
                        copy(folded.canonicalBytes()), copy(event.after().canonicalBytes()))) {
                    throw new IllegalStateException("SLO Source merge edge changes conservative v1 semantics");
                }
                foldedAt = event.appliedAt();
                version = version(selectedSample, folded, foldedAt);
                final var digest = key(version.digest());
                if (versions.putIfAbsent(digest, version) != null) {
                    throw new IllegalStateException("SLO Source merge repeated one full sample version");
                }
                keep(digest);
            } else {
                final var exported = row.custody().sample();
                final var known = versions.get(key(exported.digest()));
                if (known == null || !Arrays.equals(copy(known.canonicalBytes()), copy(exported.canonicalBytes()))) {
                    throw new IllegalStateException("SLO custody lacks its exact earlier exported sample version");
                }
                exported.mutation().requireAtOrBefore(row.appliedAt());
            }
        }

        private TargetSloObservationSample version(
                TargetSloObservationSample sample, SloObservationOutbox observation, TargetQuotaMutation at) {
            final byte[] lineage = copy(sample.lineage()),
                    enable = copy(sample.enableId()),
                    policy = copy(sample.policyDigest());
            // The constructor owns these three fixed arrays; the original first/admission are shared.
            retained.accept(Math.addExact(lineage.length, Math.addExact(enable.length, policy.length)));
            copied.accept(Math.addExact(lineage.length, Math.addExact(enable.length, policy.length)));
            return new TargetSloObservationSample(
                    sample.scope(),
                    lineage,
                    sample.generation(),
                    enable,
                    sample.enabledAt(),
                    policy,
                    sample.declaration(),
                    sample.first(),
                    observation,
                    at,
                    sample.admission(),
                    sample.eligibility());
        }

        private void businessFirst(TargetResultRecord first) {
            if (first.kind() == TargetResultRecord.Kind.SYSTEM) {
                final var sample = dueFirsts.remove(key(first.key()));
                if (sample != null) {
                    final var selected = controls.lowerEntry(first.mutation().source());
                    if (selected == null
                            || selected.getValue().generation() != sample.generation()
                            || !Arrays.equals(
                                    copy(first.canonicalBytes()),
                                    copy(sample.first().canonicalBytes()))) {
                        throw new IllegalStateException("due sample changed its original effective Admission first");
                    }
                    pendingSamples.remove(key(sample.key()));
                }
                return;
            }
            if (first.kind() != TargetResultRecord.Kind.COMMAND
                    && first.kind() != TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED) {
                return;
            }
            final var eligibility = eligibilityFirsts.remove(key(first.key()));
            if (eligibility != null) {
                if (!Arrays.equals(
                        copy(first.canonicalBytes()), copy(eligibility.first().canonicalBytes()))) {
                    throw new IllegalStateException("eligibility changed original birth first");
                }
                pendingSamples.remove(key(eligibility.key()));
            }
            final var selected = controls.lowerEntry(first.mutation().source());
            if (selected == null) {
                return;
            }
            final var generation = selected.getValue();
            if (generation.stop() != null
                    && first.mutation()
                                    .source()
                                    .compareTo(generation.stop().appliedAt().source())
                            >= 0) {
                return;
            }
            final var declaration = commandDeclarations.get(generation.generation());
            if (declaration == null) {
                return;
            }
            final var start = SloAuthoritativeStartFactory.commandApplied(
                    declaration.objective(), first.mutation().source());
            final var sample = pendingSamples.remove(key(TargetSloObservationSample.key(copy(start.sampleId()))));
            if (sample == null
                    || sample.generation() != generation.generation()
                    || !Arrays.equals(copy(sample.first().canonicalBytes()), copy(first.canonicalBytes()))
                    || !Arrays.equals(
                            copy(sample.observation().start().canonicalBytes()), copy(start.canonicalBytes()))) {
                throw new IllegalStateException(
                        "Source-applied certification Command lost its required original Start/binding");
            }
        }

        private void eligibilityMessage(TargetMessageRecord message) {
            if (message.nativeDeliveryPolicy() != NativeDeliveryPolicy.FORBID) {
                return;
            }
            final var selected = controls.lowerEntry(message.scheduleSource());
            if (selected == null) {
                return;
            }
            final var generation = selected.getValue();
            if (generation.stop() != null
                    && message.scheduleSource()
                                    .compareTo(generation.stop().appliedAt().source())
                            >= 0) {
                return;
            }
            final var declaration = dueDeclarations.get(generation.generation());
            if (declaration == null) {
                return;
            }
            final var sample = births.get(birth(message.locator()));
            if (sample == null
                    || sample.generation() != generation.generation()
                    || !sample.eligibility().locator().equals(message.locator())
                    || sample.eligibility().deliverAt() != message.deliverAtEpochMs()
                    || sample.eligibility().expireAt() != message.expireAtEpochMs()
                    || !Arrays.equals(
                            copy(sample.eligibility().source().canonicalBytes()),
                            copy(message.scheduleSource().canonicalBytes()))
                    || !Arrays.equals(
                            copy(sample.declaration().canonicalBytes()), copy(declaration.canonicalBytes()))) {
                throw new IllegalStateException(
                        ("durable ordinary eligibility is missing its original ALL_ACCEPTED Start, "
                                + "independently of Admission"));
            }
        }

        private String birth(TargetMessageLocator locator) {
            return key(Bytes.concat(copy(locator.messageId().bytes()), copy(Bytes.u32beBits(locator.generation()))));
        }

        private long stored(TargetSloObservationSample sample) {
            final var raw = copy(
                    TargetValueEnvelope.encode(TargetSloObservationSample.VALUE_TYPE, copy(sample.canonicalBytes())));
            long bytes = 0;
            // These are the codec's at most four fixed projections, not a retained-history traversal.
            for (var key : sample.keys()) {
                bytes = Math.addExact(bytes, Math.addExact((long) copy(key).length, raw.length));
            }
            return bytes;
        }

        private long stored(TargetSloObservationEventRecord event) {
            final var raw = copy(TargetValueEnvelope.encode(
                    TargetSloObservationEventRecord.VALUE_TYPE, copy(event.canonicalBytes())));
            return Math.addExact((long) copy(event.key()).length, raw.length);
        }

        private long stored(TargetSloCustodyRecord custody) {
            final var raw =
                    copy(TargetValueEnvelope.encode(TargetSloCustodyRecord.VALUE_TYPE, copy(custody.canonicalBytes())));
            return Math.addExact((long) copy(custody.key()).length, raw.length);
        }

        private void keepDigest() {
            retained.accept(32);
            copied.accept(32);
        }

        private byte[] copy(byte[] bytes) {
            copied.accept(bytes.length);
            return bytes;
        }

        private String key(byte[] bytes) {
            copy(bytes);
            final var value = Bytes.hex(bytes);
            // Java char material only; this does not assert a String/map object heap layout.
            copied.accept(Math.multiplyExact((long) value.length(), Character.BYTES));
            return value;
        }

        private void keep(String value) {
            retained.accept(Math.multiplyExact((long) value.length(), Character.BYTES));
        }
    }

    public static void requireComplete(
            List<TargetSloObservationState> states,
            List<TargetSloObservationGeneration> generations,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaMutation frontier) {
        if (states.isEmpty() && generations.isEmpty()) {
            return;
        }
        if (states.size() != 1 || generations.isEmpty()) {
            throw new IllegalStateException("Target SLO history lacks its sole current head or generations");
        }
        final var ordered = new TreeMap<Long, TargetSloObservationGeneration>(Long::compareUnsigned);
        for (var generation : generations) {
            if (!scope.equals(generation.policy().scope())
                    || !Arrays.equals(lineage, generation.policy().lineage())
                    || ordered.putIfAbsent(generation.generation(), generation) != null) {
                throw new IllegalStateException("Target SLO retained generations changed scope or repeat an ordinal");
            }
            generation.mutation().requireAtOrBefore(frontier);
            generation.policy().requireSource(frontier.source());
            if (Long.compareUnsigned(
                            generation.generation(),
                            generation.policy().limits().maximumConcurrentGenerations())
                    > 0) {
                throw new IllegalStateException(
                        "Target SLO generation exceeded its original retained-generation window");
            }
        }
        long expected = 1;
        TargetSloObservationGeneration previous = null;
        for (var generation : ordered.values()) {
            if (generation.generation() != expected) {
                throw new IllegalStateException("Target SLO retained generation history has a gap");
            }
            if (previous != null) {
                if (previous.active()
                        || !Arrays.equals(
                                TargetSloObservationState.from(previous).digest(),
                                generation.enable().body().request().expectedPriorDigest())) {
                    throw new IllegalStateException("Target SLO enable lost its complete stopped predecessor");
                }
                generation.enable().appliedAt().requireAfter(previous.control().appliedAt());
            }
            previous = generation;
            expected = TargetQuotaMutation.increment(expected);
        }
        final var state = states.getFirst();
        state.requireGeneration(Objects.requireNonNull(previous));
        state.mutation().requireAtOrBefore(frontier);
    }

    /** Every applied mode input and every stored endpoint must correlate, including when all mode rows are missing. */
    public static void requireControlInputs(
            List<TargetSloObservationGeneration> generations,
            List<TargetCheckpointDependencies.ControlImage> images,
            List<TargetResultRecord> firsts) {
        final var pending = new HashMap<String, TargetSloObservationGeneration.Control>();
        for (var generation : generations) {
            put(pending, generation.enable());
            if (generation.stop() != null) {
                put(pending, generation.stop());
            }
        }
        final var actual = new HashMap<String, TargetResultRecord>();
        for (var first : firsts) {
            actual.put(Bytes.hex(first.logicalId()), first);
        }
        for (var image : images) {
            if (image.image().type() != SystemMutationType.APPLY_SHARD_CONTROL
                    || !TargetSloObservationControlBody.isControl(image.image())) {
                continue;
            }
            final var key = Bytes.hex(image.image().systemMutationId());
            final var first = Objects.requireNonNull(actual.get(key), "original Target SLO Control first");
            final var endpoint = pending.remove(key);
            if (first.appliedFirst()) {
                if (endpoint == null
                        || !Arrays.equals(
                                endpoint.input().canonicalEnvelope(),
                                image.image().canonicalEnvelope())
                        || !Arrays.equals(
                                endpoint.appliedAt().source().canonicalBytes(),
                                image.source().canonicalBytes())) {
                    throw new IllegalStateException(
                            "APPLIED Target SLO Control lost its exact retained generation endpoint");
                }
            } else if (endpoint != null) {
                throw new IllegalStateException("rejected Target SLO Control appears as an applied generation");
            }
        }
        if (!pending.isEmpty()) {
            throw new IllegalStateException("Target SLO generation has missing original Source Control inputs");
        }
    }

    private static void put(
            HashMap<String, TargetSloObservationGeneration.Control> values,
            TargetSloObservationGeneration.Control control) {
        if (values.putIfAbsent(Bytes.hex(control.input().systemMutationId()), control) != null) {
            throw new IllegalStateException("duplicate Target SLO generation Control endpoint");
        }
    }

    /** Actual firsts, complete immutable history and exact current custody independently reconstruct every duty. */
    public static void requireSamples(
            List<TargetSloObservationGeneration> generations,
            List<TargetSloObservationSample> samples,
            List<TargetResultRecord> firsts) {
        requireSamples(generations, samples, firsts, List.of(), List.of());
    }

    public static void requireSamples(
            List<TargetSloObservationGeneration> generations,
            List<TargetSloObservationSample> samples,
            List<TargetResultRecord> firsts,
            List<TargetSloObservationEventRecord> events) {
        requireSamples(generations, samples, firsts, events, List.of());
    }

    public static void requireSamples(
            List<TargetSloObservationGeneration> generations,
            List<TargetSloObservationSample> samples,
            List<TargetResultRecord> firsts,
            List<TargetSloObservationEventRecord> events,
            List<TargetSloCustodyRecord> custodies) {
        requireSamples(generations, samples, firsts, events, custodies, List.of());
    }
    /** Writer rows must be the actual Native62 material collected and independently authenticated by the checkpoint. */
    public static void requireSamples(
            List<TargetSloObservationGeneration> generations,
            List<TargetSloObservationSample> samples,
            List<TargetResultRecord> firsts,
            List<TargetSloObservationEventRecord> events,
            List<TargetSloCustodyRecord> custodies,
            List<TargetSloWriterEpoch> writers) {
        final var writerRows = new HashMap<String, TargetSloWriterEpoch>();
        for (var writer : writers) {
            if (writerRows.putIfAbsent(Bytes.hex(writer.key()), writer) != null) {
                throw new IllegalStateException("duplicate SLO writer epoch in actual checkpoint history");
            }
        }
        for (var generation : generations) {
            requirePolicyWriter(generation, writerRows);
        }
        requireEventHistory(generations, samples, firsts, events, custodies, writerRows);
        final var controls = new TreeMap<SourcePosition, TargetSloObservationGeneration>();
        final var byGeneration = new HashMap<Long, TargetSloObservationGeneration>();
        final var counts = new HashMap<Long, long[]>();
        final var latest = new HashMap<Long, TargetQuotaMutation>();
        final var pending = new HashMap<String, TargetSloObservationSample>();
        final var dueFirsts = new HashMap<String, TargetSloObservationSample>();
        final var eligibilityFirsts = new HashMap<String, TargetSloObservationSample>();
        final var custodyByKey = new HashMap<String, TargetSloCustodyRecord>();
        for (var custody : custodies) {
            if (custodyByKey.putIfAbsent(Bytes.hex(custody.key()), custody) != null) {
                throw new IllegalStateException("duplicate SLO custody row");
            }
        }
        for (var generation : generations) {
            controls.put(generation.enable().appliedAt().source(), generation);
            byGeneration.put(generation.generation(), generation);
            counts.put(generation.generation(), new long[7]);
            latest.put(generation.generation(), generation.control().appliedAt());
        }
        for (var sample : samples) {
            final var generation = Objects.requireNonNull(
                    byGeneration.get(sample.generation()), "Target SLO sample lost original generation");
            final var usage = counts.get(sample.generation());
            if (pending.putIfAbsent(Bytes.hex(sample.key()), sample) != null) {
                throw new IllegalStateException("duplicate Target SLO sample identity");
            }
            if (sample.admission() != null
                    && dueFirsts.putIfAbsent(Bytes.hex(sample.first().key()), sample) != null) {
                throw new IllegalStateException("Admission event was bound more than once");
            }
            if (sample.eligibility() != null
                    && eligibilityFirsts.putIfAbsent(Bytes.hex(sample.first().key()), sample) != null) {
                throw new IllegalStateException("Message birth eligibility was bound more than once");
            }
            usage[0] = Math.addExact(usage[0], 1);
            usage[1] = Math.addExact(usage[1], sample.storedBytes());
            final var custody = custodyByKey.get(
                    Bytes.hex(TargetSloCustodyRecord.key(sample.observation().sampleId(), sample.digest())));
            final boolean currentCustodied = custody != null;
            if (currentCustodied) {
                if (!Arrays.equals(custody.sample().canonicalBytes(), sample.canonicalBytes())) {
                    throw new IllegalStateException("SLO custody changed exact current sample bytes");
                }
                usage[4] = Math.addExact(usage[4], 1);
            }
            if (generation.counts().reservation() != null) {
                final var reservation =
                        TargetSloObservationReservations.forSample(generation, sample, currentCustodied);
                usage[5] = Math.addExact(usage[5], reservation.records());
                usage[6] = Math.addExact(usage[6], reservation.bytes());
            } else if (currentCustodied) {
                throw new IllegalStateException("legacy SLO generation cannot pretend to have reserved custody");
            }
            latest(latest, sample.generation(), sample.mutation());
        }
        for (var event : events) {
            final var usage =
                    Objects.requireNonNull(counts.get(event.body().generation()), "SLO event lost original generation");
            usage[2] = Math.addExact(usage[2], 1);
            usage[3] = Math.addExact(usage[3], event.storedBytes());
            latest(latest, event.body().generation(), event.appliedAt());
        }
        for (var custody : custodies) {
            final var usage = Objects.requireNonNull(
                    counts.get(custody.sample().generation()), "SLO custody lost original generation");
            usage[2] = Math.addExact(usage[2], 1);
            usage[3] = Math.addExact(usage[3], custody.storedBytes());
            latest(latest, custody.sample().generation(), custody.appliedAt());
        }
        for (var first : firsts) {
            if (first.kind() == TargetResultRecord.Kind.SYSTEM) {
                final var sample = dueFirsts.remove(Bytes.hex(first.key()));
                if (sample != null) {
                    final var selected = controls.lowerEntry(first.mutation().source());
                    if (selected == null
                            || selected.getValue().generation() != sample.generation()
                            || !Arrays.equals(
                                    first.canonicalBytes(), sample.first().canonicalBytes())) {
                        throw new IllegalStateException("due sample changed its original effective Admission first");
                    }
                    pending.remove(Bytes.hex(sample.key()));
                }
                continue;
            }
            if (first.kind() != TargetResultRecord.Kind.COMMAND
                    && first.kind() != TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED) {
                continue;
            }
            final var eligibility = eligibilityFirsts.remove(Bytes.hex(first.key()));
            if (eligibility != null) {
                if (!Arrays.equals(first.canonicalBytes(), eligibility.first().canonicalBytes())) {
                    throw new IllegalStateException("eligibility changed original birth first");
                }
                pending.remove(Bytes.hex(eligibility.key()));
            }
            final var selected = controls.lowerEntry(first.mutation().source());
            if (selected == null) {
                continue;
            }
            final var generation = selected.getValue();
            if (generation.stop() != null
                    && first.mutation()
                                    .source()
                                    .compareTo(generation.stop().appliedAt().source())
                            >= 0) {
                continue;
            }
            final var declaration = generation.policy().declarations().stream()
                    .filter(d -> d.objective().name() == SloObjectiveName.COMMAND_APPLIED_LATENCY)
                    .findFirst()
                    .orElse(null);
            if (declaration == null) {
                continue;
            }
            final var start = SloAuthoritativeStartFactory.commandApplied(
                    declaration.objective(), first.mutation().source());
            final var sample = pending.remove(Bytes.hex(TargetSloObservationSample.key(start.sampleId())));
            if (sample == null
                    || sample.generation() != generation.generation()
                    || !Arrays.equals(sample.first().canonicalBytes(), first.canonicalBytes())
                    || !Arrays.equals(sample.observation().start().canonicalBytes(), start.canonicalBytes())) {
                throw new IllegalStateException(
                        "Source-applied certification Command lost its required original Start/binding");
            }
        }
        if (!pending.isEmpty() || !dueFirsts.isEmpty() || !eligibilityFirsts.isEmpty()) {
            throw new IllegalStateException("Target SLO sample has no eligible original business first");
        }
        for (var generation : generations) {
            final var usage = counts.get(generation.generation());
            final var reservation = generation.counts().reservation() == null
                    ? null
                    : new TargetSloObservationGeneration.Counts.Reservation(usage[5], usage[6]);
            final var expected = new TargetSloObservationGeneration.Counts(
                    usage[0], usage[0], usage[1], usage[4], usage[2], usage[3], reservation);
            if (!expected.equals(generation.counts())
                    || !latest.get(generation.generation()).equals(generation.mutation())) {
                throw new IllegalStateException(
                        "Target SLO generation disagrees with complete actual/reserved sample/event/custody fold");
            }
        }
    }

    private static void latest(HashMap<Long, TargetQuotaMutation> values, long generation, TargetQuotaMutation at) {
        final var before = Objects.requireNonNull(values.get(generation), "SLO event lost original generation");
        if (Long.compareUnsigned(at.sequence(), before.sequence()) > 0) {
            values.put(generation, at);
        } else if (at.sequence() == before.sequence() && !at.equals(before)) {
            throw new IllegalStateException("SLO history changed one actual Source mutation");
        }
    }

    private record HistoryEntry(
            SystemMutation input,
            TargetQuotaMutation appliedAt,
            TargetSloObservationEventRecord finalEvent,
            TargetSloCustodyRecord custody) {}

    /** Every APPLIED type15 first must name exactly one immutable Final or custody event. */
    private static void requireEventHistory(
            List<TargetSloObservationGeneration> generations,
            List<TargetSloObservationSample> samples,
            List<TargetResultRecord> firsts,
            List<TargetSloObservationEventRecord> events,
            List<TargetSloCustodyRecord> custodies,
            HashMap<String, TargetSloWriterEpoch> writers) {
        final var bySample = new HashMap<String, TargetSloObservationSample>();
        final var byGeneration = new HashMap<Long, TargetSloObservationGeneration>();
        final var histories = new HashMap<String, TreeMap<Long, HistoryEntry>>();
        final var pending = new HashMap<String, HistoryEntry>();
        for (var generation : generations) {
            byGeneration.put(generation.generation(), generation);
        }
        for (var sample : samples) {
            bySample.put(Bytes.hex(sample.observation().sampleId()), sample);
        }
        final var rows = new java.util.ArrayList<HistoryEntry>();
        for (var event : events) {
            rows.add(new HistoryEntry(event.input(), event.appliedAt(), event, null));
        }
        for (var custody : custodies) {
            rows.add(new HistoryEntry(custody.input(), custody.appliedAt(), null, custody));
        }
        for (var row : rows) {
            final var sampleId = row.finalEvent() != null
                    ? row.finalEvent().body().sampleId()
                    : row.custody().sample().observation().sampleId();
            final var sample =
                    Objects.requireNonNull(bySample.get(Bytes.hex(sampleId)), "orphaned SLO Source event/custody");
            final var generation =
                    Objects.requireNonNull(byGeneration.get(sample.generation()), "SLO event original generation");
            requireEventWriter(
                    generation,
                    TargetSloObservationEventBody.decode(row.input().canonicalBody()),
                    row.input(),
                    row.appliedAt(),
                    writers);
            generation.policy().requireSource(row.appliedAt().source());
            row.appliedAt().requireAfter(sample.first().mutation());
            if (row.finalEvent() != null) {
                final var event = row.finalEvent();
                if (event.body().generation() != sample.generation()
                        || !Arrays.equals(event.body().bindingDigest(), sample.bindingDigest())) {
                    throw new IllegalStateException("SLO Final changed original sample binding");
                }
                event.appliedAt().requireAtOrBefore(sample.mutation());
                if (event.body().evidence() != null) {
                    event.body().evidence().requireObservation(sample, event.incoming());
                }
            } else if (row.custody().sample().generation() != sample.generation()
                    || !Arrays.equals(row.custody().sample().bindingDigest(), sample.bindingDigest())) {
                throw new IllegalStateException("SLO custody changed original sample binding");
            }
            if (pending.putIfAbsent(Bytes.hex(row.input().systemMutationId()), row) != null
                    || histories
                                    .computeIfAbsent(
                                            Bytes.hex(sampleId), ignored -> new TreeMap<>(Long::compareUnsigned))
                                    .putIfAbsent(row.appliedAt().sequence(), row)
                            != null) {
                throw new IllegalStateException("duplicate SLO Source history edge");
            }
        }
        for (var first : firsts) {
            if (first.kind() != TargetResultRecord.Kind.SYSTEM) {
                continue;
            }
            final var result = SystemMutationResult.decode(first.typedPayload());
            if (result.mutationType() != SystemMutationType.TARGET_SLO_OBSERVATION) {
                continue;
            }
            final var row = pending.remove(Bytes.hex(result.mutationId()));
            if (first.appliedFirst()) {
                if (row == null
                        || !first.mutation().equals(row.appliedAt())
                        || !Arrays.equals(
                                first.typedPayload(),
                                SystemMutationResult.from(
                                                row.input(),
                                                ApplyStatus.APPLIED,
                                                StableCode.OK,
                                                row.appliedAt().source().canonicalBytes())
                                        .encode())) {
                    throw new IllegalStateException(
                            "APPLIED SLO Source first lost its exact immutable Final/custody event");
                }
            } else if (row != null) {
                throw new IllegalStateException("rejected SLO event has an applied history edge");
            }
        }
        if (!pending.isEmpty()) {
            throw new IllegalStateException("SLO event lost original first result");
        }
        for (var sample : samples) {
            var folded = SloObservationOutbox.open(sample.observation().start());
            var latest = sample.first().mutation();
            var version = new TargetSloObservationSample(
                    sample.scope(),
                    sample.lineage(),
                    sample.generation(),
                    sample.enableId(),
                    sample.enabledAt(),
                    sample.policyDigest(),
                    sample.declaration(),
                    sample.first(),
                    folded,
                    latest,
                    sample.admission(),
                    sample.eligibility());
            final var versions = new HashMap<String, TargetSloObservationSample>();
            versions.put(Bytes.hex(version.digest()), version);
            final var history = histories.get(Bytes.hex(sample.observation().sampleId()));
            if (history != null) {
                for (var row : history.values()) {
                    if (row.finalEvent() != null) {
                        final var event = row.finalEvent();
                        if (!Arrays.equals(folded.recordDigest(), event.beforeDigest())) {
                            throw new IllegalStateException("SLO Source merge history has a missing predecessor");
                        }
                        event.appliedAt().requireAfter(latest);
                        event.requireIncomingFor(version);
                        folded = folded.mergeFinal(
                                event.incoming(),
                                sample.declaration().objective().direction());
                        if (!Arrays.equals(
                                folded.canonicalBytes(), event.after().canonicalBytes())) {
                            throw new IllegalStateException("SLO Source merge edge changes conservative v1 semantics");
                        }
                        latest = event.appliedAt();
                        version = new TargetSloObservationSample(
                                sample.scope(),
                                sample.lineage(),
                                sample.generation(),
                                sample.enableId(),
                                sample.enabledAt(),
                                sample.policyDigest(),
                                sample.declaration(),
                                sample.first(),
                                folded,
                                latest,
                                sample.admission(),
                                sample.eligibility());
                        if (versions.putIfAbsent(Bytes.hex(version.digest()), version) != null) {
                            throw new IllegalStateException("SLO Source merge repeated one full sample version");
                        }
                    } else {
                        final var exported = row.custody().sample();
                        final var known = versions.get(Bytes.hex(exported.digest()));
                        if (known == null || !Arrays.equals(known.canonicalBytes(), exported.canonicalBytes())) {
                            throw new IllegalStateException(
                                    "SLO custody lacks its exact earlier exported sample version");
                        }
                        exported.mutation().requireAtOrBefore(row.appliedAt());
                    }
                }
            }
            if (!latest.equals(sample.mutation())
                    || !Arrays.equals(version.canonicalBytes(), sample.canonicalBytes())) {
                throw new IllegalStateException("SLO sample lost complete original Source event history");
            }
        }
    }

    private static TargetSloWriterEpoch requirePolicyWriter(
            TargetSloObservationGeneration generation, HashMap<String, TargetSloWriterEpoch> writers) {
        final var policy = generation.policy();
        if (!policy.requiresWriterEpoch()) {
            return null;
        }
        final var reference = policy.requiredWriterEpoch();
        final var writer = Objects.requireNonNull(
                writers.get(Bytes.hex(reference.key(policy.scope()))),
                "Policy2 lost its actual original Native62 writer epoch");
        final var contract = writer.contract();
        if (!writer.reference().equals(reference)
                || !contract.scope().equals(policy.scope())
                || !Arrays.equals(contract.lineage(), policy.lineage())) {
            throw new IllegalStateException("Policy2 changed its exact original applied writer contract");
        }
        contract.requireSource(generation.enable().appliedAt().source());
        generation.enable().appliedAt().requireAfter(writer.register().appliedAt());
        return writer;
    }

    private static void requireEventWriter(
            TargetSloObservationGeneration generation,
            TargetSloObservationEventBody body,
            SystemMutation input,
            TargetQuotaMutation appliedAt,
            HashMap<String, TargetSloWriterEpoch> writers) {
        final var policy = generation.policy();
        if (body.writerEpoch() == null) {
            if (policy.requiresWriterEpoch()
                    || !Arrays.equals(input.authorIdentity(), policy.service().canonicalBytes())
                    || input.signingKeyVersion() != policy.keyVersion()) {
                throw new IllegalStateException("legacy SLO Source event changed its original Policy1 writer");
            }
            return;
        }
        final var writer = Objects.requireNonNull(
                writers.get(Bytes.hex(body.writerEpoch().key(policy.scope()))),
                "SLO body5 lost its actual original Native62 writer epoch");
        final var contract = writer.contract();
        if (!writer.reference().equals(body.writerEpoch())
                || !policy.scope().equals(contract.scope())
                || !Arrays.equals(policy.lineage(), contract.lineage())
                || !body.scope().equals(contract.scope())
                || !Arrays.equals(body.lineage(), contract.lineage())
                || !Arrays.equals(input.authorIdentity(), contract.service().canonicalBytes())
                || input.signingKeyVersion() != contract.keyVersion()
                || input.retryUntilEpochMs() > contract.maximumRetryUntil()
                || input.encodeFrame().length > contract.limits().maximumFrameBytes()
                || appliedAt.source().brokerPersistenceTimeEpochMs() > input.retryUntilEpochMs()) {
            throw new IllegalStateException(
                    "SLO body5 changed its original registered writer or finite frame/deadline bounds");
        }
        contract.requireSource(appliedAt.source());
        appliedAt.requireAfter(writer.register().appliedAt());
        if (writer.closed()) {
            final var fenceAt = writer.close().body().request().fenceAt();
            if (appliedAt.source().compareTo(fenceAt.source()) >= 0) {
                throw new IllegalStateException("SLO body5 follows its original writer's actual closure Fence");
            }
            appliedAt.requireAtOrBefore(fenceAt);
        }
    }

    /** Legacy Admission-only binding correlation is not an ALL_ACCEPTED population oracle. */
    public static void requireAdmissionInputs(
            List<TargetSloObservationGeneration> generations,
            List<TargetSloObservationSample> samples,
            List<TargetCheckpointDependencies.AdmissionImage> inputs) {
        final var pending = new HashMap<String, TargetSloObservationSample>();
        for (var sample : samples) {
            if (sample.admission() != null) {
                pending.put(Bytes.hex(sample.admission().systemMutationId()), sample);
            }
        }
        if (pending.isEmpty()) {
            return;
        }
        for (var input : inputs) {
            final var sample = pending.remove(Bytes.hex(input.image().systemMutationId()));
            if (sample != null
                    && (!Arrays.equals(
                                    sample.admission().canonicalEnvelope(),
                                    input.image().canonicalEnvelope())
                            || !Arrays.equals(
                                    sample.first().mutation().source().canonicalBytes(),
                                    input.source().canonicalBytes()))) {
                throw new IllegalStateException("legacy Admission binding changed exact original input");
            }
        }
        if (!pending.isEmpty()) {
            throw new IllegalStateException("legacy due sample lost its original Admission image");
        }
    }

    /**
     * Current durable Message generations positively require their original ordinary eligibility Start, even without
     * Admission.
     */
    public static void requireEligibilityMessages(
            List<TargetSloObservationGeneration> generations,
            List<TargetSloObservationSample> samples,
            List<TargetMessageRecord> messages) {
        final var controls = new TreeMap<SourcePosition, TargetSloObservationGeneration>();
        for (var generation : generations) {
            controls.put(generation.enable().appliedAt().source(), generation);
        }
        final var births = new HashMap<String, TargetSloObservationSample>();
        for (var sample : samples) {
            if (sample.eligibility() != null) {
                final var locator = sample.eligibility().locator();
                final var token =
                        Bytes.hex(Bytes.concat(locator.messageId().bytes(), Bytes.u32beBits(locator.generation())));
                if (births.putIfAbsent(token, sample) != null) {
                    throw new IllegalStateException("duplicate ordinary eligibility birth");
                }
            }
        }
        for (var message : messages) {
            if (message.nativeDeliveryPolicy() != NativeDeliveryPolicy.FORBID) {
                continue;
            }
            final var selected = controls.lowerEntry(message.scheduleSource());
            if (selected == null) {
                continue;
            }
            final var generation = selected.getValue();
            if (generation.stop() != null
                    && message.scheduleSource()
                                    .compareTo(generation.stop().appliedAt().source())
                            >= 0) {
                continue;
            }
            final var declaration = generation.policy().declarations().stream()
                    .filter(d -> d.objective().name() == SloObjectiveName.DUE_ADMISSION_LAG
                            && d.path() == SloPath.ORDINARY_MANAGED)
                    .findFirst()
                    .orElse(null);
            if (declaration == null) {
                continue;
            }
            final var token = Bytes.hex(Bytes.concat(
                    message.locator().messageId().bytes(),
                    Bytes.u32beBits(message.locator().generation())));
            final var sample = births.get(token);
            if (sample == null
                    || sample.generation() != generation.generation()
                    || !sample.eligibility().locator().equals(message.locator())
                    || sample.eligibility().deliverAt() != message.deliverAtEpochMs()
                    || sample.eligibility().expireAt() != message.expireAtEpochMs()
                    || !Arrays.equals(
                            sample.eligibility().source().canonicalBytes(),
                            message.scheduleSource().canonicalBytes())
                    || !Arrays.equals(sample.declaration().canonicalBytes(), declaration.canonicalBytes())) {
                throw new IllegalStateException(
                        ("durable ordinary eligibility is missing its original ALL_ACCEPTED Start, "
                                + "independently of Admission"));
            }
        }
    }
}
