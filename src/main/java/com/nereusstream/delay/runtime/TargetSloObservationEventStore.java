package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SloFinalOutcome;
import com.nereusstream.delay.protocol.SloObservationOutbox;
import com.nereusstream.delay.protocol.SloSampleFinal;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetProtocolActivationRecord;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSloCustodyReceipt;
import com.nereusstream.delay.protocol.TargetSloCustodyRecord;
import com.nereusstream.delay.protocol.TargetSloFinalEvidence;
import com.nereusstream.delay.protocol.TargetSloObservationEventBody;
import com.nereusstream.delay.protocol.TargetSloObservationEventRecord;
import com.nereusstream.delay.protocol.TargetSloObservationGeneration;
import com.nereusstream.delay.protocol.TargetSloObservationSample;
import com.nereusstream.delay.protocol.TargetSloWriterEpoch;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Original ordered Source apply for a pure observation; no Claim, Attempt, Message or publication transition. */
public final class TargetSloObservationEventStore {
    @FunctionalInterface
    public interface Keys {
        PublicKey resolve(long version, SourcePosition source);
    }

    public interface ObservationAuthority {
        default TargetScopeFeeAuthority scopeFees() {
            return TargetScopeFeeAuthority.UNAVAILABLE;
        }
        /**
         * Original semantic endpoint/time/history and real retained budget qualification; already-held local checks.
         */
        void requireAuthorized(
                TargetSloObservationGeneration generation,
                TargetSloObservationSample sample,
                SystemMutation input,
                SourcePosition source);
        /**
         * Original protected recovery has reconciled old immutable frames/UNKNOWN writes and independently
         * confirmed the specified irrecoverable gap. Includes actual discovery UTC, history and resource custody.
         * A temporary read failure, incomplete recovery or unresolved old writer never grants this authority.
         */
        default void requireRecoveryGap(
                TargetSloObservationGeneration generation,
                TargetSloObservationSample sample,
                SystemMutation input,
                SourcePosition source) {
            throw new IllegalStateException("original protected SLO recovery-gap qualification unavailable");
        }

        void requireCurrent();
        /** Held independent collector contract/key at this exact Source; resolve remotely during preparation only. */
        default PublicKey collectorKey(TargetSloCustodyReceipt receipt, SourcePosition source) {
            throw new IllegalStateException("original independent collector custody key/contract unavailable");
        }
    }

    public record Authority(Keys keys, ObservationAuthority observation) {
        public Authority {
            Objects.requireNonNull(keys);
            Objects.requireNonNull(observation);
        }
    }

    public static final class Prepared {
        private final TargetSloObservationEventStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;
        private final Runnable qualification;

        private Prepared(
                TargetSloObservationEventStore owner,
                TargetStoreBackend.Prepared batch,
                SystemMutationResult result,
                Runnable qualification) {
            this.owner = owner;
            this.batch = batch;
            this.result = result;
            this.qualification = qualification;
        }
    }

    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int counters, domains;

    public TargetSloObservationEventStore(
            TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage, int counters, int domains) {
        this.backend = Objects.requireNonNull(backend);
        this.scope = Objects.requireNonNull(scope);
        this.lineage = Bytes.copy(lineage);
        Bytes.requireLength(lineage, 16, "SLO event Source lineage");
        if (scope.target() != null || counters < 2 || domains < 1 || domains > TargetQueueState.MAX_DOMAIN_SLOTS) {
            throw new IllegalArgumentException("bounded original SLO Source scope required");
        }
        this.counters = counters;
        this.domains = domains;
    }

    public Prepared prepareFirst(
            BoundedReadBudget budget, SystemMutation input, SourcePosition source, Authority authority) {
        Objects.requireNonNull(authority);
        TargetSourcePosition.requireBounded(source);
        final var body = TargetSloObservationEventBody.decode(input.canonicalBody());
        if (input.type() != SystemMutationType.TARGET_SLO_OBSERVATION
                || !scope.equals(body.scope())
                || !Arrays.equals(lineage, body.lineage())
                || !input.shardId().equals(scope.shard())
                || !source.shardId().equals(scope.shard())) {
            throw new IllegalArgumentException("SLO event changed original Source scope");
        }
        final var result = new SystemMutationResult[1];
        final boolean[] applied = {false};
        final var batch = backend.prepare(budget, reader -> {
            if (reader.source() == null || source.compareTo(reader.source()) <= 0) {
                throw new IllegalStateException("SLO event requires advancing Source");
            }
            final var stamp = new TargetQuotaMutation(
                    TargetQuotaMutation.increment(reader.sourceSequence()),
                    source,
                    Bytes.sha256(input.canonicalEnvelope()));
            final var firstKey = TargetSloObservationStore.firstKey(input.systemMutationId());
            final var positionKey = TargetSloObservationStore.positionKey(source);
            absent(reader, ColumnFamily.DEDUPE, firstKey);
            absent(reader, ColumnFamily.DEDUPE, positionKey);
            final var root = root(reader);
            final var edits = new ArrayList<TargetStoreBackend.Edit>();
            final var generation =
                    TargetSloObservationVerifier.readGeneration(reader, scope, lineage, body.generation());
            final var sampleKey = TargetSloObservationSample.key(body.sampleId());
            final var sampleRaw = reader.get(ColumnFamily.META, sampleKey);
            final var sample = sampleRaw == null
                    ? null
                    : TargetSloObservationSample.decodeForStore(
                            sampleKey,
                            TargetValueEnvelope.decode(sampleRaw, TargetSloObservationSample.VALUE_TYPE)
                                    .payload(),
                            scope,
                            lineage);
            final StableCode rejection = verify(reader, body, input, source, generation, sample, authority);
            if (rejection == null) {
                final var before = generation.counts();
                final var reserved = TargetSloObservationReservations.requireReserved(generation);
                final boolean wasCustodied =
                        TargetSloObservationReservations.currentCustodied(reader, List.of(), scope, lineage, sample);
                final TargetSloObservationGeneration.Counts counts;
                if (body.isCustody()) {
                    final var record = new TargetSloCustodyRecord(input, stamp);
                    final boolean nowCustodied =
                            wasCustodied || Arrays.equals(body.exportedSample().digest(), sample.digest());
                    final var afterReserve = reserved.minus(
                                    TargetSloObservationReservations.forSample(generation, sample, wasCustodied))
                            .plus(TargetSloObservationReservations.forSample(generation, sample, nowCustodied));
                    counts = new TargetSloObservationGeneration.Counts(
                            before.issued(),
                            before.outboxRecords(),
                            before.outboxBytes(),
                            Math.addExact(before.custodied(), nowCustodied && !wasCustodied ? 1 : 0),
                            Math.addExact(before.eventRecords(), 1),
                            Math.addExact(before.eventBytes(), record.storedBytes()),
                            afterReserve);
                    if (record.storedBytes() > generation.policy().limits().maximumRecordBytes()) {
                        throw new IllegalStateException("custody record exceeds frozen single-record cap");
                    }
                    absent(reader, ColumnFamily.META, record.key());
                    edits.add(reader.replace(
                            ColumnFamily.META,
                            record.key(),
                            TargetSloCustodyRecord.VALUE_TYPE,
                            record.canonicalBytes()));
                } else {
                    final var incoming = body.incomingFor(sample);
                    final var merged = sample.observation()
                            .mergeFinal(
                                    incoming, sample.declaration().objective().direction());
                    final var next = new TargetSloObservationSample(
                            scope,
                            lineage,
                            sample.generation(),
                            sample.enableId(),
                            sample.enabledAt(),
                            sample.policyDigest(),
                            sample.declaration(),
                            sample.first(),
                            merged,
                            stamp,
                            sample.admission(),
                            sample.eligibility());
                    final var event = body.isSourceAssigned()
                            ? new TargetSloObservationEventRecord(
                                    input, stamp, sample.observation().recordDigest(), merged, incoming)
                            : new TargetSloObservationEventRecord(
                                    input, stamp, sample.observation().recordDigest(), merged);
                    final var afterReserve = reserved.minus(
                                    TargetSloObservationReservations.forSample(generation, sample, wasCustodied))
                            .plus(TargetSloObservationReservations.forSample(generation, next, false));
                    counts = new TargetSloObservationGeneration.Counts(
                            before.issued(),
                            before.outboxRecords(),
                            Math.addExact(
                                    before.outboxBytes(), Math.subtractExact(next.storedBytes(), sample.storedBytes())),
                            Math.subtractExact(before.custodied(), wasCustodied ? 1 : 0),
                            Math.addExact(before.eventRecords(), 1),
                            Math.addExact(before.eventBytes(), event.storedBytes()),
                            afterReserve);
                    final var maximumRecord = generation.policy().limits().maximumRecordBytes();
                    if (next.keys().stream()
                                    .anyMatch(k -> k.length
                                                    + (long) TargetValueEnvelope.encode(
                                                                    TargetSloObservationSample.VALUE_TYPE,
                                                                    next.canonicalBytes())
                                                            .length
                                            > maximumRecord)
                            || event.storedBytes() > maximumRecord) {
                        throw new IllegalStateException("SLO event record exceeds original complete record budget");
                    }
                    for (var key : next.keys()) {
                        edits.add(reader.replace(
                                ColumnFamily.META, key, TargetSloObservationSample.VALUE_TYPE, next.canonicalBytes()));
                    }
                    absent(reader, ColumnFamily.META, event.key());
                    edits.add(reader.replace(
                            ColumnFamily.META,
                            event.key(),
                            TargetSloObservationEventRecord.VALUE_TYPE,
                            event.canonicalBytes()));
                }
                final var after = generation.withCounts(counts, stamp);
                edits.add(reader.replace(
                        ColumnFamily.META,
                        after.key(),
                        TargetSloObservationGeneration.VALUE_TYPE,
                        after.canonicalBytes()));
                authority.observation().requireCurrent();
                applied[0] = true;
            }
            result[0] = SystemMutationResult.from(
                    input,
                    rejection == null ? ApplyStatus.APPLIED : ApplyStatus.REJECTED,
                    rejection == null ? StableCode.OK : rejection,
                    source.canonicalBytes());
            final TargetResultRecord.CreationAuthority results = (record, first) -> {
                record.requireOwner(root);
                if (!record.mutation().equals(stamp) || record.allocation() != null) {
                    throw new IllegalStateException("SLO event result changed original Root/Source");
                }
                absent(reader, ColumnFamily.DEDUPE, record.key());
                if (record.kind() == TargetResultRecord.Kind.SYSTEM) {
                    if (first != null
                            || !Arrays.equals(record.key(), firstKey)
                            || !Arrays.equals(record.typedPayload(), result[0].encode())) {
                        throw new IllegalStateException("SLO event first changed decision");
                    }
                } else if (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                    if (first == null || !Arrays.equals(record.key(), positionKey)) {
                        throw new IllegalStateException("SLO event POSITION lost first");
                    }
                    record.requireFirst(first);
                } else {
                    throw new IllegalStateException("SLO observation cannot create another result kind");
                }
            };
            final var first = TargetResultRecord.system(root, result[0], stamp, null, results);
            final var position = TargetResultRecord.position(root, first, stamp, results);
            for (var record : List.of(first, position)) {
                edits.add(reader.replace(
                        ColumnFamily.DEDUPE, record.key(), TargetResultRecord.VALUE_TYPE, record.canonicalBytes()));
            }
            return new TargetSourceAccounting(scope, lineage, source, stamp.mutationDigest(), counters, 1, domains)
                    .withScopeSource(input, authority.observation().scopeFees())
                    .assemble(reader, edits);
        });
        return new Prepared(this, batch, result[0], applied[0] ? authority.observation()::requireCurrent : () -> {});
    }

    private StableCode verify(
            TargetStoreBackend.Reader reader,
            TargetSloObservationEventBody body,
            SystemMutation input,
            SourcePosition source,
            TargetSloObservationGeneration generation,
            TargetSloObservationSample sample,
            Authority authority) {
        if (reader.closedIngressDeadlineThrough() >= input.retryUntilEpochMs()
                || source.brokerPersistenceTimeEpochMs() > input.retryUntilEpochMs()) {
            return StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED;
        }
        if (generation == null
                || sample == null
                || sample.generation() != body.generation()
                || !Arrays.equals(sample.bindingDigest(), body.bindingDigest())) {
            return StableCode.STALE_SYSTEM_MUTATION;
        }
        final var activation = TargetProtocolActivationStore.applied(
                reader, scope, lineage, generation.enable().body().tuple());
        final var eventActivation = TargetProtocolActivationStore.applied(reader, scope, lineage, body.tuple());
        if (activation == null
                || activation.body().payload().descriptor().schemaVersion() < 15
                || eventActivation == null
                || eventActivation.body().payload().descriptor().schemaVersion() < 15) {
            return StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION;
        }
        generation.policy().requireSource(source);
        sample.mutation().requireAtOrBefore(reader.aggregate().mutation());
        if (body.writerEpoch() == null) {
            if (generation.policy().requiresWriterEpoch()
                    || !Arrays.equals(
                            input.authorIdentity(),
                            generation.policy().service().canonicalBytes())
                    || input.signingKeyVersion() != generation.policy().keyVersion()) {
                return StableCode.UNAUTHORIZED_SYSTEM_MUTATION;
            }
        } else {
            final var epochKey = body.writerEpoch().key(scope);
            final var epochRaw = reader.get(ColumnFamily.META, epochKey);
            if (epochRaw == null) {
                return StableCode.UNAUTHORIZED_SYSTEM_MUTATION;
            }
            final var epoch = TargetSloWriterEpoch.decodeForStore(
                    epochKey,
                    TargetValueEnvelope.decode(epochRaw, TargetSloWriterEpoch.VALUE_TYPE)
                            .payload(),
                    scope,
                    lineage);
            if (!epoch.reference().equals(body.writerEpoch())) {
                return StableCode.UNAUTHORIZED_SYSTEM_MUTATION;
            }
            if (epoch.closed()) {
                return StableCode.STALE_SYSTEM_MUTATION;
            }
            final var contract = epoch.contract();
            if (!Arrays.equals(input.authorIdentity(), contract.service().canonicalBytes())
                    || input.signingKeyVersion() != contract.keyVersion()
                    || input.retryUntilEpochMs() > contract.maximumRetryUntil()
                    || input.encodeFrame().length > contract.limits().maximumFrameBytes()) {
                return StableCode.UNAUTHORIZED_SYSTEM_MUTATION;
            }
            final var root = new TargetQuotaIdentity(
                    TargetQuotaIdentity.Kind.SHARD,
                    scope.shard(),
                    reader.aggregate().accountingIncarnation(),
                    null,
                    null);
            requireWriterOrigin(
                    reader,
                    List.of(),
                    scope,
                    lineage,
                    root,
                    generation,
                    body,
                    input,
                    source,
                    reader.aggregate().mutation());
        }
        final var key = authority.keys().resolve(Integer.toUnsignedLong(input.signingKeyVersion()), source);
        if (key == null || !input.verifySignature(key)) {
            return StableCode.UNAUTHORIZED_SYSTEM_MUTATION;
        }
        if (body.isCustody()) {
            body.custody().requireSample(generation, body.exportedSample());
            requireStoredVersion(
                    reader,
                    List.of(),
                    scope,
                    lineage,
                    sample,
                    body.exportedSample(),
                    reader.aggregate().mutation());
            final var collectorKey = authority.observation().collectorKey(body.custody(), source);
            if (collectorKey == null || !body.custody().verifySignature(collectorKey)) {
                return StableCode.UNAUTHORIZED_SYSTEM_MUTATION;
            }
            if (reader.get(
                            ColumnFamily.META,
                            TargetSloCustodyRecord.key(
                                    body.sampleId(), body.exportedSample().digest()))
                    != null) {
                return StableCode.STALE_SYSTEM_MUTATION;
            }
        } else {
            if (body.evidence() != null
                    && body.evidence().isRecoveryGap()
                    && sample.observation().finalObservation() != null) {
                return StableCode.STALE_SYSTEM_MUTATION;
            }
            final boolean timeout =
                    body.evidence() != null && body.evidence().kind() == TargetSloFinalEvidence.Kind.TIMEOUT
                            || body.finalObservation() != null
                                    && body.finalObservation().outcome() == SloFinalOutcome.BAD_TIMEOUT;
            if (timeout) {
                final var candidate = TargetSloTimeoutCandidates.inspect(
                        reader, scope, lineage, reader.aggregate().mutation(), sample);
                switch (candidate) {
                    case CURRENT_FINAL, KNOWN_COMMAND_SYNC, KNOWN_ADMISSION_SYNC -> {
                        return StableCode.STALE_SYSTEM_MUTATION;
                    }
                    case UNKNOWN_REQUIRED_HISTORY ->
                        throw new IllegalStateException(
                                "SLO timeout lacks the original same-generation no-Admission lifecycle/history");
                    case NO_KNOWN_ADMISSION_AT_CUT -> {
                        /* Original time/history authority below still qualifies this actual Source input. */
                    }
                }
            }
            final var incoming = body.incomingFor(sample);
            incoming.validateAgainst(sample.observation().start());
            final var prior = sample.observation().finalObservation();
            if (!body.isSourceAssigned()
                    && prior != null
                    && Long.compareUnsigned(incoming.observationRevision(), prior.observationRevision()) <= 0
                    && !Arrays.equals(incoming.canonicalBytes(), prior.canonicalBytes())) {
                return StableCode.STALE_SYSTEM_MUTATION;
            }
            requireEvidenceOrigin(
                    reader,
                    List.of(),
                    scope,
                    lineage,
                    sample,
                    body,
                    incoming,
                    reader.aggregate().mutation());
        }
        authority.observation().requireCurrent();
        if (body.evidence() != null && body.evidence().isRecoveryGap()) {
            authority.observation().requireRecoveryGap(generation, sample, input, source);
        } else {
            authority
                    .observation()
                    .requireAuthorized(generation, body.isCustody() ? body.exportedSample() : sample, input, source);
        }
        authority.observation().requireCurrent();
        return null;
    }

    public SystemMutationResult commit(Prepared prepared, TargetStoreBackend.CommitAuthority original) {
        if (prepared.owner != this) {
            throw new IllegalArgumentException("foreign SLO Source plan");
        }
        prepared.qualification.run();
        backend.commit(prepared.batch, (metadata, actualScope, mutation) -> {
            final var held = original.acquire(metadata, actualScope, mutation);
            return new TargetStoreBackend.CommitGuard() {
                public void requireCurrent() {
                    prepared.qualification.run();
                    held.requireCurrent();
                    prepared.qualification.run();
                }

                public void close() {
                    held.close();
                }
            };
        });
        return prepared.result;
    }

    private TargetQuotaIncarnation root(TargetStoreBackend.Reader reader) {
        final var identity = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.SHARD,
                scope.shard(),
                reader.aggregate().accountingIncarnation(),
                null,
                null);
        final var key = identity.key();
        key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var value = TargetQuotaIncarnation.decodeForStore(
                key,
                TargetValueEnvelope.decode(
                                Objects.requireNonNull(reader.get(ColumnFamily.META, key)),
                                TargetQuotaIncarnation.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope());
        if (!Arrays.equals(value.recoveryLineage(), lineage)) {
            throw new IllegalStateException("SLO Source Root changed lineage");
        }
        return value;
    }

    private static void absent(TargetStoreBackend.Reader reader, ColumnFamily family, byte[] key) {
        if (reader.get(family, key) != null) {
            throw new IllegalStateException("SLO first Source key already exists");
        }
    }

    public static void requireOrigin(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaIdentity root,
            TargetSloObservationEventRecord event,
            TargetQuotaMutation frontier) {
        event.appliedAt().requireAtOrBefore(frontier);
        final var activationKey =
                TargetProtocolActivationRecord.key(event.body().tuple());
        final var activation = TargetProtocolActivationRecord.decodeForStore(
                activationKey,
                payload(view, overlay, ColumnFamily.META, activationKey, TargetProtocolActivationRecord.VALUE_TYPE),
                scope,
                lineage);
        if (activation.body().payload().descriptor().schemaVersion() < 15
                || activation.appliedAt().source().compareTo(event.appliedAt().source()) >= 0) {
            throw new IllegalStateException("SLO event lost its prior independent reader activation");
        }
        TargetProtocolActivationStore.requireApplied(view, overlay, scope, lineage, root, activation, frontier);
        final var first = result(
                view, overlay, TargetSloObservationStore.firstKey(event.input().systemMutationId()));
        if (!first.appliedFirst()
                || first.kind() != TargetResultRecord.Kind.SYSTEM
                || !first.mutation().equals(event.appliedAt())
                || !first.primaryIdentity().equals(root)
                || !Arrays.equals(first.tenantScope(), scope.tenantScope())
                || !Arrays.equals(first.recoveryLineage(), lineage)
                || !Arrays.equals(
                        first.typedPayload(),
                        SystemMutationResult.from(
                                        event.input(),
                                        ApplyStatus.APPLIED,
                                        StableCode.OK,
                                        event.appliedAt().source().canonicalBytes())
                                .encode())) {
            throw new IllegalStateException("SLO history lost actual APPLIED first");
        }
        result(
                        view,
                        overlay,
                        TargetSloObservationStore.positionKey(event.appliedAt().source()))
                .requireFirst(first);
        final var key = TargetSloObservationSample.key(event.body().sampleId());
        final var sample = TargetSloObservationSample.decodeForStore(
                key,
                payload(view, overlay, ColumnFamily.META, key, TargetSloObservationSample.VALUE_TYPE),
                scope,
                lineage);
        if (!Arrays.equals(sample.bindingDigest(), event.body().bindingDigest())
                || sample.generation() != event.body().generation()) {
            throw new IllegalStateException("SLO Source history changed original sample binding");
        }
        event.appliedAt().requireAfter(sample.first().mutation());
        event.appliedAt().requireAtOrBefore(sample.mutation());
        final var generationKey = TargetSloObservationGeneration.key(scope, sample.generation());
        final var generation = TargetSloObservationGeneration.decodeForStore(
                generationKey,
                payload(view, overlay, ColumnFamily.META, generationKey, TargetSloObservationGeneration.VALUE_TYPE),
                scope,
                lineage);
        generation.policy().requireSource(event.appliedAt().source());
        requireWriterOrigin(
                view,
                overlay,
                scope,
                lineage,
                root,
                generation,
                event.body(),
                event.input(),
                event.appliedAt().source(),
                frontier);
        event.incoming().validateAgainst(sample.observation().start());
        requireEvidenceOrigin(view, overlay, scope, lineage, sample, event.body(), event.incoming(), event.appliedAt());
    }
    /** Validates an older exact export without mistaking it for today's mutable projection. */
    public static void requireStoredVersion(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetSloObservationSample current,
            TargetSloObservationSample exported,
            TargetQuotaMutation frontier) {
        if (!Arrays.equals(current.bindingDigest(), exported.bindingDigest())
                || !scope.equals(exported.scope())
                || !Arrays.equals(lineage, exported.lineage())) {
            throw new IllegalStateException("custody export changed immutable original binding");
        }
        exported.mutation().requireAtOrBefore(current.mutation());
        exported.mutation().requireAtOrBefore(frontier);
        if (exported.observation().finalObservation() == null) {
            if (!exported.mutation().equals(exported.first().mutation())
                    || !Arrays.equals(
                            exported.observation().canonicalBytes(),
                            SloObservationOutbox.open(current.observation().start())
                                    .canonicalBytes())) {
                throw new IllegalStateException("custody invented an original open-Start version");
            }
        } else {
            final var key = TargetSloObservationEventRecord.key(
                    exported.observation().sampleId(), exported.mutation().sequence());
            final var event = TargetSloObservationEventRecord.decodeForStore(
                    key,
                    payload(view, overlay, ColumnFamily.META, key, TargetSloObservationEventRecord.VALUE_TYPE),
                    scope,
                    lineage);
            requireOrigin(
                    view, overlay, scope, lineage, birthRoot(view, overlay, scope, lineage, current), event, frontier);
            if (!event.appliedAt().equals(exported.mutation())
                    || !Arrays.equals(
                            event.after().canonicalBytes(),
                            exported.observation().canonicalBytes())) {
                throw new IllegalStateException("custody export lost its exact original Final Source version");
            }
        }
    }

    public static void requireCustodyOrigin(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaIdentity root,
            TargetSloCustodyRecord record,
            TargetQuotaMutation frontier) {
        record.appliedAt().requireAtOrBefore(frontier);
        final var body = record.body();
        final var activationKey = TargetProtocolActivationRecord.key(body.tuple());
        final var activation = TargetProtocolActivationRecord.decodeForStore(
                activationKey,
                payload(view, overlay, ColumnFamily.META, activationKey, TargetProtocolActivationRecord.VALUE_TYPE),
                scope,
                lineage);
        if (activation.body().payload().descriptor().schemaVersion() < 15
                || activation.appliedAt().source().compareTo(record.appliedAt().source()) >= 0) {
            throw new IllegalStateException("custody lacks prior independent reader activation");
        }
        TargetProtocolActivationStore.requireApplied(view, overlay, scope, lineage, root, activation, frontier);
        final var first = result(
                view, overlay, TargetSloObservationStore.firstKey(record.input().systemMutationId()));
        if (!first.appliedFirst()
                || first.kind() != TargetResultRecord.Kind.SYSTEM
                || !root.equals(first.primaryIdentity())
                || !Arrays.equals(first.tenantScope(), scope.tenantScope())
                || !Arrays.equals(first.recoveryLineage(), lineage)
                || !first.mutation().equals(record.appliedAt())
                || !Arrays.equals(
                        first.typedPayload(),
                        SystemMutationResult.from(
                                        record.input(),
                                        ApplyStatus.APPLIED,
                                        StableCode.OK,
                                        record.appliedAt().source().canonicalBytes())
                                .encode())) {
            throw new IllegalStateException("custody lost actual original APPLIED first");
        }
        result(
                        view,
                        overlay,
                        TargetSloObservationStore.positionKey(record.appliedAt().source()))
                .requireFirst(first);
        final var sampleKey = TargetSloObservationSample.key(body.sampleId());
        final var current = TargetSloObservationSample.decodeForStore(
                sampleKey,
                payload(view, overlay, ColumnFamily.META, sampleKey, TargetSloObservationSample.VALUE_TYPE),
                scope,
                lineage);
        requireStoredVersion(view, overlay, scope, lineage, current, record.sample(), record.appliedAt());
        final var generationKey = TargetSloObservationGeneration.key(scope, body.generation());
        final var generation = TargetSloObservationGeneration.decodeForStore(
                generationKey,
                payload(view, overlay, ColumnFamily.META, generationKey, TargetSloObservationGeneration.VALUE_TYPE),
                scope,
                lineage);
        record.receipt().requireSample(generation, record.sample());
        requireWriterOrigin(
                view,
                overlay,
                scope,
                lineage,
                root,
                generation,
                body,
                record.input(),
                record.appliedAt().source(),
                frontier);
    }

    /** Original epoch scope, actual REGISTER/Fence history and immutable per-frame bounds; not key authority. */
    public static TargetSloWriterEpoch requireWriterOrigin(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaIdentity root,
            TargetSloObservationGeneration generation,
            TargetSloObservationEventBody body,
            SystemMutation input,
            SourcePosition source,
            TargetQuotaMutation frontier) {
        if (body.writerEpoch() == null) {
            if (generation.policy().requiresWriterEpoch()
                    || !Arrays.equals(
                            input.authorIdentity(),
                            generation.policy().service().canonicalBytes())
                    || input.signingKeyVersion() != generation.policy().keyVersion()) {
                throw new IllegalStateException("SLO event lost its mandatory original writer contract");
            }
            return null;
        }
        final var reference = body.writerEpoch();
        final var key = reference.key(scope);
        final var epoch = TargetSloWriterEpoch.decodeForStore(
                key, payload(view, overlay, ColumnFamily.META, key, TargetSloWriterEpoch.VALUE_TYPE), scope, lineage);
        final var contract = epoch.contract();
        if (!Arrays.equals(epoch.contractDigest(), reference.contractDigest())
                || !Arrays.equals(input.authorIdentity(), contract.service().canonicalBytes())
                || input.signingKeyVersion() != contract.keyVersion()
                || input.retryUntilEpochMs() > contract.maximumRetryUntil()
                || input.encodeFrame().length > contract.limits().maximumFrameBytes()
                || epoch.register().appliedAt().source().compareTo(source) >= 0
                || epoch.closed()
                        && epoch.close().body().request().fenceAt().source().compareTo(source) <= 0) {
            throw new IllegalStateException(
                    "SLO event changed its original registered writer, deadline or Fence range");
        }
        contract.requireSource(source);
        TargetSloWriterEpochStore.requireOrigin(view, overlay, scope, lineage, root, epoch, frontier);
        return epoch;
    }

    public static void requireEvidenceOrigin(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetSloObservationSample sample,
            TargetSloObservationEventBody body,
            SloSampleFinal incoming,
            TargetQuotaMutation frontier) {
        final var evidence = body.evidence();
        if (evidence == null) {
            return;
        }
        evidence.requireObservation(sample, incoming);
        evidence.inspectedAt().requireAtOrBefore(frontier);
        final var cut = result(
                view,
                overlay,
                TargetSloObservationStore.positionKey(evidence.inspectedAt().source()));
        final var root = birthRoot(view, overlay, scope, lineage, sample);
        if (!cut.mutation().equals(evidence.inspectedAt())
                || !Arrays.equals(cut.tenantScope(), scope.tenantScope())
                || !Arrays.equals(cut.recoveryLineage(), lineage)
                || !cut.primaryIdentity().equals(root)) {
            throw new IllegalStateException("SLO endpoint observation changed actual protected inspected Source cut");
        }
        if (evidence.isRecoveryGap()) {
            final var gap = evidence.gap();
            final var known = result(view, overlay, gap.knownFirst().key());
            final var position = result(view, overlay, gap.knownPosition().key());
            if (!Arrays.equals(known.canonicalBytes(), gap.knownFirst().canonicalBytes())
                    || !Arrays.equals(
                            position.canonicalBytes(), gap.knownPosition().canonicalBytes())) {
                throw new IllegalStateException("SLO recovery gap changed the actual known sync first/POSITION");
            }
        }
        if (evidence.admission() != null) {
            final var first = result(
                    view,
                    overlay,
                    TargetSloObservationStore.firstKey(evidence.admission().systemMutationId()));
            if (!first.appliedFirst()
                    || first.kind() != TargetResultRecord.Kind.SYSTEM
                    || !first.mutation().equals(evidence.admissionAt())
                    || !first.primaryIdentity().equals(root)
                    || !Arrays.equals(first.tenantScope(), scope.tenantScope())
                    || !Arrays.equals(first.recoveryLineage(), lineage)
                    || !Arrays.equals(
                            first.typedPayload(),
                            SystemMutationResult.from(
                                            evidence.admission(),
                                            ApplyStatus.APPLIED,
                                            StableCode.OK,
                                            evidence.admissionAt().source().canonicalBytes())
                                    .encode())) {
                throw new IllegalStateException("SLO due success lacks exact original sync first");
            }
            result(
                            view,
                            overlay,
                            TargetSloObservationStore.positionKey(
                                    evidence.admissionAt().source()))
                    .requireFirst(first);
        }
    }

    private static TargetResultRecord result(
            TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay, byte[] key) {
        final var value = TargetResultRecord.decode(
                payload(view, overlay, ColumnFamily.DEDUPE, key, TargetResultRecord.VALUE_TYPE));
        value.requireStored(key, TargetResultRecord.VALUE_TYPE, value.canonicalBytes());
        return value;
    }

    private static TargetQuotaIdentity birthRoot(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetSloObservationSample sample) {
        final var position = result(view, overlay, sample.positionKey());
        if (position.primaryIdentity().kind() != TargetQuotaIdentity.Kind.SHARD
                || !scope.shard().equals(position.primaryIdentity().shard())
                || !Arrays.equals(scope.tenantScope(), position.tenantScope())
                || !Arrays.equals(lineage, position.recoveryLineage())
                || !position.mutation().equals(sample.first().mutation())) {
            throw new IllegalStateException("SLO sample lost its original physical Root POSITION");
        }
        if (sample.first().kind() == TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED) {
            if (!Arrays.equals(position.canonicalBytes(), sample.first().canonicalBytes())) {
                throw new IllegalStateException("SLO expired sample changed its sole original POSITION");
            }
        } else {
            position.requireFirst(sample.first());
        }
        return position.primaryIdentity();
    }

    static byte[] payload(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            ColumnFamily family,
            byte[] key,
            int type) {
        return TargetValueEnvelope.decode(
                        Objects.requireNonNull(view.projected(family, key, overlay), "original SLO Source dependency"),
                        type)
                .payload();
    }
}
