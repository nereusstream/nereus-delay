package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetCloseBody;
import com.nereusstream.delay.protocol.TargetCloseCursorRecord;
import com.nereusstream.delay.protocol.TargetCloseRecord;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.IngressFenceState;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Atomic Target Close marker, queue admission/head freeze, first results and exact source accounting. */
public final class TargetCloseStore {
    public static final class Prepared {
        private final TargetCloseStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;

        private Prepared(TargetCloseStore owner, TargetStoreBackend.Prepared batch, SystemMutationResult result) {
            this.owner = owner;
            this.batch = batch;
            this.result = result;
        }
    }

    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumCounters;
    private final int maximumDomains;

    public TargetCloseStore(
            TargetStoreBackend backend,
            TargetQuotaScope scope,
            byte[] lineage,
            int maximumCounters,
            int maximumDomains) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.scope = Objects.requireNonNull(scope, "scope");
        Bytes.requireLength(lineage, 16, "lineage");
        if (scope.target() != null
                || Arrays.equals(lineage, new byte[16])
                || maximumCounters < 2
                || maximumDomains < 1
                || maximumDomains > 64) {
            throw new IllegalArgumentException("Close Store requires bounded Shard/lineage accounting");
        }
        this.lineage = Bytes.copy(lineage);
        this.maximumCounters = maximumCounters;
        this.maximumDomains = maximumDomains;
    }

    /** Caller must route immutable first-result replay before this method and protect authority through commit. */
    public Prepared prepareFirst(
            BoundedReadBudget budget,
            PreparedControlOperation prepared,
            SystemMutation mutation,
            SourcePosition source,
            TargetCloseVerifier.Authority authority) {
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(authority, "authority");
        TargetSourcePosition.requireBounded(source);
        if (!scope.shard().equals(source.shardId())
                || !scope.shard().equals(mutation.shardId())
                || mutation.type() != SystemMutationType.APPLY_SHARD_CONTROL) {
            throw new IllegalArgumentException("close source/operation mismatch");
        }
        final var body = TargetCloseBody.decode(mutation.canonicalBody());
        final var result = new SystemMutationResult[1];
        final var batch = backend.prepare(budget, reader -> {
            if (reader.source() == null || source.compareTo(reader.source()) <= 0) {
                throw new IllegalStateException("first Close needs an established strictly earlier source frontier");
            }
            final byte[] firstKey =
                    Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, 1}, mutation.systemMutationId());
            final byte[] positionKey =
                    Bytes.concat(new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, 1}, source.canonicalBytes());
            requireAbsent(reader, ColumnFamily.DEDUPE, firstKey);
            requireAbsent(reader, ColumnFamily.DEDUPE, positionKey);
            final var stamp = new TargetQuotaMutation(
                    TargetQuotaMutation.increment(reader.sourceSequence()),
                    source,
                    Bytes.sha256(mutation.canonicalEnvelope()));
            final var root = root(reader, stamp);
            final byte[] priorBytes = reader.get(ColumnFamily.META, KeyCodec.metaFixed(4));
            final var prior = priorBytes == null
                    ? new IngressFenceState(IngressFenceState.OPEN, null)
                    : IngressFenceState.decode(
                            TargetValueEnvelope.decode(priorBytes, 1).payload());
            final byte[] queueKey = TargetKeyCodec.state(body.request().target());
            final byte[] queueRaw = reader.get(ColumnFamily.META, queueKey);
            final var queue = queueRaw == null
                    ? null
                    : TargetQueueState.decode(TargetValueEnvelope.decode(queueRaw, TargetQueueState.VALUE_TYPE)
                            .payload());
            if (queue != null && !queue.targetId().equals(body.request().target())) {
                throw new IllegalStateException("Target Close queue identity differs from its Store key");
            }
            final byte[] markerKey = TargetKeyCodec.close(body.request().target());
            final byte[] priorMarker = reader.get(ColumnFamily.META, markerKey);
            if (priorMarker != null) {
                final var marker = TargetCloseRecord.decodeForStore(
                        markerKey,
                        TargetValueEnvelope.decode(priorMarker, TargetCloseRecord.VALUE_TYPE)
                                .payload(),
                        scope.shard(),
                        lineage);
                marker.requireQueue(Objects.requireNonNull(queue, "closed queue"));
                marker.mutation().requireAtOrBefore(reader.aggregate().mutation());
            } else if (queue != null && queue.admissionState() == TargetQueueState.AdmissionState.CLOSED) {
                throw new IllegalStateException("closed Target lacks its durable first marker");
            }
            final TargetCloseVerifier.Decision decision;
            if (prior.closedThroughEpochMs() >= mutation.retryUntilEpochMs()) {
                decision = null;
            } else {
                try {
                    decision = TargetCloseVerifier.decideFirstApplication(
                            scope, prepared, mutation, source, queue, authority);
                } catch (ReadIncompleteException external) {
                    throw new IllegalStateException("external close authority did not complete", external);
                }
            }
            final var rejection =
                    decision == null ? StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED : decision.rejection();
            final var outcome = SystemMutationResult.from(
                    mutation,
                    rejection == null ? ApplyStatus.APPLIED : ApplyStatus.REJECTED,
                    rejection == null ? StableCode.OK : rejection,
                    source.canonicalBytes());
            final TargetResultRecord.CreationAuthority resultAuthority = (record, first) -> {
                record.requireOwner(root);
                if (!record.mutation().equals(stamp) || record.allocation() != null) {
                    throw new IllegalStateException("close result changed its root/source");
                }
                requireAbsent(reader, ColumnFamily.DEDUPE, record.key());
                if (record.kind() == TargetResultRecord.Kind.SYSTEM) {
                    if (first != null
                            || !Arrays.equals(record.key(), firstKey)
                            || !Arrays.equals(record.typedPayload(), outcome.encode())) {
                        throw new IllegalStateException("close first result differs from verified decision");
                    }
                } else if (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                    if (first == null
                            || !Arrays.equals(first.key(), firstKey)
                            || !Arrays.equals(first.typedPayload(), outcome.encode())
                            || !Arrays.equals(record.key(), positionKey)) {
                        throw new IllegalStateException("close POSITION differs from first result");
                    }
                    record.requireFirst(first);
                } else {
                    throw new IllegalStateException("unexpected close result kind");
                }
            };
            final var first = TargetResultRecord.system(root, outcome, stamp, null, resultAuthority);
            final var position = TargetResultRecord.position(root, first, stamp, resultAuthority);
            final var edits = new ArrayList<>(List.of(
                    reader.replace(
                            ColumnFamily.DEDUPE, first.key(), TargetResultRecord.VALUE_TYPE, first.canonicalBytes()),
                    reader.replace(
                            ColumnFamily.DEDUPE,
                            position.key(),
                            TargetResultRecord.VALUE_TYPE,
                            position.canonicalBytes())));
            if (rejection == null) {
                requireAbsent(reader, ColumnFamily.META, markerKey);
                final var domains = queue.domains().stream()
                        .map(domain -> new TargetDomainState(
                                domain.domain(),
                                domain.lifecycle(),
                                domain.dispatchCompatibilityRef(),
                                domain.controlScopeRef(),
                                domain.nativePolicyScopeRef(),
                                null,
                                null))
                        .toList();
                final var closed = new TargetQueueState(
                        queue.targetId(),
                        TargetQueueState.nextRevision(queue.headRevision()),
                        TargetQueueState.nextRevision(queue.controlVersion()),
                        TargetQueueState.AdmissionState.CLOSED,
                        queue.accountingIncarnation(),
                        queue.nativeIndexLeadCapMs(),
                        domains);
                closed.requireSuccessorOf(queue);
                final var marker = new TargetCloseRecord(decision.body(), prior.closedThroughEpochMs(), stamp, lineage);
                marker.requireQueue(closed);
                edits.add(reader.replace(
                        ColumnFamily.META, queueKey, TargetQueueState.VALUE_TYPE, closed.canonicalBytes()));
                edits.add(reader.replace(
                        ColumnFamily.META, markerKey, TargetCloseRecord.VALUE_TYPE, marker.canonicalBytes()));
                final var cursor = TargetCloseCursorRecord.initial(marker);
                requireAbsent(reader, ColumnFamily.META, cursor.key());
                edits.add(reader.replace(
                        ColumnFamily.META, cursor.key(), TargetCloseCursorRecord.VALUE_TYPE, cursor.canonicalBytes()));
            }
            result[0] = outcome;
            return new TargetSourceAccounting(
                            scope, lineage, source, stamp.mutationDigest(), maximumCounters, 1, maximumDomains)
                    .assemble(reader, TargetQueueHeadUpdater.complete(reader, edits, maximumDomains, maximumDomains));
        });
        return new Prepared(this, batch, result[0]);
    }

    public SystemMutationResult commit(Prepared prepared, TargetStoreBackend.CommitAuthority authority) {
        Objects.requireNonNull(prepared, "prepared");
        if (prepared.owner != this) {
            throw new IllegalArgumentException("foreign close plan");
        }
        backend.commit(prepared.batch, Objects.requireNonNull(authority, "authority"));
        return prepared.result;
    }

    /** Read the durable global Target Close and combine it with mandatory binding/domain/legacy scope evidence. */
    public TargetReservationControls.Authority reservationControls(
            TargetReservationControls.Authority remainingScopes) {
        Objects.requireNonNull(remainingScopes, "remainingScopes");
        return TargetReservationControls.withStoreReads((reader, binding) -> {
            final var remaining = TargetReservationControls.readClosure(remainingScopes, reader, binding);
            final byte[] key = TargetKeyCodec.close(binding.target());
            final byte[] raw = reader.get(ColumnFamily.META, key);
            if (raw == null) {
                return remaining;
            }
            final var marker = TargetCloseRecord.decodeForStore(
                    key,
                    TargetValueEnvelope.decode(raw, TargetCloseRecord.VALUE_TYPE)
                            .payload(),
                    scope.shard(),
                    lineage);
            marker.mutation().requireAtOrBefore(reader.aggregate().mutation());
            final var queue = TargetQueueState.decode(TargetValueEnvelope.decode(
                            reader.get(ColumnFamily.META, TargetKeyCodec.state(binding.target())),
                            TargetQueueState.VALUE_TYPE)
                    .payload());
            marker.requireQueue(queue);
            binding.requireQueueProjection(queue);
            final var global = new TargetReservationControls.Closure(
                    binding.target(),
                    binding.digest(),
                    lineage,
                    marker.mutation().source(),
                    marker.fenceAtClose());
            if (remaining.isEmpty()) {
                return Optional.of(global);
            }
            final var other = remaining.orElseThrow();
            if (!other.target().equals(binding.target())
                    || !Arrays.equals(other.bindingDigest(), binding.digest())
                    || !Arrays.equals(other.recoveryLineage(), lineage)
                    || !other.source().shardId().equals(scope.shard())
                    || reader.source() == null
                    || other.source().compareTo(reader.source()) > 0
                    || other.fenceAtClose() > reader.closedIngressDeadlineThrough()
                    || (other.source().compareTo(reader.source()) == 0
                            && (!Arrays.equals(
                                            other.source().canonicalBytes(),
                                            reader.source().canonicalBytes())
                                    || other.fenceAtClose() != reader.closedIngressDeadlineThrough()))) {
                throw new IllegalStateException("remaining Close scopes returned foreign evidence");
            }
            final int order = other.source().compareTo(global.source());
            if (order == 0
                    && (!Arrays.equals(
                                    other.source().canonicalBytes(),
                                    global.source().canonicalBytes())
                            || other.fenceAtClose() != global.fenceAtClose())) {
                throw new IllegalStateException("same-source Close scopes disagree");
            }
            return Optional.of(order < 0 ? other : global);
        });
    }

    private TargetQuotaIncarnation root(final TargetStoreBackend.Reader reader, final TargetQuotaMutation operation) {
        final var identity = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.SHARD,
                scope.shard(),
                reader.aggregate().accountingIncarnation(),
                null,
                null);
        final byte[] key = identity.key();
        key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            throw new IllegalStateException("close apply lacks its controlled Shard accounting root");
        }
        final var root = TargetQuotaIncarnation.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetQuotaIncarnation.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope());
        if (!root.identity().equals(identity) || !Arrays.equals(root.recoveryLineage(), lineage)) {
            throw new IllegalStateException("close result root differs from its actual Shard/lineage");
        }
        final var prior = reader.aggregate().mutation();
        if (prior == null) {
            throw new IllegalStateException("close root lacks its applied mutation frontier");
        }
        root.latestMutation().requireAtOrBefore(prior);
        prior.requireAtOrBefore(operation);
        return root;
    }

    private static void requireAbsent(
            final TargetStoreBackend.Reader reader, final ColumnFamily family, final byte[] key) {
        if (reader.get(family, key) != null) {
            throw new IllegalStateException("first Close application encountered an existing logical/physical record");
        }
    }
}
