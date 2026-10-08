package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Bounded retained-Admission discovery. A reference identifies history bytes; it is never send authority. */
public final class TargetPublishRecoveryDiscovery {
    public enum Stop { RANGE_END, PAGE_LIMIT, READ_BUDGET }

    /** A caller may restart this scan; other integrity failures must not be mistaken for revision changes. */
    public static final class StaleCursor extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private StaleCursor() { super("Target recovery continuation belongs to another Store view"); }
    }

    /** An opaque continuation for this exact discovery instance and unchanged physical Store view. */
    public static final class Cursor {
        private final TargetPublishRecoveryDiscovery owner;
        private final byte[] after;
        private final TargetQueueSnapshotReader.Cut cut;
        private final boolean retainedInputs;

        private Cursor(TargetPublishRecoveryDiscovery owner, byte[] after, TargetQueueSnapshotReader.Cut cut,
                boolean retainedInputs) {
            this.owner = owner;
            this.after = after == null ? null : Bytes.copy(after);
            this.cut = cut;
            this.retainedInputs = retainedInputs;
        }
    }

    public record Page(List<Reference> entries, Cursor continuation, Stop stop) {
        public Page {
            entries = List.copyOf(entries);
            Objects.requireNonNull(continuation, "continuation");
            Objects.requireNonNull(stop, "stop");
        }

        public boolean complete() { return stop == Stop.RANGE_END; }
    }

    /** Exact source locator and accepted-image hashes derived from retained Budget/SYSTEM/POSITION facts. */
    public static final class Reference {
        private final TargetQuotaAttemptBudget attempt;
        private final SystemMutationResult first;
        private final com.nereusstream.delay.protocol.TargetQuotaMutation admission;

        private Reference(TargetQuotaAttemptBudget attempt, SystemMutationResult first) {
            this(attempt, first, attempt.mutation());
        }

        private Reference(TargetQuotaAttemptBudget attempt, SystemMutationResult first,
                com.nereusstream.delay.protocol.TargetQuotaMutation admission) {
            this.attempt = attempt;
            this.first = first;
            this.admission = admission;
        }

        public TargetQuotaAttemptBudget attempt() { return attempt; }
        public SourcePosition source() { return admission.source(); }
        public byte[] envelopeDigest() { return admission.mutationDigest(); }
        public OwnerIdentity admittedOwner() {
            return AuthorIdentity.decode(first.authorIdentity()).asOwnerIdentity();
        }

        /** Correlates fetched bytes; the caller still needs authenticated, retained Broker history and live guards. */
        public SystemMutation requireImage(final SystemMutation image, final SourcePosition actualSource) {
            Objects.requireNonNull(image, "image");
            TargetSourcePosition.requireBounded(actualSource);
            if (image.type() != SystemMutationType.TARGET_PUBLISH_ADMISSION
                    || !Arrays.equals(actualSource.canonicalBytes(), source().canonicalBytes())
                    || !Arrays.equals(image.systemMutationId(), first.mutationId())
                    || !Arrays.equals(image.mutationHash(), first.mutationHash())
                    || !Arrays.equals(image.authorIdentity(), first.authorIdentity())
                    || image.retryUntilEpochMs() != first.retryUntilEpochMs()
                    || !Arrays.equals(Bytes.sha256(image.canonicalEnvelope()), envelopeDigest())) {
                throw new IllegalStateException("Target recovery history differs from its retained Admission image");
            }
            final var body = TargetPublishAdmissionBody.decode(image.canonicalBody());
            if (!body.locator().equals(attempt.locator())
                    || !body.owner().equals(admittedOwner())
                    || !Arrays.equals(body.publishAttemptId(), attempt.publishAttemptId())
                    || body.executionBytes() != attempt.executionBytes()
                    || !body.commitment().equals(attempt.commitment())
                    || !attempt.commitment().covers(body.allocated())
                    || attempt.phase() == TargetQuotaAttemptBudget.Phase.ADMITTED
                            && !body.allocated().equals(attempt.allocated())) {
                throw new IllegalStateException("Target recovery history changes its frozen attempt/budget");
            }
            return image;
        }
    }

    private static final byte[] LOWER = {TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG, TargetKeyCodec.KEY_FORMAT};
    private static final byte[] UPPER = {TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG, TargetKeyCodec.KEY_FORMAT + 1};
    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;

    public TargetPublishRecoveryDiscovery(TargetStoreBackend backend, TargetQuotaScope scope, byte[] recoveryLineage) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.scope = Objects.requireNonNull(scope, "scope");
        Bytes.requireLength(recoveryLineage, 16, "recoveryLineage");
        if (scope.target() != null || Arrays.equals(recoveryLineage, new byte[16])) {
            throw new IllegalArgumentException("Target recovery discovery requires a Shard/lineage scope");
        }
        lineage = Bytes.copy(recoveryLineage);
    }

    /** Limits scanned rows, including resolved budgets; a partial page never proves absence of pending work. */
    public Page scan(final BoundedReadBudget budget, final Cursor continuation, final int maximumRows,
            final TargetStoreBackend.ReadAuthority authority) {
        return scan(budget, continuation, maximumRows, authority, false);
    }

    /** Checkpoint/input protection includes UNKNOWN and resolved retained budgets; this never authorizes SEND. */
    public Page scanRetainedInputs(final BoundedReadBudget budget, final Cursor continuation, final int maximumRows,
            final TargetStoreBackend.ReadAuthority authority) {
        return scan(budget, continuation, maximumRows, authority, true);
    }

    private Page scan(final BoundedReadBudget budget, final Cursor continuation, final int maximumRows,
            final TargetStoreBackend.ReadAuthority authority, final boolean retainedInputs) {
        if (maximumRows < 1 || continuation != null
                && (continuation.owner != this || continuation.retainedInputs != retainedInputs)) {
            throw new IllegalArgumentException("invalid Target recovery page limit or foreign continuation");
        }
        return backend.guardedRead(Objects.requireNonNull(budget, "budget"), reader -> {
            final var cut = new TargetQueueSnapshotReader.Cut(
                    reader.metadata().storeIncarnation(), reader.nativeSequence());
            if (continuation != null && !continuation.cut.equals(cut)) {
                throw new StaleCursor();
            }
            final var entries = new ArrayList<Reference>();
            byte[] after = continuation == null ? null : continuation.after;
            byte[] lower = after == null ? LOWER : afterKey(after);
            for (int rowIndex = 0; rowIndex < maximumRows; rowIndex++) {
                try {
                    final var row = reader.first(ColumnFamily.META, lower, UPPER, List.of());
                    if (row == null) {
                        reader.requireWithinElapsedBudget();
                        return new Page(entries, new Cursor(this, after, cut, retainedInputs), Stop.RANGE_END);
                    }
                    final var attempt = TargetQuotaAttemptBudget.decodeForStore(row.key(),
                            TargetValueEnvelope.decode(row.value(), TargetQuotaAttemptBudget.VALUE_TYPE).payload(),
                            scope.shard());
                    if (retainedInputs || attempt.phase() == TargetQuotaAttemptBudget.Phase.ADMITTED) {
                        final var reference = retainedInputs
                                ? retainedReference(reader, attempt) : verify(reader, attempt);
                        reader.requireWithinElapsedBudget();
                        entries.add(reference);
                    }
                    after = row.key();
                    lower = afterKey(after);
                } catch (ReadIncompleteException incomplete) {
                    return new Page(entries, new Cursor(this, after, cut, retainedInputs), Stop.READ_BUDGET);
                }
            }
            return new Page(entries, new Cursor(this, after, cut, retainedInputs), Stop.PAGE_LIMIT);
        }, Objects.requireNonNull(authority, "authority"));
    }

    /** Rechecks immutable Budget identity; an applied Outcome can retire this initial queue while history loads. */
    public boolean stillAdmitted(final BoundedReadBudget budget, final Reference reference,
            final TargetStoreBackend.ReadAuthority authority) {
        final var expected = Objects.requireNonNull(reference, "reference").attempt();
        return backend.guardedRead(budget, reader -> {
            final var current = TargetQuotaAttemptBudget.decodeForStore(expected.key(),
                    TargetValueEnvelope.decode(required(reader, ColumnFamily.META, expected.key()),
                            TargetQuotaAttemptBudget.VALUE_TYPE).payload(), scope.shard());
            if (!current.locator().equals(expected.locator())
                    || !Arrays.equals(current.admissionDigest(), expected.admissionDigest())
                    || !Arrays.equals(current.tenantScope(), expected.tenantScope())
                    || !Arrays.equals(current.recoveryLineage(), lineage)
                    || !current.accounting().equals(expected.accounting())
                    || current.executionBytes() != expected.executionBytes()
                    || !current.commitment().equals(expected.commitment())) {
                throw new IllegalStateException("Target initial recovery Budget changed its immutable identity");
            }
            current.mutation().requireAtOrBefore(reader.aggregate().mutation());
            if (current.phase() == TargetQuotaAttemptBudget.Phase.ADMITTED) {
                if (!Arrays.equals(current.canonicalBytes(), expected.canonicalBytes())) {
                    throw new IllegalStateException("Target ADMITTED recovery Budget changed without an Outcome");
                }
                verify(reader, current);
            } else {
                expected.mutation().requireAtOrBefore(current.mutation());
            }
            reader.requireWithinElapsedBudget();
            return current.phase() == TargetQuotaAttemptBudget.Phase.ADMITTED;
        }, authority);
    }

    private Reference verify(final TargetStoreBackend.Reader reader, final TargetQuotaAttemptBudget attempt) {
        attempt.mutation().requireAtOrBefore(reader.aggregate().mutation());
        final var root = root(reader);
        final byte[] positionKey = Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                attempt.mutation().source().canonicalBytes());
        final var position = result(reader, positionKey, root);
        final byte[] firstKey = Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, position.logicalId());
        final var first = result(reader, firstKey, root);
        position.requireFirst(first);
        final var outcome = SystemMutationResult.decode(first.typedPayload());
        final var owner = AuthorIdentity.decode(outcome.authorIdentity()).asOwnerIdentity();
        if (position.kind() != TargetResultRecord.Kind.POSITION_SYSTEM
                || first.kind() != TargetResultRecord.Kind.SYSTEM || first.allocation() != null
                || !position.mutation().equals(attempt.mutation()) || !first.mutation().equals(attempt.mutation())
                || outcome.applyStatus() != ApplyStatus.APPLIED
                || outcome.stableCode() != com.nereusstream.delay.protocol.StableCode.OK
                || outcome.mutationType() != SystemMutationType.TARGET_PUBLISH_ADMISSION
                || !Arrays.equals(outcome.mutationId(), first.logicalId())
                || !Arrays.equals(outcome.mutationHash(), attempt.admissionDigest())
                || !Arrays.equals(outcome.appliedSourcePosition(), attempt.mutation().source().canonicalBytes())) {
            throw new IllegalStateException("Target recovery budget lacks its exact accepted Admission first result");
        }
        final var runtime = TargetPublishAdmissionStore.retainedGeneration(
                reader, scope, lineage, attempt.locator()).runtime();
        final var obligation = runtime.attemptObligations().stream()
                .filter(ref -> Arrays.equals(ref.publishAttemptId(), attempt.publishAttemptId()))
                .findFirst().orElse(null);
        if (obligation == null || obligation.ledgerState() != AttemptLedgerState.PUBLISHING
                || obligation.ownerEpoch() != owner.ownerEpoch()) {
            throw new IllegalStateException("Target ADMITTED budget lacks its original PUBLISHING obligation");
        }
        return new Reference(attempt, outcome);
    }

    private Reference retainedReference(
            final TargetStoreBackend.Reader reader, final TargetQuotaAttemptBudget attempt) {
        attempt.mutation().requireAtOrBefore(reader.aggregate().mutation());
        if (!Arrays.equals(attempt.tenantScope(), scope.tenantScope())
                || !Arrays.equals(attempt.recoveryLineage(), lineage)) {
            throw new IllegalStateException("Target retained input Budget belongs to another tenant/lineage");
        }
        final var root = root(reader);
        final byte[] id = SystemMutation.computeSystemMutationId(scope.shard(),
                SystemMutationType.TARGET_PUBLISH_ADMISSION, attempt.publishAttemptId(), attempt.admissionDigest());
        final var first = result(reader, Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, id), root);
        final var position = result(reader, Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                first.mutation().source().canonicalBytes()), root);
        position.requireFirst(first);
        first.mutation().requireAtOrBefore(attempt.mutation());
        if (attempt.phase() == TargetQuotaAttemptBudget.Phase.ADMITTED) {
            if (!first.mutation().equals(attempt.mutation())) {
                throw new IllegalStateException("Target retained ADMITTED input changes its original source");
            }
        } else {
            attempt.mutation().requireAfter(first.mutation());
        }
        final var outcome = SystemMutationResult.decode(first.typedPayload());
        if (first.kind() != TargetResultRecord.Kind.SYSTEM || first.allocation() != null
                || position.kind() != TargetResultRecord.Kind.POSITION_SYSTEM
                || !position.mutation().equals(first.mutation())
                || outcome.applyStatus() != ApplyStatus.APPLIED
                || outcome.stableCode() != com.nereusstream.delay.protocol.StableCode.OK
                || outcome.mutationType() != SystemMutationType.TARGET_PUBLISH_ADMISSION
                || AuthorIdentity.decode(outcome.authorIdentity()).kind() != AuthorIdentity.Kind.OWNER
                || !Arrays.equals(outcome.mutationId(), id)
                || !Arrays.equals(outcome.mutationHash(), attempt.admissionDigest())
                || !Arrays.equals(outcome.appliedSourcePosition(), first.mutation().source().canonicalBytes())) {
            throw new IllegalStateException("Target retained input lacks its original accepted Admission proof");
        }
        return new Reference(attempt, outcome, first.mutation());
    }

    private TargetResultRecord result(TargetStoreBackend.Reader reader, byte[] key, TargetQuotaIncarnation root) {
        final byte[] payload = TargetValueEnvelope.decode(required(reader, ColumnFamily.DEDUPE, key),
                TargetResultRecord.VALUE_TYPE).payload();
        final var result = TargetResultRecord.decode(payload);
        result.requireStored(key, TargetResultRecord.VALUE_TYPE, payload);
        result.requireOwner(root);
        result.mutation().requireAtOrBefore(reader.aggregate().mutation());
        return result;
    }

    private TargetQuotaIncarnation root(TargetStoreBackend.Reader reader) {
        final var id = new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD, scope.shard(),
                reader.aggregate().accountingIncarnation(), null, null);
        final byte[] key = id.key();
        key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var root = TargetQuotaIncarnation.decodeForStore(key,
                TargetValueEnvelope.decode(required(reader, ColumnFamily.META, key), TargetQuotaIncarnation.VALUE_TYPE)
                        .payload(), scope.shard(), scope.tenantScope());
        if (!Arrays.equals(root.recoveryLineage(), lineage)) {
            throw new IllegalStateException("Target recovery discovery belongs to another lineage");
        }
        return root;
    }

    private static byte[] required(TargetStoreBackend.Reader reader, ColumnFamily family, byte[] key) {
        final byte[] raw = reader.get(family, key);
        if (raw == null) {
            throw new IllegalStateException("Target recovery lacks retained primary proof");
        }
        return raw;
    }

    private static byte[] afterKey(byte[] key) { return Bytes.concat(key, new byte[] {0}); }
}
