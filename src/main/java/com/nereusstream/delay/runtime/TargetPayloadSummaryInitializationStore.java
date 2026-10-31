package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlOperationAuthorization;
import com.nereusstream.delay.protocol.ControlTargetMutationBinding;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationBody;
import com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationRequest;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

/** The explicit Source consumer for one fully qualified old Native32 version upgrade. */
public final class TargetPayloadSummaryInitializationStore {
    private TargetPayloadSummaryInitializationStore() {}

    @FunctionalInterface
    public interface OriginalInputs {
        TargetPayloadObligationSummaryBootstrap.Fold requireActual(
                PreparedControlOperation prepared,
                SystemMutation input,
                SourcePosition source,
                TargetPayloadSummaryInitializationRequest request);
    }

    public record Authority(
            ControlTargetRegistrationAuthority registrations,
            TargetMigrationStore.Keys keys,
            ControlAuthorizationContext actor,
            ControlOperationAuthorization.TargetScopeProof scopeProof,
            OriginalInputs inputs) {
        public Authority {
            Objects.requireNonNull(registrations);
            Objects.requireNonNull(keys);
            Objects.requireNonNull(actor);
            Objects.requireNonNull(scopeProof);
            Objects.requireNonNull(inputs);
        }
    }

    public static final class Prepared {
        private final TargetStoreBackend backend;
        private final TargetStoreBackend.Prepared batch;
        private final TargetPayloadObligationSummaryBootstrap.Fold fold;
        private final SystemMutationResult result;

        private Prepared(
                TargetStoreBackend backend,
                TargetStoreBackend.Prepared batch,
                TargetPayloadObligationSummaryBootstrap.Fold fold,
                SystemMutationResult result) {
            this.backend = backend;
            this.batch = batch;
            this.fold = fold;
            this.result = result;
        }
    }

    /** Authentication and protected complete-image acquisition run before entering the Native monitor. */
    public static Prepared prepareFirst(
            TargetStoreBackend backend,
            TargetQuotaScope scope,
            byte[] lineage,
            int maximumCounters,
            int maximumDomains,
            BoundedReadBudget budget,
            PreparedControlOperation control,
            SystemMutation input,
            SourcePosition source,
            Authority authority) {
        Objects.requireNonNull(backend);
        Objects.requireNonNull(authority);
        TargetSourcePosition.requireBounded(source);
        final var body = TargetPayloadSummaryInitializationBody.decode(input.canonicalBody());
        body.requireInput(input);
        final var request = body.request();
        if (!scope.equals(request.scope())
                || !Arrays.equals(lineage, request.lineage())
                || !scope.shard().equals(source.shardId())
                || control.kind() != request.operationKind()
                || !control.request().equals(request.operationRequest())
                || control.targets().size() != 1) {
            throw new IllegalArgumentException("summary initialization changed exact registered Source/scope/window");
        }
        final var controlKey = Objects.requireNonNull(
                authority.keys().resolve(control.signingKeyVersion(), source), "original management Control key");
        final var inputKey = Objects.requireNonNull(
                authority.keys().resolve(Integer.toUnsignedLong(input.signingKeyVersion()), source),
                "original summary Source key");
        if (!control.verifySignature(controlKey) || !input.verifySignature(inputKey)) {
            throw new IllegalArgumentException("summary initialization lacks original signatures");
        }
        final var author = AuthorIdentity.decode(input.authorIdentity());
        if (!Arrays.equals(author.first(), control.author().operationActorIdHash())
                || !Arrays.equals(author.second(), control.author().authenticatedRoleSetHash())
                || !Arrays.equals(author.digest(), control.author().tenantResourceScopeHash())) {
            throw new IllegalArgumentException("summary Source changed the original management actor");
        }
        ControlOperationAuthorization.authorize(control, authority.actor(), authority.scopeProof());
        final var registered =
                authority.registrations().find(control.operationId()).orElseThrow();
        if (!Arrays.equals(registered.canonicalBytes(), control.canonicalBytes())) {
            throw new IllegalArgumentException("summary initialization lacks exact original Prepared registration");
        }
        ControlTargetMutationBinding.validate(control, control.targets().getFirst(), input);
        authority.registrations().validateMutation(control, control.targets().getFirst(), input);
        final boolean selected = source.brokerPersistenceTimeEpochMs() <= input.retryUntilEpochMs()
                && backend.prepareRead(
                                budget,
                                reader -> reader.closedIngressDeadlineThrough() < input.retryUntilEpochMs()
                                        && reader.aggregate().mutation().equals(request.auditedCut())
                                        && Arrays.equals(
                                                reader.get(
                                                        ColumnFamily.META,
                                                        request.expectedOwner().key()),
                                                TargetValueEnvelope.encode(
                                                        TargetQuotaPayloadOwner.VALUE_TYPE,
                                                        request.expectedOwner().canonicalBytes())))
                        .value();
        final var fold = selected
                ? Objects.requireNonNull(
                        authority.inputs().requireActual(control, input, source, request),
                        "actual protected complete old-owner fold")
                : null;
        if (fold != null) {
            fold.requireOriginalRequest(request);
        }
        final var outcome = new SystemMutationResult[1];
        final var batch = backend.prepare(budget, reader -> {
            if (fold != null) {
                fold.requireLocalCurrent();
            }
            if (reader.source() == null || source.compareTo(reader.source()) <= 0) {
                throw new IllegalStateException("summary initialization must follow its actual Source predecessor");
            }
            final var activation = TargetProtocolActivationStore.applied(reader, scope, lineage, body.tuple());
            final var root = TargetParentControlCreditVerifier.readView(reader, scope, lineage)
                    .actualRoot();
            final var stamp = new TargetQuotaMutation(
                    TargetQuotaMutation.increment(reader.sourceSequence()),
                    source,
                    Bytes.sha256(input.canonicalEnvelope()));
            final StableCode rejected = source.brokerPersistenceTimeEpochMs() > input.retryUntilEpochMs()
                            || reader.closedIngressDeadlineThrough() >= input.retryUntilEpochMs()
                    ? StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED
                    : activation == null || activation.appliedAt().source().compareTo(source) >= 0
                            ? StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION
                            : fold == null || !fold.currentReader(reader) ? StableCode.STALE_SYSTEM_MUTATION : null;
            final var qualification = rejected == null ? fold.forSource(reader, stamp) : null;
            outcome[0] = SystemMutationResult.from(
                    input,
                    rejected == null ? ApplyStatus.APPLIED : ApplyStatus.REJECTED,
                    rejected == null ? StableCode.OK : rejected,
                    source.canonicalBytes());
            final var edits = new ArrayList<TargetStoreBackend.Edit>();
            if (qualification != null) {
                final var next = request.expectedOwner()
                        .initializeSummary(
                                qualification, stamp, (prior, after, floor) -> qualification.requireOwner(prior));
                edits.add(reader.replace(
                        ColumnFamily.META, next.key(), TargetQuotaPayloadOwner.VALUE_TYPE, next.canonicalBytes()));
            }
            final TargetResultRecord.CreationAuthority creation = (record, first) -> {
                record.requireOwner(root);
                if (!record.mutation().equals(stamp)
                        || record.allocation() != null
                        || reader.get(ColumnFamily.DEDUPE, record.key()) != null) {
                    throw new IllegalStateException("summary initialization changed its actual first/POS");
                }
                if (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                    record.requireFirst(Objects.requireNonNull(first));
                } else if (record.kind() != TargetResultRecord.Kind.SYSTEM
                        || first != null
                        || !Arrays.equals(record.typedPayload(), outcome[0].encode())) {
                    throw new IllegalStateException("unexpected summary initialization result kind");
                }
            };
            final var first = TargetResultRecord.system(root, outcome[0], stamp, null, creation);
            final var position = TargetResultRecord.position(root, first, stamp, creation);
            edits.add(reader.replace(
                    ColumnFamily.DEDUPE, first.key(), TargetResultRecord.VALUE_TYPE, first.canonicalBytes()));
            edits.add(reader.replace(
                    ColumnFamily.DEDUPE, position.key(), TargetResultRecord.VALUE_TYPE, position.canonicalBytes()));
            final var accounting = new TargetSourceAccounting(
                    scope, lineage, source, stamp.mutationDigest(), maximumCounters, 1, maximumDomains);
            return (qualification == null ? accounting : accounting.withPayloadSummaryInitialization(qualification))
                    .assemble(reader, edits);
        });
        return new Prepared(backend, batch, fold, outcome[0]);
    }

    public static SystemMutationResult commit(Prepared prepared, TargetStoreBackend.CommitAuthority authority) {
        Objects.requireNonNull(prepared);
        prepared.backend.commit(prepared.batch, (store, scope, mutation) -> {
            final var original = Objects.requireNonNull(authority.acquire(store, scope, mutation));
            return new TargetStoreBackend.CommitGuard() {
                @Override
                public void requireCurrent() {
                    original.requireCurrent();
                    if (prepared.fold != null) {
                        prepared.fold.requireLocalCurrent();
                    }
                    original.requireCurrent();
                }

                @Override
                public void close() {
                    original.close();
                }
            };
        });
        return prepared.result;
    }
}
