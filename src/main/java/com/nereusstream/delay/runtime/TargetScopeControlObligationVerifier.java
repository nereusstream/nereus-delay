package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlOperationAuthorization;
import com.nereusstream.delay.protocol.ControlOperationKind;
import com.nereusstream.delay.protocol.ControlTargetMutationBinding;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetParentControlCredit;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetScopeControlObligation;
import com.nereusstream.delay.protocol.TargetScopeControlObligationBody;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.Objects;

/** Original bootstrap authorization and guarded paid-parent read set for one immutable Scope obligation. */
public final class TargetScopeControlObligationVerifier {
    private TargetScopeControlObligationVerifier() {}

    /** Original registry/physical Source funding and retention are independently held, never inferred from Native bytes. */
    public interface CapacityAuthority {
        default void requireHistoricalAuthorized(
                TargetScopeControlObligation obligation, TargetParentControlCredit parent) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable(
                    "original historical Scope obligation authority is unavailable");
        }

        default void requireAuthorized(
                TargetScopeControlObligation.Definition definition,
                TargetParentControlCreditVerifier.View view,
                TargetParentControlCredit parent,
                SourcePosition source,
                TargetStoreBackend.Mutation complete) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable(
                    "original paid Scope obligation authority is unavailable");
        }

        void requireCurrent();
    }

    public record Authority(
            ControlTargetRegistrationAuthority registrations,
            TargetParentControlCreditVerifier.KeyAuthority keys,
            ControlAuthorizationContext actor,
            ControlOperationAuthorization.TargetScopeProof scopeProof,
            CapacityAuthority capacity) {
        public Authority {
            Objects.requireNonNull(registrations);
            Objects.requireNonNull(keys);
            Objects.requireNonNull(actor);
            Objects.requireNonNull(scopeProof);
            Objects.requireNonNull(capacity);
        }
    }

    public record Change(
            TargetScopeControlObligationBody body,
            TargetParentControlCreditVerifier.View view,
            TargetParentControlCredit parent) {
        public Change {
            Objects.requireNonNull(body);
            Objects.requireNonNull(view);
            Objects.requireNonNull(parent);
        }
    }

    public static Change verifyFirstApplication(
            TargetStoreBackend.Reader reader,
            TargetQuotaScope scope,
            byte[] lineage,
            PreparedControlOperation control,
            SystemMutation input,
            SourcePosition source,
            Authority authority) {
        Objects.requireNonNull(reader);
        Objects.requireNonNull(control);
        Objects.requireNonNull(authority);
        TargetSourcePosition.requireBounded(source);
        authority.capacity().requireCurrent();
        final var body = TargetScopeControlObligationBody.decode(input.canonicalBody());
        final var request = body.request();
        if (scope.target() != null
                || !scope.equals(request.scope())
                || !Arrays.equals(lineage, request.lineage())
                || !scope.shard().equals(source.shardId())
                || !scope.shard().equals(input.shardId())
                || input.type() != SystemMutationType.APPLY_SHARD_CONTROL
                || input.retryUntilEpochMs() != body.retryUntil()
                || !Arrays.equals(input.logicalOperationIdentity(), body.logicalIdentity())
                || control.kind() != ControlOperationKind.ALLOCATE_SCOPE_CONTROL_OBLIGATION
                || !request.operationRequest().equals(control.request())
                || control.targets().size() != 1
                || input.canonicalEnvelope().length > TargetScopeControlObligationBody.MAX_INPUT_BYTES) {
            throw denied("Scope obligation changed its original request/Source binding");
        }
        if (Long.compareUnsigned(source.brokerPersistenceTimeEpochMs(), body.retryUntil()) > 0) {
            throw new CommandResolutionException(
                    StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED,
                    "Scope obligation exceeded its original Source retry window");
        }
        final var controlKey = authority.keys().resolve(control.signingKeyVersion(), source);
        final var inputKey = authority.keys().resolve(Integer.toUnsignedLong(input.signingKeyVersion()), source);
        if (controlKey == null
                || inputKey == null
                || !control.verifySignature(controlKey)
                || !input.verifySignature(inputKey)) {
            throw denied("Scope obligation lacks its original signatures");
        }
        final var author = AuthorIdentity.decode(input.authorIdentity());
        if (author.kind() != AuthorIdentity.Kind.CONTROL
                || !Arrays.equals(author.first(), control.author().operationActorIdHash())
                || !Arrays.equals(author.second(), control.author().authenticatedRoleSetHash())
                || !Arrays.equals(author.digest(), control.author().tenantResourceScopeHash())) {
            throw denied("Scope obligation changed its original Control actor");
        }
        try {
            ControlOperationAuthorization.authorize(control, authority.actor(), authority.scopeProof());
            ControlTargetMutationBinding.validate(control, control.targets().getFirst(), input);
        } catch (IllegalArgumentException unauthorized) {
            throw denied(unauthorized.getMessage());
        }
        final var registered =
                authority.registrations().find(control.operationId()).orElse(null);
        if (registered == null || !Arrays.equals(control.canonicalBytes(), registered.canonicalBytes())) {
            throw denied("Scope obligation lacks exact original point registration");
        }
        authority.registrations().validateMutation(control, control.targets().getFirst(), input);
        final var activation = TargetProtocolActivationStore.applied(reader, scope, lineage, body.tuple());
        if (activation == null || activation.appliedAt().source().compareTo(source) >= 0) {
            throw new CommandResolutionException(
                    StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION,
                    "Scope obligation lacks independent prior tuple-24 activation");
        }
        final var view = TargetParentControlCreditVerifier.readView(reader, scope, lineage);
        final var definition = request.definition();
        try {
            definition.plan().requireRoot(view.actualRoot());
            definition.plan().requireSource(source);
            definition.plan().requireSource(view.priorCut().source());
        } catch (IllegalArgumentException foreign) {
            throw denied(foreign.getMessage());
        }
        if (view.actualRoot().draining()) {
            throw stale("draining SHARD Root cannot allocate new Scope obligations");
        }
        if (source.compareTo(view.priorCut().source()) <= 0
                || definition.plan().sourceAnchor().compareTo(view.priorCut().source()) > 0
                || definition.header().sourceAnchor().compareTo(view.priorCut().source()) > 0) {
            throw stale("Scope obligation does not follow its actual Source/header/resource anchors");
        }
        if (reader.get(ColumnFamily.META, definition.key()) != null) {
            throw stale("original Scope ID already has a retained obligation");
        }
        final var raw =
                reader.get(ColumnFamily.META, definition.parentReference().key());
        if (raw == null) {
            throw stale("Scope obligation has no actual paid parent credit");
        }
        final var parent = TargetParentControlCredit.decodeForStore(
                definition.parentReference().key(),
                TargetValueEnvelope.decode(raw, TargetParentControlCredit.VALUE_TYPE)
                        .payload(),
                scope,
                lineage);
        parent.requireReference(definition.parentReference());
        parent.requireRoot(view.actualRoot());
        TargetParentControlCreditStore.requireOrigin(
                reader, java.util.List.of(), scope, lineage, view.actualRoot(), parent, view.priorCut());
        if (parent.revision() != request.expectedParentRevision()
                || !Arrays.equals(parent.digest(), request.expectedParentDigest())) {
            throw stale("Scope obligation changed its exact original parent revision/digest");
        }
        if (parent.allocations().size() >= parent.definition().maximumAllocations()
                || parent.allocations().stream()
                        .anyMatch(a -> Arrays.equals(
                                a.scope().scopeId(), definition.header().scopeId()))) {
            throw stale("parent already allocated this Scope ID or exhausted its finite child slots");
        }
        authority.capacity().requireCurrent();
        reader.requireWithinElapsedBudget();
        return new Change(body, view, parent);
    }

    /** The complete native transfer must conserve the already-paid vector, including this Control's first/POS. */
    public static void requireComplete(
            Change change, TargetStoreBackend.Mutation complete, SourcePosition source, Authority authority) {
        Objects.requireNonNull(change);
        Objects.requireNonNull(complete);
        Objects.requireNonNull(authority);
        authority.capacity().requireCurrent();
        final var view = change.view();
        final var counters = complete.quota().counters();
        final var stamp = counters.mutation();
        stamp.requireSourceApplied();
        stamp.requireAfter(view.priorCut());
        if (!complete.quota().shardScope().equals(change.body().request().scope())
                || !Arrays.equals(
                        counters.priorAggregate().canonicalBytes(),
                        view.actualAggregate().canonicalBytes())
                || counters.expectedMutationSequence() != view.priorCut().sequence()
                || !Arrays.equals(stamp.source().canonicalBytes(), source.canonicalBytes())
                || !Arrays.equals(
                        counters.nextAggregate().accountingIncarnation(),
                        view.actualRoot().identity().accountingIncarnation())) {
            throw new IllegalStateException("Scope allocation changed its actual guarded payer/Source cut");
        }
        if (!Arrays.equals(
                counters.nextAggregate().usage().canonicalBytes(),
                view.actualAggregate().usage().canonicalBytes())) {
            throw new IllegalStateException("Scope allocation did not conserve its paid parent Native vector");
        }
        if (!view.actualGrant()
                .grant()
                .limit()
                .permitsGrowth(
                        view.actualAggregate().usage(), counters.nextAggregate().usage())) {
            throw new CommandResolutionException(
                    StableCode.HARD_QUOTA_EXCEEDED, "actual Native SHARD grant cannot carry this Scope obligation");
        }
        authority
                .capacity()
                .requireAuthorized(change.body().request().definition(), view, change.parent(), source, complete);
        authority.capacity().requireCurrent();
    }

    private static CommandResolutionException denied(String message) {
        return new CommandResolutionException(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, message);
    }

    private static CommandResolutionException stale(String message) {
        return new CommandResolutionException(StableCode.STALE_SYSTEM_MUTATION, message);
    }
}
