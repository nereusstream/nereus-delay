package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.store.*;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Objects;

/** Exact original Control authorization and ordered certification-head transition. */
public final class TargetSloObservationVerifier {
    private TargetSloObservationVerifier() {}

    @FunctionalInterface public interface KeyAuthority {
        PublicKey resolve(long signingKeyVersion, SourcePosition originalSource);
    }

    /** Already-held actual ingress/load, old-generation, reader, collector and reservation qualifications. */
    public interface MaterializationAuthority {
        void requireAuthorized(TargetSloObservationControlRequest request, TargetSloObservationState prior,
                TargetSloObservationGeneration retained, SourcePosition source);
        void requireCurrent();
    }

    public record Authority(ControlTargetRegistrationAuthority registrations, KeyAuthority keys,
            ControlAuthorizationContext actor, ControlOperationAuthorization.TargetScopeProof scopeProof,
            MaterializationAuthority materialization) {
        public Authority {
            Objects.requireNonNull(registrations); Objects.requireNonNull(keys); Objects.requireNonNull(actor);
            Objects.requireNonNull(scopeProof); Objects.requireNonNull(materialization);
        }
    }

    public record Change(TargetSloObservationControlBody body, TargetSloObservationState prior,
            TargetSloObservationGeneration retained) {}

    public static Change verifyFirstApplication(TargetStoreBackend.Reader reader, TargetQuotaScope scope,
            byte[] lineage, PreparedControlOperation prepared, SystemMutation input, SourcePosition source,
            Authority authority) {
        Objects.requireNonNull(authority); authority.materialization().requireCurrent();
        final var body = TargetSloObservationControlBody.decode(input.canonicalBody());
        final var request = body.request();
        if (!scope.equals(request.scope()) || !Arrays.equals(lineage, request.lineage())
                || !scope.shard().equals(source.shardId()) || !scope.shard().equals(input.shardId())
                || input.type() != SystemMutationType.APPLY_SHARD_CONTROL
                || input.retryUntilEpochMs() != body.retryUntil()
                || !Arrays.equals(input.logicalOperationIdentity(), body.logicalIdentity())
                || !request.operationRequest().equals(prepared.request())) throw denied("Target SLO Source/registered scope mismatch");
        if (Long.compareUnsigned(source.brokerPersistenceTimeEpochMs(), body.retryUntil()) > 0) {
            throw new CommandResolutionException(StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED, "Target SLO Control exceeded its retry window");
        }
        final var preparedKey = authority.keys().resolve(prepared.signingKeyVersion(), source);
        final var inputKey = authority.keys().resolve(Integer.toUnsignedLong(input.signingKeyVersion()), source);
        if (preparedKey == null || inputKey == null || !prepared.verifySignature(preparedKey) || !input.verifySignature(inputKey)) throw denied("Target SLO Control lacks original signatures");
        final var author = AuthorIdentity.decode(input.authorIdentity());
        if (author.kind() != AuthorIdentity.Kind.CONTROL
                || !Arrays.equals(author.first(), prepared.author().operationActorIdHash())
                || !Arrays.equals(author.second(), prepared.author().authenticatedRoleSetHash())
                || !Arrays.equals(author.digest(), prepared.author().tenantResourceScopeHash())) throw denied("Target SLO Source author differs from original Control author");
        try {
            ControlOperationAuthorization.authorize(prepared, authority.actor(), authority.scopeProof());
            ControlTargetMutationBinding.validate(prepared, prepared.targets().getFirst(), input);
        } catch (IllegalArgumentException rejected) { throw denied(rejected.getMessage()); }
        final var registered = authority.registrations().find(prepared.operationId()).orElse(null);
        if (registered == null || !Arrays.equals(prepared.canonicalBytes(), registered.canonicalBytes())) throw denied("Target SLO Control lacks its exact original registration");
        authority.registrations().validateMutation(prepared, prepared.targets().getFirst(), input);
        if(body.admissionScope()!=null)authority.registrations().validateScopedMutation(prepared,prepared.targets().getFirst(),input);

        final var activation = TargetProtocolActivationStore.applied(reader, scope, lineage, body.tuple());
        if (activation == null || activation.body().payload().descriptor().schemaVersion() < 15
                || activation.appliedAt().source().compareTo(source) >= 0) {
            throw new CommandResolutionException(StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION, "Target SLO certification requires its independent prior activation");
        }
        final var prior = readState(reader, scope, lineage);
        final var retained = prior == null ? null : readGeneration(reader, scope, lineage, prior.generation());
        if (prior != null) {
            prior.requireGeneration(Objects.requireNonNull(retained, "original Target SLO generation"));
            retained.mutation().requireAtOrBefore(reader.aggregate().mutation());
            if (!Arrays.equals(prior.digest(), request.expectedPriorDigest())) throw stale("Target SLO Control changed its exact current head");
            if (source.compareTo(retained.mutation().source()) <= 0) throw stale("Target SLO Control does not advance the original Source");
            retained.policy().requireSource(source);
        } else if (request.expectedPriorDigest() != null) throw stale("Target SLO predecessor is absent");

        if (request.operationKind() == ControlOperationKind.ENABLE_TARGET_SLO_CERTIFICATION) {
            if (prior == null ? request.generation() != 1
                    : prior.active() || request.generation() != TargetQuotaMutation.increment(prior.generation())) throw stale("Target SLO enable is not the next stopped generation");
            final var policy = request.policy();
            policy.requireSource(source);
            if(policy.requiresWriterEpoch()){
                final var epochKey=policy.requiredWriterEpoch().key(scope);final var epochRaw=reader.get(ColumnFamily.META,epochKey);
                if(epochRaw==null)throw denied("certification ENABLE lost its registered writer contract");
                final var epoch=TargetSloWriterEpoch.decodeForStore(epochKey,TargetValueEnvelope.decode(epochRaw,TargetSloWriterEpoch.VALUE_TYPE).payload(),scope,lineage);
                if(!epoch.reference().equals(policy.requiredWriterEpoch()))throw denied("certification ENABLE changed its registered writer reference");
                final var root=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),reader.aggregate().accountingIncarnation(),null,null);
                TargetSloWriterEpochStore.requireOrigin(reader,java.util.List.of(),scope,lineage,root,epoch,reader.aggregate().mutation());
                if(epoch.closed()||reader.closedIngressDeadlineThrough()>=epoch.contract().maximumRetryUntil()
                        ||!policy.service().equals(epoch.contract().service())||policy.keyVersion()!=epoch.contract().keyVersion()
                        ||epoch.register().appliedAt().source().compareTo(source)>=0)
                    throw denied("certification ENABLE lacks its open original registered writer contract");
            }
            if (source.compareTo(policy.sourceAnchor()) <= 0) throw denied("Target SLO enable requires its original prior resource anchor");
            // Retained generations still carry original Source/history duties and consume the declared finite slots.
            if (Long.compareUnsigned(request.generation(), policy.limits().maximumConcurrentGenerations()) > 0) {
                throw new CommandResolutionException(StableCode.HARD_QUOTA_EXCEEDED, "Target SLO retained generation window is full");
            }
        } else if (prior == null || !prior.active() || request.generation() != prior.generation()) {
            throw stale("Target SLO stop does not name the active original generation");
        }
        authority.materialization().requireAuthorized(request, prior, retained, source);
        authority.materialization().requireCurrent();
        return new Change(body, prior, retained);
    }

    public static TargetSloObservationState readState(TargetStoreBackend.Reader reader, TargetQuotaScope scope, byte[] lineage) {
        final var key = TargetSloObservationState.key(scope);
        final var raw = reader.get(ColumnFamily.META, key);
        return raw == null ? null : TargetSloObservationState.decodeForStore(key,
                TargetValueEnvelope.decode(raw, TargetSloObservationState.VALUE_TYPE).payload(), scope, lineage);
    }
    public static TargetSloObservationGeneration readGeneration(TargetStoreBackend.Reader reader, TargetQuotaScope scope,
            byte[] lineage, long generation) {
        final var key = TargetSloObservationGeneration.key(scope, generation);
        final var raw = reader.get(ColumnFamily.META, key);
        return raw == null ? null : TargetSloObservationGeneration.decodeForStore(key,
                TargetValueEnvelope.decode(raw, TargetSloObservationGeneration.VALUE_TYPE).payload(), scope, lineage);
    }
    private static CommandResolutionException denied(String message) { return new CommandResolutionException(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, message); }
    private static CommandResolutionException stale(String message) { return new CommandResolutionException(StableCode.STALE_SYSTEM_MUTATION, message); }
}
