package com.nereusstream.delay.store;

import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlOperationAuthorization;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.ControlTargetMutationBinding;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationBodyCodec;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetInitialControlSnapshot;
import com.nereusstream.delay.protocol.TargetParentControlCredit;
import com.nereusstream.delay.protocol.TargetParentControlCreditBody;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetScopeControlObligation;
import com.nereusstream.delay.protocol.TargetScopeControlObligationBody;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetQuotaGrantControlVerifier;
import com.nereusstream.delay.runtime.TargetParentControlCreditVerifier;
import com.nereusstream.delay.runtime.TargetScopeControlObligationVerifier;
import com.nereusstream.delay.runtime.TargetResultRecord;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Objects;
import java.util.TreeMap;

/** Exact retained Control authentication; external original-Source key/role/scope/history protection is mandatory. */
public final class TargetCheckpointControlAuthority implements TargetCheckpointDependencies.ControlImageAuthority {
    public record Authorization(ControlAuthorizationContext actor,
            ControlOperationAuthorization.TargetScopeProof scope) {
        public Authorization {
            Objects.requireNonNull(actor, "actor");
            Objects.requireNonNull(scope, "scope");
        }
    }

    /** Must authenticate original Broker history and retain its credential/retention/pin protection. */
    @FunctionalInterface
    public interface HistoryAuthority {
        void requireOriginal(TargetCheckpointDependencies.ControlImage input, TargetResultRecord first);
    }

    /**
     * Authenticates this immutable Prepared registration's visibility, actor/roles and complete target scope at the
     * exact original Source. Current registry existence or signed author hashes alone do not establish that fact.
     */
    @FunctionalInterface
    public interface SourceAuthority {
        Authorization resolve(PreparedControlOperation registered, TargetCheckpointDependencies.ControlImage input);
    }

    private final ControlTargetRegistrationAuthority registrations;
    private final TargetQuotaGrantControlVerifier.KeyAuthority keys;
    private final HistoryAuthority history;
    private final SourceAuthority source;
    private final BoundedReadBudget budget;
    private final Runnable protection;
    private final com.nereusstream.delay.runtime.TargetSloObservationEventStore.ObservationAuthority observations;
    private final com.nereusstream.delay.runtime.TargetSloWriterEpochVerifier.MaterializationAuthority writers;
    private final TargetParentControlCreditVerifier.CapacityAuthority parentCredits;
    private final TargetScopeControlObligationVerifier.CapacityAuthority scopeObligations;
    private final com.nereusstream.delay.runtime.TargetScopeFeeAuthority scopeFees;

    /** All external I/O must have finite provider deadlines; use outside Store locks with a held protection set. */
    public TargetCheckpointControlAuthority(ControlTargetRegistrationAuthority registrations,
            TargetQuotaGrantControlVerifier.KeyAuthority keys, HistoryAuthority history, SourceAuthority source,
            BoundedReadBudget budget, Runnable protection) {
        this(registrations,keys,history,source,budget,protection,null);
    }

    public TargetCheckpointControlAuthority(ControlTargetRegistrationAuthority registrations,
            TargetQuotaGrantControlVerifier.KeyAuthority keys, HistoryAuthority history, SourceAuthority source,
            BoundedReadBudget budget, Runnable protection,
            com.nereusstream.delay.runtime.TargetSloObservationEventStore.ObservationAuthority observations) {
        this(registrations,keys,history,source,budget,protection,observations,null);
    }

    public TargetCheckpointControlAuthority(ControlTargetRegistrationAuthority registrations,
            TargetQuotaGrantControlVerifier.KeyAuthority keys,HistoryAuthority history,SourceAuthority source,
            BoundedReadBudget budget,Runnable protection,
            com.nereusstream.delay.runtime.TargetSloObservationEventStore.ObservationAuthority observations,
            com.nereusstream.delay.runtime.TargetSloWriterEpochVerifier.MaterializationAuthority writers) {
        this(registrations,keys,history,source,budget,protection,observations,writers,null);
    }

    public TargetCheckpointControlAuthority(ControlTargetRegistrationAuthority registrations,
            TargetQuotaGrantControlVerifier.KeyAuthority keys,HistoryAuthority history,SourceAuthority source,
            BoundedReadBudget budget,Runnable protection,
            com.nereusstream.delay.runtime.TargetSloObservationEventStore.ObservationAuthority observations,
            com.nereusstream.delay.runtime.TargetSloWriterEpochVerifier.MaterializationAuthority writers,
            TargetParentControlCreditVerifier.CapacityAuthority parentCredits) {
        this(registrations,keys,history,source,budget,protection,observations,writers,parentCredits,null);
    }

    public TargetCheckpointControlAuthority(ControlTargetRegistrationAuthority registrations,
            TargetQuotaGrantControlVerifier.KeyAuthority keys,HistoryAuthority history,SourceAuthority source,
            BoundedReadBudget budget,Runnable protection,
            com.nereusstream.delay.runtime.TargetSloObservationEventStore.ObservationAuthority observations,
            com.nereusstream.delay.runtime.TargetSloWriterEpochVerifier.MaterializationAuthority writers,
            TargetParentControlCreditVerifier.CapacityAuthority parentCredits,
            TargetScopeControlObligationVerifier.CapacityAuthority scopeObligations) {
        this(registrations,keys,history,source,budget,protection,observations,writers,parentCredits,scopeObligations,null);
    }

    public TargetCheckpointControlAuthority(ControlTargetRegistrationAuthority registrations,
            TargetQuotaGrantControlVerifier.KeyAuthority keys,HistoryAuthority history,SourceAuthority source,
            BoundedReadBudget budget,Runnable protection,
            com.nereusstream.delay.runtime.TargetSloObservationEventStore.ObservationAuthority observations,
            com.nereusstream.delay.runtime.TargetSloWriterEpochVerifier.MaterializationAuthority writers,
            TargetParentControlCreditVerifier.CapacityAuthority parentCredits,
            TargetScopeControlObligationVerifier.CapacityAuthority scopeObligations,
            com.nereusstream.delay.runtime.TargetScopeFeeAuthority scopeFees) {
        this.registrations = Objects.requireNonNull(registrations, "registrations");
        this.keys = Objects.requireNonNull(keys, "keys");
        this.history = Objects.requireNonNull(history, "history");
        this.source = Objects.requireNonNull(source, "source");
        this.budget = Objects.requireNonNull(budget, "budget");
        this.protection = Objects.requireNonNull(protection, "protection");
        this.observations = observations;
        this.writers = writers;
        this.parentCredits = parentCredits;
        this.scopeObligations = scopeObligations;
        this.scopeFees = scopeFees;
    }

    @Override
    public com.nereusstream.delay.runtime.TargetScopeFeeAuthority scopeFeeAuthority(){return scopeFees;}

    @Override
    public PublicKey authenticateObservation(TargetCheckpointDependencies.ControlImage input,TargetResultRecord first,
            com.nereusstream.delay.protocol.TargetSloObservationGeneration generation,
            com.nereusstream.delay.protocol.TargetSloObservationSample sample) {
        return authenticateObservation(input,first,generation,sample,null);
    }

    @Override
    public PublicKey authenticateObservation(TargetCheckpointDependencies.ControlImage input,TargetResultRecord first,
            com.nereusstream.delay.protocol.TargetSloObservationGeneration generation,
            com.nereusstream.delay.protocol.TargetSloObservationSample sample,
            com.nereusstream.delay.protocol.TargetSloWriterEpoch writer) {
        protection.run(); before(); history.requireOriginal(input,first); after();
        if(!first.appliedFirst())return null;
        final var body=com.nereusstream.delay.protocol.TargetSloObservationEventBody.decode(input.image().canonicalBody());
        if(generation==null||sample==null||sample.generation()!=body.generation()||generation.generation()!=body.generation()
                ||!Arrays.equals(sample.bindingDigest(),body.bindingDigest()))throw new IllegalStateException("checkpoint SLO event lost original frozen generation/binding");
        generation.policy().requireSource(input.source());
        if(body.writerEpoch()==null){
            if(writer!=null||generation.policy().requiresWriterEpoch()
                    ||!Arrays.equals(input.image().authorIdentity(),generation.policy().service().canonicalBytes())
                    ||input.image().signingKeyVersion()!=generation.policy().keyVersion())
                throw new IllegalStateException("checkpoint SLO event lost its mandatory original writer epoch");
        }else{
            if(writer==null||!writer.reference().equals(body.writerEpoch()))throw new IllegalStateException("checkpoint body5 lacks its exact original applied writer epoch");
            final var writerAuthority=Objects.requireNonNull(writers,"original checkpoint writer qualification authority");
            before();writerAuthority.requireCurrent();after();
            final var contract=writer.contract();contract.requireSource(input.source());
            if(!generation.policy().scope().equals(contract.scope())||!Arrays.equals(generation.policy().lineage(),contract.lineage())
                    ||!Arrays.equals(input.image().authorIdentity(),contract.service().canonicalBytes())
                    ||input.image().signingKeyVersion()!=contract.keyVersion()
                    ||input.image().retryUntilEpochMs()>contract.maximumRetryUntil()
                    ||input.image().encodeFrame().length>contract.limits().maximumFrameBytes()
                    ||input.source().brokerPersistenceTimeEpochMs()>input.image().retryUntilEpochMs()
                    ||writer.register().appliedAt().source().compareTo(input.source())>=0
                    ||writer.closed()&&writer.close().body().request().fenceAt().source().compareTo(input.source())<=0)
                throw new IllegalStateException("checkpoint body5 changed its actual registered writer/deadline/Fence range");
        }
        final var authority=Objects.requireNonNull(observations,"original checkpoint SLO endpoint/time/history and budget authority");
        final var original=body.isCustody()?body.exportedSample():sample;
        if(body.isCustody()){
            body.custody().requireSample(generation,original);
            before();final var collectorKey=authority.collectorKey(body.custody(),input.source());after();
            if(collectorKey==null||!body.custody().verifySignature(collectorKey))throw new IllegalStateException("checkpoint custody lacks its original independent collector signature");
        }else{
            if(body.isSourceAssigned())body.evidence().requireSample(sample);
            else{
                body.finalObservation().validateAgainst(sample.observation().start());
                if(body.evidence()!=null)body.evidence().requireObservation(sample,body.finalObservation());
            }
        }
        before();authority.requireCurrent();
        if(body.evidence()!=null&&body.evidence().isRecoveryGap())authority.requireRecoveryGap(generation,original,input.image(),input.source());
        else authority.requireAuthorized(generation,original,input.image(),input.source());
        authority.requireCurrent();after();
        before();final var key=keys.resolve(input.image().signingKeyVersion(),input.source());
        if(body.writerEpoch()!=null)writers.requireCurrent();after();
        return key;
    }

    @Override
    public PublicKey authenticate(TargetCheckpointDependencies.ControlImage input, TargetResultRecord first) {
        if(first.appliedFirst()&&com.nereusstream.delay.protocol.TargetSloWriterControlBody.isControl(input.image()))
            throw new IllegalStateException("checkpoint writer Control requires its retained Native62 and independent materialization authority");
        if(first.appliedFirst()&&TargetParentControlCreditBody.isControl(input.image()))
            throw new IllegalStateException("checkpoint parent Control credit requires its retained Native63 and original historical funding authority");
        if(first.appliedFirst()&&TargetScopeControlObligationBody.isControl(input.image()))
            throw new IllegalStateException("checkpoint Scope Control obligation requires its retained Native64 and original historical resource authority");
        final var verified = authenticateControl(input, first);
        return verified == null ? null : verified.key();
    }

    @Override
    public PublicKey authenticateParentCredit(TargetCheckpointDependencies.ControlImage input,TargetResultRecord first,
            TargetParentControlCredit credit){
        final var authenticated=authenticateControl(input,first);
        if(authenticated==null)return null;
        final var actual=Objects.requireNonNull(credit,"APPLIED parent Control lacks its complete retained Native63");
        final var body=TargetParentControlCreditBody.decode(input.image().canonicalBody());
        final var funding=actual.funding();
        if(!body.request().definition().equals(actual.definition())
                ||!Arrays.equals(funding.input().canonicalEnvelope(),input.image().canonicalEnvelope())
                ||!funding.appliedAt().equals(first.mutation())
                ||!Arrays.equals(input.source().canonicalBytes(),funding.appliedAt().source().canonicalBytes()))
            throw new IllegalStateException("checkpoint parent credit changed its actual original funding Control/first/Source");
        if(parentCredits==null)throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable(
                "original checkpoint parent Control credit funding authority is unavailable");
        before();parentCredits.requireCurrent();parentCredits.requireHistoricalAuthorized(actual);parentCredits.requireCurrent();after();
        return authenticated.key();
    }

    @Override
    public PublicKey authenticateScopeObligation(TargetCheckpointDependencies.ControlImage input,TargetResultRecord first,
            TargetScopeControlObligation obligation,TargetParentControlCredit parent){
        final var authenticated=authenticateControl(input,first);
        if(authenticated==null)return null;
        final var actual=Objects.requireNonNull(obligation,"APPLIED Scope Control lacks its complete retained Native64");
        final var payer=Objects.requireNonNull(parent,"APPLIED Scope Control lacks its actual paid Native63 parent");
        payer.requireRoot(actual.definition().originalRoot());
        final var body=TargetScopeControlObligationBody.decode(input.image().canonicalBody());
        final var funding=actual.funding();
        if(!body.request().definition().equals(actual.definition())
                ||!Arrays.equals(funding.input().encodeFrame(),input.image().encodeFrame())
                ||!funding.appliedAt().equals(first.mutation())
                ||!Arrays.equals(input.source().canonicalBytes(),funding.appliedAt().source().canonicalBytes())
                ||!payer.reference().equals(actual.parentReference())||!payer.scope().equals(actual.scope())
                ||!Arrays.equals(payer.lineage(),actual.lineage()))
            throw new IllegalStateException("checkpoint Scope obligation changed its actual original allocation Control/first/Source/parent");
        final var allocation=payer.allocations().stream().filter(value->value.scope().equals(actual.headerReference()))
                .findFirst().orElseThrow(()->new IllegalStateException("checkpoint Scope obligation lost its exact permanent parent allocation"));
        if(!Arrays.equals(allocation.allocationGrantDigest(),actual.allocationGrantDigest())
                ||!allocation.commitment().equals(actual.parentCommitment())||!allocation.allocatedAt().equals(actual.fundedAt()))
            throw new IllegalStateException("checkpoint Scope obligation changed its actual parent grant/commitment/Source");
        if(scopeObligations==null)throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable(
                "original checkpoint Scope Control obligation resource authority is unavailable");
        before();scopeObligations.requireCurrent();scopeObligations.requireHistoricalAuthorized(actual,payer);scopeObligations.requireCurrent();after();
        return authenticated.key();
    }

    @Override
    public PublicKey authenticateWriterEpoch(TargetCheckpointDependencies.ControlImage input,TargetResultRecord first,
            com.nereusstream.delay.protocol.TargetSloWriterEpoch writer,
            com.nereusstream.delay.protocol.TargetSloWriterEpoch predecessor){
        final var authenticated=authenticateControl(input,first);
        if(authenticated==null)return null;
        final var request=com.nereusstream.delay.protocol.TargetSloWriterControlBody.decode(input.image().canonicalBody()).request();
        final var actual=Objects.requireNonNull(writer,"APPLIED writer Control lacks its complete retained Native62");
        final var original=request.contract()!=null?actual.register():Objects.requireNonNull(actual.close(),"APPLIED writer CLOSE lost its original Control");
        if(!Arrays.equals(original.input().canonicalEnvelope(),input.image().canonicalEnvelope())||!original.appliedAt().equals(first.mutation()))
            throw new IllegalStateException("checkpoint writer authority changed its actual original Control/Source");
        final com.nereusstream.delay.protocol.TargetSloWriterEpoch retained;
        if(request.contract()!=null){
            if(request.epoch()==1){
                if(predecessor!=null||request.expectedPriorDigest()!=null)throw new IllegalStateException("first writer REGISTER changed its predecessor");
                retained=null;
            }else{
                if(predecessor==null||!Arrays.equals(predecessor.contractDigest(),request.expectedPriorDigest())
                        ||!Arrays.equals(predecessor.contract().writerIdentityDigest(),actual.contract().writerIdentityDigest())
                        ||predecessor.contract().epoch()+1!=request.epoch()
                        ||predecessor.register().appliedAt().source().compareTo(input.source())>=0)
                    throw new IllegalStateException("checkpoint writer REGISTER lost its original applied predecessor");
                // A later CLOSE was not visible to this original REGISTER's materialization decision.
                retained=predecessor.closed()&&predecessor.close().appliedAt().source().compareTo(input.source())>=0
                        ?new com.nereusstream.delay.protocol.TargetSloWriterEpoch(predecessor.register(),null):predecessor;
            }
        }else retained=new com.nereusstream.delay.protocol.TargetSloWriterEpoch(actual.register(),null);
        final var authority=Objects.requireNonNull(writers,"original checkpoint writer resource/Fence qualification authority");
        before();authority.requireCurrent();
        if(request.fence()!=null)authority.requireFenceAuthorized(request.fence(),request.fenceAt());
        authority.requireAuthorized(request,retained,input.source());authority.requireCurrent();after();
        return authenticated.key();
    }

    private AuthenticatedControl authenticateControl(TargetCheckpointDependencies.ControlImage input,
            TargetResultRecord first) {
        protection.run();
        final var result = SystemMutationResult.decode(first.typedPayload());
        final var image = input.image();
        if (first.kind() != TargetResultRecord.Kind.SYSTEM
                || result.mutationType() != SystemMutationType.APPLY_SHARD_CONTROL
                || !Arrays.equals(first.mutation().source().canonicalBytes(), input.source().canonicalBytes())
                || !Arrays.equals(first.mutation().mutationDigest(), Bytes.sha256(image.canonicalEnvelope()))
                || !Arrays.equals(result.mutationId(), image.systemMutationId())
                || !Arrays.equals(result.mutationHash(), image.mutationHash())
                || !Arrays.equals(result.authorIdentity(), image.authorIdentity())
                || result.retryUntilEpochMs() != image.retryUntilEpochMs()) {
            throw new IllegalArgumentException("Control authentication differs from its original first result");
        }
        before();
        history.requireOriginal(input, first);
        after();
        // An expired/unauthorized input may be rejected before key/registration/RBAC authentication.
        if (result.applyStatus() != ApplyStatus.APPLIED) { return null; }
        final var decoded = SystemMutation.decodeTargetControlFrame(image.encodeFrame());
        final var fields = SystemMutationBodyCodec.fields(decoded.type(), decoded.canonicalBody());
        final var refField = fields.stream().filter(field -> field.number() == 10).findFirst().orElseThrow();
        final var ref = ControlRef.decode(QueryCodecSupport.bytes(refField, 10));
        before();
        final var registered = registrations.find(ref.operationId())
                .orElseThrow(() -> new IllegalStateException("retained APPLIED Control has no immutable registration"));
        if (!budget.tryCharge(ref.operationId().length, registered.canonicalBytes().length)) {
            throw budget.incomplete();
        }
        after();
        final var target = registered.targets().stream()
                .filter(value -> value.targetIndex() == ref.targetIndex())
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Control target absent from registration"));
        ControlTargetMutationBinding.validate(registered, target, decoded);
        if(com.nereusstream.delay.ownership.TargetSloControlScopeAuthority.reference(decoded).isPresent()){
            before();
            try(var member=registrations.captureScopedMutation(registered,target,decoded,budget.maxBytes()-budget.chargedBytes(),this::after)){
                if(member.retainedBytes()>Integer.MAX_VALUE||!budget.tryCharge(ref.operationId().length,(int)member.retainedBytes()))throw budget.incomplete();
                member.validate(registered,target,decoded);after();
            }
        }
        before();
        final var preparedKey = keys.resolve((int) registered.signingKeyVersion(), input.source());
        final var imageKey = keys.resolve(decoded.signingKeyVersion(), input.source());
        after();
        if (preparedKey == null || imageKey == null || !registered.verifySignature(preparedKey)
                || !decoded.verifySignature(imageKey)) {
            throw new IllegalArgumentException("retained APPLIED Control has no authorized original signature");
        }
        final var expectedAuthor = AuthorIdentity.control(registered.author().operationActorIdHash(),
                registered.author().authenticatedRoleSetHash(), registered.author().tenantResourceScopeHash());
        if (!Arrays.equals(expectedAuthor.canonicalBytes(), decoded.authorIdentity())
                || input.source().brokerPersistenceTimeEpochMs() > decoded.retryUntilEpochMs()) {
            throw new IllegalArgumentException("APPLIED Control changes its Prepared author/original retry window");
        }
        before();
        final var authorization = Objects.requireNonNull(source.resolve(registered, input), "Source authorization");
        ControlOperationAuthorization.authorize(registered, authorization.actor(), authorization.scope());
        after();
        return new AuthenticatedControl(registered, imageKey);
    }

    /**
     * Reauthenticates the persisted initial reader/Profile input and every original Control on this image cut.
     * Invoke outside live Store monitors. Snapshot issuer trust/reader set/Profile pins are distinct from Control
     * issuer trust and must be established at the original initial Source, never at the later checkpoint cut.
     */
    public void authenticateSnapshot(TargetCheckpointDependencies dependencies,
            TargetCheckpointSemanticSnapshot snapshot, TargetCheckpointSemanticSnapshot.Limits limits,
            TargetStoreBootstrap.InitialControlAuthority initialAuthority) {
        Objects.requireNonNull(initialAuthority, "initialAuthority");
        if (snapshot.schemaVersion() < 2) {
            throw new IllegalArgumentException("Control authentication requires complete original-Control schema 2");
        }
        snapshot.validateAgainst(dependencies, limits);
        final var initial = dependencies.controls().stream().filter(control -> control.family() == ColumnFamily.META
                && Arrays.equals(control.key(), KeyCodec.metaFixed(10)))
                .map(control -> TargetInitialControlSnapshot.decode(control.payload())).findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Control authentication lacks initial reader snapshot"));
        final var firstResults = new TreeMap<String, TargetResultRecord>();
        for (var first : dependencies.controlResults()) {
            firstResults.put(Bytes.hex(SystemMutationResult.decode(first.typedPayload()).mutationId()), first);
        }
        boolean initialAuthenticated = false;
        for (var input : dependencies.correlateControlImages(snapshot.controls(), limits.maximumBytes())) {
            final var first = firstResults.remove(Bytes.hex(input.image().systemMutationId()));
            before();
            if (!budget.tryCharge(input.source().canonicalBytes().length, input.image().encodeFrame().length)) {
                throw budget.incomplete();
            }
            final boolean isInitial=Arrays.equals(initial.initialGrant().systemMutationId(),input.image().systemMutationId());
            final var authenticated=isInitial?authenticateControl(input,first):null;
            if(!isInitial){
                final var key=dependencies.authenticateInput(input,first,this);
                if(first.appliedFirst()&&(key==null||!input.image().verifySignature(key)))
                    throw new IllegalArgumentException("checkpoint original Control/event lacks its authorized signature");
            }
            if (isInitial) {
                if (authenticated == null) {
                    throw new IllegalArgumentException("initial Control was not APPLIED/authenticated");
                }
                final var original = TargetQuotaGrantControlBody.decode(input.image().canonicalBody());
                if (!Arrays.equals(initial.initialGrant().request().canonicalBytes(),
                        original.request().canonicalBytes())
                        || !initial.initialGrant().controlRef().equals(original.controlRef())
                        || !Arrays.equals(initial.authorIdentity(), input.image().authorIdentity())) {
                    throw new IllegalArgumentException("initial snapshot changes the original signed Shard grant");
                }
                initial.input().requireInitial(dependencies.root().root().scope(),
                        dependencies.root().root().recoveryLineage(), original.request().next(), input.source(),
                        Bytes.sha256(input.image().canonicalEnvelope()));
                before();
                if (!budget.tryCharge(initial.key().length, initial.input().canonicalBytes().length)) {
                    throw budget.incomplete();
                }
                final var key = initialAuthority.authenticate(initial.input(), authenticated.registered(),
                        input.image(), input.source());
                after();
                if (key == null || !initial.input().verifySignature(key)) {
                    throw new IllegalArgumentException("initial reader/control snapshot issuer signature invalid");
                }
                initialAuthenticated = true;
            }
            after();
        }
        if (!initialAuthenticated) {
            throw new IllegalArgumentException("initial reader snapshot has no authenticated original Control");
        }
        TargetScopeNativeUsageAudit.require(dependencies,snapshot.controls(),scopeFees,budget,protection);
        after();
    }

    private record AuthenticatedControl(PreparedControlOperation registered, PublicKey key) {}

    private void before() {
        protection.run();
        if (!budget.beforeRead()) { throw budget.incomplete(); }
    }

    private void after() {
        protection.run();
        if (!budget.beforeTimedWork()) { throw budget.incomplete(); }
    }
}
