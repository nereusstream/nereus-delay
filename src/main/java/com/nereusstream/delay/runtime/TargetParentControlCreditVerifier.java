package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Objects;

/** Original bootstrap Control authentication and actual SHARD read set for paid parent credit creation. */
public final class TargetParentControlCreditVerifier {
    private TargetParentControlCreditVerifier() {}

    /** Original protected Control signing keys, authorized at this exact physical Source position. */
    @FunctionalInterface public interface KeyAuthority {
        PublicKey resolve(long signingKeyVersion,SourcePosition originalSource);
    }

    /**
     * Already-held complete tenant policy, placement, physical reservation and original funding entitlement.
     * Prove that this exact funding slice is exclusive and actually paid for this complete Native batch.
     * A hash, declared usage or NON_OUTCOME_CONTROL grant alone is not Native capacity authority.
     * Methods recheck retained local protection only; acquisition and remote I/O precede the Store view.
     */
    public interface CapacityAuthority {
        default void requireHistoricalAuthorized(com.nereusstream.delay.protocol.TargetParentControlCredit credit){
            throw new TargetSourceAuthorityPreparation.InputUnavailable("original historical paid parent credit authority is unavailable");
        }
        default void requireAuthorized(TargetParentControlCredit.Definition definition,View view,
                SourcePosition source,TargetStoreBackend.Mutation complete) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable("original parent Control credit funding authority is unavailable");
        }
        void requireCurrent();
    }

    public record Authority(ControlTargetRegistrationAuthority registrations,KeyAuthority keys,
            ControlAuthorizationContext actor,ControlOperationAuthorization.TargetScopeProof scopeProof,
            CapacityAuthority capacity) {
        public Authority {
            Objects.requireNonNull(registrations);Objects.requireNonNull(keys);Objects.requireNonNull(actor);
            Objects.requireNonNull(scopeProof);Objects.requireNonNull(capacity);
        }
    }

    /** Exact records read under the original Store guard. A constructed view grants no funding permission. */
    public record View(TargetQuotaIncarnation actualRoot,TargetQuotaGrantActivation actualGrant,
            TargetQuotaAggregate actualAggregate,TargetQuotaMutation priorCut) {
        public View {
            Objects.requireNonNull(actualRoot);Objects.requireNonNull(actualGrant);Objects.requireNonNull(actualAggregate);Objects.requireNonNull(priorCut);
            if(actualRoot.identity().kind()!=TargetQuotaIdentity.Kind.SHARD||!actualRoot.scope().equals(actualGrant.grant().scope())
                    ||!actualRoot.identity().shard().equals(actualAggregate.shard())
                    ||!Arrays.equals(actualRoot.identity().accountingIncarnation(),actualAggregate.accountingIncarnation())
                    ||!Arrays.equals(actualRoot.accounting().canonicalBytes(),actualGrant.grant().accounting().canonicalBytes())
                    ||!priorCut.equals(actualAggregate.mutation()))
                throw new IllegalStateException("parent Control credit read set changed its actual SHARD Root/grant/cut");
            actualRoot.latestMutation().requireAtOrBefore(priorCut);actualGrant.mutation().requireAtOrBefore(priorCut);
        }
    }

    public record Change(TargetParentControlCreditBody body,View view) {
        public Change {Objects.requireNonNull(body);Objects.requireNonNull(view);}
    }

    public static Change verifyFirstApplication(TargetStoreBackend.Reader reader,TargetQuotaScope scope,
            byte[] lineage,PreparedControlOperation control,SystemMutation input,SourcePosition source,Authority authority) {
        Objects.requireNonNull(reader);Objects.requireNonNull(scope);Objects.requireNonNull(control);Objects.requireNonNull(input);
        Objects.requireNonNull(authority);TargetSourcePosition.requireBounded(source);authority.capacity().requireCurrent();
        final var body=TargetParentControlCreditBody.decode(input.canonicalBody());final var request=body.request();
        if(scope.target()!=null||!scope.equals(request.scope())||!Arrays.equals(lineage,request.lineage())
                ||!scope.shard().equals(source.shardId())||!scope.shard().equals(input.shardId())
                ||input.type()!=SystemMutationType.APPLY_SHARD_CONTROL||input.retryUntilEpochMs()!=body.retryUntil()
                ||!Arrays.equals(input.logicalOperationIdentity(),body.logicalIdentity())
                ||control.kind()!=ControlOperationKind.ALLOCATE_PARENT_CONTROL_CREDIT
                ||!request.operationRequest().equals(control.request())||control.targets().size()!=1
                ||input.canonicalEnvelope().length>TargetParentControlCreditBody.MAX_INPUT_BYTES)
            throw denied("parent Control credit changed its registered Source/request scope");
        if(Long.compareUnsigned(source.brokerPersistenceTimeEpochMs(),body.retryUntil())>0)
            throw new CommandResolutionException(StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED,
                    "parent Control credit exceeded its original Source retry window");
        final var controlKey=authority.keys().resolve(control.signingKeyVersion(),source);
        final var inputKey=authority.keys().resolve(Integer.toUnsignedLong(input.signingKeyVersion()),source);
        if(controlKey==null||inputKey==null||!control.verifySignature(controlKey)||!input.verifySignature(inputKey))
            throw denied("parent Control credit lacks its original signatures");
        final var author=AuthorIdentity.decode(input.authorIdentity());
        if(author.kind()!=AuthorIdentity.Kind.CONTROL||!Arrays.equals(author.first(),control.author().operationActorIdHash())
                ||!Arrays.equals(author.second(),control.author().authenticatedRoleSetHash())
                ||!Arrays.equals(author.digest(),control.author().tenantResourceScopeHash()))
            throw denied("parent Control credit Source author differs from its original Control actor");
        try {
            ControlOperationAuthorization.authorize(control,authority.actor(),authority.scopeProof());
            ControlTargetMutationBinding.validate(control,control.targets().getFirst(),input);
        } catch(IllegalArgumentException rejected) {throw denied(rejected.getMessage());}
        final var registered=authority.registrations().find(control.operationId()).orElse(null);
        if(registered==null||!Arrays.equals(control.canonicalBytes(),registered.canonicalBytes()))
            throw denied("parent Control credit lacks its exact original point registration");
        authority.registrations().validateMutation(control,control.targets().getFirst(),input);

        final var activation=TargetProtocolActivationStore.applied(reader,scope,lineage,body.tuple());
        if(activation==null||activation.appliedAt().source().compareTo(source)>=0)
            throw new CommandResolutionException(StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION,
                    "parent Control credit requires its independent prior tuple-23 activation");
        final var view=readView(reader,scope,lineage);final var definition=request.definition();
        try {definition.requireRoot(view.actualRoot());definition.requireSource(source);definition.requireSource(view.priorCut().source());}
        catch(IllegalArgumentException foreign) {throw denied(foreign.getMessage());}
        if(view.actualRoot().draining())throw stale("draining SHARD Root cannot admit new parent Control credit");
        if(!Arrays.equals(definition.expectedPayerGrant().canonicalBytes(),view.actualGrant().grant().canonicalBytes()))
            throw stale("parent Control credit changed its exact current Native SHARD payer grant");
        if(source.compareTo(view.priorCut().source())<=0||source.compareTo(definition.sourceAnchor())<=0
                ||definition.sourceAnchor().compareTo(view.priorCut().source())>0)
            throw stale("parent Control credit does not follow its original physical Source anchor/current cut");
        if(reader.get(ColumnFamily.META,definition.reference().key())!=null)
            throw stale("parent Control credit original funding identity already has a retained row");
        authority.capacity().requireCurrent();reader.requireWithinElapsedBudget();return new Change(body,view);
    }

    /** Actual guarded reads, also reusable by a cold-path caller before its independent original-input checks. */
    public static View readView(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage) {
        Objects.requireNonNull(reader);Objects.requireNonNull(scope);Bytes.requireLength(lineage,16,"parent Control credit lineage");
        if(scope.target()!=null||!scope.shard().equals(reader.shardId()))
            throw new IllegalArgumentException("parent Control credit requires its actual SHARD Store");
        final var aggregate=reader.aggregate();final var cut=Objects.requireNonNull(aggregate.mutation(),"parent Control credit lacks an actual aggregate cut");
        if(reader.source()==null||cut.sequence()!=reader.sourceSequence()
                ||!Arrays.equals(cut.source().canonicalBytes(),reader.source().canonicalBytes()))
            throw new IllegalStateException("parent Control credit aggregate differs from the actual Source frontier");
        final var owner=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),aggregate.accountingIncarnation(),null,null);
        final byte[] rootKey=owner.key();rootKey[0]=TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var root=TargetQuotaIncarnation.decodeForStore(rootKey,payload(reader,rootKey,TargetQuotaIncarnation.VALUE_TYPE),scope.shard(),scope.tenantScope());
        final var bookkeeping=TargetQuotaBookkeeping.decode(payload(reader,TargetQuotaBookkeeping.keyFor(scope.shard()),TargetQuotaBookkeeping.VALUE_TYPE));
        if(!root.identity().equals(owner)||!root.scope().equals(scope)||!Arrays.equals(root.recoveryLineage(),lineage)
                ||!bookkeeping.owner().equals(owner)||!Arrays.equals(bookkeeping.tenantScope(),scope.tenantScope())
                ||!Arrays.equals(bookkeeping.accounting().canonicalBytes(),root.accounting().canonicalBytes()))
            throw new IllegalStateException("parent Control credit changed actual Root/bookkeeping tenant/accounting/lineage");
        bookkeeping.mutation().requireAtOrBefore(cut);
        final byte[] grantKey=Bytes.concat(new byte[]{TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG,TargetKeyCodec.KEY_FORMAT},scope.keySuffix());
        final var grant=TargetQuotaGrantActivation.decodeForStore(grantKey,payload(reader,grantKey,TargetQuotaGrantActivation.VALUE_TYPE),scope.shard(),scope.tenantScope());
        final var view=new View(root,grant,aggregate,cut);reader.requireWithinElapsedBudget();return view;
    }

    /** Complete assembly includes paid credit/future capacity, actual STATE and original first/POS charges. */
    public static void requireComplete(Change change,TargetStoreBackend.Mutation complete,SourcePosition source,Authority authority) {
        Objects.requireNonNull(change);Objects.requireNonNull(complete);Objects.requireNonNull(authority);TargetSourcePosition.requireBounded(source);
        authority.capacity().requireCurrent();
        final var view=change.view();final var definition=change.body().request().definition();final var quota=complete.quota();final var counters=quota.counters();
        final var stamp=counters.mutation();stamp.requireSourceApplied();stamp.requireAfter(view.priorCut());
        definition.requireRoot(view.actualRoot());definition.requireSource(source);
        if(view.actualRoot().draining()||!quota.shardScope().equals(definition.scope())
                ||!Arrays.equals(counters.priorAggregate().canonicalBytes(),view.actualAggregate().canonicalBytes())
                ||counters.expectedMutationSequence()!=view.priorCut().sequence()
                ||!Arrays.equals(stamp.source().canonicalBytes(),source.canonicalBytes())
                ||!Arrays.equals(counters.nextAggregate().accountingIncarnation(),view.actualRoot().identity().accountingIncarnation()))
            throw new IllegalStateException("parent Control credit complete batch changed its original actual read set/Source");
        if(!view.actualGrant().grant().limit().permitsGrowth(view.actualAggregate().usage(),counters.nextAggregate().usage()))
            throw new CommandResolutionException(StableCode.HARD_QUOTA_EXCEEDED,"Native SHARD grant cannot pay the complete parent Control credit batch");
        authority.capacity().requireAuthorized(definition,view,source,complete);authority.capacity().requireCurrent();
    }

    private static byte[] payload(TargetStoreBackend.Reader reader,byte[] key,int type) {
        final var raw=Objects.requireNonNull(reader.get(ColumnFamily.META,key),"parent Control credit lacks its actual retained payer record");
        return TargetValueEnvelope.decode(raw,type).payload();
    }
    private static CommandResolutionException denied(String message) {return new CommandResolutionException(StableCode.UNAUTHORIZED_SYSTEM_MUTATION,message);}
    private static CommandResolutionException stale(String message) {return new CommandResolutionException(StableCode.STALE_SYSTEM_MUTATION,message);}
}
