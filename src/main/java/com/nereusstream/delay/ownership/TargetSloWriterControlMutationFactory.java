package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import java.security.PrivateKey;
import java.util.List;
import java.util.Objects;

/** Freezes original writer REGISTER/CLOSE Controls through the existing registration and RBAC protocol. */
public final class TargetSloWriterControlMutationFactory {
    private final int keyVersion;
    private final PrivateKey key;
    private final long queryPolicyVersion;
    private final long maximumFrameBytes;

    public TargetSloWriterControlMutationFactory(int keyVersion,PrivateKey key,long queryPolicyVersion,long maximumFrameBytes){
        if(keyVersion==0||queryPolicyVersion==0||maximumFrameBytes<=0||maximumFrameBytes==Long.MAX_VALUE)
            throw new IllegalArgumentException("writer Control requires assigned signing/query and finite frame limits");
        this.keyVersion=keyVersion;this.key=Objects.requireNonNull(key);this.queryPolicyVersion=queryPolicyVersion;this.maximumFrameBytes=maximumFrameBytes;
    }
    /** Signing neither applies a writer registration nor qualifies its Scope resource or TIME_FENCE authority. */
    public Prepared prepare(byte[] operationId,TargetSloWriterControlRequest request,ControlAuthorizationContext authenticated,
            ControlOperationAuthorization.TargetScopeProof scope,long registrationRetryUntil,long sourceRetryUntil){
        return prepare(operationId,request,authenticated,scope,null,registrationRetryUntil,sourceRetryUntil);
    }
    public Prepared prepare(byte[] operationId,TargetSloWriterControlRequest request,ControlAuthorizationContext authenticated,
            ControlOperationAuthorization.TargetScopeProof scope,TargetSloControlScopeHeader.Reference admissionScope,
            long registrationRetryUntil,long sourceRetryUntil){
        final var exact=Objects.requireNonNull(request);final var actor=Objects.requireNonNull(authenticated);
        final var author=new ControlAuthor(actor.actorIdHash(),actor.roleSet().digest(),actor.tenantResourceScopeHash());
        final var operationRequest=exact.operationRequest();
        final var ref=new ControlRef(operationId,PreparedControlOperation.requestHash(exact.operationKind(),operationRequest),0);
        final var body=new TargetSloWriterControlBody(exact.scope().shard(),sourceRetryUntil,ref,exact,
                exact.operationKind()==ControlOperationKind.REGISTER_TARGET_SLO_WRITER_EPOCH?1:0,admissionScope);
        final var image=SystemMutation.signed(body.shard(),SystemMutationType.APPLY_SHARD_CONTROL,sourceRetryUntil,
                body.logicalIdentity(),body.canonicalBytes(),AuthorIdentity.control(author.operationActorIdHash(),author.authenticatedRoleSetHash(),
                        author.tenantResourceScopeHash()).canonicalBytes(),keyVersion,key);
        final var target=new ControlTargetRef(0,ControlTargetKind.SHARD,new ShardSubject(body.shard()),image.systemMutationId(),image.mutationHash());
        final var control=PreparedControlOperation.prepare(operationId,exact.operationKind(),author,operationRequest,List.of(target),
                queryPolicyVersion,registrationRetryUntil,Integer.toUnsignedLong(keyVersion),key);
        ControlOperationAuthorization.authorize(control,actor,Objects.requireNonNull(scope));
        ControlTargetMutationBinding.validate(control,target,image);
        if(image.encodeFrame().length>maximumFrameBytes||control.canonicalBytes().length>maximumFrameBytes
                ||image.canonicalEnvelope().length>TargetSloWriterEpoch.Control.MAX_INPUT_BYTES)
            throw new IllegalArgumentException("writer Control exceeds its original registration/Source envelope bound");
        return new Prepared(control,image);
    }

    /** Retained exact retry inputs. No SERVICE epoch contract is needed to issue its bootstrap Control. */
    public static final class Prepared {
        private final PreparedControlOperation control;
        private final SystemMutation mutation;
        private Prepared(PreparedControlOperation control,SystemMutation mutation){this.control=control;this.mutation=mutation;}
        public PreparedControlOperation control(){return control;}
        public SystemMutation mutation(){return mutation;}
        public static Prepared restore(PreparedControlOperation control,SystemMutation mutation){
            final var image=Objects.requireNonNull(mutation);final var original=Objects.requireNonNull(control);
            if(image.type()!=SystemMutationType.APPLY_SHARD_CONTROL||image.canonicalEnvelope().length>TargetSloWriterEpoch.Control.MAX_INPUT_BYTES)
                throw new IllegalArgumentException("recovered writer Control changed its original protocol/bound");
            final var body=TargetSloWriterControlBody.decode(image.canonicalBody());
            if(original.kind()!=body.request().operationKind()||original.targets().size()!=1)
                throw new IllegalArgumentException("recovered writer Control differs from its original operation");
            ControlTargetMutationBinding.validate(original,original.targets().getFirst(),image);return new Prepared(original,image);
        }
    }
}
