package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.Objects;

/** Original Source-applied writer registration and optional closure. Hashes do not grant writer or fence authority. */
public record TargetSloWriterEpoch(Control register,Control close) {
    public static final int VALUE_TYPE=62;
    public static final int MAX_CANONICAL_BYTES=2+2*(6+Control.MAX_CANONICAL_BYTES)+34;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-slo-writer-epoch\0");
    private static final byte[] CONTRACT_DOMAIN=Bytes.utf8("nereus-delay-target-slo-applied-writer-contract\0");

    /** Complete original signed Control and its actual Source stamp; registration and first/POS are authenticated separately. */
    public record Control(SystemMutation input,TargetQuotaMutation appliedAt) {
        public static final int MAX_INPUT_BYTES=TargetSloWriterControlBody.MAX_CANONICAL_BYTES
                +(TargetSloObservationGeneration.Control.MAX_INPUT_BYTES-TargetSloObservationControlBody.MAX_CANONICAL_BYTES);
        public static final int MAX_CANONICAL_BYTES=2+(6+MAX_INPUT_BYTES)+(6+TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES);
        public Control {
            Objects.requireNonNull(input);Objects.requireNonNull(appliedAt).requireSourceApplied();
            if(input.type()!=SystemMutationType.APPLY_SHARD_CONTROL||input.canonicalEnvelope().length>MAX_INPUT_BYTES)
                throw new IllegalArgumentException("writer epoch requires its bounded original ApplyShardControl");
            final var body=TargetSloWriterControlBody.decode(input.canonicalBody());
            if(!Arrays.equals(input.logicalOperationIdentity(),body.logicalIdentity())||!input.shardId().equals(body.shard())
                    ||!body.shard().equals(appliedAt.source().shardId())||input.retryUntilEpochMs()!=body.retryUntil()
                    ||appliedAt.source().brokerPersistenceTimeEpochMs()>body.retryUntil()
                    ||!Arrays.equals(appliedAt.mutationDigest(),Bytes.sha256(input.canonicalEnvelope()))
                    ||AuthorIdentity.decode(input.authorIdentity()).kind()!=AuthorIdentity.Kind.CONTROL)
                throw new IllegalArgumentException("writer epoch changed its exact original applied Control input/stamp");
        }
        public TargetSloWriterControlBody body(){return TargetSloWriterControlBody.decode(input.canonicalBody());}
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,input.canonicalEnvelope());
            CanonicalProtobuf.bytes(out,3,appliedAt.canonicalBytes());
        });}
        public static Control decode(byte[] encoded){
            final var f=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,3,false,"Target SLO writer applied Control");
            QueryCodecSupport.requireNumbers(f,new int[]{1,2,3},"Target SLO writer applied Control");
            if(QueryCodecSupport.uint32(f.getFirst(),1)!=1)throw new IllegalArgumentException("unknown applied writer Control schema");
            final var input=QueryCodecSupport.bytes(f.get(1),2);
            if(input.length>MAX_INPUT_BYTES)throw new IllegalArgumentException("original writer Control exceeds its codec bound");
            final var value=new Control(SystemMutation.decodeTargetEnvelope(input,SystemMutationType.APPLY_SHARD_CONTROL),
                    TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(2),3)));
            QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Target SLO writer applied Control");return value;
        }
    }

    public TargetSloWriterEpoch {
        Objects.requireNonNull(register);
        final var registered=register.body().request();
        if(registered.operationKind()!=ControlOperationKind.REGISTER_TARGET_SLO_WRITER_EPOCH)
            throw new IllegalArgumentException("writer epoch requires its original REGISTER");
        final var contract=registered.contract();
        contract.requireSource(register.appliedAt().source());
        if(register.appliedAt().source().compareTo(contract.sourceAnchor())<=0)
            throw new IllegalArgumentException("writer REGISTER does not follow its frozen physical Source anchor");
        if(close!=null){
            final var closed=close.body().request();
            if(closed.operationKind()!=ControlOperationKind.CLOSE_TARGET_SLO_WRITER_EPOCH
                    ||!contract.scope().equals(closed.scope())||!Arrays.equals(contract.lineage(),closed.lineage())
                    ||!reference(register).equals(closed.reference()))
                throw new IllegalArgumentException("writer CLOSE changed its original applied registration");
            contract.requireSource(closed.fenceAt().source());contract.requireSource(close.appliedAt().source());
            closed.fenceAt().requireAfter(register.appliedAt());close.appliedAt().requireAfter(closed.fenceAt());
            final var fence=TargetTimeFenceBody.decode(closed.fence().canonicalBody());
            if(fence.closeThrough()<contract.maximumRetryUntil()
                    ||fence.fenceKeyVersion()!=closed.fence().signingKeyVersion()
                    ||closed.fenceAt().source().brokerPersistenceTimeEpochMs()>closed.fence().retryUntilEpochMs())
                throw new IllegalArgumentException("writer CLOSE lacks an original subsequent TIME_FENCE covering its absolute deadline");
        }
    }
    public static TargetSloWriterEpoch registered(SystemMutation input,TargetQuotaMutation at){return new TargetSloWriterEpoch(new Control(input,at),null);}
    public TargetSloWriterEpoch closed(SystemMutation input,TargetQuotaMutation at){
        if(close!=null)throw new IllegalStateException("writer epoch is already closed");
        return new TargetSloWriterEpoch(register,new Control(input,at));
    }
    public boolean closed(){return close!=null;}
    public TargetSloWriterContract contract(){return register.body().request().contract();}
    /** Stable applied registration digest; the pure declaration digest and later CLOSE have distinct domains. */
    public byte[] contractDigest(){return contractDigest(register);}
    private static byte[] contractDigest(Control register){return Bytes.sha256(CONTRACT_DOMAIN,register.canonicalBytes());}
    public TargetSloWriterContract.Reference reference(){return reference(register);}
    private static TargetSloWriterContract.Reference reference(Control register){
        final var contract=register.body().request().contract();
        return new TargetSloWriterContract.Reference(contract.writerIdentityDigest(),contract.epoch(),contractDigest(register));
    }
    public TargetQuotaMutation mutation(){return (close==null?register:close).appliedAt();}
    public byte[] key(){return reference().key(contract().scope());}
    public static byte[] key(TargetQuotaScope scope,byte[] writerIdentityDigest,long epoch){return TargetSloWriterContract.epochKey(scope,writerIdentityDigest,epoch);}
    private byte[] fields(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,register.canonicalBytes());
        if(close!=null)CanonicalProtobuf.bytes(out,3,close.canonicalBytes());
    });}
    public byte[] digest(){return Bytes.sha256(DOMAIN,fields());}
    public byte[] canonicalBytes(){
        final var bytes=Bytes.concat(fields(),CanonicalProtobuf.message(out->CanonicalProtobuf.bytes(out,4,digest())));
        if(bytes.length>MAX_CANONICAL_BYTES)throw new IllegalArgumentException("writer epoch exceeds its closed codec bound");return bytes;
    }
    public long storedBytes(){return Math.addExact((long)key().length,TargetValueEnvelope.encode(VALUE_TYPE,canonicalBytes()).length);}
    public static TargetSloWriterEpoch decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,4,false,"Target SLO writer epoch");
        final boolean closed=f.size()==4;
        QueryCodecSupport.requireNumbers(f,closed?new int[]{1,2,3,4}:new int[]{1,2,4},"Target SLO writer epoch");
        if(QueryCodecSupport.uint32(f.getFirst(),1)!=1)throw new IllegalArgumentException("unknown writer epoch schema");
        final var value=new TargetSloWriterEpoch(Control.decode(QueryCodecSupport.bytes(f.get(1),2)),closed?Control.decode(QueryCodecSupport.bytes(f.get(2),3)):null);
        if(!Arrays.equals(value.digest(),QueryCodecSupport.fixed(f.getLast(),4,32)))throw new IllegalArgumentException("writer epoch digest differs");
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Target SLO writer epoch");return value;
    }
    public static TargetSloWriterEpoch decodeForStore(byte[] key,byte[] encoded,TargetQuotaScope scope,byte[] lineage){
        final var value=decode(encoded);final var contract=value.contract();
        if(!Arrays.equals(key,value.key())||!scope.equals(contract.scope())||!Arrays.equals(lineage,contract.lineage()))
            throw new IllegalArgumentException("writer epoch changed its exact key/scope/recovery lineage");return value;
    }
}
