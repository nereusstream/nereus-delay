package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Original Scope Control obligation allocation; these bytes do not authorize payment or Scope birth. */
public final class TargetScopeControlObligationRequest implements ControlOperationRequestBranch {
    public static final int VERSION=1;
    public static final int EXPOSURE_VERSION=3;
    public static final int CONTROL_KIND=33;
    public static final int V2_MAX_CANONICAL_BYTES=3_185_409;
    public static final int MAX_CANONICAL_BYTES=TargetScopeControlObligation.Definition.MAX_CANONICAL_BYTES+64;
    private final TargetScopeControlObligation.Definition definition;
    private final long expectedParentRevision;
    private final byte[] expectedParentDigest;
    private final int expectedRecordState;

    public TargetScopeControlObligationRequest(TargetScopeControlObligation.Definition definition,
            long expectedParentRevision,byte[] expectedParentDigest){
        this(definition,expectedParentRevision,expectedParentDigest,1);
    }
    public TargetScopeControlObligationRequest(TargetScopeControlObligation.Definition definition,
            long expectedParentRevision,byte[] expectedParentDigest,int expectedRecordState){
        this.definition=Objects.requireNonNull(definition);
        if(definition.scope().target()!=null||expectedParentRevision<1||expectedParentRevision==Long.MAX_VALUE||expectedRecordState!=1)
            throw new IllegalArgumentException("Scope Control obligation requires its original Shard, finite parent revision and absent row");
        this.expectedParentRevision=expectedParentRevision;
        this.expectedParentDigest=TargetCompatibilityCodec.assigned(expectedParentDigest,32,"original parent Control credit digest");
        this.expectedRecordState=expectedRecordState;
    }
    public TargetScopeControlObligation.Definition definition(){return definition;}
    public int schemaVersion(){return definition.schemaVersion();}
    public long expectedParentRevision(){return expectedParentRevision;}
    public byte[] expectedParentDigest(){return Bytes.copy(expectedParentDigest);}
    public int expectedRecordState(){return expectedRecordState;}
    public TargetQuotaScope scope(){return definition.scope();}
    public byte[] lineage(){return definition.lineage();}
    public ControlOperationKind operationKind(){return ControlOperationKind.ALLOCATE_SCOPE_CONTROL_OBLIGATION;}
    public ControlOperationRequest operationRequest(){return new ControlOperationRequest(operationKind(),this);}
    public void requireControlRef(ControlRef ref){
        final var exact=Objects.requireNonNull(ref);
        TargetCompatibilityCodec.assigned(exact.operationId(),32,"Scope Control obligation operation ID");
        if(exact.targetIndex()!=0||!Arrays.equals(exact.requestHash(),PreparedControlOperation.requestHash(operationKind(),operationRequest())))
            throw new IllegalArgumentException("Scope Control obligation changed its original Control request");
    }
    @Override public byte[] canonicalBytes(){
        final var encoded=CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,schemaVersion());CanonicalProtobuf.bytes(out,2,definition.canonicalBytes());
            CanonicalProtobuf.uint64(out,3,expectedParentRevision);CanonicalProtobuf.bytes(out,4,expectedParentDigest);
            CanonicalProtobuf.uint32(out,5,expectedRecordState);
        });
        if(encoded.length>(schemaVersion()==EXPOSURE_VERSION?MAX_CANONICAL_BYTES:V2_MAX_CANONICAL_BYTES))
            throw new IllegalArgumentException("Scope Control obligation request exceeds its codec bound");
        return encoded;
    }
    public static TargetScopeControlObligationRequest decode(byte[] encoded){
        final var fields=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,5,false,"Target Scope Control obligation request");
        QueryCodecSupport.requireNumbers(fields,new int[]{1,2,3,4,5},"Target Scope Control obligation request");
        final int version=QueryCodecSupport.uint32(fields.getFirst(),1);
        if(version!=VERSION&&version!=EXPOSURE_VERSION)throw new IllegalArgumentException("unknown Scope Control obligation request schema");
        final var result=new TargetScopeControlObligationRequest(TargetScopeControlObligation.Definition.decode(QueryCodecSupport.bytes(fields.get(1),2)),
                QueryCodecSupport.uint64Bits(fields.get(2),3),QueryCodecSupport.fixed(fields.get(3),4,32),QueryCodecSupport.uint32(fields.get(4),5));
        if(result.schemaVersion()!=version)throw new IllegalArgumentException("Scope Control obligation request changed its original schema");
        QueryCodecSupport.requireCanonical(encoded,result.canonicalBytes(),"Target Scope Control obligation request");return result;
    }
    @Override public boolean equals(Object other){return other instanceof TargetScopeControlObligationRequest request
            &&Arrays.equals(canonicalBytes(),request.canonicalBytes());}
    @Override public int hashCode(){return Arrays.hashCode(canonicalBytes());}
}
