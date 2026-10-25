package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Original parent Control credit allocation request; its bytes do not prove that funding has been paid. */
public final class TargetParentControlCreditRequest implements ControlOperationRequestBranch {
    public static final int VERSION=1;
    public static final int CONTROL_KIND=32;
    public static final int MAX_CANONICAL_BYTES=2+6+TargetParentControlCredit.Definition.MAX_CANONICAL_BYTES+2;
    private final TargetParentControlCredit.Definition definition;
    private final int expectedRecordState;

    public TargetParentControlCreditRequest(TargetParentControlCredit.Definition definition){this(definition,1);}
    public TargetParentControlCreditRequest(TargetParentControlCredit.Definition definition,int expectedRecordState){
        this.definition=Objects.requireNonNull(definition);
        if(definition.scope().target()!=null||expectedRecordState!=1)
            throw new IllegalArgumentException("parent Control credit requires its original Shard scope and absent row");
        this.expectedRecordState=expectedRecordState;
    }
    public TargetParentControlCredit.Definition definition(){return definition;}
    public TargetQuotaScope scope(){return definition.scope();}
    public byte[] lineage(){return definition.lineage();}
    public int expectedRecordState(){return expectedRecordState;}
    public ControlOperationKind operationKind(){return ControlOperationKind.ALLOCATE_PARENT_CONTROL_CREDIT;}
    public ControlOperationRequest operationRequest(){return new ControlOperationRequest(operationKind(),this);}
    public void requireControlRef(ControlRef ref){
        final var exact=Objects.requireNonNull(ref);
        TargetCompatibilityCodec.assigned(exact.operationId(),32,"parent Control credit operation ID");
        if(exact.targetIndex()!=0||!Arrays.equals(exact.requestHash(),PreparedControlOperation.requestHash(operationKind(),operationRequest())))
            throw new IllegalArgumentException("parent Control credit changed its original Control request");
    }
    @Override public byte[] canonicalBytes(){
        final var encoded=CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint32(out,1,VERSION);CanonicalProtobuf.bytes(out,2,definition.canonicalBytes());
            CanonicalProtobuf.uint32(out,3,expectedRecordState);
        });
        if(encoded.length>MAX_CANONICAL_BYTES)throw new IllegalArgumentException("parent Control credit request exceeds its codec bound");
        return encoded;
    }
    public static TargetParentControlCreditRequest decode(byte[] encoded){
        final var fields=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,3,false,"Target parent Control credit request");
        QueryCodecSupport.requireNumbers(fields,new int[]{1,2,3},"Target parent Control credit request");
        if(QueryCodecSupport.uint32(fields.getFirst(),1)!=VERSION)throw new IllegalArgumentException("unknown parent Control credit request schema");
        final var result=new TargetParentControlCreditRequest(TargetParentControlCredit.Definition.decode(QueryCodecSupport.bytes(fields.get(1),2)),
                QueryCodecSupport.uint32(fields.get(2),3));
        QueryCodecSupport.requireCanonical(encoded,result.canonicalBytes(),"Target parent Control credit request");return result;
    }
    @Override public boolean equals(Object other){return other instanceof TargetParentControlCreditRequest request
            &&Arrays.equals(canonicalBytes(),request.canonicalBytes());}
    @Override public int hashCode(){return Arrays.hashCode(canonicalBytes());}
}
