package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** New Target certification Control payloads; the original ApplyShardControl envelope stays unchanged. */
public record TargetSloObservationControlBody(ShardId shard, long retryUntil, ControlRef controlRef,
        TargetSloObservationControlRequest request, int expectedRecordState,TargetSloControlScopeHeader.Reference admissionScope) {
    public static final int MAX_CANONICAL_BYTES = TargetSloObservationControlRequest.MAX_CANONICAL_BYTES + 192
            +TargetSloControlScopeHeader.Reference.MAX_CANONICAL_BYTES+8;
    public static final int MAX_V1_CANONICAL_BYTES=TargetSloObservationControlRequest.MAX_V1_CANONICAL_BYTES+192;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-slo-observation-control\0");
    public TargetSloObservationControlBody(ShardId shard,long retryUntil,ControlRef controlRef,
            TargetSloObservationControlRequest request,int expectedRecordState){
        this(shard,retryUntil,controlRef,request,expectedRecordState,null);
    }

    public TargetSloObservationControlBody {
        Objects.requireNonNull(shard); Objects.requireNonNull(controlRef); Objects.requireNonNull(request);
        if(admissionScope!=null&&request.policy()!=null&&!request.policy().requiresWriterEpoch())
            throw new IllegalArgumentException("scoped certification ENABLE requires its original finite writer epoch contract");
        TargetCompatibilityCodec.assigned(controlRef.operationId(), 32, "Target SLO Control operation ID");
        if (retryUntil < 0 || controlRef.targetIndex() != 0 || !shard.equals(request.scope().shard())
                || !Arrays.equals(controlRef.requestHash(), PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()))
                || expectedRecordState != (request.expectedPriorDigest() == null ? 1 : 0)) {
            throw new IllegalArgumentException("Target SLO Control changed registered Source/request/precondition");
        }
    }

    public byte[] logicalIdentity() { return controlRef.logicalOperationIdentity(request.controlKind()); }
    public int schemaVersion(){return admissionScope!=null?3:request.policy()!=null&&request.policy().requiresWriterEpoch()?2:1;}
    public ProtocolTuple tuple(){return schemaVersion()==3?ProtocolTuple.targetSloScopedObservation():schemaVersion()==2?ProtocolTuple.targetSloObservationEpochPolicy():ProtocolTuple.targetSloObservation();}
    public byte[] semanticHash() { return admissionScope==null?Bytes.sha256(DOMAIN, Bytes.u16be(request.controlKind()), request.canonicalBytes())
            :Bytes.sha256(DOMAIN,Bytes.u16be(3),Bytes.u16be(request.controlKind()),request.canonicalBytes(),admissionScope.canonicalBytes()); }

    public static boolean isControl(SystemMutation input) {
        if (input.type() != SystemMutationType.APPLY_SHARD_CONTROL) return false;
        final var reader = new CanonicalProtobuf.Reader(input.canonicalBody());
        for (int count = 0; count < 5 && reader.hasRemaining(); count++) {
            final var field = reader.next();
            if (field.number() == 11) {
                if (field.wireType() != 0) throw new IllegalArgumentException("Target SLO Control discriminator is not a varint");
                return field.unsignedValue() == TargetSloObservationControlRequest.ENABLE_CONTROL_KIND
                        || field.unsignedValue() == TargetSloObservationControlRequest.STOP_CONTROL_KIND;
            }
        }
        return false;
    }

    public byte[] canonicalBytes() {
        final var encoded = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, new ShardSubject(shard).canonicalBytes());
            CanonicalProtobuf.uint32(out, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.uint64Bits(out, 3, retryUntil);
            CanonicalProtobuf.bytes(out, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(out, 11, request.controlKind());
            CanonicalProtobuf.uint32(out, 12, schemaVersion());
            CanonicalProtobuf.bytes(out, 13, semanticHash());
            CanonicalProtobuf.uint32(out, 14, expectedRecordState);
            CanonicalProtobuf.bytes(out, 15, CanonicalProtobuf.message(payload ->
                    CanonicalProtobuf.bytes(payload, request.controlKind(), request.canonicalBytes())));
            if(admissionScope!=null)CanonicalProtobuf.bytes(out,16,admissionScope.canonicalBytes());
        });
        if (encoded.length > (schemaVersion()==1?MAX_V1_CANONICAL_BYTES:MAX_CANONICAL_BYTES)) throw new IllegalArgumentException("Target SLO Control body exceeds bound");
        return encoded;
    }

    public static TargetSloObservationControlBody decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 10, false, "Target SLO Control body");
        if(f.size()!=9&&f.size()!=10)throw new IllegalArgumentException("unknown Target SLO Control field count");
        QueryCodecSupport.requireNumbers(f,f.size()==9?new int[]{1,2,3,10,11,12,13,14,15}:new int[]{1,2,3,10,11,12,13,14,15,16},"Target SLO Control body");
        final int kind = QueryCodecSupport.uint32(f.get(4), 11);
        final ControlOperationKind operation = switch (kind) {
            case TargetSloObservationControlRequest.ENABLE_CONTROL_KIND -> ControlOperationKind.ENABLE_TARGET_SLO_CERTIFICATION;
            case TargetSloObservationControlRequest.STOP_CONTROL_KIND -> ControlOperationKind.STOP_TARGET_SLO_CERTIFICATION;
            default -> throw new IllegalArgumentException("unknown Target SLO Control kind");
        };
        if (QueryCodecSupport.uint32(f.get(1), 2) != SystemMutationType.APPLY_SHARD_CONTROL.wireValue()
                || QueryCodecSupport.uint32(f.get(5),12)<1||QueryCodecSupport.uint32(f.get(5),12)>3) throw new IllegalArgumentException("unknown Target SLO Control type/schema");
        final var shard = QueryCodecSupport.bytes(f.getFirst(), 1);
        TargetCompatibilityCodec.read(shard, 24, 2, false, "Target SLO Shard subject");
        final var ref = QueryCodecSupport.bytes(f.get(3), 10);
        TargetCompatibilityCodec.read(ref, 74, 3, false, "Target SLO ControlRef");
        final var payload = TargetCompatibilityCodec.read(QueryCodecSupport.bytes(f.get(8), 15),
                TargetSloObservationControlRequest.MAX_CANONICAL_BYTES + 8, 1, false, "Target SLO Control payload");
        QueryCodecSupport.requireNumbers(payload, new int[]{kind}, "Target SLO Control payload");
        final var result = new TargetSloObservationControlBody(ShardSubject.decode(shard).shardId(),
                QueryCodecSupport.uint64Bits(f.get(2), 3), ControlRef.decode(ref),
                TargetSloObservationControlRequest.decode(operation, QueryCodecSupport.bytes(payload.getFirst(), kind)),
                QueryCodecSupport.uint32(f.get(7), 14),f.size()==9?null:TargetSloControlScopeHeader.Reference.decode(QueryCodecSupport.bytes(f.get(9),16)));
        if(QueryCodecSupport.uint32(f.get(5),12)!=result.schemaVersion())throw new IllegalArgumentException("Target certification Control schema changed its frozen writer policy");
        if (!Arrays.equals(result.semanticHash(), QueryCodecSupport.fixed(f.get(6), 13, 32))) {
            throw new IllegalArgumentException("Target SLO Control semantic hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target SLO Control body");
        return result;
    }
}
