package com.nereusstream.delay.protocol;

import java.security.KeyFactory;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Objects;

/** Original bounded closure inputs. Embedded public bytes never replace the independent original Route/Source pin. */
public record TargetNativeReclaimFacts(TargetInlinePayloadSourceFacts.Frame fence,byte[] route,byte[] routeKey) {
    public static final int SCHEMA_VERSION=1;
    public static final int MAX_ROUTE_BYTES=16*1024*1024;
    public static final int MAX_BYTES=MAX_ROUTE_BYTES+TargetTimeFenceBody.MAX_CANONICAL_BYTES+(1<<20)+TargetQuotaMutation.MAX_CANONICAL_BYTES+8192;
    public TargetNativeReclaimFacts {
        Objects.requireNonNull(fence);
        if(fence.kind()!=2||SystemMutation.decodeEnvelope(fence.frame()).type()!=SystemMutationType.TIME_FENCE)
            throw new IllegalArgumentException("Native reclaim requires its original Source TimeFence");
        if((route==null)!=(routeKey==null)||route!=null&&(route.length==0||route.length>MAX_ROUTE_BYTES))
            throw new IllegalArgumentException("bounded complete original Route and public material required together");
        route=route==null?null:Bytes.copy(route);routeKey=routeKey==null?null:TargetCompatibilityCodec.assigned(routeKey,44,"original Route public key material");
        if(route!=null)parseRoute(route,routeKey);
    }
    @Override public byte[] route(){return route==null?null:Bytes.copy(route);}
    @Override public byte[] routeKey(){return routeKey==null?null:Bytes.copy(routeKey);}
    public RouteSnapshot originalRoute(){if(route==null)throw new IllegalStateException("record reclaim lacks original frozen Route retention");return parseRoute(route,routeKey);}
    private static RouteSnapshot parseRoute(byte[] bytes,byte[] key){
        try{return RouteSnapshot.decode(bytes,KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(key)));}
        catch(java.security.GeneralSecurityException invalid){throw new IllegalArgumentException("invalid original Route public material",invalid);}
    }
    public void requireFor(TargetResourceReclaimBody body){
        if(!body.nativeRetainedReclaim())throw new IllegalArgumentException("Native retained operation group required");
        fence.appliedAt().requireCoveredByFloor(body.floor());
        if(body.operations().stream().anyMatch(o->o instanceof TargetResourceReclaimBody.RetainedRecordDelete)){
            final var original=originalRoute();
            if(!original.routeIncarnation().equals(body.shard().routeIncarnation())||!Arrays.equals(original.tenantRoutingScope(),body.scope().tenantScope()))
                throw new IllegalArgumentException("record reclaim Route changed original tenant/Shard incarnation");
            original.partitionPolicy(body.shard().partition());
        }
    }
    public byte[] canonicalBytes(){
        final byte[] bytes=CanonicalProtobuf.message(out->{CanonicalProtobuf.uint32(out,1,SCHEMA_VERSION);CanonicalProtobuf.bytes(out,2,fence.canonicalBytes());
            if(route!=null){CanonicalProtobuf.bytes(out,3,route);CanonicalProtobuf.bytes(out,4,routeKey);}});
        if(bytes.length>MAX_BYTES)throw new IllegalArgumentException("Native reclaim facts exceed complete encoded bound");return bytes;
    }
    public static TargetNativeReclaimFacts decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,4,false,"Native reclaim facts");
        QueryCodecSupport.requireNumbers(f,f.size()==4?new int[]{1,2,3,4}:new int[]{1,2},"Native reclaim facts");
        if(QueryCodecSupport.uint32(f.getFirst(),1)!=SCHEMA_VERSION)throw new IllegalArgumentException("unknown Native reclaim facts schema");
        final var value=new TargetNativeReclaimFacts(TargetInlinePayloadSourceFacts.Frame.decode(QueryCodecSupport.bytes(f.get(1),2)),
                f.size()==4?QueryCodecSupport.bytes(f.get(2),3):null,f.size()==4?QueryCodecSupport.bytes(f.get(3),4):null);
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Native reclaim facts");return value;
    }
}
