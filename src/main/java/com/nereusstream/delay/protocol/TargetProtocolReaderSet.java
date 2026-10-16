package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Complete public reader snapshot; its bytes are not membership/session or CONTROL authority by themselves. */
public record TargetProtocolReaderSet(TargetProtocolActivationDescriptor descriptor,List<Reader> readers) {
    public static final int MAX_BYTES=1<<20;
    public static final int MAX_READERS=64;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-protocol-eligible-reader-set\0");
    public record Reader(long revision,ProtocolCapabilityDeclaration declaration) {
        public Reader { Objects.requireNonNull(declaration);if(revision<=0)throw new IllegalArgumentException("reader publication revision must be positive"); }
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
            CanonicalProtobuf.uint64Bits(out,1,revision);CanonicalProtobuf.bytes(out,2,declaration.canonicalBytes());
        });}
    }
    public TargetProtocolReaderSet {
        Objects.requireNonNull(descriptor);readers=List.copyOf(readers);
        if(readers.isEmpty()||readers.size()>MAX_READERS)throw new IllegalArgumentException("Target readers require a complete finite nonempty membership");
        String prior=null;long bytes=128;
        for(var reader:readers){
            final var declaration=reader.declaration();
            bytes=Math.addExact(bytes,declaration.canonicalBytes().length+32L);
            if(bytes>MAX_BYTES)throw new IllegalArgumentException("complete Target reader set exceeds byte bound before full encoding");
            if(prior!=null&&prior.compareTo(declaration.workerId())>=0||!declaration.supports(descriptor)
                    ||!declaration.supports(descriptor.tuple())||!declaration.supports(descriptor.base()))
                throw new IllegalArgumentException("Target reader set is duplicate/unsorted or lacks the exact descriptor/base/tuple");
            prior=declaration.workerId();
        }
    }
    private byte[] fields(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.bytes(out,2,descriptor.digest());
        for(var reader:readers)CanonicalProtobuf.bytes(out,3,reader.canonicalBytes());
    });}
    public byte[] digest(){return Bytes.sha256(DOMAIN,fields());}
    public byte[] canonicalBytes(){
        final byte[] encoded=Bytes.concat(fields(),CanonicalProtobuf.message(out->CanonicalProtobuf.bytes(out,4,digest())));
        if(encoded.length>MAX_BYTES)throw new IllegalArgumentException("complete Target reader snapshot exceeds finite byte bound");return encoded;
    }
    public void requireMembership(List<String> completeEligibleWorkerIds){
        final var expected=new ArrayList<>(completeEligibleWorkerIds);expected.sort(String::compareTo);
        if(!expected.equals(readers.stream().map(r->r.declaration().workerId()).toList()))
            throw new IllegalStateException("Target reader evidence omits/changes original complete eligible membership");
    }
    public static TargetProtocolReaderSet decode(byte[] encoded,TargetProtocolActivationDescriptor descriptor){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_BYTES,MAX_READERS+3,true,"Target reader snapshot");
        if(f.size()<4||QueryCodecSupport.uint(f.get(0),1)!=1||!Arrays.equals(QueryCodecSupport.fixed(f.get(1),2,32),descriptor.digest()))
            throw new IllegalArgumentException("Target reader snapshot version/descriptor mismatch");
        int n=2;final List<Reader> readers=new ArrayList<>();
        while(n<f.size()&&f.get(n).number()==3){
            final var r=TargetCompatibilityCodec.read(QueryCodecSupport.bytes(f.get(n++),3),MAX_BYTES,2,false,"Target reader publication");
            QueryCodecSupport.requireNumbers(r,new int[]{1,2},"Target reader publication");
            readers.add(new Reader(QueryCodecSupport.uint(r.get(0),1),ProtocolCapabilityDeclaration.decode(QueryCodecSupport.bytes(r.get(1),2))));
        }
        QueryCodecSupport.requireNumbers(f.subList(n,f.size()),new int[]{4},"Target reader evidence seal");
        final var set=new TargetProtocolReaderSet(descriptor,readers);
        if(!Arrays.equals(QueryCodecSupport.fixed(f.get(n),4,32),set.digest()))throw new IllegalArgumentException("Target reader evidence digest mismatch");
        QueryCodecSupport.requireCanonical(encoded,set.canonicalBytes(),"Target reader snapshot");return set;
    }
}
