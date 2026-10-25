package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardLogFrame;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetTimeFenceBody;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.TargetTimeFenceVerifier;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.Objects;

/** Exact type4 signing primitive. Original FENCE/UTC/key/history and resource authority remain independent. */
public final class TargetTimeFenceMutationFactory {
    private final AuthorIdentity author;
    private final int keyVersion;
    private final PrivateKey key;
    private final long maximumFrameBytes;

    public TargetTimeFenceMutationFactory(byte[] writerId,long configGeneration,int keyVersion,
            PrivateKey key,long maximumFrameBytes){
        Objects.requireNonNull(writerId,"original FENCE writer ID");
        if(writerId.length==0||writerId.length>TargetTimeFenceVerifier.MAX_WRITER_ID_BYTES||configGeneration==0||keyVersion==0)
            throw new IllegalArgumentException("TIME_FENCE needs its assigned bounded FENCE writer/config/key");
        requireFrameLimit(maximumFrameBytes);
        this.author=AuthorIdentity.fence(writerId,configGeneration);this.keyVersion=keyVersion;
        this.key=Objects.requireNonNull(key,"original FENCE private key");this.maximumFrameBytes=maximumFrameBytes;
    }

    /** Records the supplied original proof. It neither samples UTC nor grants permission to append or apply. */
    public Prepared prepare(ShardId shard,long closeThrough,long retryUntil,TrustedUtcIntervalEvidence proof,
            long safetyMarginMs,long maximumProofWidthMs){
        requireTime(closeThrough,retryUntil,proof,safetyMarginMs,maximumProofWidthMs);
        final var body=new TargetTimeFenceBody(Objects.requireNonNull(shard),retryUntil,closeThrough,keyVersion,proof);
        final var mutation=SystemMutation.signed(shard,SystemMutationType.TIME_FENCE,retryUntil,body.proofId(),
                body.canonicalBytes(),author.canonicalBytes(),keyVersion,key);
        return Prepared.restore(mutation,safetyMarginMs,maximumProofWidthMs,maximumFrameBytes);
    }

    private static void requireFrameLimit(long maximumFrameBytes){
        if(maximumFrameBytes<=0||maximumFrameBytes==Long.MAX_VALUE)
            throw new IllegalArgumentException("TIME_FENCE requires its explicit finite Source frame limit");
    }
    private static void requireTime(long closeThrough,long retryUntil,TrustedUtcIntervalEvidence proof,
            long safetyMarginMs,long maximumProofWidthMs){
        final var original=Objects.requireNonNull(proof,"original protected UTC proof");
        if(closeThrough<0||closeThrough==Long.MAX_VALUE||retryUntil<=closeThrough||retryUntil==Long.MAX_VALUE
                ||retryUntil<original.latestEpochMs()||safetyMarginMs<0||maximumProofWidthMs<0)
            throw new IllegalArgumentException("TIME_FENCE requires separate finite closure/retry bounds and its original time policy");
        final long earliest;
        final long width;
        try{
            earliest=Math.addExact(closeThrough,safetyMarginMs);
            width=Math.subtractExact(original.latestEpochMs(),original.earliestEpochMs());
        }catch(ArithmeticException overflow){
            throw new IllegalArgumentException("TIME_FENCE time policy arithmetic overflows",overflow);
        }
        if(original.earliestEpochMs()<earliest||width>maximumProofWidthMs)
            throw new IllegalArgumentException("original TIME_FENCE proof does not cover its closure/margin/width");
    }

    /** Preserve these exact signed bytes across UNKNOWN; restoration needs no old private key or new time sample. */
    public static final class Prepared {
        private static final int MAX_AUTHOR_BYTES=AuthorIdentity.fence(
                new byte[TargetTimeFenceVerifier.MAX_WRITER_ID_BYTES],-1L).canonicalBytes().length;
        private final SystemMutation mutation;
        private final TargetTimeFenceBody body;
        private Prepared(SystemMutation mutation,TargetTimeFenceBody body){this.mutation=mutation;this.body=body;}
        public SystemMutation mutation(){return mutation;}
        public TargetTimeFenceBody body(){return body;}

        /** Structural/mathematical recovery only; the supplied bounds must come from the original protected material. */
        public static Prepared restore(SystemMutation original,long safetyMarginMs,long maximumProofWidthMs,long maximumFrameBytes){
            requireFrameLimit(maximumFrameBytes);
            final var mutation=Objects.requireNonNull(original,"original signed TIME_FENCE");
            if(mutation.type()!=SystemMutationType.TIME_FENCE||mutation.canonicalBodyLength()>TargetTimeFenceBody.MAX_CANONICAL_BYTES
                    ||mutation.authorIdentityLength()>MAX_AUTHOR_BYTES||mutation.encodeFrame().length>maximumFrameBytes)
                throw new IllegalArgumentException("original TIME_FENCE changed protocol or its explicit codec/frame bounds");
            final var body=TargetTimeFenceBody.decode(mutation.canonicalBody());
            final var author=AuthorIdentity.decode(mutation.authorIdentity());
            if(author.kind()!=AuthorIdentity.Kind.FENCE||author.first().length>TargetTimeFenceVerifier.MAX_WRITER_ID_BYTES
                    ||author.generation()==0||mutation.signingKeyVersion()==0||body.fenceKeyVersion()!=mutation.signingKeyVersion()
                    ||!body.shard().equals(mutation.shardId())||body.retryUntil()!=mutation.retryUntilEpochMs()
                    ||!Bytes.constantTimeEquals(body.proofId(),mutation.logicalOperationIdentity()))
                throw new IllegalArgumentException("original TIME_FENCE lost its exact FENCE author/config/key/body/ProofId");
            requireTime(body.closeThrough(),body.retryUntil(),body.proof(),safetyMarginMs,maximumProofWidthMs);
            return new Prepared(mutation,body);
        }
        public static Prepared restore(byte[] originalFrame,long safetyMarginMs,long maximumProofWidthMs,long maximumFrameBytes){
            requireFrameLimit(maximumFrameBytes);
            final var frame=Objects.requireNonNull(originalFrame,"protected original TIME_FENCE frame");
            if(frame.length>maximumFrameBytes)throw new IllegalArgumentException("original TIME_FENCE frame exceeds its protected limit");
            final var decoded=ShardLogFrame.decode(frame);
            if(decoded.recordKind()!=ShardLogFrame.SYSTEM_MUTATION_KIND)throw new IllegalArgumentException("original frame is not a TIME_FENCE System Mutation");
            final var value=restore(SystemMutation.decodeTargetEnvelope(decoded.canonicalEnvelope(),SystemMutationType.TIME_FENCE),
                    safetyMarginMs,maximumProofWidthMs,maximumFrameBytes);
            if(!Arrays.equals(frame,value.mutation.encodeFrame()))throw new IllegalArgumentException("restored TIME_FENCE changed its original exact frame bytes");
            return value;
        }
    }
}
