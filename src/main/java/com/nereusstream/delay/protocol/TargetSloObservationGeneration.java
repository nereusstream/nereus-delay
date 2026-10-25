package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Retained certification generation and exact Source Control inputs. Counts require independent ledger validation. */
public record TargetSloObservationGeneration(Control enable, Control stop, Counts counts, TargetQuotaMutation mutation) {
    public static final int VALUE_TYPE = 58;
    public static final int MAX_CANONICAL_BYTES = 2 * (Control.MAX_CANONICAL_BYTES + 8)
            + Counts.MAX_CANONICAL_BYTES + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 96;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-slo-observation-generation\0");
    private static final byte[] CONTROL_DOMAIN = Bytes.utf8("nereus-delay-target-slo-observation-generation-control\0");

    /** Full immutable Source input; signature/history/registration authority is rechecked independently. */
    public record Control(SystemMutation input, TargetQuotaMutation appliedAt) {
        public static final int MAX_INPUT_BYTES = TargetSloObservationControlBody.MAX_CANONICAL_BYTES + 1024;
        public static final int MAX_CANONICAL_BYTES = MAX_INPUT_BYTES + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 32;

        public Control {
            Objects.requireNonNull(input); Objects.requireNonNull(appliedAt);
            appliedAt.requireSourceApplied();
            final var body = TargetSloObservationControlBody.decode(input.canonicalBody());
            if (input.type() != SystemMutationType.APPLY_SHARD_CONTROL || input.canonicalEnvelope().length > MAX_INPUT_BYTES
                    || !Arrays.equals(input.logicalOperationIdentity(), body.logicalIdentity())
                    || !input.shardId().equals(body.shard()) || !body.shard().equals(appliedAt.source().shardId())
                    || input.retryUntilEpochMs() != body.retryUntil()
                    || Long.compareUnsigned(appliedAt.source().brokerPersistenceTimeEpochMs(), body.retryUntil()) > 0
                    || !Arrays.equals(appliedAt.mutationDigest(), Bytes.sha256(input.canonicalEnvelope()))) {
                throw new IllegalArgumentException("Target SLO generation lost original applied Control input/source");
            }
        }

        public TargetSloObservationControlBody body() { return TargetSloObservationControlBody.decode(input.canonicalBody()); }
        public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, input.canonicalEnvelope());
            CanonicalProtobuf.bytes(out, 3, appliedAt.canonicalBytes());
        }); }
        public static Control decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 3, false, "Target SLO applied Control");
            QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3}, "Target SLO applied Control");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) throw new IllegalArgumentException("unknown Target SLO applied Control schema");
            final var input = QueryCodecSupport.bytes(f.get(1), 2);
            if (input.length > MAX_INPUT_BYTES) throw new IllegalArgumentException("Target SLO applied input exceeds bound");
            final var result = new Control(SystemMutation.decodeTargetEnvelope(input), TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(2), 3)));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target SLO applied Control");
            return result;
        }
    }

    public record Counts(long issued, long outboxRecords, long outboxBytes, long custodied,long eventRecords,long eventBytes,
            Reservation reservation) {
        public static final int MAX_CANONICAL_BYTES = 2 + 8 * 11;
        public record Reservation(long records,long bytes) {
            public Reservation {
                if(records<0||bytes<0||(records==0)!=(bytes==0))throw new IllegalArgumentException("invalid SLO completion reservation");
            }
            public static Reservation empty(){return new Reservation(0,0);}
            public Reservation plus(Reservation other){return new Reservation(Math.addExact(records,other.records),Math.addExact(bytes,other.bytes));}
            public Reservation minus(Reservation other){return new Reservation(Math.subtractExact(records,other.records),Math.subtractExact(bytes,other.bytes));}
        }
        public Counts(long issued,long records,long bytes,long custodied){this(issued,records,bytes,custodied,0,0);}
        /** Original schemas carry no reservation proof; null is never an already-reserved zero. */
        public Counts(long issued,long records,long bytes,long custodied,long events,long eventBytes){
            this(issued,records,bytes,custodied,events,eventBytes,null);
        }
        public Counts {
            // Custody does not remove a retained outbox or refund its storage charge.
            if (issued < 0 || outboxRecords < 0 || outboxRecords > issued || outboxBytes < 0
                    || custodied < 0 || custodied > issued || (outboxRecords == 0) != (outboxBytes == 0)
                    ||eventRecords<0||eventBytes<0||(eventRecords==0)!=(eventBytes==0)) {
                throw new IllegalArgumentException("Target SLO generation counts are inconsistent");
            }
        }
        public static Counts empty() { return new Counts(0, 0, 0, 0); }
        public static Counts reservedEmpty(){return new Counts(0,0,0,0,0,0,Reservation.empty());}
        public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, reservation!=null?3:eventRecords==0?1:2);
            CanonicalProtobuf.uint64Bits(out, 2, issued);
            CanonicalProtobuf.uint64Bits(out, 3, outboxRecords);
            CanonicalProtobuf.uint64Bits(out, 4, outboxBytes);
            CanonicalProtobuf.uint64Bits(out, 5, custodied);
            if(eventRecords!=0||reservation!=null){CanonicalProtobuf.uint64Bits(out,6,eventRecords);CanonicalProtobuf.uint64Bits(out,7,eventBytes);}
            if(reservation!=null){CanonicalProtobuf.uint64Bits(out,8,reservation.records());CanonicalProtobuf.uint64Bits(out,9,reservation.bytes());}
        }); }
        public static Counts decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "Target SLO generation counts");
            if(f.isEmpty())throw new IllegalArgumentException("missing Target SLO counts schema");
            final int version=QueryCodecSupport.uint32(f.getFirst(),1);
            if(version<1||version>3)throw new IllegalArgumentException("unknown Target SLO counts schema");
            QueryCodecSupport.requireNumbers(f,version==1?new int[]{1,2,3,4,5}:version==2?new int[]{1,2,3,4,5,6,7}:new int[]{1,2,3,4,5,6,7,8,9},"Target SLO generation counts");
            final var result = new Counts(QueryCodecSupport.uint64Bits(f.get(1), 2), QueryCodecSupport.uint64Bits(f.get(2), 3),
                    QueryCodecSupport.uint64Bits(f.get(3), 4), QueryCodecSupport.uint64Bits(f.get(4), 5),
                    version==1?0:QueryCodecSupport.uint64Bits(f.get(5),6),version==1?0:QueryCodecSupport.uint64Bits(f.get(6),7),
                    version<3?null:new Reservation(QueryCodecSupport.uint64Bits(f.get(7),8),QueryCodecSupport.uint64Bits(f.get(8),9)));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target SLO generation counts");
            return result;
        }
    }

    public TargetSloObservationGeneration {
        Objects.requireNonNull(enable); Objects.requireNonNull(counts); Objects.requireNonNull(mutation);
        final var request = enable.body().request();
        if (request.operationKind() != ControlOperationKind.ENABLE_TARGET_SLO_CERTIFICATION) throw new IllegalArgumentException("Target SLO generation requires its original enable");
        request.policy().requireSource(enable.appliedAt().source());
        if (enable.appliedAt().source().compareTo(request.policy().sourceAnchor()) <= 0) throw new IllegalArgumentException("Target SLO enable does not follow its original resource anchor");
        if (stop != null) {
            final var stopped = stop.body().request();
            if (stopped.operationKind() != ControlOperationKind.STOP_TARGET_SLO_CERTIFICATION
                    || !request.scope().equals(stopped.scope()) || !Arrays.equals(request.lineage(), stopped.lineage())
                    || request.generation() != stopped.generation()) throw new IllegalArgumentException("Target SLO stop changed its original observation generation");
            stop.appliedAt().requireAfter(enable.appliedAt());
            final var prior = new TargetSloObservationState(request.scope(), request.lineage(), request.generation(), true,
                    enable.body().logicalIdentity(), controlDigest(enable, null), enable.appliedAt());
            if (!Arrays.equals(prior.digest(), stopped.expectedPriorDigest())) throw new IllegalArgumentException("Target SLO stop lost its exact enabled Control head");
        }
        mutation.requireSourceApplied();
        (stop == null ? enable : stop).appliedAt().requireAtOrBefore(mutation);
        request.policy().requireSource(mutation.source());
        final var reservation=counts.reservation();
        if (Math.addExact(Math.addExact(counts.outboxRecords(),counts.eventRecords()),reservation==null?0:reservation.records()) > request.policy().limits().maximumOutboxRecords()
                || Math.addExact(Math.addExact(counts.outboxBytes(),counts.eventBytes()),reservation==null?0:reservation.bytes()) > request.policy().limits().maximumOutboxBytes()) throw new IllegalArgumentException("Target SLO generation exceeds its frozen complete observation outbox limits");
    }

    public static TargetSloObservationGeneration enabled(SystemMutation input, TargetQuotaMutation at) {
        return new TargetSloObservationGeneration(new Control(input, at), null, Counts.empty(), at);
    }
    public TargetSloObservationGeneration stopped(SystemMutation input, TargetQuotaMutation at) {
        if (stop != null) throw new IllegalStateException("Target SLO generation is already stopped");
        at.requireAfter(mutation);
        return new TargetSloObservationGeneration(enable, new Control(input, at), counts, at);
    }
    public TargetSloObservationGeneration withCounts(Counts actualDerivedCounts, TargetQuotaMutation at) {
        at.requireAfter(mutation);
        return new TargetSloObservationGeneration(enable, stop, actualDerivedCounts, at);
    }
    public Control control() { return stop == null ? enable : stop; }
    public boolean active() { return stop == null; }
    public TargetSloObservationPolicy policy() { return enable.body().request().policy(); }
    public long generation() { return enable.body().request().generation(); }
    public byte[] key() { return key(policy().scope(), generation()); }
    public static byte[] key(TargetQuotaScope scope, long generation) {
        if (scope.target() != null || generation == 0) throw new IllegalArgumentException("invalid Target SLO generation key");
        return Bytes.concat(new byte[]{TargetKeyCodec.SLO_GENERATION_TAG, TargetKeyCodec.KEY_FORMAT}, scope.keySuffix(), Bytes.u64beBits(generation));
    }
    public byte[] controlDigest() { return controlDigest(enable, stop); }
    private static byte[] controlDigest(Control enable, Control stop) {
        return Bytes.sha256(CONTROL_DOMAIN, enable.canonicalBytes(), stop == null ? new byte[0] : stop.canonicalBytes());
    }
    private byte[] fields() { return CanonicalProtobuf.message(out -> {
        CanonicalProtobuf.uint32(out, 1, 1);
        CanonicalProtobuf.bytes(out, 2, enable.canonicalBytes());
        if (stop != null) CanonicalProtobuf.bytes(out, 3, stop.canonicalBytes());
        CanonicalProtobuf.bytes(out, 4, counts.canonicalBytes());
        CanonicalProtobuf.bytes(out, 5, mutation.canonicalBytes());
    }); }
    public byte[] digest() { return Bytes.sha256(DOMAIN, fields()); }
    public byte[] canonicalBytes() {
        final var encoded = Bytes.concat(fields(), CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, 6, digest())));
        if (encoded.length > MAX_CANONICAL_BYTES) throw new IllegalArgumentException("Target SLO generation exceeds bound");
        return encoded;
    }
    public static TargetSloObservationGeneration decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 6, false, "Target SLO generation");
        final boolean stopped = f.size() == 6;
        QueryCodecSupport.requireNumbers(f, stopped ? new int[]{1, 2, 3, 4, 5, 6} : new int[]{1, 2, 4, 5, 6}, "Target SLO generation");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) throw new IllegalArgumentException("unknown Target SLO generation schema");
        final var result = new TargetSloObservationGeneration(Control.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                stopped ? Control.decode(QueryCodecSupport.bytes(f.get(2), 3)) : null,
                Counts.decode(QueryCodecSupport.bytes(f.get(stopped ? 3 : 2), 4)), TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(stopped ? 4 : 3), 5)));
        if (!Arrays.equals(result.digest(), QueryCodecSupport.fixed(f.getLast(), 6, 32))) throw new IllegalArgumentException("Target SLO generation digest mismatch");
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target SLO generation");
        return result;
    }
    public static TargetSloObservationGeneration decodeForStore(byte[] key, byte[] encoded, TargetQuotaScope scope, byte[] lineage) {
        final var result = decode(encoded);
        if (!scope.equals(result.policy().scope()) || !Arrays.equals(lineage, result.policy().lineage()) || !Arrays.equals(key, result.key())) throw new IllegalArgumentException("Target SLO generation changed key/scope/lineage");
        return result;
    }
}
