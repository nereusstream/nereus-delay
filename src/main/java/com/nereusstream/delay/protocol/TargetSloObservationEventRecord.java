package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.Objects;

/** Immutable Source merge edge; source-assigned incoming bytes survive even when the merged after is worse. */
public record TargetSloObservationEventRecord(
        SystemMutation input,
        TargetQuotaMutation appliedAt,
        byte[] beforeDigest,
        SloObservationOutbox after,
        SloSampleFinal incoming) {
    public static final int VALUE_TYPE = 60;
    public static final int MAX_V1_INPUT_BYTES = TargetSloObservationEventBody.MAX_FINAL_CANONICAL_BYTES
            + TargetSloObservationPolicy.MAX_SERVICE_BYTES
            + 512;
    public static final int MAX_LEGACY_INPUT_BYTES = Math.max(
                    TargetSloObservationEventBody.MAX_FINAL_CANONICAL_BYTES,
                    TargetSloObservationEventBody.MAX_SOURCE_ASSIGNED_CANONICAL_BYTES)
            + TargetSloObservationPolicy.MAX_SERVICE_BYTES
            + 512;
    public static final int MAX_INPUT_BYTES = Math.max(
            MAX_LEGACY_INPUT_BYTES,
            TargetSloObservationEventBody.MAX_EPOCH_FINAL_CANONICAL_BYTES
                    + TargetSloObservationPolicy.MAX_SERVICE_BYTES
                    + 512);
    public static final int MAX_V1_CANONICAL_BYTES = MAX_V1_INPUT_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + TargetSloObservationSample.MAX_START_BYTES
            + TargetSloObservationSample.MAX_FINAL_BYTES
            + 192;
    public static final int MAX_LEGACY_CANONICAL_BYTES = MAX_LEGACY_INPUT_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + TargetSloObservationSample.MAX_START_BYTES
            + 2 * TargetSloObservationSample.MAX_FINAL_BYTES
            + 200;
    public static final int MAX_CANONICAL_BYTES = MAX_INPUT_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + TargetSloObservationSample.MAX_START_BYTES
            + 2 * TargetSloObservationSample.MAX_FINAL_BYTES
            + 200;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-slo-observation-source-record\0");

    public TargetSloObservationEventRecord(
            SystemMutation input, TargetQuotaMutation appliedAt, byte[] beforeDigest, SloObservationOutbox after) {
        this(input, appliedAt, beforeDigest, after, null);
    }

    public TargetSloObservationEventRecord {
        Objects.requireNonNull(input);
        Objects.requireNonNull(appliedAt).requireSourceApplied();
        Objects.requireNonNull(after);
        beforeDigest = TargetCompatibilityCodec.assigned(beforeDigest, 32, "actual original outbox digest");
        final var body = TargetSloObservationEventBody.decode(input.canonicalBody());
        if (body.isCustody()
                || input.type() != SystemMutationType.TARGET_SLO_OBSERVATION
                || !input.shardId().equals(body.scope().shard())
                || !input.shardId().equals(appliedAt.source().shardId())
                || input.retryUntilEpochMs() != body.retryUntil()
                || !Arrays.equals(input.logicalOperationIdentity(), body.logicalIdentity())
                || input.canonicalEnvelope().length
                        > (body.writerEpoch() != null
                                ? MAX_INPUT_BYTES
                                : body.isSourceAssigned() ? MAX_LEGACY_INPUT_BYTES : MAX_V1_INPUT_BYTES)
                || !Arrays.equals(appliedAt.mutationDigest(), Bytes.sha256(input.canonicalEnvelope()))
                || !Arrays.equals(body.sampleId(), after.sampleId())
                || after.finalObservation() == null
                || appliedAt.source().brokerPersistenceTimeEpochMs() > input.retryUntilEpochMs()) {
            throw new IllegalArgumentException("SLO event record lost exact applied Source input");
        }
        if (body.isSourceAssigned()) {
            Objects.requireNonNull(incoming, "Source-assigned SLO history requires the actual incoming v1 Final");
            if (!Arrays.equals(
                    incoming.sourceEventEvidenceSha256(), body.evidence().digest())) {
                throw new IllegalArgumentException("Source-assigned incoming Final changed original signed evidence");
            }
        } else {
            if (incoming != null) {
                throw new IllegalArgumentException("explicit Source incoming Final requires Source-assigned evidence");
            }
            incoming = Objects.requireNonNull(body.finalObservation());
        }
        incoming.validateAgainst(after.start());
        if (incoming.canonicalBytes().length > TargetSloObservationSample.MAX_FINAL_BYTES
                || Long.compareUnsigned(after.finalObservation().observationRevision(), incoming.observationRevision())
                        < 0) {
            throw new IllegalArgumentException("SLO history changed its bounded incoming Final or merged revision");
        }
    }

    @Override
    public byte[] beforeDigest() {
        return Bytes.copy(beforeDigest);
    }

    public TargetSloObservationEventBody body() {
        return TargetSloObservationEventBody.decode(input.canonicalBody());
    }

    public void requireIncomingFor(TargetSloObservationSample actualBefore) {
        if (!Arrays.equals(body().incomingFor(actualBefore).canonicalBytes(), incoming.canonicalBytes())) {
            throw new IllegalStateException("SLO history incoming Final differs from the actual Source predecessor");
        }
    }

    public int schemaVersion() {
        return body().isSourceAssigned() ? 2 : 1;
    }

    public byte[] key() {
        return key(body().sampleId(), appliedAt.sequence());
    }

    public static byte[] key(byte[] sampleId, long sequence) {
        Bytes.requireLength(sampleId, 32, "original SLO sample ID");
        if (sequence == 0) {
            throw new IllegalArgumentException("SLO event requires actual Source sequence");
        }
        return Bytes.concat(
                new byte[] {TargetKeyCodec.SLO_EVENT_TAG, TargetKeyCodec.KEY_FORMAT},
                sampleId,
                Bytes.u64beBits(sequence));
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, schemaVersion());
            CanonicalProtobuf.bytes(out, 2, input.canonicalEnvelope());
            CanonicalProtobuf.bytes(out, 3, appliedAt.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, beforeDigest);
            CanonicalProtobuf.bytes(out, 5, after.canonicalBytes());
            if (schemaVersion() == 2) {
                CanonicalProtobuf.bytes(out, 6, incoming.canonicalBytes());
            }
        });
    }

    public byte[] canonicalBytes() {
        final var bytes = Bytes.concat(
                fields(),
                CanonicalProtobuf.message(out ->
                        CanonicalProtobuf.bytes(out, schemaVersion() == 1 ? 6 : 7, Bytes.sha256(DOMAIN, fields()))));
        if (bytes.length
                > (schemaVersion() == 1
                        ? MAX_V1_CANONICAL_BYTES
                        : body().writerEpoch() == null ? MAX_LEGACY_CANONICAL_BYTES : MAX_CANONICAL_BYTES)) {
            throw new IllegalArgumentException("SLO event history exceeds finite bound");
        }
        return bytes;
    }

    public long storedBytes() {
        return Math.addExact((long) key().length, TargetValueEnvelope.encode(VALUE_TYPE, canonicalBytes()).length);
    }

    public static long maximumStoredBytes(
            SystemMutation input, TargetSloObservationSample sample, SourcePosition sourceAnchor) {
        final var body = TargetSloObservationEventBody.decode(input.canonicalBody());
        if (body.isCustody()) {
            throw new IllegalArgumentException("Final size requested for custody");
        }
        final long outboxBytes;
        if (body.isSourceAssigned()) {
            body.evidence().requireSample(sample);
            // The actual Source may choose any still-representable uint64 revision after this captured read.
            outboxBytes = 2
                    + delimited(sample.observation().start().canonicalBytes().length)
                    + delimited(TargetSloObservationSample.MAX_FINAL_BYTES)
                    + 34;
        } else {
            outboxBytes = sample.observation()
                    .mergeFinal(
                            body.finalObservation(),
                            sample.declaration().objective().direction())
                    .canonicalBytes()
                    .length;
        }
        final long stamp = 11 + delimited(sourceAnchor.canonicalBytes().length) + 34;
        return 42
                + 12
                + 2
                + delimited(input.canonicalEnvelope().length)
                + delimited(stamp)
                + 34
                + delimited(outboxBytes)
                + (body.isSourceAssigned() ? delimited(TargetSloObservationSample.MAX_FINAL_BYTES) : 0)
                + 34;
    }

    private static long delimited(long bytes) {
        long value = bytes;
        int width = 1;
        while ((value >>>= 7) != 0) {
            width++;
        }
        return 1L + width + bytes;
    }

    public static TargetSloObservationEventRecord decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(
                encoded, MAX_CANONICAL_BYTES, 7, false, "Target original SLO Source event");
        final int version = QueryCodecSupport.uint32(f.getFirst(), 1);
        if (version < 1 || version > 2) {
            throw new IllegalArgumentException("unknown SLO event history schema");
        }
        QueryCodecSupport.requireNumbers(
                f,
                version == 1 ? new int[] {1, 2, 3, 4, 5, 6} : new int[] {1, 2, 3, 4, 5, 6, 7},
                "Target original SLO Source event");
        if (version == 1 && encoded.length > MAX_V1_CANONICAL_BYTES) {
            throw new IllegalArgumentException("legacy SLO history exceeds its unchanged bound");
        }
        final var input = QueryCodecSupport.bytes(f.get(1), 2);
        if (input.length > MAX_INPUT_BYTES) {
            throw new IllegalArgumentException("SLO event input exceeds bound");
        }
        final var mutation = SystemMutation.decodeTargetEnvelope(input);
        final var value = version == 1
                ? new TargetSloObservationEventRecord(
                        mutation,
                        TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                        QueryCodecSupport.fixed(f.get(3), 4, 32),
                        SloObservationOutbox.decode(QueryCodecSupport.bytes(f.get(4), 5)))
                : new TargetSloObservationEventRecord(
                        mutation,
                        TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                        QueryCodecSupport.fixed(f.get(3), 4, 32),
                        SloObservationOutbox.decode(QueryCodecSupport.bytes(f.get(4), 5)),
                        SloSampleFinal.decode(QueryCodecSupport.bytes(f.get(5), 6)));
        if (value.schemaVersion() != version) {
            throw new IllegalArgumentException("SLO history schema changed original Source body");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target original SLO Source event");
        return value;
    }

    public static TargetSloObservationEventRecord decodeForStore(
            byte[] key, byte[] encoded, TargetQuotaScope scope, byte[] lineage) {
        final var value = decode(encoded);
        if (!Arrays.equals(key, value.key())
                || !scope.equals(value.body().scope())
                || !Arrays.equals(lineage, value.body().lineage())) {
            throw new IllegalArgumentException("SLO history changed key/scope/lineage");
        }
        return value;
    }
}
