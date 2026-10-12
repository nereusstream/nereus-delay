package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Source-applied current generation of one stable Producer slot; STOPPED never grants new Admission. */
public final class TargetChannelSlot {
    public static final int VALUE_TYPE = 44;
    public static final int MAX_CANONICAL_BYTES = TargetChannelIdentity.MAX_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 128;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-channel-slot-record\0");

    public enum Phase { OPEN, STOPPED }

    private final TargetChannelIdentity channel;
    private final Phase phase;
    private final byte[] operationIdentity;
    private final TargetQuotaMutation mutation;
    private final byte[] lineage;
    private final byte[] digest;

    public TargetChannelSlot(final TargetChannelControlRecord control) {
        final var request = Objects.requireNonNull(control, "control").body().request();
        channel = request.channel();
        phase = request.operationKind() == ControlOperationKind.STOP_TARGET_CHANNEL_NEW_WORK
                ? Phase.STOPPED : Phase.OPEN;
        operationIdentity = control.body().logicalIdentity();
        mutation = control.mutation();
        lineage = control.recoveryLineage();
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    private TargetChannelSlot(final TargetChannelIdentity channel, final Phase phase, final byte[] operationIdentity,
            final TargetQuotaMutation mutation, final byte[] lineage) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.phase = Objects.requireNonNull(phase, "phase");
        this.operationIdentity = TargetCompatibilityCodec.assigned(operationIdentity, 32, "channelControlIdentity");
        this.mutation = Objects.requireNonNull(mutation, "mutation");
        this.lineage = TargetCompatibilityCodec.assigned(lineage, 16, "recoveryLineage");
        mutation.requireSourceApplied();
        if (!channel.context().sourceShard().equals(mutation.source().shardId())) {
            throw new IllegalArgumentException("channel slot belongs to another Source Shard");
        }
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public TargetChannelIdentity channel() { return channel; }
    public Phase phase() { return phase; }
    public TargetQuotaMutation mutation() { return mutation; }
    public byte[] recoveryLineage() { return Bytes.copy(lineage); }
    public byte[] controlKey() { return TargetKeyCodec.channelControl(operationIdentity); }
    public byte[] key() { return TargetKeyCodec.channelSlot(channel.context().producerIdentity()); }

    public void requireControl(final TargetChannelControlRecord control) {
        final var expected = new TargetChannelSlot(control);
        if (!Arrays.equals(canonicalBytes(), expected.canonicalBytes())) {
            throw new IllegalStateException("current channel slot differs from its exact Source lifecycle event");
        }
    }

    public void requireNewWork(final TargetChannelIdentity proposed) {
        if (phase != Phase.OPEN || !Arrays.equals(channel.canonicalBytes(), proposed.canonicalBytes())) {
            throw new IllegalStateException("channel slot is stopped or references another current generation");
        }
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, channel.canonicalBytes());
            CanonicalProtobuf.uint32(out, 3, phase == Phase.OPEN ? 1 : 2);
            CanonicalProtobuf.bytes(out, 4, operationIdentity);
            CanonicalProtobuf.bytes(out, 5, mutation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, lineage);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 7, digest);
        });
    }

    public static TargetChannelSlot decode(final byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "Target channel slot");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7}, "Target channel slot");
        if (QueryCodecSupport.uint32(fields.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown channel slot version");
        }
        final var phase = switch (QueryCodecSupport.uint32(fields.get(2), 3)) {
            case 1 -> Phase.OPEN;
            case 2 -> Phase.STOPPED;
            default -> throw new IllegalArgumentException("unknown channel slot phase");
        };
        final var result = new TargetChannelSlot(
                TargetChannelIdentity.decode(QueryCodecSupport.bytes(fields.get(1), 2)), phase,
                QueryCodecSupport.fixed(fields.get(3), 4, 32),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(4), 5)),
                QueryCodecSupport.fixed(fields.get(5), 6, 16));
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(fields.get(6), 7, 32))) {
            throw new IllegalArgumentException("channel slot digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target channel slot");
        return result;
    }

    public static TargetChannelSlot decodeForStore(final byte[] key, final byte[] encoded, final ShardId shard) {
        final var result = decode(encoded);
        if (!Arrays.equals(key, result.key()) || !shard.equals(result.channel.context().sourceShard())) {
            throw new IllegalArgumentException("channel slot key/Shard mismatch");
        }
        return result;
    }
}
