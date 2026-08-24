package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardLogFrame;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SystemMutation;
import java.util.Objects;

/** Decodes the two ordered V1 Shard Log record kinds for the Pulsar source set. */
final class PulsarClientArtifactSourceRecordDecoder {
    private PulsarClientArtifactSourceRecordDecoder() {}

    static SourceReplayEntry decode(
            final byte[] frame,
            final ShardId shard,
            final SourcePosition position,
            final Long sourceConnectionGeneration,
            final byte[] guardAttestationDigest) {
        Objects.requireNonNull(frame, "frame");
        final ShardId expectedShard = Objects.requireNonNull(shard, "shard");
        final SourcePosition sourcePosition = Objects.requireNonNull(position, "position");
        if (!expectedShard.equals(sourcePosition.shardId())) {
            throw new IllegalArgumentException("Pulsar source position belongs to another Shard");
        }
        final ShardLogFrame.Decoded decoded = ShardLogFrame.decode(frame);
        return switch (decoded.recordKind()) {
            case ShardLogFrame.CLIENT_COMMAND_KIND ->
                command(frame, expectedShard, sourcePosition, sourceConnectionGeneration, guardAttestationDigest);
            case ShardLogFrame.SYSTEM_MUTATION_KIND ->
                mutation(frame, expectedShard, sourcePosition, sourceConnectionGeneration, guardAttestationDigest);
            default -> throw new IllegalArgumentException("unsupported Pulsar Shard Log record kind");
        };
    }

    private static SourceReplayRecord command(
            final byte[] frame,
            final ShardId shard,
            final SourcePosition position,
            final Long sourceConnectionGeneration,
            final byte[] guardAttestationDigest) {
        final PreparedCommand command = CommandCodec.decodeFrameV1(frame);
        if (!shard.equals(command.shardId())) {
            throw new IllegalArgumentException("Pulsar source command belongs to another Shard");
        }
        return new SourceReplayRecord(command, position, sourceConnectionGeneration, guardAttestationDigest);
    }

    private static SourceReplayMutation mutation(
            final byte[] frame,
            final ShardId shard,
            final SourcePosition position,
            final Long sourceConnectionGeneration,
            final byte[] guardAttestationDigest) {
        final SystemMutation mutation = SystemMutation.decodeFrame(frame);
        if (!shard.equals(mutation.shardId())) {
            throw new IllegalArgumentException("Pulsar source mutation belongs to another Shard");
        }
        return new SourceReplayMutation(mutation, position, sourceConnectionGeneration, guardAttestationDigest);
    }
}
