package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.SourcePosition;

import java.util.Objects;

/**
 * One command record replayed from the assigned source partition during shard
 * catch-up. The broker adapter supplies the optional Pulsar connection proof;
 * {@link OwnedDelayShard} validates it against the accepted activation
 * barrier before applying the command.
 */
public record SourceReplayRecord(PreparedCommand command, SourcePosition position,
                                 Long sourceConnectionGeneration, byte[] guardAttestationDigest)
        implements SourceReplayEntry {
    public SourceReplayRecord {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(position, "position");
        if ((sourceConnectionGeneration == null) != (guardAttestationDigest == null)) {
            throw new IllegalArgumentException("source connection proof fields must be present together");
        }
        if (sourceConnectionGeneration != null && sourceConnectionGeneration == 0) {
            throw new IllegalArgumentException("sourceConnectionGeneration must be nonzero");
        }
        if (guardAttestationDigest != null) {
            Bytes.requireLength(guardAttestationDigest, 32, "guardAttestationDigest");
            guardAttestationDigest = Bytes.copy(guardAttestationDigest);
        }
    }

    @Override
    public byte[] guardAttestationDigest() {
        return guardAttestationDigest == null ? null : Bytes.copy(guardAttestationDigest);
    }
}
