package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.SystemMutation;

import java.util.Objects;

/** One signed System Mutation record replayed from the assigned Shard Log. */
public record SourceReplayMutation(SystemMutation mutation, SourcePosition position,
                                   Long sourceConnectionGeneration, byte[] guardAttestationDigest)
        implements SourceReplayEntry {
    public SourceReplayMutation {
        Objects.requireNonNull(mutation, "mutation");
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
