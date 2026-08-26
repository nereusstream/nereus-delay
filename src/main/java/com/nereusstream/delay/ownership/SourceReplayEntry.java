package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.SourcePosition;

/**
 * One record from the single source-ordered Shard Log replay stream.
 *
 * <p>Commands and signed System Mutations share one source cursor. Keeping
 * the entry boundary typed prevents a caller from replaying the two record
 * kinds through independent streams and accidentally changing their relative
 * order.</p>
 */
public sealed interface SourceReplayEntry permits SourceReplayRecord, SourceReplayMutation {
    SourcePosition position();

    Long sourceConnectionGeneration();

    byte[] guardAttestationDigest();
}
