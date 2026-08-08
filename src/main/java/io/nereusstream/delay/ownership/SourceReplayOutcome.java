package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.runtime.CommandResult;
import io.nereusstream.delay.runtime.SystemMutationResult;

import java.util.Arrays;
import java.util.Objects;

/**
 * Result of one ordered Shard Log replay entry. Exactly one branch is set.
 *
 * <p>The result branch is projected to this physical entry's Source Position
 * by the mixed replay seam.  A logical duplicate can therefore carry a
 * different anchor here while its durable Command/System Mutation result
 * remains anchored at the first occurrence.</p>
 */
public record SourceReplayOutcome(SourcePosition position, CommandResult commandResult,
                                  SystemMutationResult systemMutationResult) {
    public SourceReplayOutcome {
        Objects.requireNonNull(position, "position");
        if ((commandResult == null) == (systemMutationResult == null)) {
            throw new IllegalArgumentException("replay outcome must select exactly one result branch");
        }
        final byte[] sourceBytes = position.canonicalBytes();
        if (commandResult != null && !Arrays.equals(sourceBytes, commandResult.appliedSourcePosition())) {
            throw new IllegalArgumentException("command replay result source position does not match entry");
        }
        if (systemMutationResult != null
                && !Arrays.equals(sourceBytes, systemMutationResult.appliedSourcePosition())) {
            throw new IllegalArgumentException("system mutation replay result source position does not match entry");
        }
    }

    public static SourceReplayOutcome command(final SourcePosition position, final CommandResult result) {
        return new SourceReplayOutcome(position, Objects.requireNonNull(result, "result"), null);
    }

    public static SourceReplayOutcome systemMutation(final SourcePosition position,
                                                      final SystemMutationResult result) {
        return new SourceReplayOutcome(position, null, Objects.requireNonNull(result, "result"));
    }

    public boolean isCommand() {
        return commandResult != null;
    }
}
