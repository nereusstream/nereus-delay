package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.runtime.CommandResult;
import io.nereusstream.delay.runtime.SystemMutationResult;

import java.util.Objects;

/** Result of one ordered Shard Log replay entry. Exactly one branch is set. */
public record SourceReplayOutcome(SourcePosition position, CommandResult commandResult,
                                  SystemMutationResult systemMutationResult) {
    public SourceReplayOutcome {
        Objects.requireNonNull(position, "position");
        if ((commandResult == null) == (systemMutationResult == null)) {
            throw new IllegalArgumentException("replay outcome must select exactly one result branch");
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
