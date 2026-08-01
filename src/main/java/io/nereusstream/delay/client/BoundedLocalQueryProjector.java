package io.nereusstream.delay.client;

import io.nereusstream.delay.protocol.ActiveMessageViewV1;
import io.nereusstream.delay.protocol.CommandApplyStatusV1;
import io.nereusstream.delay.protocol.CommandQueryResponseV1;
import io.nereusstream.delay.protocol.DlqExportStateV1;
import io.nereusstream.delay.protocol.MessageGenerationStateV1;
import io.nereusstream.delay.protocol.MessageQueryResponseV1;
import io.nereusstream.delay.protocol.PublicCommandResultV1;
import io.nereusstream.delay.protocol.PublicDestinationBindingViewV1;
import io.nereusstream.delay.protocol.PublicEvidenceRefV1;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.protocol.TerminalMessageViewV1;
import io.nereusstream.delay.runtime.CommandResult;
import io.nereusstream.delay.runtime.MessageQuerySnapshot;
import io.nereusstream.delay.runtime.PayloadAvailability;

import java.util.Objects;

/**
 * Converts already-authorized local runtime projections into the closed V1
 * wire unions.  It deliberately performs no receipt routing, barrier wait,
 * authorization lookup or retention calculation.
 */
public final class BoundedLocalQueryProjector {
    private BoundedLocalQueryProjector() {
    }

    /** Projects a durable local Command result after the caller supplied policy inputs. */
    public static CommandQueryResponseV1 command(final CommandResult result,
                                                 final long fullResultRetainUntilEpochMs,
                                                 final PublicDestinationBindingViewV1 binding) {
        Objects.requireNonNull(result, "result");
        final CommandApplyStatusV1 status = switch (result.applyStatus()) {
            case APPLIED -> CommandApplyStatusV1.APPLIED;
            case REJECTED -> CommandApplyStatusV1.REJECTED;
        };
        final Integer generation = result.generation() < 0 ? null : result.generation();
        final Long stateVersion = result.stateVersion() <= 0 ? null : result.stateVersion();
        if (generation == null && (stateVersion != null || binding != null)) {
            throw new IllegalArgumentException("Command result lacks a real Message generation");
        }
        final PublicCommandResultV1 view = new PublicCommandResultV1(status, result.stableCode(),
                SourcePositionCodec.decode(result.appliedSourcePosition()), generation, stateVersion, binding,
                fullResultRetainUntilEpochMs);
        return status == CommandApplyStatusV1.APPLIED
                ? CommandQueryResponseV1.applied(view) : CommandQueryResponseV1.rejected(view);
    }

    /** Projects a local Message snapshot only when a safe binding has been authorized separately. */
    public static MessageQueryResponseV1 message(final MessageQuerySnapshot snapshot,
                                                 final PublicDestinationBindingViewV1 binding,
                                                 final DlqExportStateV1 dlqExportState,
                                                 final PublicEvidenceRefV1 evidence) {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(dlqExportState, "dlqExportState");
        final MessageGenerationStateV1 state = MessageGenerationStateV1.fromWire(snapshot.state().wireValue());
        if (state.active()) {
            final ActiveMessageViewV1 view = new ActiveMessageViewV1(snapshot.generation(), snapshot.stateVersion(),
                    state, snapshot.deliverAtEpochMs(), snapshot.expireAtEpochMs(), binding,
                    payload(snapshot.payloadAvailability()), snapshot.possibleDestinationDuplicate());
            return MessageQueryResponseV1.active(view);
        }
        final TerminalMessageViewV1 view = new TerminalMessageViewV1(snapshot.generation(), snapshot.stateVersion(),
                state, Objects.requireNonNull(snapshot.terminalCode(), "terminalCode"), binding,
                payload(snapshot.payloadAvailability()), dlqExportState, snapshot.possibleDestinationDuplicate(),
                evidence);
        return MessageQueryResponseV1.terminal(view);
    }

    private static io.nereusstream.delay.protocol.PayloadAvailabilityV1 payload(
            final PayloadAvailability availability) {
        return io.nereusstream.delay.protocol.PayloadAvailabilityV1.valueOf(availability.name());
    }
}
