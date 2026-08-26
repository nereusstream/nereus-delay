package com.nereusstream.delay.client;

import com.nereusstream.delay.adapter.CommandResultRetentionPolicy;
import com.nereusstream.delay.protocol.ActiveMessageView;
import com.nereusstream.delay.protocol.CommandApplyStatus;
import com.nereusstream.delay.protocol.CommandQueryResponse;
import com.nereusstream.delay.protocol.CompactCommandResult;
import com.nereusstream.delay.protocol.DlqExportState;
import com.nereusstream.delay.protocol.MessageGenerationState;
import com.nereusstream.delay.protocol.MessageQueryResponse;
import com.nereusstream.delay.protocol.PublicCommandResult;
import com.nereusstream.delay.protocol.PublicDestinationBindingView;
import com.nereusstream.delay.protocol.PublicEvidenceRef;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.TerminalMessageView;
import com.nereusstream.delay.runtime.CommandResult;
import com.nereusstream.delay.runtime.MessageQuerySnapshot;
import com.nereusstream.delay.runtime.PayloadAvailability;
import java.util.Objects;

/**
 * Converts already-authorized local runtime projections into the closed
 * wire unions. It deliberately performs no receipt routing, barrier wait,
 * authorization lookup or retention calculation.
 */
public final class BoundedLocalQueryProjector {
    private BoundedLocalQueryProjector() {}

    /** Projects a durable local Command result after the caller supplied policy inputs. */
    public static CommandQueryResponse command(
            final CommandResult result,
            final long fullResultRetainUntilEpochMs,
            final PublicDestinationBindingView binding) {
        Objects.requireNonNull(result, "result");
        final CommandApplyStatus status =
                switch (result.applyStatus()) {
                    case APPLIED -> CommandApplyStatus.APPLIED;
                    case REJECTED -> CommandApplyStatus.REJECTED;
                };
        final Integer generation = result.hasGeneration() ? result.generation() : null;
        final Long stateVersion = result.stateVersion() <= 0 ? null : result.stateVersion();
        if (generation == null && (stateVersion != null || binding != null)) {
            throw new IllegalArgumentException("Command result lacks a real Message generation");
        }
        final PublicCommandResult view = new PublicCommandResult(
                status,
                result.stableCode(),
                SourcePositionCodec.decode(result.appliedSourcePosition()),
                generation,
                stateVersion,
                binding,
                fullResultRetainUntilEpochMs);
        return status == CommandApplyStatus.APPLIED
                ? CommandQueryResponse.applied(view)
                : CommandQueryResponse.rejected(view);
    }

    /** Projects a full result using the immutable retention policy. */
    public static CommandQueryResponse command(
            final CommandResult result,
            final CommandResultRetentionPolicy retentionPolicy,
            final PublicDestinationBindingView binding) {
        Objects.requireNonNull(retentionPolicy, "retentionPolicy");
        return command(
                result,
                retentionPolicy.retainUntil(SourcePositionCodec.decode(result.appliedSourcePosition())),
                binding);
    }

    /** Projects the compact historical branch after the full result retention boundary. */
    public static CommandQueryResponse compactCommand(
            final CommandResult result, final long fullResultRetainUntilEpochMs) {
        Objects.requireNonNull(result, "result");
        final CommandApplyStatus status =
                switch (result.applyStatus()) {
                    case APPLIED -> CommandApplyStatus.APPLIED;
                    case REJECTED -> CommandApplyStatus.REJECTED;
                };
        final CompactCommandResult view = new CompactCommandResult(
                status,
                result.stableCode(),
                SourcePositionCodec.decode(result.appliedSourcePosition()),
                fullResultRetainUntilEpochMs);
        return CommandQueryResponse.resultExpired(view);
    }

    /** Projects the compact result using the immutable retention policy. */
    public static CommandQueryResponse compactCommand(
            final CommandResult result, final CommandResultRetentionPolicy retentionPolicy) {
        Objects.requireNonNull(retentionPolicy, "retentionPolicy");
        return compactCommand(
                result, retentionPolicy.retainUntil(SourcePositionCodec.decode(result.appliedSourcePosition())));
    }

    /** Projects a local Message snapshot only when a safe binding has been authorized separately. */
    public static MessageQueryResponse message(
            final MessageQuerySnapshot snapshot,
            final PublicDestinationBindingView binding,
            final PublicEvidenceRef evidence) {
        Objects.requireNonNull(snapshot, "snapshot");
        return message(snapshot, binding, snapshot.dlqExportState(), evidence);
    }

    /** Projects a local Message snapshot only when a safe binding has been authorized separately. */
    public static MessageQueryResponse message(
            final MessageQuerySnapshot snapshot,
            final PublicDestinationBindingView binding,
            final DlqExportState dlqExportState,
            final PublicEvidenceRef evidence) {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(dlqExportState, "dlqExportState");
        if (snapshot.dlqExportState() != dlqExportState) {
            throw new IllegalArgumentException("caller DLQ state disagrees with durable message snapshot");
        }
        final MessageGenerationState state =
                MessageGenerationState.fromWire(snapshot.state().wireValue());
        if (state.active()) {
            final ActiveMessageView view = new ActiveMessageView(
                    snapshot.generation(),
                    snapshot.stateVersion(),
                    state,
                    snapshot.deliverAtEpochMs(),
                    snapshot.expireAtEpochMs(),
                    binding,
                    payload(snapshot.payloadAvailability()),
                    snapshot.possibleDestinationDuplicate());
            return MessageQueryResponse.active(view);
        }
        final TerminalMessageView view = new TerminalMessageView(
                snapshot.generation(),
                snapshot.stateVersion(),
                state,
                Objects.requireNonNull(snapshot.terminalCode(), "terminalCode"),
                binding,
                payload(snapshot.payloadAvailability()),
                dlqExportState,
                snapshot.possibleDestinationDuplicate(),
                evidence);
        return MessageQueryResponse.terminal(view);
    }

    private static com.nereusstream.delay.protocol.PayloadAvailability payload(final PayloadAvailability availability) {
        return com.nereusstream.delay.protocol.PayloadAvailability.valueOf(availability.name());
    }
}
