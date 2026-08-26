package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Public view of a terminal Message generation and its safe evidence summary. */
public final class TerminalMessageView implements QueryResponseBranch {
    private final int generation;
    private final long stateVersion;
    private final MessageGenerationState state;
    private final StableCode terminalCode;
    private final PublicDestinationBindingView binding;
    private final PayloadAvailability payloadAvailability;
    private final DlqExportState dlqExportState;
    private final boolean possibleDestinationDuplicate;
    private final PublicEvidenceRef evidence;

    public TerminalMessageView(
            final int generation,
            final long stateVersion,
            final MessageGenerationState state,
            final StableCode terminalCode,
            final PublicDestinationBindingView binding,
            final PayloadAvailability payloadAvailability,
            final DlqExportState dlqExportState,
            final boolean possibleDestinationDuplicate,
            final PublicEvidenceRef evidence) {
        if (stateVersion <= 0) {
            throw new IllegalArgumentException("invalid terminal message view numbers");
        }
        if (state == null || !state.terminal()) {
            throw new IllegalArgumentException("terminal view requires a terminal generation state");
        }
        this.generation = generation;
        this.stateVersion = stateVersion;
        this.state = state;
        this.terminalCode = Objects.requireNonNull(terminalCode, "terminalCode");
        this.binding = Objects.requireNonNull(binding, "binding");
        this.payloadAvailability = Objects.requireNonNull(payloadAvailability, "payloadAvailability");
        this.dlqExportState = Objects.requireNonNull(dlqExportState, "dlqExportState");
        this.possibleDestinationDuplicate = possibleDestinationDuplicate;
        this.evidence = evidence;
    }

    public int generation() {
        return generation;
    }

    public long stateVersion() {
        return stateVersion;
    }

    public MessageGenerationState state() {
        return state;
    }

    public StableCode terminalCode() {
        return terminalCode;
    }

    public PublicDestinationBindingView binding() {
        return binding;
    }

    public PayloadAvailability payloadAvailability() {
        return payloadAvailability;
    }

    public DlqExportState dlqExportState() {
        return dlqExportState;
    }

    public boolean possibleDestinationDuplicate() {
        return possibleDestinationDuplicate;
    }

    public PublicEvidenceRef evidence() {
        return evidence;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32Bits(output, 1, generation);
            CanonicalProtobuf.uint64(output, 2, stateVersion);
            CanonicalProtobuf.uint32(output, 3, state.wireValue());
            CanonicalProtobuf.uint32(output, 4, terminalCode.wireValue());
            CanonicalProtobuf.bytes(output, 5, binding.canonicalBytes());
            CanonicalProtobuf.uint32(output, 6, payloadAvailability.wireValue());
            CanonicalProtobuf.uint32(output, 7, dlqExportState.wireValue());
            CanonicalProtobuf.uint32(output, 8, possibleDestinationDuplicate ? 1 : 0);
            if (evidence != null) {
                CanonicalProtobuf.bytes(output, 9, evidence.canonicalBytes());
            }
        });
    }

    public static TerminalMessageView decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "TerminalMessageView");
        if (fields.size() != 8 && fields.size() != 9) {
            throw new IllegalArgumentException("invalid TerminalMessageView field count");
        }
        for (int index = 0; index < 8; index++) {
            if (fields.get(index).number() != index + 1) {
                throw new IllegalArgumentException("invalid TerminalMessageView field order");
            }
        }
        final PublicEvidenceRef evidence =
                fields.size() == 9 ? PublicEvidenceRef.decode(QueryCodecSupport.nested(fields.get(8), 9)) : null;
        final TerminalMessageView result = new TerminalMessageView(
                QueryCodecSupport.uint32Bits(fields.get(0), 1),
                QueryCodecSupport.uint(fields.get(1), 2),
                MessageGenerationState.fromWire(QueryCodecSupport.uint(fields.get(2), 3)),
                StableCode.fromWire(QueryCodecSupport.uint32(fields.get(3), 4)),
                PublicDestinationBindingView.decode(QueryCodecSupport.nested(fields.get(4), 5)),
                PayloadAvailability.fromWire(QueryCodecSupport.uint(fields.get(5), 6)),
                DlqExportState.fromWire(QueryCodecSupport.uint(fields.get(6), 7)),
                QueryCodecSupport.bool(fields.get(7), 8),
                evidence);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TerminalMessageView");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof TerminalMessageView that)) {
            return false;
        }
        return generation == that.generation
                && stateVersion == that.stateVersion
                && possibleDestinationDuplicate == that.possibleDestinationDuplicate
                && state == that.state
                && terminalCode == that.terminalCode
                && binding.equals(that.binding)
                && payloadAvailability == that.payloadAvailability
                && dlqExportState == that.dlqExportState
                && Objects.equals(evidence, that.evidence);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                generation,
                stateVersion,
                state,
                terminalCode,
                binding,
                payloadAvailability,
                dlqExportState,
                possibleDestinationDuplicate,
                evidence);
    }
}
