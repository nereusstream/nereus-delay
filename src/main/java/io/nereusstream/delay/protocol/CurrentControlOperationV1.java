package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Current public-safe projection of one registered Control Operation. */
public final class CurrentControlOperationV1 {
    private static final int HASH_LENGTH = 32;

    private final byte[] operationId;
    private final byte[] requestHash;
    private final byte[] authenticatedScopeHash;
    private final ControlOperationStateV1 state;
    private final long operationRevision;
    private final List<ControlTargetStateViewV1> targetStates;
    private final ControlTypedResultV1 typedResult;

    public CurrentControlOperationV1(final byte[] operationId, final byte[] requestHash,
                                     final byte[] authenticatedScopeHash, final ControlOperationStateV1 state,
                                     final long operationRevision, final List<ControlTargetStateViewV1> targetStates,
                                     final ControlTypedResultV1 typedResult) {
        this.operationId = nonZero(operationId, "operationId");
        this.requestHash = fixed(requestHash, "requestHash");
        this.authenticatedScopeHash = fixed(authenticatedScopeHash, "authenticatedScopeHash");
        this.state = Objects.requireNonNull(state, "state");
        if (operationRevision <= 0) {
            throw new IllegalArgumentException("operationRevision must be positive");
        }
        this.operationRevision = operationRevision;
        this.targetStates = sortedTargets(targetStates);
        if ((state == ControlOperationStateV1.SUCCEEDED
                || state == ControlOperationStateV1.SUCCEEDED_WITH_OUTSTANDING) && typedResult == null) {
            throw new IllegalArgumentException("successful control operation requires typed result");
        }
        this.typedResult = typedResult;
    }

    public byte[] operationId() {
        return Bytes.copy(operationId);
    }

    public byte[] requestHash() {
        return Bytes.copy(requestHash);
    }

    public byte[] authenticatedScopeHash() {
        return Bytes.copy(authenticatedScopeHash);
    }

    public ControlOperationStateV1 state() {
        return state;
    }

    public long operationRevision() {
        return operationRevision;
    }

    public List<ControlTargetStateViewV1> targetStates() {
        return targetStates;
    }

    public ControlTypedResultV1 typedResult() {
        return typedResult;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, operationId);
            CanonicalProtobuf.bytes(output, 2, requestHash);
            CanonicalProtobuf.bytes(output, 3, authenticatedScopeHash);
            CanonicalProtobuf.uint32(output, 4, state.wireValue());
            CanonicalProtobuf.uint64(output, 5, operationRevision);
            for (ControlTargetStateViewV1 target : targetStates) {
                CanonicalProtobuf.bytes(output, 6, target.canonicalBytes());
            }
            if (typedResult != null) {
                CanonicalProtobuf.bytes(output, 7, typedResult.canonicalBytes());
            }
        });
    }

    public static CurrentControlOperationV1 decode(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() < 5 || fields.get(0).number() != 1 || fields.get(1).number() != 2
                || fields.get(2).number() != 3 || fields.get(3).number() != 4
                || fields.get(4).number() != 5) {
            throw new IllegalArgumentException("invalid CurrentControlOperationV1 fields");
        }
        final List<ControlTargetStateViewV1> targets = new ArrayList<>();
        int index = 5;
        while (index < fields.size() && fields.get(index).number() == 6) {
            targets.add(ControlTargetStateViewV1.decode(fields.get(index).rawValue()));
            index++;
        }
        final ControlTypedResultV1 typed;
        if (index < fields.size() && fields.get(index).number() == 7) {
            typed = ControlTypedResultV1.decode(fields.get(index).rawValue());
            index++;
        } else {
            typed = null;
        }
        if (index != fields.size()) {
            throw new IllegalArgumentException("invalid CurrentControlOperationV1 target/result order");
        }
        final CurrentControlOperationV1 result = new CurrentControlOperationV1(
                nonZero(fields.get(0).rawValue(), "operationId"),
                fixed(fields.get(1).rawValue(), "requestHash"),
                fixed(fields.get(2).rawValue(), "authenticatedScopeHash"),
                ControlOperationStateV1.fromWire(uint32(fields.get(3), 4)),
                uint(fields.get(4), 5), targets, typed);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CurrentControlOperationV1");
        return result;
    }

    private static List<ControlTargetStateViewV1> sortedTargets(final List<ControlTargetStateViewV1> values) {
        Objects.requireNonNull(values, "targetStates");
        final List<ControlTargetStateViewV1> copy = new ArrayList<>(values);
        copy.sort(java.util.Comparator.comparingInt(ControlTargetStateViewV1::targetIndex));
        for (int index = 1; index < copy.size(); index++) {
            if (copy.get(index - 1).targetIndex() >= copy.get(index).targetIndex()) {
                throw new IllegalArgumentException("control targets must be strictly index-sorted");
            }
        }
        return List.copyOf(copy);
    }

    private static int uint32(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = uint(field, number);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("control uint32 exceeds local range");
        }
        return (int) value;
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid control uint field " + number);
        }
        return field.unsignedValue();
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static byte[] nonZero(final byte[] value, final String name) {
        final byte[] result = fixed(value, name);
        boolean nonZero = false;
        for (byte current : result) {
            nonZero |= current != 0;
        }
        if (!nonZero) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CurrentControlOperationV1 that && operationRevision == that.operationRevision
                && state == that.state && Arrays.equals(operationId, that.operationId)
                && Arrays.equals(requestHash, that.requestHash)
                && Arrays.equals(authenticatedScopeHash, that.authenticatedScopeHash)
                && targetStates.equals(that.targetStates) && Objects.equals(typedResult, that.typedResult);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(operationId), Arrays.hashCode(requestHash),
                Arrays.hashCode(authenticatedScopeHash), state, operationRevision, targetStates, typedResult);
    }
}
