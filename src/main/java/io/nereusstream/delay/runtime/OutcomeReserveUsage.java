package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.PublishAdmissionBody;

import java.nio.ByteBuffer;

/**
 * Durable shard-local usage of the non-borrowable outcome/control reserve.
 *
 * <p>This is deliberately only the outcome projection of a full V1 capacity
 * grant.  Placement, Oxia grant identity and the other control classes remain
 * outside this embedded state machine.</p>
 */
public record OutcomeReserveUsage(long records, long bytes) {
    public OutcomeReserveUsage {
        if (records < 0 || bytes < 0) {
            throw new IllegalArgumentException("outcome reserve usage must be non-negative");
        }
    }

    public static OutcomeReserveUsage empty() {
        return new OutcomeReserveUsage(0, 0);
    }

    public static OutcomeReserveUsage from(final PublishAdmissionBody.ChargeVector charge) {
        if (charge == null) {
            return empty();
        }
        charge.requireLocalCapacityRange();
        return new OutcomeReserveUsage(charge.outcomeReserveRecords(), charge.outcomeReserveBytes());
    }

    public OutcomeReserveUsage add(final OutcomeReserveUsage other) {
        return new OutcomeReserveUsage(Math.addExact(records, other.records), Math.addExact(bytes, other.bytes));
    }

    public OutcomeReserveUsage remove(final OutcomeReserveUsage other) {
        if (records < other.records || bytes < other.bytes) {
            throw new IllegalStateException("outcome reserve usage underflow");
        }
        return new OutcomeReserveUsage(records - other.records, bytes - other.bytes);
    }

    public boolean fits(final OutcomeReserveUsage charge, final long maxRecords, final long maxBytes) {
        if (charge.records < 0 || charge.bytes < 0 || maxRecords <= 0 || maxBytes <= 0) {
            return false;
        }
        try {
            return Math.addExact(records, charge.records) <= maxRecords
                    && Math.addExact(bytes, charge.bytes) <= maxBytes;
        } catch (ArithmeticException overflow) {
            return false;
        }
    }

    public byte[] encode() {
        return Bytes.concat(Bytes.u32be(1), Bytes.u64be(records), Bytes.u64be(bytes));
    }

    public static OutcomeReserveUsage decode(final byte[] encoded) {
        if (encoded == null || encoded.length != 20) {
            throw new IllegalArgumentException("invalid outcome reserve usage length");
        }
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported outcome reserve usage version");
        }
        final OutcomeReserveUsage result = new OutcomeReserveUsage(input.getLong(), input.getLong());
        if (!java.util.Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical outcome reserve usage");
        }
        return result;
    }
}
