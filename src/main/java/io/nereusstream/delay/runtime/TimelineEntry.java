package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;

import java.nio.ByteBuffer;

/** Current-work reference stored in timeline_cf. */
public record TimelineEntry(DelayMessageId messageId, int generation) {
    public TimelineEntry {
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
    }

    public byte[] encode() {
        return Bytes.concat(Bytes.u32be(1), messageId.bytes(), Bytes.u32be(generation));
    }

    public static TimelineEntry decode(final byte[] bytes) {
        if (bytes.length != 4 + DelayMessageId.LENGTH + 4) {
            throw new IllegalArgumentException("invalid timeline entry length");
        }
        final ByteBuffer input = ByteBuffer.wrap(bytes);
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported timeline entry version");
        }
        final byte[] id = new byte[DelayMessageId.LENGTH];
        input.get(id);
        final TimelineEntry result = new TimelineEntry(new DelayMessageId(id), input.getInt());
        if (!java.util.Arrays.equals(bytes, result.encode())) {
            throw new IllegalArgumentException("non-canonical timeline entry");
        }
        return result;
    }
}
