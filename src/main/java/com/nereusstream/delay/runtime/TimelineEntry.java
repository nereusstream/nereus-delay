package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import java.nio.ByteBuffer;

/**
 * Legacy message/generation pointer retained for reading pre-TimelineWorkRef
 * local stores during migration. New timeline writes use {@link TimelineWorkRef}
 * directly so the value carries the complete work projection.
 */
public record TimelineEntry(DelayMessageId messageId, int generation) {
    public byte[] encode() {
        return Bytes.concat(Bytes.u32be(1), messageId.bytes(), Bytes.u32beBits(generation));
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
