package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** Compact command identity evidence retained independently from full results. */
record CommandDedupeRecord(byte[] commandHash, CommandResult result) {
    CommandDedupeRecord {
        Bytes.requireLength(commandHash, 32, "commandHash");
        commandHash = Bytes.copy(commandHash);
    }

    @Override
    public byte[] commandHash() {
        return Bytes.copy(commandHash);
    }

    byte[] encode() {
        final byte[] resultBytes = result.encode();
        return Bytes.concat(Bytes.u32be(1), commandHash, Bytes.u32be(resultBytes.length), resultBytes);
    }

    static CommandDedupeRecord decode(final byte[] bytes) {
        final ByteBuffer input = ByteBuffer.wrap(bytes);
        if (input.remaining() < 4 + 32 + 4) {
            throw new IllegalArgumentException("command dedupe record truncated");
        }
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported command dedupe version");
        }
        final byte[] hash = new byte[32];
        input.get(hash);
        final int resultLength = input.getInt();
        if (resultLength < 0 || resultLength != input.remaining()) {
            throw new IllegalArgumentException("command dedupe result length mismatch");
        }
        final byte[] result = new byte[resultLength];
        input.get(result);
        final CommandDedupeRecord decoded = new CommandDedupeRecord(hash, CommandResult.decode(result));
        if (!Arrays.equals(bytes, decoded.encode())) {
            throw new IllegalArgumentException("non-canonical command dedupe record");
        }
        return decoded;
    }
}

