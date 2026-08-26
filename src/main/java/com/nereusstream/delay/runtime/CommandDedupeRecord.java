package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ProtocolTuple;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Compact command identity evidence retained independently from full results. */
record CommandDedupeRecord(ProtocolTuple protocolTuple, byte[] commandHash, CommandResult result) {
    private static final int LEGACY_VERSION = 1;
    private static final int VERSION = 2;

    CommandDedupeRecord(final byte[] commandHash, final CommandResult result) {
        this(ProtocolTuple.managedCommand(), commandHash, result);
    }

    CommandDedupeRecord {
        Objects.requireNonNull(protocolTuple, "protocolTuple");
        if (protocolTuple.recordKind() != ProtocolTuple.CLIENT_COMMAND) {
            throw new IllegalArgumentException("command dedupe requires a Client Command protocol tuple");
        }
        Bytes.requireLength(commandHash, 32, "commandHash");
        commandHash = Bytes.copy(commandHash);
        Objects.requireNonNull(result, "result");
    }

    @Override
    public byte[] commandHash() {
        return Bytes.copy(commandHash);
    }

    byte[] encode() {
        final byte[] tupleBytes = protocolTuple.canonicalBytes();
        final byte[] resultBytes = result.encode();
        return Bytes.concat(
                Bytes.u32be(VERSION),
                Bytes.u32be(tupleBytes.length),
                tupleBytes,
                commandHash,
                Bytes.u32be(resultBytes.length),
                resultBytes);
    }

    private byte[] encodeLegacy() {
        final byte[] resultBytes = result.encode();
        return Bytes.concat(Bytes.u32be(LEGACY_VERSION), commandHash, Bytes.u32be(resultBytes.length), resultBytes);
    }

    static CommandDedupeRecord decode(final byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length < 4) {
            throw new IllegalArgumentException("command dedupe record truncated");
        }
        final ByteBuffer input = ByteBuffer.wrap(bytes);
        final int version = input.getInt();
        if (version == LEGACY_VERSION) {
            final CommandDedupeRecord decoded = decodeLegacy(input);
            if (!Arrays.equals(bytes, decoded.encodeLegacy())) {
                throw new IllegalArgumentException("non-canonical legacy command dedupe record");
            }
            return decoded;
        }
        if (version != VERSION) {
            throw new IllegalArgumentException("unsupported command dedupe version");
        }
        final byte[] tupleBytes = readBytes(input, "protocol tuple");
        final ProtocolTuple tuple = ProtocolTuple.decode(tupleBytes);
        if (tuple.recordKind() != ProtocolTuple.CLIENT_COMMAND) {
            throw new IllegalArgumentException("command dedupe tuple is not a Client Command");
        }
        final byte[] hash = readFixed(input, 32, "command hash");
        final CommandResult result = CommandResult.decode(readBytes(input, "command result"));
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("command dedupe record has trailing bytes");
        }
        final CommandDedupeRecord decoded = new CommandDedupeRecord(tuple, hash, result);
        if (!Arrays.equals(bytes, decoded.encode())) {
            throw new IllegalArgumentException("non-canonical command dedupe record");
        }
        return decoded;
    }

    private static CommandDedupeRecord decodeLegacy(final ByteBuffer input) {
        final byte[] hash = readFixed(input, 32, "command hash");
        final CommandResult result = CommandResult.decode(readBytes(input, "command result"));
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("legacy command dedupe record has trailing bytes");
        }
        return new CommandDedupeRecord(ProtocolTuple.managedCommand(), hash, result);
    }

    private static byte[] readBytes(final ByteBuffer input, final String name) {
        if (input.remaining() < 4) {
            throw new IllegalArgumentException("command dedupe " + name + " length is truncated");
        }
        final int length = input.getInt();
        if (length < 0 || length > input.remaining()) {
            throw new IllegalArgumentException("command dedupe " + name + " length is invalid");
        }
        final byte[] value = new byte[length];
        input.get(value);
        return value;
    }

    private static byte[] readFixed(final ByteBuffer input, final int length, final String name) {
        if (input.remaining() < length) {
            throw new IllegalArgumentException("command dedupe " + name + " is truncated");
        }
        final byte[] value = new byte[length];
        input.get(value);
        return value;
    }
}
