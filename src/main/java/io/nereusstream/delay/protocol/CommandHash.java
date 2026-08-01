package io.nereusstream.delay.protocol;

/** Version-bound command hash preimage from Protocol Registry §4. */
public final class CommandHash {
    private CommandHash() {
    }

    public static byte[] compute(final CommandType type, final CommandId commandId, final DelayMessageId messageId,
                                 final long retryUntilEpochMs, final byte[] canonicalBody) {
        if (retryUntilEpochMs < 0) {
            throw new IllegalArgumentException("retryUntil must be non-negative");
        }
        return Bytes.sha256(
                Bytes.utf8("nereus-delay-command-hash-v1\0"),
                Bytes.u8(1),
                Bytes.u32be(1),
                Bytes.u32be(1),
                Bytes.u32be(1),
                Bytes.u16be(type.wireValue()),
                Bytes.lp32(commandId.bytes()),
                Bytes.lp32(messageId.bytes()),
                Bytes.i64be(retryUntilEpochMs),
                Bytes.lp32(canonicalBody));
    }
}

