package io.nereusstream.delay.protocol;

/** Version-bound command hash preimage from Protocol Registry §4. */
public final class CommandHash {
    private CommandHash() {
    }

    public static byte[] compute(final CommandType type, final CommandId commandId, final DelayMessageId messageId,
                                 final long retryUntilEpochMs, final byte[] canonicalBody) {
        return compute(ProtocolTupleV1.managedCommandV1(), type, commandId, messageId, retryUntilEpochMs,
                canonicalBody);
    }

    /** Computes the command identity with every protocol-version component in the preimage. */
    public static byte[] compute(final ProtocolTupleV1 protocolTuple, final CommandType type,
                                 final CommandId commandId, final DelayMessageId messageId,
                                 final long retryUntilEpochMs, final byte[] canonicalBody) {
        if (protocolTuple == null || protocolTuple.recordKind() != ProtocolTupleV1.CLIENT_COMMAND) {
            throw new IllegalArgumentException("command hash requires a Client Command protocol tuple");
        }
        if (retryUntilEpochMs < 0) {
            throw new IllegalArgumentException("retryUntil must be non-negative");
        }
        return Bytes.sha256(
                Bytes.utf8("nereus-delay-command-hash-v1\0"),
                Bytes.u8(Math.toIntExact(protocolTuple.framingVersion())),
                Bytes.u32be(protocolTuple.logEnvelopeVersion()),
                Bytes.u32be(protocolTuple.envelopeVersion()),
                Bytes.u32be(protocolTuple.bodyVersion()),
                Bytes.u16be(type.wireValue()),
                Bytes.lp32(commandId.bytes()),
                Bytes.lp32(messageId.bytes()),
                Bytes.i64be(retryUntilEpochMs),
                Bytes.lp32(canonicalBody));
    }
}
