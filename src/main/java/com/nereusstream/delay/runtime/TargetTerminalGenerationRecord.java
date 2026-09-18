package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Generation history without a second payload copy; its original payload owner remains independently retained. */
public record TargetTerminalGenerationRecord(
        TargetMessageLocator locator,
        long stateVersion,
        StableCode terminalCode,
        TargetGenerationRuntimeIndex runtime,
        TargetQuotaMutation mutation,
        byte[] recoveryLineage) {
    public static final int VALUE_TYPE = 37;
    public static final int MAX_CANONICAL_BYTES = TargetMessageLocator.MAX_CANONICAL_BYTES
            + TargetGenerationRuntimeIndex.MAX_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + 128;

    public TargetTerminalGenerationRecord {
        Objects.requireNonNull(locator, "locator");
        Objects.requireNonNull(terminalCode, "terminalCode");
        Objects.requireNonNull(runtime, "runtime").requireMessageProjection(locator);
        Objects.requireNonNull(mutation, "mutation").requireSourceApplied();
        Bytes.requireLength(recoveryLineage, 16, "lineage");
        if (!runtime.terminal()
                || stateVersion == 0
                || !locator.messageId()
                        .routingId()
                        .shardId()
                        .equals(mutation.source().shardId())
                || Arrays.equals(recoveryLineage, new byte[16])) {
            throw new IllegalArgumentException("invalid Target terminal generation/source/lineage");
        }
        recoveryLineage = Bytes.copy(recoveryLineage);
    }

    @Override
    public byte[] recoveryLineage() {
        return Bytes.copy(recoveryLineage);
    }

    public byte[] key() {
        return key(locator);
    }

    public static byte[] key(final TargetMessageLocator locator) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.TERMINAL_GENERATION_TAG, 1},
                locator.messageId().bytes(),
                Bytes.u32beBits(locator.generation()));
    }

    public void requireOwner(final TargetQuotaPayloadOwner owner) {
        if (!owner.messageId().equals(locator.messageId())
                || !owner.primaryIdentity().target().equals(locator.target())
                || !Arrays.equals(owner.primaryIdentity().accountingIncarnation(), locator.accountingIncarnation())
                || !Arrays.equals(owner.initialBindingDigest(), locator.scheduleBindingDigest())
                || !Arrays.equals(owner.recoveryLineage(), recoveryLineage)) {
            throw new IllegalStateException("terminal history changed its frozen payload owner");
        }
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, locator.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 3, stateVersion);
            CanonicalProtobuf.uint32(out, 4, terminalCode.wireValue());
            CanonicalProtobuf.bytes(out, 5, runtime.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, mutation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, recoveryLineage);
        });
    }

    public byte[] canonicalBytes() {
        final byte[] fields = fields();
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields);
            CanonicalProtobuf.bytes(out, 8, Bytes.sha256(Bytes.utf8("nereus-delay-target-terminal\0"), fields));
        });
    }

    public static TargetTerminalGenerationRecord decode(final byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target terminal exceeds bounded schema");
        }
        final var f = QueryCodecSupport.read(encoded, "TargetTerminalGenerationRecord");
        QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4, 5, 6, 7, 8}, "TargetTerminalGenerationRecord");
        if (QueryCodecSupport.uint(f.get(0), 1) != 1) {
            throw new IllegalArgumentException("unknown Target terminal version");
        }
        final var result = new TargetTerminalGenerationRecord(
                TargetMessageLocator.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.uint64Bits(f.get(2), 3),
                StableCode.fromWire(QueryCodecSupport.uint32(f.get(3), 4)),
                TargetGenerationRuntimeIndex.decode(QueryCodecSupport.bytes(f.get(4), 5)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(5), 6)),
                QueryCodecSupport.fixed(f.get(6), 7, 16));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetTerminalGenerationRecord");
        return result;
    }
}
