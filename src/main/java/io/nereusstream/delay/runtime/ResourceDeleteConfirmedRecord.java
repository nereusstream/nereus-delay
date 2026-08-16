package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ResourceDeleteConfirmedBody;
import io.nereusstream.delay.protocol.ResourceRetireIntentBody;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Durable gc_cf tombstone retaining the exact retire intent and delete evidence. */
public record ResourceDeleteConfirmedRecord(
        byte[] confirmationMutationId,
        byte[] confirmationMutationHash,
        ResourceRetireIntentRecord retireIntent,
        ResourceDeleteConfirmedBody.DeleteOutcome outcome,
        long appliedMutationSequence,
        byte[] providerRequestIdHash,
        byte[] observedImmutableVersion,
        byte[] observedEtag,
        byte[] responseHash,
        byte[] observedAt,
        byte[] confirmedAt,
        byte[] appliedSourcePosition) {
    public static final int VALUE_TYPE = 7;
    private static final int HASH_LENGTH = 32;

    public ResourceDeleteConfirmedRecord {
        Bytes.requireLength(confirmationMutationId, HASH_LENGTH, "confirmationMutationId");
        Bytes.requireLength(confirmationMutationHash, HASH_LENGTH, "confirmationMutationHash");
        Objects.requireNonNull(retireIntent, "retireIntent");
        Objects.requireNonNull(outcome, "outcome");
        Bytes.requireLength(providerRequestIdHash, HASH_LENGTH, "providerRequestIdHash");
        Objects.requireNonNull(observedImmutableVersion, "observedImmutableVersion");
        Objects.requireNonNull(observedEtag, "observedEtag");
        Bytes.requireLength(responseHash, HASH_LENGTH, "responseHash");
        final TrustedUtcIntervalEvidence observedEvidence = TrustedUtcIntervalEvidence.decode(
                Objects.requireNonNull(observedAt, "observedAt"));
        final TrustedUtcIntervalEvidence confirmedEvidence = TrustedUtcIntervalEvidence.decode(
                Objects.requireNonNull(confirmedAt, "confirmedAt"));
        confirmedEvidence.requireEarliestAtLeast(observedEvidence.latestEpochMs());
        Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
        if (outcome == ResourceDeleteConfirmedBody.DeleteOutcome.ALREADY_ABSENT
                && (observedImmutableVersion.length != 0 || observedEtag.length != 0)) {
            throw new IllegalArgumentException("ALREADY_ABSENT cannot carry observed identity fields");
        }
        ResourceRetireIntentBody.validateExternalDeleteIdentity(retireIntent.resourceKind(),
                retireIntent.resourceIdentity(), observedImmutableVersion, observedEtag, outcome);
        if (appliedSourcePosition.length == 0) {
            throw new IllegalArgumentException("appliedSourcePosition must not be empty");
        }
        final SourcePosition intentSourcePosition = SourcePositionCodec.decode(
                retireIntent.appliedSourcePosition());
        final SourcePosition confirmationSourcePosition = SourcePositionCodec.decode(appliedSourcePosition);
        if (!intentSourcePosition.shardId().equals(confirmationSourcePosition.shardId())
                || !intentSourcePosition.sameSourceIdentity(confirmationSourcePosition)) {
            throw new IllegalArgumentException("delete confirmation source position does not match retire intent");
        }
        if (confirmationSourcePosition.compareTo(intentSourcePosition) <= 0) {
            throw new IllegalArgumentException("delete confirmation must follow retire intent");
        }
        confirmationMutationId = Bytes.copy(confirmationMutationId);
        confirmationMutationHash = Bytes.copy(confirmationMutationHash);
        providerRequestIdHash = Bytes.copy(providerRequestIdHash);
        observedImmutableVersion = Bytes.copy(observedImmutableVersion);
        observedEtag = Bytes.copy(observedEtag);
        responseHash = Bytes.copy(responseHash);
        observedAt = Bytes.copy(observedAt);
        confirmedAt = Bytes.copy(confirmedAt);
        appliedSourcePosition = confirmationSourcePosition.canonicalBytes();
    }

    @Override
    public byte[] confirmationMutationId() {
        return Bytes.copy(confirmationMutationId);
    }

    @Override
    public byte[] confirmationMutationHash() {
        return Bytes.copy(confirmationMutationHash);
    }

    @Override
    public byte[] providerRequestIdHash() {
        return Bytes.copy(providerRequestIdHash);
    }

    @Override
    public byte[] observedImmutableVersion() {
        return Bytes.copy(observedImmutableVersion);
    }

    @Override
    public byte[] observedEtag() {
        return Bytes.copy(observedEtag);
    }

    @Override
    public byte[] responseHash() {
        return Bytes.copy(responseHash);
    }

    @Override
    public byte[] observedAt() {
        return Bytes.copy(observedAt);
    }

    @Override
    public byte[] confirmedAt() {
        return Bytes.copy(confirmedAt);
    }

    @Override
    public byte[] appliedSourcePosition() {
        return Bytes.copy(appliedSourcePosition);
    }

    public byte[] encode() {
        return Bytes.concat(Bytes.u32be(2), confirmationMutationId, confirmationMutationHash,
                Bytes.lp32(retireIntent.encode()), Bytes.u8(outcome.wireValue()), providerRequestIdHash,
                Bytes.u64beBits(appliedMutationSequence),
                Bytes.lp32(observedImmutableVersion), Bytes.lp32(observedEtag), responseHash,
                Bytes.lp32(observedAt), Bytes.lp32(confirmedAt), Bytes.lp32(appliedSourcePosition));
    }

    public static ResourceDeleteConfirmedRecord decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(Objects.requireNonNull(encoded, "encoded"));
        requireRemaining(input, 4 + HASH_LENGTH * 2 + 4 + 1 + HASH_LENGTH + 4 + 4 + HASH_LENGTH + 4 + 4 + 4);
        final int version = input.getInt();
        if (version != 1 && version != 2) {
            throw new IllegalArgumentException("unsupported delete confirmation version");
        }
        final byte[] mutationId = readFixed(input, HASH_LENGTH, "confirmationMutationId");
        final byte[] mutationHash = readFixed(input, HASH_LENGTH, "confirmationMutationHash");
        final ResourceRetireIntentRecord intent = ResourceRetireIntentRecord.decode(readLp32(input,
                "retireIntent"));
        final ResourceDeleteConfirmedBody.DeleteOutcome outcome = ResourceDeleteConfirmedBody.DeleteOutcome
                .fromWire(input.get() & 0xff);
        final byte[] providerRequestIdHash = readFixed(input, HASH_LENGTH, "providerRequestIdHash");
        final long mutationSequence = version == 2 ? readU64(input, "appliedMutationSequence") : 0;
        final byte[] observedVersion = readLp32(input, "observedImmutableVersion");
        final byte[] observedEtag = readLp32(input, "observedEtag");
        final byte[] responseHash = readFixed(input, HASH_LENGTH, "responseHash");
        final byte[] observedAt = readLp32(input, "observedAt");
        final byte[] confirmedAt = readLp32(input, "confirmedAt");
        final byte[] source = readLp32(input, "appliedSourcePosition");
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("trailing delete confirmation bytes");
        }
        final ResourceDeleteConfirmedRecord result = new ResourceDeleteConfirmedRecord(mutationId, mutationHash,
                intent, outcome, mutationSequence, providerRequestIdHash, observedVersion, observedEtag,
                responseHash, observedAt, confirmedAt, source);
        if (version == 1 ? !Arrays.equals(encoded, result.encodeLegacy()) : !Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical delete confirmation");
        }
        return result;
    }

    private byte[] encodeLegacy() {
        return Bytes.concat(Bytes.u32be(1), confirmationMutationId, confirmationMutationHash,
                Bytes.lp32(retireIntent.encode()), Bytes.u8(outcome.wireValue()), providerRequestIdHash,
                Bytes.lp32(observedImmutableVersion), Bytes.lp32(observedEtag), responseHash,
                Bytes.lp32(observedAt), Bytes.lp32(confirmedAt), Bytes.lp32(appliedSourcePosition));
    }

    private static byte[] readLp32(final ByteBuffer input, final String name) {
        requireRemaining(input, Integer.BYTES);
        final long length = Integer.toUnsignedLong(input.getInt());
        if (length > input.remaining()) {
            throw new IllegalArgumentException(name + " length outside delete confirmation");
        }
        return readFixed(input, Math.toIntExact(length), name);
    }

    private static byte[] readFixed(final ByteBuffer input, final int length, final String name) {
        requireRemaining(input, length);
        final byte[] result = new byte[length];
        input.get(result);
        return result;
    }

    private static long readU64(final ByteBuffer input, final String name) {
        requireRemaining(input, Long.BYTES);
        final long value = input.getLong();
        return value;
    }

    private static void requireRemaining(final ByteBuffer input, final int length) {
        if (length < 0 || input.remaining() < length) {
            throw new IllegalArgumentException("delete confirmation is truncated");
        }
    }
}
