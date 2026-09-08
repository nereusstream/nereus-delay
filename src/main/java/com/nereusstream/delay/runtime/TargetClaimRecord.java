package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaClaimCharge;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

/** Durable reversible ownership of an exact Target current work; this record confers no Producer permission. */
public final class TargetClaimRecord {
    public static final int VALUE_TYPE = 36;
    public static final int VERSION = 1;
    public static final int MAX_CANONICAL_BYTES = TargetGenerationRuntimeIndex.MAX_CANONICAL_BYTES
            + TargetHeadRef.MAX_CANONICAL_BYTES
            + TargetQuotaClaimCharge.MAX_OWNER_BYTES
            + TargetQuotaMutation.MAX_CANONICAL_BYTES
            + 1024;
    private static final byte[] ID_DOMAIN = Bytes.utf8("nereus-delay-target-claim-id\0");
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-claim-record\0");
    private final TargetGenerationRuntimeIndex original;
    private final long messageVersion;
    private final byte[] messageDigest;
    private final TargetHeadRef selected;
    private final OwnerIdentity owner;
    private final byte[] store;
    private final long sequence;
    private final long deadline;
    private final long executionBytes;
    private final long controlVersion;
    private final TargetQuotaMutation creation;
    private final byte[] claimId;
    private final byte[] digest;

    public TargetClaimRecord(
            final TargetGenerationRuntimeIndex original,
            final long messageVersion,
            final byte[] messageDigest,
            final TargetHeadRef selected,
            final OwnerIdentity owner,
            final byte[] storeIncarnation,
            final long sequence,
            final long deadline,
            final long executionBytes,
            final long controlVersion,
            final TargetQuotaMutation creation) {
        this.original = Objects.requireNonNull(original, "original");
        this.selected = Objects.requireNonNull(selected, "selected");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.creation = Objects.requireNonNull(creation, "creation");
        this.messageDigest = assigned(messageDigest, 32, "messageDigest");
        store = assigned(storeIncarnation, 16, "storeIncarnation");
        if (original.currentWorkKind() != CurrentSendWorkKind.TIMELINE
                || messageVersion == 0
                || sequence == 0
                || deadline < 0
                || executionBytes <= 0
                || controlVersion == 0
                || owner.canonicalBytes().length > TargetQuotaClaimCharge.MAX_OWNER_BYTES
                || !creation.isLocalClaim()) {
            throw new IllegalArgumentException("invalid Target Claim work, bounds or local stamp");
        }
        final var work = original.timeline();
        if (!work.locator().messageId().equals(selected.messageId())
                || work.locator().generation() != selected.generation()
                || !work.locator().target().equals(selected.target())
                || !work.locator().domain().equals(selected.domain())
                || !creation.source()
                        .shardId()
                        .equals(work.locator().messageId().routingId().shardId())
                || (selected.nativeCandidate() && !work.nativeCandidate())) {
            throw new IllegalArgumentException("Target Claim head differs from original work");
        }
        work.requireHeadProjection(selected);
        this.messageVersion = messageVersion;
        this.sequence = sequence;
        this.deadline = deadline;
        this.executionBytes = executionBytes;
        this.controlVersion = controlVersion;
        claimId = Bytes.sha256(ID_DOMAIN, fields());
        digest = Bytes.sha256(DIGEST_DOMAIN, fields(), claimId);
    }

    public TargetTimelineWorkRef work() {
        return original.timeline();
    }

    public OwnerIdentity owner() {
        return owner;
    }

    public byte[] storeIncarnation() {
        return Bytes.copy(store);
    }

    public long sequence() {
        return sequence;
    }

    public long deadlineEpochMs() {
        return deadline;
    }

    public long executionBytes() {
        return executionBytes;
    }

    public TargetQuotaMutation creation() {
        return creation;
    }

    public byte[] claimId() {
        return Bytes.copy(claimId);
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public TargetHeadRef selected() {
        return selected;
    }

    public long controlVersion() {
        return controlVersion;
    }

    public static byte[] key(final byte[] claimId) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.CLAIM_TAG, TargetKeyCodec.KEY_FORMAT}, assigned(claimId, 32, "claimId"));
    }

    public byte[] key() {
        return key(claimId);
    }

    public byte[] chargeKey() {
        final var shard = work().locator().messageId().routingId().shardId();
        return Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_CLAIM_CHARGE_TAG, TargetKeyCodec.KEY_FORMAT},
                shard.routeIncarnation().bytes(),
                Bytes.u32beBits(shard.partition()),
                Bytes.u64beBits(owner.ownerEpoch()),
                claimId);
    }

    public TargetMessageRecord claimed(final TargetMessageRecord before) {
        if (before.stateVersion() != messageVersion
                || !Arrays.equals(before.digest(), messageDigest)
                || !before.runtime().equals(original)) {
            throw new IllegalStateException("Target Claim original Message changed");
        }
        return copy(before, TargetQueueState.nextRevision(messageVersion), claimedRuntime());
    }

    public void requireCurrent(final TargetMessageRecord current) {
        if (current.stateVersion() != TargetQueueState.nextRevision(messageVersion)
                || !current.runtime().equals(claimedRuntime())
                || !Arrays.equals(copy(current, messageVersion, original).digest(), messageDigest)) {
            throw new IllegalStateException("Target Claim is no longer the exact current Message work");
        }
    }

    public TargetMessageRecord revoked(final TargetMessageRecord current) {
        requireCurrent(current);
        final long revision = TargetQueueState.nextRevision(current.runtime().runtimeRevision());
        final var old = work();
        final var restored = new TargetTimelineWorkRef(
                old.locator(),
                old.workKind(),
                old.deliverAtEpochMs(),
                old.retryEligibilityAtEpochMs(),
                old.sourceOrderToken(),
                old.candidateAttemptNo(),
                revision,
                old.uncertainRetryAuthority(),
                old.uncertainRetryControl(),
                old.uncertainRetryControlPosition(),
                old.nativeCandidate());
        final var runtime = new TargetGenerationRuntimeIndex(
                original.generation(),
                original.aggregateState(),
                CurrentSendWorkKind.TIMELINE,
                restored,
                null,
                null,
                original.attemptObligations(),
                original.admissionsUsed(),
                original.uncertainRetryAdmissionsUsed(),
                original.possibleDestinationDuplicate(),
                revision);
        return copy(current, TargetQueueState.nextRevision(current.stateVersion()), runtime);
    }

    private TargetGenerationRuntimeIndex claimedRuntime() {
        return new TargetGenerationRuntimeIndex(
                original.generation(),
                original.aggregateState() == GenerationAggregateState.UNCERTAIN
                        ? GenerationAggregateState.UNCERTAIN
                        : GenerationAggregateState.CLAIMED,
                CurrentSendWorkKind.CLAIMED,
                null,
                claimId,
                null,
                original.attemptObligations(),
                original.admissionsUsed(),
                original.uncertainRetryAdmissionsUsed(),
                original.possibleDestinationDuplicate(),
                TargetQueueState.nextRevision(original.runtimeRevision()));
    }

    public void requireCharge(final TargetQuotaClaimCharge charge) {
        if (!Arrays.equals(claimId, charge.claimId())
                || !Arrays.equals(work().canonicalBytes(), charge.work().canonicalBytes())
                || !owner.equals(charge.owner())
                || !Arrays.equals(store, charge.storeIncarnation())
                || sequence != charge.claimSequence()
                || deadline != charge.deadlineEpochMs()
                || executionBytes != charge.executionBytes()
                || !creation.equals(charge.creation())
                || !Arrays.equals(Bytes.sha256(canonicalBytes()), charge.claimRecordDigest())) {
            throw new IllegalStateException("Target Claim and frozen charge disagree");
        }
    }

    public void requireStored(final byte[] key) {
        if (!Arrays.equals(key(), key)) {
            throw new IllegalStateException("Target Claim storage key mismatch");
        }
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, original.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 3, messageVersion);
            CanonicalProtobuf.bytes(out, 4, messageDigest);
            CanonicalProtobuf.bytes(out, 5, selected.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, owner.canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, store);
            CanonicalProtobuf.uint64Bits(out, 8, sequence);
            CanonicalProtobuf.uint64(out, 9, deadline);
            CanonicalProtobuf.uint64(out, 10, executionBytes);
            CanonicalProtobuf.uint64Bits(out, 11, controlVersion);
            CanonicalProtobuf.bytes(out, 12, creation.canonicalBytes());
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 13, claimId);
            CanonicalProtobuf.bytes(out, 14, digest);
        });
    }

    public static TargetClaimRecord decode(final byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target Claim exceeds its byte bound");
        }
        final var reader = new CanonicalProtobuf.Reader(encoded);
        final var f = new ArrayList<CanonicalProtobuf.Reader.Field>();
        while (reader.hasRemaining()) {
            if (f.size() == 14) {
                throw new IllegalArgumentException("Target Claim field count exceeded");
            }
            f.add(reader.next());
        }
        QueryCodecSupport.requireNumbers(
                f, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, "TargetClaimRecord");
        if (QueryCodecSupport.uint32(f.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unknown Target Claim version");
        }
        final byte[] owner = QueryCodecSupport.bytes(f.get(5), 6);
        if (owner.length > TargetQuotaClaimCharge.MAX_OWNER_BYTES) {
            throw new IllegalArgumentException("Target Claim Owner exceeds its byte bound");
        }
        final var result = new TargetClaimRecord(
                TargetGenerationRuntimeIndex.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.uint64Bits(f.get(2), 3),
                QueryCodecSupport.fixed(f.get(3), 4, 32),
                TargetHeadRef.decode(QueryCodecSupport.bytes(f.get(4), 5)),
                OwnerIdentity.decode(owner),
                QueryCodecSupport.fixed(f.get(6), 7, 16),
                QueryCodecSupport.uint64Bits(f.get(7), 8),
                QueryCodecSupport.uint(f.get(8), 9),
                QueryCodecSupport.uint(f.get(9), 10),
                QueryCodecSupport.uint64Bits(f.get(10), 11),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(11), 12)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetClaimRecord");
        return result;
    }

    private static TargetMessageRecord copy(
            final TargetMessageRecord message, final long version, final TargetGenerationRuntimeIndex runtime) {
        return new TargetMessageRecord(
                message.locator(),
                version,
                message.deliverAtEpochMs(),
                message.expireAtEpochMs(),
                message.retryEligibilityAtEpochMs(),
                message.nativeDeliveryPolicy(),
                message.scheduleSource(),
                message.inlinePayload(),
                message.payloadReference(),
                runtime);
    }

    private static byte[] assigned(final byte[] bytes, final int length, final String name) {
        Bytes.requireLength(bytes, length, name);
        if (Arrays.equals(bytes, new byte[length])) {
            throw new IllegalArgumentException(name + " is unassigned");
        }
        return Bytes.copy(bytes);
    }
}
