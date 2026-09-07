package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** NDIP-3 Target queue projection; physical identity lives in a separate immutable record. */
public final class TargetQueueState {
    public static final int VERSION = 1;
    /** Reserved for the Target Store format; the current Lane ValueEnvelope reader still rejects it. */
    public static final int VALUE_TYPE = 12;

    public static final int MAX_DOMAIN_SLOTS = 64;
    public static final int MAX_CANONICAL_BYTES =
            2 + 34 + 11 + 11 + 2 + 18 + 11 + MAX_DOMAIN_SLOTS * (3 + TargetDomainState.MAX_CANONICAL_BYTES) + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-queue-state\0");

    public enum AdmissionState {
        OPEN(1),
        PAUSED(2),
        CLOSED(3);
        private final int wire;

        AdmissionState(final int wire) {
            this.wire = wire;
        }

        public int wireValue() {
            return wire;
        }

        private static AdmissionState decode(final int value) {
            for (AdmissionState state : values()) {
                if (state.wire == value) {
                    return state;
                }
            }
            throw new IllegalArgumentException("unknown Target admission state");
        }
    }

    private final TargetPartitionId targetId;
    private final long headRevision;
    private final long controlVersion;
    private final AdmissionState admissionState;
    private final byte[] accountingIncarnation;
    private final long nativeIndexLeadCapMs;
    private final List<TargetDomainState> domains;
    private final byte[] digest;

    public TargetQueueState(
            final TargetPartitionId targetId,
            final long headRevision,
            final long controlVersion,
            final AdmissionState admissionState,
            final byte[] accountingIncarnation,
            final long nativeIndexLeadCapMs,
            final List<TargetDomainState> domains) {
        this.targetId = Objects.requireNonNull(targetId, "targetId");
        if (headRevision == 0 || controlVersion == 0 || nativeIndexLeadCapMs < 0) {
            throw new IllegalArgumentException("invalid Target revision/control/cap");
        }
        this.headRevision = headRevision;
        this.controlVersion = controlVersion;
        this.admissionState = Objects.requireNonNull(admissionState, "admissionState");
        Bytes.requireLength(accountingIncarnation, 16, "accountingIncarnation");
        if (Arrays.equals(accountingIncarnation, new byte[16])) {
            throw new IllegalArgumentException("Target accounting incarnation is unassigned");
        }
        this.accountingIncarnation = Bytes.copy(accountingIncarnation);
        this.nativeIndexLeadCapMs = nativeIndexLeadCapMs;
        Objects.requireNonNull(domains, "domains");
        if (domains.size() > MAX_DOMAIN_SLOTS) {
            throw new IllegalArgumentException("Target domain count exceeds its schema bound");
        }
        this.domains = List.copyOf(domains);
        for (int slot = 0; slot < this.domains.size(); slot++) {
            final TargetDomainState domain = this.domains.get(slot);
            if (domain.domain().slot() != slot) {
                throw new IllegalArgumentException("Target slots must retain contiguous generation history");
            }
            requireHead(domain.ordinaryHead());
            requireHead(domain.nativeHead());
        }
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    private void requireHead(final TargetHeadRef head) {
        if (head != null && (!head.target().equals(targetId) || admissionState == AdmissionState.CLOSED)) {
            throw new IllegalArgumentException("Target head belongs to another target or a closed target");
        }
    }

    public TargetPartitionId targetId() {
        return targetId;
    }

    public long headRevision() {
        return headRevision;
    }

    public long controlVersion() {
        return controlVersion;
    }

    public AdmissionState admissionState() {
        return admissionState;
    }

    public byte[] accountingIncarnation() {
        return Bytes.copy(accountingIncarnation);
    }

    public long nativeIndexLeadCapMs() {
        return nativeIndexLeadCapMs;
    }

    public List<TargetDomainState> domains() {
        return domains;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, targetId.bytes());
            CanonicalProtobuf.uint64Bits(out, 3, headRevision);
            CanonicalProtobuf.uint64Bits(out, 4, controlVersion);
            CanonicalProtobuf.uint32(out, 5, admissionState.wireValue());
            CanonicalProtobuf.bytes(out, 6, accountingIncarnation);
            CanonicalProtobuf.uint64(out, 7, nativeIndexLeadCapMs);
            for (TargetDomainState domain : domains) {
                CanonicalProtobuf.bytes(out, 8, domain.canonicalBytes());
            }
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 9, digest);
        });
    }

    public static TargetQueueState decode(final byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target state exceeds its byte bound");
        }
        final var reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            if (fields.size() >= 8 + MAX_DOMAIN_SLOTS) {
                throw new IllegalArgumentException("Target state exceeds its field count bound");
            }
            fields.add(reader.next());
        }
        if (fields.size() < 8 || QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported or missing Target state version");
        }
        int index = 7;
        final List<TargetDomainState> domains = new ArrayList<>();
        while (index < fields.size() && fields.get(index).number() == 8) {
            domains.add(TargetDomainState.decode(QueryCodecSupport.bytes(fields.get(index++), 8)));
        }
        if (index != fields.size() - 1) {
            throw new IllegalArgumentException("unknown or missing Target state field");
        }
        final TargetQueueState state = new TargetQueueState(
                new TargetPartitionId(QueryCodecSupport.fixed(fields.get(1), 2, 32)),
                QueryCodecSupport.uint64Bits(fields.get(2), 3),
                QueryCodecSupport.uint64Bits(fields.get(3), 4),
                AdmissionState.decode(QueryCodecSupport.uint32(fields.get(4), 5)),
                QueryCodecSupport.fixed(fields.get(5), 6, 16),
                QueryCodecSupport.uint(fields.get(6), 7),
                domains);
        if (!Bytes.constantTimeEquals(state.digest, QueryCodecSupport.fixed(fields.get(index), 9, 32))) {
            throw new IllegalArgumentException("Target state digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, state.canonicalBytes(), "TargetQueueState");
        return state;
    }

    /** Applies DB-key, source-Shard and activated slot-count checks in addition to canonical validation. */
    public static TargetQueueState decodeForStore(
            final byte[] key,
            final byte[] encoded,
            final CanonicalTargetPartition physicalIdentity,
            final ShardId sourceShard,
            final int maximumDomainSlots) {
        Objects.requireNonNull(sourceShard, "sourceShard");
        Objects.requireNonNull(physicalIdentity, "physicalIdentity");
        if (maximumDomainSlots < 1 || maximumDomainSlots > MAX_DOMAIN_SLOTS) {
            throw new IllegalArgumentException("invalid activated Target domain limit");
        }
        final TargetQueueState state = decode(encoded);
        if (!Arrays.equals(key, TargetKeyCodec.state(state.targetId))
                || state.domains.size() > maximumDomainSlots
                || !state.targetId.equals(physicalIdentity.id())) {
            throw new IllegalArgumentException(
                    "Target state key, physical identity or activated domain limit mismatch");
        }
        for (TargetDomainState domain : state.domains) {
            if (domain.nativeHead() != null
                    && physicalIdentity.resource().kind() != BrokerResourceIdentity.Kind.PULSAR) {
                throw new IllegalArgumentException("native Target head requires a Pulsar physical resource");
            }
            requireSourceShard(domain.ordinaryHead(), sourceShard);
            requireSourceShard(domain.nativeHead(), sourceShard);
        }
        return state;
    }

    private static void requireSourceShard(final TargetHeadRef head, final ShardId shard) {
        if (head != null && !head.messageId().routingId().shardId().equals(shard)) {
            throw new IllegalArgumentException("Target head belongs to another source Shard");
        }
    }

    public static long nextRevision(final long prior) {
        if (prior == 0) {
            throw new IllegalArgumentException("Target revision is unassigned");
        }
        if (prior == -1L) {
            throw new IllegalStateException("Target revision exhausted without a permitted wrap");
        }
        return prior + 1;
    }

    /** Checks structural evolution only; source authority and obligation-release proofs remain separate gates. */
    public void requireSuccessorOf(final TargetQueueState previous) {
        Objects.requireNonNull(previous, "previous");
        if (!targetId.equals(previous.targetId)
                || !Arrays.equals(accountingIncarnation, previous.accountingIncarnation)
                || nativeIndexLeadCapMs != previous.nativeIndexLeadCapMs
                || headRevision != nextRevision(previous.headRevision)) {
            throw new IllegalArgumentException("Target successor changes immutable identity/cap or skips revision");
        }
        if ((controlVersion != previous.controlVersion && controlVersion != nextRevision(previous.controlVersion))
                || (admissionState != previous.admissionState && controlVersion == previous.controlVersion)
                || (previous.admissionState == AdmissionState.CLOSED && admissionState != AdmissionState.CLOSED)) {
            throw new IllegalArgumentException("Target successor violates source-ordered control evolution");
        }
        if (domains.size() < previous.domains.size()) {
            throw new IllegalArgumentException("Target successor forgets slot generation history");
        }
        for (int slot = 0; slot < domains.size(); slot++) {
            final TargetDomainState next = domains.get(slot);
            if (slot >= previous.domains.size()) {
                if (next.domain().generation() != 1 || next.lifecycle() != TargetDomainState.Lifecycle.ACTIVE) {
                    throw new IllegalArgumentException("new Target slot must start ACTIVE at generation one");
                }
                continue;
            }
            final TargetDomainState prior = previous.domains.get(slot);
            if (prior.lifecycle() == TargetDomainState.Lifecycle.VACANT) {
                final boolean stillVacant = next.lifecycle() == TargetDomainState.Lifecycle.VACANT;
                if (next.domain().generation()
                                != (stillVacant
                                        ? prior.domain().generation()
                                        : nextRevision(prior.domain().generation()))
                        || (!stillVacant && next.lifecycle() != TargetDomainState.Lifecycle.ACTIVE)) {
                    throw new IllegalArgumentException("Target slot reuse did not advance its generation");
                }
            } else if (next.domain().generation() != prior.domain().generation()
                    || (prior.lifecycle() == TargetDomainState.Lifecycle.DRAINING
                            && next.lifecycle() == TargetDomainState.Lifecycle.ACTIVE)
                    || (prior.lifecycle() == TargetDomainState.Lifecycle.ACTIVE
                            && next.lifecycle() == TargetDomainState.Lifecycle.VACANT)) {
                throw new IllegalArgumentException("Target bound slot bypasses draining or rewrites generation");
            } else if (next.lifecycle() != TargetDomainState.Lifecycle.VACANT
                    && (!Arrays.equals(prior.dispatchCompatibilityRef(), next.dispatchCompatibilityRef())
                            || !Arrays.equals(prior.controlScopeRef(), next.controlScopeRef())
                            || !Arrays.equals(prior.nativePolicyScopeRef(), next.nativePolicyScopeRef()))) {
                throw new IllegalArgumentException("Target bound slot rewrites its execution/control/policy scope");
            }
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetQueueState that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
