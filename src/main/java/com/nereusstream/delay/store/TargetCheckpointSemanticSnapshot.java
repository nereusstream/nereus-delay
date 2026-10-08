package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.runtime.PayloadProofTrustSetControlCatalog;
import com.nereusstream.delay.runtime.ProfileCatalog;
import com.nereusstream.delay.runtime.RetryPolicyCatalog;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/** Closed Target semantic contents and original Admission frames; external control/pin/authentication is required. */
public final class TargetCheckpointSemanticSnapshot {
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-checkpoint-semantic-snapshot\0");

    public record Limits(int maximumRecords, int maximumBytes, long maximumAdmissionFrameBytes) {
        public Limits {
            if (maximumRecords <= 0 || maximumRecords == Integer.MAX_VALUE || maximumBytes <= 0
                    || maximumBytes == Integer.MAX_VALUE || maximumAdmissionFrameBytes <= 0
                    || maximumAdmissionFrameBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Target semantic snapshot requires finite positive limits");
            }
        }
    }

    private final StoreMetadata metadata;
    private final SourcePosition source;
    private final long mutationSequence;
    private final byte[] lineage;
    private final byte[] controlsDigest;
    private final byte[] dependenciesDigest;
    private final List<ProfileSemanticEnvelope> profiles;
    private final List<RetryPolicySemantic> retries;
    private final List<PayloadProofTrustSetSemantic> trusts;
    private final List<TargetCheckpointDependencies.AdmissionImage> admissions;
    private final byte[] digest;

    private TargetCheckpointSemanticSnapshot(StoreMetadata metadata, SourcePosition source, long mutationSequence,
            byte[] lineage, byte[] controlsDigest, byte[] dependenciesDigest, List<ProfileSemanticEnvelope> profiles,
            List<RetryPolicySemantic> retries, List<PayloadProofTrustSetSemantic> trusts,
            List<TargetCheckpointDependencies.AdmissionImage> admissions, Limits limits) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.source = Objects.requireNonNull(source, "source");
        if (metadata.storeFormatVersion() != 2 || !metadata.shardId().equals(source.shardId())) {
            throw new IllegalArgumentException("Target semantic snapshot has an invalid Store/Source identity");
        }
        this.mutationSequence = mutationSequence;
        Bytes.requireLength(lineage, 16, "lineage");
        Bytes.requireLength(controlsDigest, 32, "controlsDigest");
        Bytes.requireLength(dependenciesDigest, 32, "dependenciesDigest");
        this.lineage = Bytes.copy(lineage);
        this.controlsDigest = Bytes.copy(controlsDigest);
        this.dependenciesDigest = Bytes.copy(dependenciesDigest);
        this.profiles = ordered(profiles, value -> value.ref().canonicalBytes());
        final var immutable = new java.util.HashSet<String>();
        for (final var profile : profiles) {
            if (!immutable.add(Bytes.hex(profile.profileId()) + '/' + Long.toUnsignedString(profile.version()))) {
                throw new IllegalArgumentException("conflicting immutable Profile version in Target semantic snapshot");
            }
        }
        this.retries = ordered(retries, value -> value.ref().canonicalBytes());
        this.trusts = ordered(trusts, value -> value.ref().canonicalBytes());
        this.admissions = ordered(admissions, TargetCheckpointDependencies.AdmissionImage::canonicalBytes);
        if ((long) profiles.size() + retries.size() + trusts.size() + admissions.size() > limits.maximumRecords()) {
            throw new IllegalArgumentException("Target semantic snapshot exceeds its record bound");
        }
        long frameBytes = 0;
        for (final var admission : admissions) {
            frameBytes = Math.addExact(frameBytes, admission.image().encodeFrame().length);
        }
        if (frameBytes > limits.maximumAdmissionFrameBytes()) {
            throw new IllegalArgumentException("Target semantic snapshot exceeds its Admission frame bound");
        }
        digest = Bytes.sha256(DOMAIN, fields());
        if (canonicalBytes().length > limits.maximumBytes()) {
            throw new IllegalArgumentException("Target semantic snapshot exceeds its byte bound");
        }
    }

    /** Providers run outside Store locks with finite deadlines; the guard protects the complete input set. */
    public static TargetCheckpointSemanticSnapshot collect(TargetCheckpointDependencies dependencies,
            List<TargetCheckpointDependencies.AdmissionImage> admissions, ProfileCatalog profiles,
            RetryPolicyCatalog retries, PayloadProofTrustSetControlCatalog trusts, BoundedReadBudget budget,
            Limits limits, Runnable protectionGuard) {
        final var complete = dependencies.withAdmissionImages(admissions, limits.maximumAdmissionFrameBytes());
        final var guard = Objects.requireNonNull(protectionGuard, "protectionGuard");
        guard.run();
        final var profileValues = new TreeMap<String, ProfileSemanticEnvelope>();
        final var pending = new TreeMap<String, ProfileRef>();
        complete.profiles().forEach(ref -> pending.put(Bytes.hex(ref.canonicalBytes()), ref));
        while (!pending.isEmpty()) {
            final var ref = pending.pollFirstEntry().getValue();
            if (profileValues.containsKey(Bytes.hex(ref.canonicalBytes()))) { continue; }
            before(budget, guard);
            final var value = profiles.resolve(ref);
            guard.run();
            if (value == null || !value.ref().equals(ref)) {
                throw new IllegalStateException("Target semantic snapshot lacks an exact Profile value");
            }
            charge(budget, ref.canonicalBytes().length, value.canonicalBytes().length);
            profileValues.put(Bytes.hex(ref.canonicalBytes()), value);
            if (value.body() instanceof DestinationProfileSemantic destination) {
                final var capability = destination.deliveryCapability();
                pending.put(Bytes.hex(capability.canonicalBytes()), capability);
            }
        }
        final var retryValues = complete.resolveRetryPolicies(retries, budget, guard);
        final var trustValues = new ArrayList<PayloadProofTrustSetSemantic>();
        for (final var ref : complete.trustSets()) {
            before(budget, guard);
            final var value = trusts.resolve(ref);
            guard.run();
            if (value == null || !value.ref().equals(ref)) {
                throw new IllegalStateException("Target semantic snapshot lacks an exact Trust Set value");
            }
            charge(budget, ref.canonicalBytes().length, value.canonicalBytes().length);
            trustValues.add(value);
        }
        guard.run();
        final var root = complete.root();
        final var snapshot = new TargetCheckpointSemanticSnapshot(
                root.metadata(), root.source(), root.mutationSequence(),
                root.root().recoveryLineage(), complete.storedControlsDigest(), complete.semanticInputsDigest(),
                List.copyOf(profileValues.values()), retryValues, trustValues, admissions, limits);
        snapshot.validateAgainst(dependencies, limits);
        guard.run();
        return snapshot;
    }

    /** Exact image contents and closure still need historical publication, keys, pins and activation authority. */
    public void validateAgainst(TargetCheckpointDependencies dependencies, Limits limits) {
        if ((long) profiles.size() + retries.size() + trusts.size() + admissions.size() > limits.maximumRecords()
                || canonicalBytes().length > limits.maximumBytes()) {
            throw new IllegalArgumentException("Target semantic snapshot exceeds validation limits");
        }
        final var complete = dependencies.withAdmissionImages(admissions, limits.maximumAdmissionFrameBytes());
        final var root = complete.root();
        if (!Arrays.equals(metadata.encode(), root.metadata().encode())
                || !Arrays.equals(source.canonicalBytes(), root.source().canonicalBytes())
                || mutationSequence != root.mutationSequence()
                || !Arrays.equals(lineage, root.root().recoveryLineage())
                || !Arrays.equals(controlsDigest, complete.storedControlsDigest())
                || !Arrays.equals(dependenciesDigest, complete.semanticInputsDigest())) {
            throw new IllegalArgumentException(
                    "Target semantic snapshot differs from the physical Store cut/dependencies");
        }
        final var expected = new TreeMap<String, ProfileRef>();
        complete.profiles().forEach(ref -> expected.put(Bytes.hex(ref.canonicalBytes()), ref));
        final var actual = new TreeMap<String, ProfileSemanticEnvelope>();
        profiles.forEach(value -> actual.put(Bytes.hex(value.ref().canonicalBytes()), value));
        final var seen = new java.util.HashSet<String>();
        final var queue = new ArrayList<>(expected.values());
        for (int index = 0; index < queue.size(); index++) {
            final var ref = queue.get(index);
            final var identity = Bytes.hex(ref.canonicalBytes());
            if (!seen.add(identity)) { continue; }
            final var value = actual.get(identity);
            if (value == null) {
                throw new IllegalArgumentException("Target semantic snapshot omits a required Profile");
            }
            if (value.body() instanceof DestinationProfileSemantic destination) {
                queue.add(destination.deliveryCapability());
            }
        }
        if (!actual.keySet().equals(seen)) {
            throw new IllegalArgumentException("Target semantic snapshot has extra Profile values");
        }
        if (!complete.retryPolicies().stream().map(ref -> Bytes.hex(ref.canonicalBytes())).sorted().toList()
                .equals(retries.stream().map(value -> Bytes.hex(value.ref().canonicalBytes())).sorted().toList())
                || !complete.trustSets().stream().map(ref -> Bytes.hex(ref.canonicalBytes())).sorted().toList()
                .equals(trusts.stream().map(value -> Bytes.hex(value.ref().canonicalBytes())).sorted().toList())) {
            throw new IllegalArgumentException("Target semantic snapshot Retry/Trust version set differs");
        }
        for (final var hash : complete.objectProfileHashes()) {
            if (profiles.stream().noneMatch(value -> value.profileKind() == ProfileKind.OBJECT_STORE
                    && Arrays.equals(hash, value.semanticHash()))) {
                throw new IllegalArgumentException("Target semantic snapshot omits an object owner's Profile");
            }
        }
    }

    /** Cheap admission identity check; the complete physical ledger is still required before placement. */
    public void requireCut(StoreMetadata expectedMetadata, SourcePosition expectedSource, long expectedSequence,
            byte[] expectedLineage) {
        if (expectedSource == null || !Arrays.equals(metadata.encode(), expectedMetadata.encode())
                || !Arrays.equals(source.canonicalBytes(), expectedSource.canonicalBytes())
                || mutationSequence != expectedSequence || !Arrays.equals(lineage, expectedLineage)) {
            throw new IllegalArgumentException("Target semantic companion request differs from its Store cut");
        }
    }

    public byte[] snapshotDigest() { return Bytes.copy(digest); }
    public List<ProfileSemanticEnvelope> profiles() { return profiles; }
    public List<RetryPolicySemantic> retryPolicies() { return retries; }
    public List<PayloadProofTrustSetSemantic> trustSets() { return trusts; }
    public List<TargetCheckpointDependencies.AdmissionImage> admissions() { return admissions; }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 12, digest);
        });
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, metadata.encode());
            CanonicalProtobuf.bytes(out, 3, source.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 4, mutationSequence);
            CanonicalProtobuf.bytes(out, 5, lineage);
            CanonicalProtobuf.bytes(out, 6, controlsDigest);
            CanonicalProtobuf.bytes(out, 7, dependenciesDigest);
            for (final var profile : profiles) { CanonicalProtobuf.bytes(out, 8, profile.canonicalBytes()); }
            for (final var retry : retries) { CanonicalProtobuf.bytes(out, 9, retry.canonicalBytes()); }
            for (final var trust : trusts) { CanonicalProtobuf.bytes(out, 10, trust.canonicalBytes()); }
            for (final var admission : admissions) { CanonicalProtobuf.bytes(out, 11, admission.canonicalBytes()); }
        });
    }

    public static TargetCheckpointSemanticSnapshot decode(byte[] encoded, Limits limits) {
        if (encoded == null || encoded.length == 0 || encoded.length > limits.maximumBytes()) {
            throw new IllegalArgumentException("invalid Target semantic snapshot length");
        }
        final var reader = new CanonicalProtobuf.Reader(encoded, true);
        final var fields = new ArrayList<CanonicalProtobuf.Reader.Field>();
        while (reader.hasRemaining()) {
            if (fields.size() >= (long) limits.maximumRecords() + 8) {
                throw new IllegalArgumentException("Target semantic snapshot exceeds its field bound");
            }
            fields.add(reader.next());
        }
        if (fields.size() < 8 || fields.size() > (long) limits.maximumRecords() + 8) {
            throw new IllegalArgumentException("invalid Target semantic snapshot field count");
        }
        if (QueryCodecSupport.uint(fields.get(0), 1) != 1) {
            throw new IllegalArgumentException("unsupported Target semantic snapshot version");
        }
        final var profiles = new ArrayList<ProfileSemanticEnvelope>();
        final var retries = new ArrayList<RetryPolicySemantic>();
        final var trusts = new ArrayList<PayloadProofTrustSetSemantic>();
        final var admissions = new ArrayList<TargetCheckpointDependencies.AdmissionImage>();
        int index = 7;
        while (index < fields.size() && fields.get(index).number() == 8) {
            profiles.add(ProfileSemanticEnvelope.decode(QueryCodecSupport.bytes(fields.get(index++), 8)));
        }
        while (index < fields.size() && fields.get(index).number() == 9) {
            retries.add(RetryPolicySemantic.decode(QueryCodecSupport.bytes(fields.get(index++), 9)));
        }
        while (index < fields.size() && fields.get(index).number() == 10) {
            trusts.add(PayloadProofTrustSetSemantic.decode(QueryCodecSupport.bytes(fields.get(index++), 10)));
        }
        while (index < fields.size() && fields.get(index).number() == 11) {
            final var image = QueryCodecSupport.read(
                    QueryCodecSupport.bytes(fields.get(index++), 11), "Admission image");
            QueryCodecSupport.requireNumbers(image, new int[] {1, 2}, "Admission image");
            admissions.add(new TargetCheckpointDependencies.AdmissionImage(
                    SourcePositionCodec.decode(QueryCodecSupport.bytes(image.get(0), 1)),
                    SystemMutation.decodeFrame(QueryCodecSupport.bytes(image.get(1), 2))));
        }
        if (index + 1 != fields.size()) {
            throw new IllegalArgumentException("unknown Target semantic snapshot fields");
        }
        final var result = new TargetCheckpointSemanticSnapshot(
                StoreMetadata.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                SourcePositionCodec.decode(QueryCodecSupport.bytes(fields.get(2), 3)),
                QueryCodecSupport.uint64Bits(fields.get(3), 4), QueryCodecSupport.fixed(fields.get(4), 5, 16),
                QueryCodecSupport.fixed(fields.get(5), 6, 32), QueryCodecSupport.fixed(fields.get(6), 7, 32),
                profiles, retries, trusts, admissions, limits);
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(fields.get(index), 12, 32))) {
            throw new IllegalArgumentException("Target semantic snapshot digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target semantic snapshot");
        return result;
    }

    private static void before(BoundedReadBudget budget, Runnable guard) {
        guard.run();
        if (!budget.beforeRead()) { throw budget.incomplete(); }
    }

    private static void charge(BoundedReadBudget budget, int key, int value) {
        if (!budget.tryCharge(key, value) || !budget.beforeTimedWork()) { throw budget.incomplete(); }
    }

    private static <T> List<T> ordered(List<T> values, java.util.function.Function<T, byte[]> identity) {
        final var ordered = new TreeMap<String, T>();
        for (final var value : values) {
            if (ordered.put(Bytes.hex(identity.apply(value)), value) != null) {
                throw new IllegalArgumentException("duplicate Target semantic snapshot identity");
            }
        }
        return List.copyOf(ordered.values());
    }
}
