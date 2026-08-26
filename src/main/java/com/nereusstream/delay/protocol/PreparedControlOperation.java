package com.nereusstream.delay.protocol;

import java.io.ByteArrayOutputStream;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Canonical prepared Control Operation envelope. All hashes/signature input
 * and the operation-specific local target matrix are fixed before registration
 * I/O; the class does not perform Oxia or actor/resource authorization.
 */
public final class PreparedControlOperation {
    public static final int VERSION = 1;
    public static final int HASH_LENGTH = 32;
    public static final int SIGNATURE_LENGTH = 64;
    private static final byte[] REQUEST_HASH_DOMAIN = Bytes.utf8("nereus-delay-control-request\0");
    private static final byte[] TARGET_SNAPSHOT_DOMAIN = Bytes.utf8("nereus-delay-control-target-snapshot\0");
    private static final byte[] PREPARED_DIGEST_DOMAIN = Bytes.utf8("nereus-delay-prepared-control\0");

    private final byte[] operationId;
    private final ControlOperationKind kind;
    private final ControlAuthor author;
    private final ControlOperationRequest request;
    private final byte[] requestHash;
    private final List<ControlTargetRef> targets;
    private final byte[] targetSnapshotHash;
    private final long controlQueryPolicyVersion;
    private final long registrationRetryUntil;
    private final byte[] preparedDigest;
    private final long signingKeyVersion;
    private final byte[] signature;

    private PreparedControlOperation(
            final byte[] operationId,
            final ControlOperationKind kind,
            final ControlAuthor author,
            final ControlOperationRequest request,
            final byte[] requestHash,
            final List<ControlTargetRef> targets,
            final byte[] targetSnapshotHash,
            final long controlQueryPolicyVersion,
            final long registrationRetryUntil,
            final byte[] preparedDigest,
            final long signingKeyVersion,
            final byte[] signature) {
        this.operationId = nonZero(operationId, "operationId");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.author = Objects.requireNonNull(author, "author");
        this.request = Objects.requireNonNull(request, "request");
        if (request.kind() != kind) {
            throw new IllegalArgumentException("prepared operation kind does not match request kind");
        }
        this.requestHash = fixed(requestHash, "requestHash");
        if (!Bytes.constantTimeEquals(this.requestHash, requestHash(kind, request))) {
            throw new IllegalArgumentException("requestHash mismatch");
        }
        this.targets = sortedTargets(targets);
        validateTargetPresence(kind, request, this.targets);
        this.targetSnapshotHash = fixed(targetSnapshotHash, "targetSnapshotHash");
        if (!Bytes.constantTimeEquals(this.targetSnapshotHash, targetSnapshotHash(this.targets))) {
            throw new IllegalArgumentException("targetSnapshotHash mismatch");
        }
        if (controlQueryPolicyVersion == 0 || registrationRetryUntil < 0) {
            throw new IllegalArgumentException("invalid control query policy or registration retry deadline");
        }
        this.controlQueryPolicyVersion = controlQueryPolicyVersion;
        this.registrationRetryUntil = registrationRetryUntil;
        this.preparedDigest = fixed(preparedDigest, "preparedDigest");
        if (!Bytes.constantTimeEquals(this.preparedDigest, calculatePreparedDigest())) {
            throw new IllegalArgumentException("preparedDigest mismatch");
        }
        if (signingKeyVersion <= 0 || signingKeyVersion > 0xffff_ffffL) {
            throw new IllegalArgumentException("signingKeyVersion must be a non-zero uint32");
        }
        this.signingKeyVersion = signingKeyVersion;
        Bytes.requireLength(signature, SIGNATURE_LENGTH, "signature");
        this.signature = Bytes.copy(signature);
    }

    /** Creates and signs a prepared operation before any registration I/O. */
    public static PreparedControlOperation prepare(
            final byte[] operationId,
            final ControlOperationKind kind,
            final ControlAuthor author,
            final ControlOperationRequest request,
            final List<ControlTargetRef> targets,
            final long controlQueryPolicyVersion,
            final long registrationRetryUntil,
            final long signingKeyVersion,
            final PrivateKey signingKey) {
        Objects.requireNonNull(signingKey, "signingKey");
        final byte[] requestHash = requestHash(kind, request);
        final List<ControlTargetRef> sorted = sortedTargets(targets);
        validateTargetPresence(kind, request, sorted);
        final byte[] snapshot = targetSnapshotHash(sorted);
        final byte[] digest = preparedDigest(
                operationId,
                kind,
                author,
                request,
                requestHash,
                sorted,
                snapshot,
                controlQueryPolicyVersion,
                registrationRetryUntil);
        return new PreparedControlOperation(
                operationId,
                kind,
                author,
                request,
                requestHash,
                sorted,
                snapshot,
                controlQueryPolicyVersion,
                registrationRetryUntil,
                digest,
                signingKeyVersion,
                sign(digest, signingKey));
    }

    public byte[] operationId() {
        return Bytes.copy(operationId);
    }

    public ControlOperationKind kind() {
        return kind;
    }

    public ControlAuthor author() {
        return author;
    }

    public ControlOperationRequest request() {
        return request;
    }

    public byte[] requestHash() {
        return Bytes.copy(requestHash);
    }

    public List<ControlTargetRef> targets() {
        return List.copyOf(targets);
    }

    public byte[] targetSnapshotHash() {
        return Bytes.copy(targetSnapshotHash);
    }

    public long controlQueryPolicyVersion() {
        return controlQueryPolicyVersion;
    }

    public long registrationRetryUntil() {
        return registrationRetryUntil;
    }

    public byte[] preparedDigest() {
        return Bytes.copy(preparedDigest);
    }

    public long signingKeyVersion() {
        return signingKeyVersion;
    }

    public byte[] signature() {
        return Bytes.copy(signature);
    }

    /** Builds the canonical revision-one PENDING projection for registration. */
    public CurrentControlOperation initialCurrentOperation() {
        final List<ControlTargetStateView> states = targets.stream()
                .map(target -> new ControlTargetStateView(
                        target.targetIndex(), TargetMarkerState.PENDING, StableCode.OK, 0, null))
                .toList();
        return new CurrentControlOperation(
                operationId,
                requestHash,
                author.tenantResourceScopeHash(),
                ControlOperationState.PENDING,
                1,
                states,
                null);
    }

    /** Validates a completed source mutation against this operation's exact target snapshot. */
    public void validateTargetMutation(final ControlTargetRef target, final SystemMutation mutation) {
        ControlTargetMutationBinding.validate(this, target, mutation);
    }

    public boolean verifySignature(final PublicKey verificationKey) {
        Objects.requireNonNull(verificationKey, "verificationKey");
        try {
            final Signature verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(verificationKey);
            verifier.update(preparedDigest);
            return verifier.verify(signature);
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("Ed25519 verification is unavailable", exception);
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            writeFieldsOneThroughTen(output);
            CanonicalProtobuf.bytes(output, 11, preparedDigest);
            CanonicalProtobuf.uint32(output, 12, signingKeyVersion);
            CanonicalProtobuf.bytes(output, 13, signature);
        });
    }

    public static PreparedControlOperation decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = readRepeated(encoded);
        if (fields.size() < 12) {
            throw new IllegalArgumentException("PreparedControlOperation has too few fields");
        }
        if (fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || fields.get(2).number() != 3
                || fields.get(3).number() != 4
                || fields.get(4).number() != 5
                || fields.get(5).number() != 6) {
            throw new IllegalArgumentException("invalid PreparedControlOperation fixed field order");
        }
        int index = 6;
        final List<ControlTargetRef> targets = new ArrayList<>();
        while (index < fields.size() && fields.get(index).number() == 7) {
            targets.add(ControlTargetRef.decode(QueryCodecSupport.nested(fields.get(index), 7)));
            index++;
        }
        if (targets.isEmpty()
                || index + 6 != fields.size()
                || fields.get(index).number() != 8
                || fields.get(index + 1).number() != 9
                || fields.get(index + 2).number() != 10
                || fields.get(index + 3).number() != 11
                || fields.get(index + 4).number() != 12
                || fields.get(index + 5).number() != 13) {
            throw new IllegalArgumentException("invalid PreparedControlOperation target/fixed field order");
        }
        final ControlOperationKind kind = ControlOperationKind.fromWire(QueryCodecSupport.uint(fields.get(2), 3));
        final PreparedControlOperation result = new PreparedControlOperation(
                QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                kind,
                ControlAuthor.decode(QueryCodecSupport.nested(fields.get(3), 4)),
                ControlOperationRequest.decode(QueryCodecSupport.nested(fields.get(4), 5)),
                QueryCodecSupport.fixed(fields.get(5), 6, HASH_LENGTH),
                targets,
                QueryCodecSupport.fixed(fields.get(index), 8, HASH_LENGTH),
                QueryCodecSupport.uint64Bits(fields.get(index + 1), 9),
                QueryCodecSupport.uint(fields.get(index + 2), 10),
                QueryCodecSupport.fixed(fields.get(index + 3), 11, HASH_LENGTH),
                QueryCodecSupport.uint(fields.get(index + 4), 12),
                QueryCodecSupport.fixed(fields.get(index + 5), 13, SIGNATURE_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PreparedControlOperation");
        return result;
    }

    /** Validates the closed operation-kind/target-kind/presence matrix locally. */
    public static void validateTargetPresence(
            final ControlOperationKind kind,
            final ControlOperationRequest request,
            final List<ControlTargetRef> targets) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(request, "request");
        if (request.kind() != kind) {
            throw new IllegalArgumentException("request kind does not match target operation kind");
        }
        final List<ControlTargetRef> values = Objects.requireNonNull(targets, "targets");
        if (values.isEmpty()) {
            throw new IllegalArgumentException("Control Operation requires at least one target");
        }
        switch (kind) {
            case STOP_NEW_SCHEDULES -> {
                requireOnlyKinds(values, ControlTargetKind.ROUTE, ControlTargetKind.SHARD);
                requireCount(values, ControlTargetKind.ROUTE, 1, 1);
                requireCount(values, ControlTargetKind.SHARD, 1, Integer.MAX_VALUE);
                requireMutationPresence(values, ControlTargetKind.ROUTE, false);
                requireMutationPresence(values, ControlTargetKind.SHARD, true);
            }
            case PAUSE_DESTINATION_LANE, RESUME_DESTINATION_LANE, CLOSE_DESTINATION_LANE, BREAK_ORDERING_DOMAIN -> {
                requireOnlyKinds(values, ControlTargetKind.LANE);
                requireCount(values, ControlTargetKind.LANE, 1, Integer.MAX_VALUE);
                requireMutationPresence(values, ControlTargetKind.LANE, true);
            }
            case DRAIN_SHARD, FENCE_SHARD_FOR_MAINTENANCE, FORCE_CHECKPOINT, GET_CHECKPOINT_CATALOG -> {
                requireOnlyKinds(values, ControlTargetKind.SHARD);
                requireCount(values, ControlTargetKind.SHARD, 1, Integer.MAX_VALUE);
                requireMutationPresence(values, ControlTargetKind.SHARD, false);
            }
            case REPLAY_DEAD_LETTER, RESOLVE_UNCERTAIN -> {
                requireOnlyKinds(values, ControlTargetKind.MESSAGE);
                requireCount(values, ControlTargetKind.MESSAGE, 1, 1);
                requireMutationPresence(values, ControlTargetKind.MESSAGE, true);
                validateMessageTarget(kind, request, values.get(0).message());
            }
            case PUBLISH_DESTINATION_PROFILE_VERSION, DEPRECATE_DESTINATION_PROFILE_VERSION -> {
                requireOnlyKinds(values, ControlTargetKind.PROFILE, ControlTargetKind.SHARD);
                requireCount(values, ControlTargetKind.PROFILE, 1, 1);
                requireCount(values, ControlTargetKind.SHARD, 1, Integer.MAX_VALUE);
                requireMutationPresence(values, ControlTargetKind.PROFILE, false);
                requireMutationPresence(values, ControlTargetKind.SHARD, true);
                validateProfileTarget(
                        kind,
                        request,
                        findSingle(values, ControlTargetKind.PROFILE).profile());
            }
            case PUBLISH_QUOTA_GRANT -> {
                requireOnlyKinds(values, ControlTargetKind.QUOTA_GRANT, ControlTargetKind.SHARD);
                requireCount(values, ControlTargetKind.QUOTA_GRANT, 1, 1);
                requireCount(values, ControlTargetKind.SHARD, 1, Integer.MAX_VALUE);
                requireMutationPresence(values, ControlTargetKind.QUOTA_GRANT, false);
                requireMutationPresence(values, ControlTargetKind.SHARD, true);
                final PublishQuotaGrantRequest branch = branch(request, PublishQuotaGrantRequest.class);
                if (!branch.quotaGrant()
                        .equals(findSingle(values, ControlTargetKind.QUOTA_GRANT)
                                .quotaGrant())) {
                    throw new IllegalArgumentException("quota target does not match request grant");
                }
            }
            case ROTATE_EQUIVALENT_SECRET_REFERENCE -> {
                requireOnlyKinds(values, ControlTargetKind.PROFILE);
                requireCount(values, ControlTargetKind.PROFILE, 1, 1);
                final ControlTargetRef target = findSingle(values, ControlTargetKind.PROFILE);
                requireMutationPresence(values, ControlTargetKind.PROFILE, false);
                validateProfileTarget(kind, request, target.profile());
                if (target.profile().expectedSecretGeneration() == null) {
                    throw new IllegalArgumentException("secret rotation requires Profile generation preconditions");
                }
            }
        }
    }

    private void writeFieldsOneThroughTen(final ByteArrayOutputStream output) {
        CanonicalProtobuf.uint32(output, 1, VERSION);
        CanonicalProtobuf.bytes(output, 2, operationId);
        CanonicalProtobuf.uint32(output, 3, kind.wireValue());
        CanonicalProtobuf.bytes(output, 4, author.canonicalBytes());
        CanonicalProtobuf.bytes(output, 5, request.canonicalBytes());
        CanonicalProtobuf.bytes(output, 6, requestHash);
        for (ControlTargetRef target : targets) {
            CanonicalProtobuf.bytes(output, 7, target.canonicalBytes());
        }
        CanonicalProtobuf.bytes(output, 8, targetSnapshotHash);
        CanonicalProtobuf.uint64Bits(output, 9, controlQueryPolicyVersion);
        CanonicalProtobuf.int64(output, 10, registrationRetryUntil);
    }

    private byte[] calculatePreparedDigest() {
        return Bytes.sha256(PREPARED_DIGEST_DOMAIN, canonicalFieldsOneThroughTen());
    }

    private byte[] canonicalFieldsOneThroughTen() {
        return CanonicalProtobuf.message(this::writeFieldsOneThroughTen);
    }

    private static byte[] preparedDigest(
            final byte[] operationId,
            final ControlOperationKind kind,
            final ControlAuthor author,
            final ControlOperationRequest request,
            final byte[] requestHash,
            final List<ControlTargetRef> targets,
            final byte[] targetSnapshotHash,
            final long controlQueryPolicyVersion,
            final long registrationRetryUntil) {
        return Bytes.sha256(PREPARED_DIGEST_DOMAIN, CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, operationId);
            CanonicalProtobuf.uint32(output, 3, kind.wireValue());
            CanonicalProtobuf.bytes(output, 4, author.canonicalBytes());
            CanonicalProtobuf.bytes(output, 5, request.canonicalBytes());
            CanonicalProtobuf.bytes(output, 6, requestHash);
            for (ControlTargetRef target : targets) {
                CanonicalProtobuf.bytes(output, 7, target.canonicalBytes());
            }
            CanonicalProtobuf.bytes(output, 8, targetSnapshotHash);
            CanonicalProtobuf.uint64Bits(output, 9, controlQueryPolicyVersion);
            CanonicalProtobuf.int64(output, 10, registrationRetryUntil);
        }));
    }

    /** Computes the canonical request hash before any Control registration I/O. */
    public static byte[] requestHash(final ControlOperationKind kind, final ControlOperationRequest request) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(request, "request");
        if (request.kind() != kind) {
            throw new IllegalArgumentException("request kind does not match Control Operation kind");
        }
        return Bytes.sha256(REQUEST_HASH_DOMAIN, Bytes.u16be(kind.wireValue()), Bytes.lp32(request.canonicalBytes()));
    }

    private static byte[] targetSnapshotHash(final List<ControlTargetRef> targets) {
        return Bytes.sha256(TARGET_SNAPSHOT_DOMAIN, Bytes.lp32(canonicalTargets(targets)));
    }

    private static byte[] canonicalTargets(final List<ControlTargetRef> targets) {
        return CanonicalProtobuf.message(output -> {
            for (ControlTargetRef target : targets) {
                CanonicalProtobuf.bytes(output, 1, target.canonicalBytes());
            }
        });
    }

    private static List<ControlTargetRef> sortedTargets(final List<ControlTargetRef> values) {
        Objects.requireNonNull(values, "targets");
        if (values.isEmpty()) {
            throw new IllegalArgumentException("prepared Control Operation requires at least one target");
        }
        final List<ControlTargetRef> copy = new ArrayList<>(values);
        long previous = -1;
        for (ControlTargetRef value : copy) {
            Objects.requireNonNull(value, "target");
            if (value.targetIndex() <= previous) {
                throw new IllegalArgumentException("Control targets must be strictly sorted and unique");
            }
            previous = value.targetIndex();
        }
        return List.copyOf(copy);
    }

    private static void requireOnlyKinds(final List<ControlTargetRef> values, final ControlTargetKind... allowed) {
        for (ControlTargetRef value : values) {
            boolean found = false;
            for (ControlTargetKind candidate : allowed) {
                found |= value.targetKind() == candidate;
            }
            if (!found) {
                throw new IllegalArgumentException("Control Operation contains an unexpected target kind");
            }
        }
    }

    private static void requireCount(
            final List<ControlTargetRef> values, final ControlTargetKind kind, final int minimum, final int maximum) {
        int count = 0;
        for (ControlTargetRef value : values) {
            if (value.targetKind() == kind) {
                count++;
            }
        }
        if (count < minimum || count > maximum) {
            throw new IllegalArgumentException("Control Operation has an invalid " + kind + " target count");
        }
    }

    private static void requireMutationPresence(
            final List<ControlTargetRef> values, final ControlTargetKind kind, final boolean required) {
        for (ControlTargetRef value : values) {
            if (value.targetKind() != kind) {
                continue;
            }
            final boolean present = value.expectedMutationId() != null && value.expectedMutationHash() != null;
            if (present != required) {
                throw new IllegalArgumentException("target mutation identity presence does not match operation kind");
            }
        }
    }

    private static ControlTargetRef findSingle(final List<ControlTargetRef> values, final ControlTargetKind kind) {
        for (ControlTargetRef value : values) {
            if (value.targetKind() == kind) {
                return value;
            }
        }
        throw new IllegalArgumentException("missing target kind " + kind);
    }

    private static void validateMessageTarget(
            final ControlOperationKind kind, final ControlOperationRequest request, final ControlMessageTarget target) {
        if (target == null) {
            throw new IllegalArgumentException("message target branch is missing");
        }
        if (kind == ControlOperationKind.REPLAY_DEAD_LETTER && target.publishAttemptId() != null) {
            throw new IllegalArgumentException("Replay target must not carry a publish attempt ID");
        }
        if (kind == ControlOperationKind.RESOLVE_UNCERTAIN) {
            final ResolveUncertainRequest branch = branch(request, ResolveUncertainRequest.class);
            if (target.publishAttemptId() == null) {
                throw new IllegalArgumentException("Resolve target requires a publish attempt ID");
            }
            if (branch.resolutionKind() == UncertainResolutionKind.RETRY_ALLOW_POSSIBLE_DUPLICATE
                    || branch.resolutionKind() == UncertainResolutionKind.TERMINALIZE_POSSIBLE_DELIVERY) {
                if (target.expectedGeneration() < 0) {
                    throw new IllegalArgumentException("Resolve target generation is invalid");
                }
            }
        }
    }

    private static void validateProfileTarget(
            final ControlOperationKind kind, final ControlOperationRequest request, final ProfileControlTarget target) {
        if (target == null) {
            throw new IllegalArgumentException("Profile target branch is missing");
        }
        final ProfileRef expected;
        switch (kind) {
            case PUBLISH_DESTINATION_PROFILE_VERSION -> {
                final PublishDestinationProfileRequest branch = branch(request, PublishDestinationProfileRequest.class);
                expected = branch.profile().ref();
                if (target.expectedSecretGeneration() != null) {
                    throw new IllegalArgumentException("Profile publication target cannot carry rotation fields");
                }
            }
            case DEPRECATE_DESTINATION_PROFILE_VERSION -> {
                expected = branch(request, DeprecateDestinationProfileRequest.class)
                        .profile();
                if (target.expectedSecretGeneration() != null) {
                    throw new IllegalArgumentException("Profile deprecation target cannot carry rotation fields");
                }
            }
            case ROTATE_EQUIVALENT_SECRET_REFERENCE -> {
                final RotateEquivalentSecretRequest branch = branch(request, RotateEquivalentSecretRequest.class);
                expected = branch.profile();
                if (!Objects.equals(target.expectedSecretGeneration(), branch.expectedSecretGeneration())
                        || !Arrays.equals(target.expectedBindingDigest(), branch.expectedBindingDigest())
                        || !Objects.equals(
                                target.expectedBindingHeadRevision(), branch.expectedBindingHeadRevision())) {
                    throw new IllegalArgumentException("secret rotation target preconditions do not match request");
                }
            }
            default -> throw new IllegalArgumentException("operation does not use a Profile target");
        }
        if (!expected.equals(target.profile())) {
            throw new IllegalArgumentException("Profile target does not match request Profile");
        }
    }

    private static <T> T branch(final ControlOperationRequest request, final Class<T> type) {
        if (!type.isInstance(request.branch())) {
            throw new IllegalArgumentException("request branch does not match operation kind");
        }
        return type.cast(request.branch());
    }

    private static List<CanonicalProtobuf.Reader.Field> readRepeated(final byte[] encoded) {
        Objects.requireNonNull(encoded, "PreparedControlOperation");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("PreparedControlOperation is empty");
        }
        return fields;
    }

    private static byte[] sign(final byte[] digest, final PrivateKey key) {
        try {
            final Signature signer = Signature.getInstance("Ed25519");
            signer.initSign(key);
            signer.update(digest);
            return signer.sign();
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("Ed25519 signing is unavailable", exception);
        }
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static byte[] nonZero(final byte[] value, final String name) {
        final byte[] result = fixed(value, name);
        for (byte current : result) {
            if (current != 0) {
                return result;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PreparedControlOperation that
                && kind == that.kind
                && controlQueryPolicyVersion == that.controlQueryPolicyVersion
                && registrationRetryUntil == that.registrationRetryUntil
                && signingKeyVersion == that.signingKeyVersion
                && author.equals(that.author)
                && request.equals(that.request)
                && targets.equals(that.targets)
                && Arrays.equals(operationId, that.operationId)
                && Arrays.equals(requestHash, that.requestHash)
                && Arrays.equals(targetSnapshotHash, that.targetSnapshotHash)
                && Arrays.equals(preparedDigest, that.preparedDigest)
                && Arrays.equals(signature, that.signature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                kind,
                author,
                request,
                targets,
                controlQueryPolicyVersion,
                registrationRetryUntil,
                signingKeyVersion,
                Arrays.hashCode(operationId),
                Arrays.hashCode(requestHash),
                Arrays.hashCode(targetSnapshotHash),
                Arrays.hashCode(preparedDigest),
                Arrays.hashCode(signature));
    }
}
