package io.nereusstream.delay.protocol;

import java.io.ByteArrayOutputStream;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Canonical prepared Control Operation envelope.  All hashes/signature input
 * and the operation-specific local target matrix are fixed before registration
 * I/O; the class does not perform Oxia or actor/resource authorization.
 */
public final class PreparedControlOperationV1 {
    public static final int VERSION = 1;
    public static final int HASH_LENGTH = 32;
    public static final int SIGNATURE_LENGTH = 64;
    private static final byte[] REQUEST_HASH_DOMAIN = Bytes.utf8("nereus-delay-control-request-v1\0");
    private static final byte[] TARGET_SNAPSHOT_DOMAIN = Bytes.utf8("nereus-delay-control-target-snapshot-v1\0");
    private static final byte[] PREPARED_DIGEST_DOMAIN = Bytes.utf8("nereus-delay-prepared-control-v1\0");

    private final byte[] operationId;
    private final ControlOperationKindV1 kind;
    private final ControlAuthorV1 author;
    private final ControlOperationRequestV1 request;
    private final byte[] requestHash;
    private final List<ControlTargetRefV1> targets;
    private final byte[] targetSnapshotHash;
    private final long controlQueryPolicyVersion;
    private final long registrationRetryUntil;
    private final byte[] preparedDigest;
    private final long signingKeyVersion;
    private final byte[] signature;

    private PreparedControlOperationV1(final byte[] operationId, final ControlOperationKindV1 kind,
                                       final ControlAuthorV1 author, final ControlOperationRequestV1 request,
                                       final byte[] requestHash, final List<ControlTargetRefV1> targets,
                                       final byte[] targetSnapshotHash, final long controlQueryPolicyVersion,
                                       final long registrationRetryUntil, final byte[] preparedDigest,
                                       final long signingKeyVersion, final byte[] signature) {
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
    public static PreparedControlOperationV1 prepare(final byte[] operationId,
                                                     final ControlOperationKindV1 kind,
                                                     final ControlAuthorV1 author,
                                                     final ControlOperationRequestV1 request,
                                                     final List<ControlTargetRefV1> targets,
                                                     final long controlQueryPolicyVersion,
                                                     final long registrationRetryUntil,
                                                     final long signingKeyVersion,
                                                     final PrivateKey signingKey) {
        Objects.requireNonNull(signingKey, "signingKey");
        final byte[] requestHash = requestHash(kind, request);
        final List<ControlTargetRefV1> sorted = sortedTargets(targets);
        validateTargetPresence(kind, request, sorted);
        final byte[] snapshot = targetSnapshotHash(sorted);
        final byte[] digest = preparedDigest(operationId, kind, author, request, requestHash, sorted, snapshot,
                controlQueryPolicyVersion, registrationRetryUntil);
        return new PreparedControlOperationV1(operationId, kind, author, request, requestHash, sorted, snapshot,
                controlQueryPolicyVersion, registrationRetryUntil, digest, signingKeyVersion,
                sign(digest, signingKey));
    }

    public byte[] operationId() {
        return Bytes.copy(operationId);
    }

    public ControlOperationKindV1 kind() {
        return kind;
    }

    public ControlAuthorV1 author() {
        return author;
    }

    public ControlOperationRequestV1 request() {
        return request;
    }

    public byte[] requestHash() {
        return Bytes.copy(requestHash);
    }

    public List<ControlTargetRefV1> targets() {
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
    public CurrentControlOperationV1 initialCurrentOperation() {
        final List<ControlTargetStateViewV1> states = targets.stream()
                .map(target -> new ControlTargetStateViewV1(target.targetIndex(),
                        TargetMarkerStateV1.PENDING,
                        StableCode.OK, 0, null))
                .toList();
        return new CurrentControlOperationV1(operationId, requestHash, author.tenantResourceScopeHash(),
                ControlOperationStateV1.PENDING, 1, states, null);
    }

    /** Validates a completed source mutation against this operation's exact target snapshot. */
    public void validateTargetMutation(final ControlTargetRefV1 target, final SystemMutation mutation) {
        ControlTargetMutationBindingV1.validate(this, target, mutation);
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

    public static PreparedControlOperationV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = readRepeated(encoded);
        if (fields.size() < 12) {
            throw new IllegalArgumentException("PreparedControlOperationV1 has too few fields");
        }
        if (fields.get(0).number() != 1 || fields.get(1).number() != 2 || fields.get(2).number() != 3
                || fields.get(3).number() != 4 || fields.get(4).number() != 5 || fields.get(5).number() != 6) {
            throw new IllegalArgumentException("invalid PreparedControlOperationV1 fixed field order");
        }
        int index = 6;
        final List<ControlTargetRefV1> targets = new ArrayList<>();
        while (index < fields.size() && fields.get(index).number() == 7) {
            targets.add(ControlTargetRefV1.decode(QueryCodecSupport.nested(fields.get(index), 7)));
            index++;
        }
        if (targets.isEmpty() || index + 6 != fields.size()
                || fields.get(index).number() != 8 || fields.get(index + 1).number() != 9
                || fields.get(index + 2).number() != 10 || fields.get(index + 3).number() != 11
                || fields.get(index + 4).number() != 12 || fields.get(index + 5).number() != 13) {
            throw new IllegalArgumentException("invalid PreparedControlOperationV1 target/fixed field order");
        }
        final ControlOperationKindV1 kind = ControlOperationKindV1.fromWire(
                QueryCodecSupport.uint(fields.get(2), 3));
        final PreparedControlOperationV1 result = new PreparedControlOperationV1(
                QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH), kind,
                ControlAuthorV1.decode(QueryCodecSupport.nested(fields.get(3), 4)),
                ControlOperationRequestV1.decode(QueryCodecSupport.nested(fields.get(4), 5)),
                QueryCodecSupport.fixed(fields.get(5), 6, HASH_LENGTH), targets,
                QueryCodecSupport.fixed(fields.get(index), 8, HASH_LENGTH),
                QueryCodecSupport.uint64Bits(fields.get(index + 1), 9), QueryCodecSupport.uint(fields.get(index + 2), 10),
                QueryCodecSupport.fixed(fields.get(index + 3), 11, HASH_LENGTH),
                QueryCodecSupport.uint(fields.get(index + 4), 12),
                QueryCodecSupport.fixed(fields.get(index + 5), 13, SIGNATURE_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PreparedControlOperationV1");
        return result;
    }

    /** Validates the closed operation-kind/target-kind/presence matrix locally. */
    public static void validateTargetPresence(final ControlOperationKindV1 kind,
                                              final ControlOperationRequestV1 request,
                                              final List<ControlTargetRefV1> targets) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(request, "request");
        if (request.kind() != kind) {
            throw new IllegalArgumentException("request kind does not match target operation kind");
        }
        final List<ControlTargetRefV1> values = Objects.requireNonNull(targets, "targets");
        if (values.isEmpty()) {
            throw new IllegalArgumentException("Control Operation requires at least one target");
        }
        switch (kind) {
            case STOP_NEW_SCHEDULES -> {
                requireOnlyKinds(values, ControlTargetKindV1.ROUTE, ControlTargetKindV1.SHARD);
                requireCount(values, ControlTargetKindV1.ROUTE, 1, 1);
                requireCount(values, ControlTargetKindV1.SHARD, 1, Integer.MAX_VALUE);
                requireMutationPresence(values, ControlTargetKindV1.ROUTE, false);
                requireMutationPresence(values, ControlTargetKindV1.SHARD, true);
            }
            case PAUSE_DESTINATION_LANE, RESUME_DESTINATION_LANE, CLOSE_DESTINATION_LANE,
                    BREAK_ORDERING_DOMAIN -> {
                requireOnlyKinds(values, ControlTargetKindV1.LANE);
                requireCount(values, ControlTargetKindV1.LANE, 1, Integer.MAX_VALUE);
                requireMutationPresence(values, ControlTargetKindV1.LANE, true);
            }
            case DRAIN_SHARD, FENCE_SHARD_FOR_MAINTENANCE, FORCE_CHECKPOINT, GET_CHECKPOINT_CATALOG -> {
                requireOnlyKinds(values, ControlTargetKindV1.SHARD);
                requireCount(values, ControlTargetKindV1.SHARD, 1, Integer.MAX_VALUE);
                requireMutationPresence(values, ControlTargetKindV1.SHARD, false);
            }
            case REPLAY_DEAD_LETTER, RESOLVE_UNCERTAIN -> {
                requireOnlyKinds(values, ControlTargetKindV1.MESSAGE);
                requireCount(values, ControlTargetKindV1.MESSAGE, 1, 1);
                requireMutationPresence(values, ControlTargetKindV1.MESSAGE, true);
                validateMessageTarget(kind, request, values.get(0).message());
            }
            case PUBLISH_DESTINATION_PROFILE_VERSION, DEPRECATE_DESTINATION_PROFILE_VERSION -> {
                requireOnlyKinds(values, ControlTargetKindV1.PROFILE, ControlTargetKindV1.SHARD);
                requireCount(values, ControlTargetKindV1.PROFILE, 1, 1);
                requireCount(values, ControlTargetKindV1.SHARD, 1, Integer.MAX_VALUE);
                requireMutationPresence(values, ControlTargetKindV1.PROFILE, false);
                requireMutationPresence(values, ControlTargetKindV1.SHARD, true);
                validateProfileTarget(kind, request, findSingle(values, ControlTargetKindV1.PROFILE).profile());
            }
            case PUBLISH_QUOTA_GRANT -> {
                requireOnlyKinds(values, ControlTargetKindV1.QUOTA_GRANT, ControlTargetKindV1.SHARD);
                requireCount(values, ControlTargetKindV1.QUOTA_GRANT, 1, 1);
                requireCount(values, ControlTargetKindV1.SHARD, 1, Integer.MAX_VALUE);
                requireMutationPresence(values, ControlTargetKindV1.QUOTA_GRANT, false);
                requireMutationPresence(values, ControlTargetKindV1.SHARD, true);
                final PublishQuotaGrantRequestV1 branch = branch(request, PublishQuotaGrantRequestV1.class);
                if (!branch.quotaGrant().equals(findSingle(values, ControlTargetKindV1.QUOTA_GRANT).quotaGrant())) {
                    throw new IllegalArgumentException("quota target does not match request grant");
                }
            }
            case ROTATE_EQUIVALENT_SECRET_REFERENCE -> {
                requireOnlyKinds(values, ControlTargetKindV1.PROFILE);
                requireCount(values, ControlTargetKindV1.PROFILE, 1, 1);
                final ControlTargetRefV1 target = findSingle(values, ControlTargetKindV1.PROFILE);
                requireMutationPresence(values, ControlTargetKindV1.PROFILE, false);
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
        for (ControlTargetRefV1 target : targets) {
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

    private static byte[] preparedDigest(final byte[] operationId, final ControlOperationKindV1 kind,
                                         final ControlAuthorV1 author, final ControlOperationRequestV1 request,
                                         final byte[] requestHash, final List<ControlTargetRefV1> targets,
                                         final byte[] targetSnapshotHash, final long controlQueryPolicyVersion,
                                         final long registrationRetryUntil) {
        return Bytes.sha256(PREPARED_DIGEST_DOMAIN, CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, operationId);
            CanonicalProtobuf.uint32(output, 3, kind.wireValue());
            CanonicalProtobuf.bytes(output, 4, author.canonicalBytes());
            CanonicalProtobuf.bytes(output, 5, request.canonicalBytes());
            CanonicalProtobuf.bytes(output, 6, requestHash);
            for (ControlTargetRefV1 target : targets) {
                CanonicalProtobuf.bytes(output, 7, target.canonicalBytes());
            }
            CanonicalProtobuf.bytes(output, 8, targetSnapshotHash);
            CanonicalProtobuf.uint64Bits(output, 9, controlQueryPolicyVersion);
            CanonicalProtobuf.int64(output, 10, registrationRetryUntil);
        }));
    }

    /** Computes the canonical request hash before any Control registration I/O. */
    public static byte[] requestHash(final ControlOperationKindV1 kind,
                                     final ControlOperationRequestV1 request) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(request, "request");
        if (request.kind() != kind) {
            throw new IllegalArgumentException("request kind does not match Control Operation kind");
        }
        return Bytes.sha256(REQUEST_HASH_DOMAIN, Bytes.u16be(kind.wireValue()), Bytes.lp32(request.canonicalBytes()));
    }

    private static byte[] targetSnapshotHash(final List<ControlTargetRefV1> targets) {
        return Bytes.sha256(TARGET_SNAPSHOT_DOMAIN, Bytes.lp32(canonicalTargets(targets)));
    }

    private static byte[] canonicalTargets(final List<ControlTargetRefV1> targets) {
        return CanonicalProtobuf.message(output -> {
            for (ControlTargetRefV1 target : targets) {
                CanonicalProtobuf.bytes(output, 1, target.canonicalBytes());
            }
        });
    }

    private static List<ControlTargetRefV1> sortedTargets(final List<ControlTargetRefV1> values) {
        Objects.requireNonNull(values, "targets");
        if (values.isEmpty()) {
            throw new IllegalArgumentException("prepared Control Operation requires at least one target");
        }
        final List<ControlTargetRefV1> copy = new ArrayList<>(values);
        long previous = -1;
        for (ControlTargetRefV1 value : copy) {
            Objects.requireNonNull(value, "target");
            if (value.targetIndex() <= previous) {
                throw new IllegalArgumentException("Control targets must be strictly sorted and unique");
            }
            previous = value.targetIndex();
        }
        return List.copyOf(copy);
    }

    private static void requireOnlyKinds(final List<ControlTargetRefV1> values,
                                         final ControlTargetKindV1... allowed) {
        for (ControlTargetRefV1 value : values) {
            boolean found = false;
            for (ControlTargetKindV1 candidate : allowed) {
                found |= value.targetKind() == candidate;
            }
            if (!found) {
                throw new IllegalArgumentException("Control Operation contains an unexpected target kind");
            }
        }
    }

    private static void requireCount(final List<ControlTargetRefV1> values, final ControlTargetKindV1 kind,
                                     final int minimum, final int maximum) {
        int count = 0;
        for (ControlTargetRefV1 value : values) {
            if (value.targetKind() == kind) {
                count++;
            }
        }
        if (count < minimum || count > maximum) {
            throw new IllegalArgumentException("Control Operation has an invalid " + kind + " target count");
        }
    }

    private static void requireMutationPresence(final List<ControlTargetRefV1> values,
                                                final ControlTargetKindV1 kind, final boolean required) {
        for (ControlTargetRefV1 value : values) {
            if (value.targetKind() != kind) {
                continue;
            }
            final boolean present = value.expectedMutationId() != null && value.expectedMutationHash() != null;
            if (present != required) {
                throw new IllegalArgumentException("target mutation identity presence does not match operation kind");
            }
        }
    }

    private static ControlTargetRefV1 findSingle(final List<ControlTargetRefV1> values,
                                                 final ControlTargetKindV1 kind) {
        for (ControlTargetRefV1 value : values) {
            if (value.targetKind() == kind) {
                return value;
            }
        }
        throw new IllegalArgumentException("missing target kind " + kind);
    }

    private static void validateMessageTarget(final ControlOperationKindV1 kind,
                                              final ControlOperationRequestV1 request,
                                              final ControlMessageTargetV1 target) {
        if (target == null) {
            throw new IllegalArgumentException("message target branch is missing");
        }
        if (kind == ControlOperationKindV1.REPLAY_DEAD_LETTER && target.publishAttemptId() != null) {
            throw new IllegalArgumentException("Replay target must not carry a publish attempt ID");
        }
        if (kind == ControlOperationKindV1.RESOLVE_UNCERTAIN) {
            final ResolveUncertainRequestV1 branch = branch(request, ResolveUncertainRequestV1.class);
            if (target.publishAttemptId() == null) {
                throw new IllegalArgumentException("Resolve target requires a publish attempt ID");
            }
            if (branch.resolutionKind() == UncertainResolutionKindV1.RETRY_ALLOW_POSSIBLE_DUPLICATE
                    || branch.resolutionKind() == UncertainResolutionKindV1.TERMINALIZE_POSSIBLE_DELIVERY) {
                if (target.expectedGeneration() < 0) {
                    throw new IllegalArgumentException("Resolve target generation is invalid");
                }
            }
        }
    }

    private static void validateProfileTarget(final ControlOperationKindV1 kind,
                                              final ControlOperationRequestV1 request,
                                              final ProfileControlTargetV1 target) {
        if (target == null) {
            throw new IllegalArgumentException("Profile target branch is missing");
        }
        final ProfileRefV1 expected;
        switch (kind) {
            case PUBLISH_DESTINATION_PROFILE_VERSION -> {
                final PublishDestinationProfileRequestV1 branch = branch(request,
                        PublishDestinationProfileRequestV1.class);
                expected = branch.profile().ref();
                if (target.expectedSecretGeneration() != null) {
                    throw new IllegalArgumentException("Profile publication target cannot carry rotation fields");
                }
            }
            case DEPRECATE_DESTINATION_PROFILE_VERSION -> {
                expected = branch(request, DeprecateDestinationProfileRequestV1.class).profile();
                if (target.expectedSecretGeneration() != null) {
                    throw new IllegalArgumentException("Profile deprecation target cannot carry rotation fields");
                }
            }
            case ROTATE_EQUIVALENT_SECRET_REFERENCE -> {
                final RotateEquivalentSecretRequestV1 branch = branch(request,
                        RotateEquivalentSecretRequestV1.class);
                expected = branch.profile();
                if (!Objects.equals(target.expectedSecretGeneration(), branch.expectedSecretGeneration())
                        || !Arrays.equals(target.expectedBindingDigest(), branch.expectedBindingDigest())
                        || !Objects.equals(target.expectedBindingHeadRevision(), branch.expectedBindingHeadRevision())) {
                    throw new IllegalArgumentException("secret rotation target preconditions do not match request");
                }
            }
            default -> throw new IllegalArgumentException("operation does not use a Profile target");
        }
        if (!expected.equals(target.profile())) {
            throw new IllegalArgumentException("Profile target does not match request Profile");
        }
    }

    private static <T> T branch(final ControlOperationRequestV1 request, final Class<T> type) {
        if (!type.isInstance(request.branch())) {
            throw new IllegalArgumentException("request branch does not match operation kind");
        }
        return type.cast(request.branch());
    }

    private static List<CanonicalProtobuf.Reader.Field> readRepeated(final byte[] encoded) {
        Objects.requireNonNull(encoded, "PreparedControlOperationV1");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("PreparedControlOperationV1 is empty");
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
        return other instanceof PreparedControlOperationV1 that && kind == that.kind
                && controlQueryPolicyVersion == that.controlQueryPolicyVersion
                && registrationRetryUntil == that.registrationRetryUntil && signingKeyVersion == that.signingKeyVersion
                && author.equals(that.author) && request.equals(that.request)
                && targets.equals(that.targets) && Arrays.equals(operationId, that.operationId)
                && Arrays.equals(requestHash, that.requestHash)
                && Arrays.equals(targetSnapshotHash, that.targetSnapshotHash)
                && Arrays.equals(preparedDigest, that.preparedDigest) && Arrays.equals(signature, that.signature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, author, request, targets, controlQueryPolicyVersion, registrationRetryUntil,
                signingKeyVersion, Arrays.hashCode(operationId), Arrays.hashCode(requestHash),
                Arrays.hashCode(targetSnapshotHash), Arrays.hashCode(preparedDigest), Arrays.hashCode(signature));
    }
}
