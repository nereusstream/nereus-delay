package com.nereusstream.delay.protocol;

import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Objects;

/** Source-ordered Control requests for Target Native publisher, activation, and member history. */
public final class TargetNativePolicyControlRequest implements ControlOperationRequestBranch {
    public static final int INSTALL_PUBLISHER_CONTROL_KIND = 19;
    public static final int CLOSE_PUBLISHER_CONTROL_KIND = 20;
    public static final int ACTIVATE_POLICY_CONTROL_KIND = 21;
    public static final int APPROVE_MEMBER_CONTROL_KIND = 22;
    public static final int CLOSE_MEMBER_CONTROL_KIND = 23;
    public static final int ED25519_X509_PUBLIC_KEY_BYTES = 44;
    public static final int MAX_CANONICAL_BYTES = TargetNativePolicyScope.MAX_CANONICAL_BYTES
            + TargetNativePolicySnapshot.MAX_CANONICAL_BYTES
            + ED25519_X509_PUBLIC_KEY_BYTES
            + 192;

    private enum Operation {
        INSTALL_PUBLISHER,
        CLOSE_PUBLISHER,
        ACTIVATE_POLICY,
        APPROVE_MEMBER,
        CLOSE_MEMBER
    }

    private final Operation operation;
    private final TargetNativePolicyScope scope;
    private final int keyGeneration;
    private final byte[] publicKey;
    private final long maximumLeaseMs;
    private final TargetNativePolicySnapshot snapshot;
    private final byte[] grantDigest;
    private final ControlReason reason;

    private TargetNativePolicyControlRequest(
            final Operation operation,
            final TargetNativePolicyScope scope,
            final int keyGeneration,
            final byte[] publicKey,
            final long maximumLeaseMs,
            final TargetNativePolicySnapshot snapshot,
            final byte[] grantDigest,
            final ControlReason reason) {
        this.operation = Objects.requireNonNull(operation, "operation");
        this.scope = Objects.requireNonNull(scope, "scope");
        this.keyGeneration = keyGeneration;
        this.publicKey = publicKey == null ? null : checkedEd25519PublicKey(publicKey);
        this.maximumLeaseMs = maximumLeaseMs;
        this.snapshot = snapshot;
        this.grantDigest = grantDigest == null ? null : assignedDigest(grantDigest, "membershipGrantDigest");
        this.reason = reason;
        switch (operation) {
            case INSTALL_PUBLISHER -> {
                if (keyGeneration == 0
                        || this.publicKey == null
                        || maximumLeaseMs <= 0
                        || snapshot != null
                        || grantDigest != null
                        || reason != null) {
                    throw new IllegalArgumentException("invalid Target Native publisher installation");
                }
            }
            case CLOSE_PUBLISHER -> {
                if (keyGeneration == 0
                        || publicKey != null
                        || maximumLeaseMs != 0
                        || snapshot != null
                        || grantDigest != null
                        || reason == null) {
                    throw new IllegalArgumentException("invalid Target Native publisher closure");
                }
            }
            case ACTIVATE_POLICY -> {
                if (keyGeneration != 0
                        || publicKey != null
                        || maximumLeaseMs != 0
                        || snapshot == null
                        || grantDigest != null
                        || reason != null) {
                    throw new IllegalArgumentException("invalid Target Native policy activation");
                }
                snapshot.requireScope(scope);
            }
            case APPROVE_MEMBER -> {
                if (keyGeneration != 0
                        || publicKey != null
                        || maximumLeaseMs != 0
                        || snapshot != null
                        || this.grantDigest == null
                        || reason != null) {
                    throw new IllegalArgumentException("invalid Target Native member approval");
                }
            }
            case CLOSE_MEMBER -> {
                if (keyGeneration != 0
                        || publicKey != null
                        || maximumLeaseMs != 0
                        || snapshot != null
                        || this.grantDigest == null
                        || reason == null) {
                    throw new IllegalArgumentException("invalid Target Native member closure");
                }
            }
        }
    }

    public static TargetNativePolicyControlRequest installPublisher(
            final TargetNativePolicyScope scope,
            final int keyGeneration,
            final PublicKey publicKey,
            final long maximumLeaseMs) {
        return new TargetNativePolicyControlRequest(
                Operation.INSTALL_PUBLISHER,
                scope,
                keyGeneration,
                Objects.requireNonNull(publicKey, "publicKey").getEncoded(),
                maximumLeaseMs,
                null,
                null,
                null);
    }

    public static TargetNativePolicyControlRequest closePublisher(
            final TargetNativePolicyScope scope, final int keyGeneration, final ControlReason reason) {
        return new TargetNativePolicyControlRequest(
                Operation.CLOSE_PUBLISHER,
                scope,
                keyGeneration,
                null,
                0,
                null,
                null,
                Objects.requireNonNull(reason, "reason"));
    }

    public static TargetNativePolicyControlRequest activate(
            final TargetNativePolicyScope scope, final TargetNativePolicySnapshot snapshot) {
        return new TargetNativePolicyControlRequest(Operation.ACTIVATE_POLICY, scope, 0, null, 0, snapshot, null, null);
    }

    public static TargetNativePolicyControlRequest approveMember(
            final TargetNativePolicyScope scope, final byte[] grantDigest) {
        return new TargetNativePolicyControlRequest(
                Operation.APPROVE_MEMBER, scope, 0, null, 0, null, grantDigest, null);
    }

    public static TargetNativePolicyControlRequest closeMember(
            final TargetNativePolicyScope scope, final byte[] grantDigest, final ControlReason reason) {
        return new TargetNativePolicyControlRequest(
                Operation.CLOSE_MEMBER, scope, 0, null, 0, null, grantDigest, Objects.requireNonNull(reason, "reason"));
    }

    public TargetNativePolicyScope scope() {
        return scope;
    }

    public int keyGeneration() {
        return keyGeneration;
    }

    public PublicKey publicKey() {
        return publicKey == null ? null : decodeEd25519PublicKey(publicKey);
    }

    public long maximumLeaseMs() {
        return maximumLeaseMs;
    }

    public TargetNativePolicySnapshot snapshot() {
        return snapshot;
    }

    public byte[] grantDigest() {
        return grantDigest == null ? null : Bytes.copy(grantDigest);
    }

    public ControlReason reason() {
        return reason;
    }

    public int controlKind() {
        return switch (operation) {
            case INSTALL_PUBLISHER -> INSTALL_PUBLISHER_CONTROL_KIND;
            case CLOSE_PUBLISHER -> CLOSE_PUBLISHER_CONTROL_KIND;
            case ACTIVATE_POLICY -> ACTIVATE_POLICY_CONTROL_KIND;
            case APPROVE_MEMBER -> APPROVE_MEMBER_CONTROL_KIND;
            case CLOSE_MEMBER -> CLOSE_MEMBER_CONTROL_KIND;
        };
    }

    public boolean createsImmutableRecord() {
        return operation == Operation.INSTALL_PUBLISHER
                || operation == Operation.ACTIVATE_POLICY
                || operation == Operation.APPROVE_MEMBER;
    }

    public ControlOperationKind operationKind() {
        return switch (operation) {
            case INSTALL_PUBLISHER -> ControlOperationKind.INSTALL_TARGET_NATIVE_PUBLISHER_PERMISSION;
            case CLOSE_PUBLISHER -> ControlOperationKind.CLOSE_TARGET_NATIVE_PUBLISHER_PERMISSION;
            case ACTIVATE_POLICY -> ControlOperationKind.ACTIVATE_TARGET_NATIVE_POLICY;
            case APPROVE_MEMBER -> ControlOperationKind.APPROVE_TARGET_NATIVE_MEMBER;
            case CLOSE_MEMBER -> ControlOperationKind.CLOSE_TARGET_NATIVE_MEMBER;
        };
    }

    public ControlOperationRequest operationRequest() {
        return new ControlOperationRequest(operationKind(), this);
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, scope.canonicalBytes());
            switch (operation) {
                case INSTALL_PUBLISHER -> {
                    CanonicalProtobuf.uint32(out, 2, keyGeneration);
                    CanonicalProtobuf.bytes(out, 3, publicKey);
                    CanonicalProtobuf.uint64Bits(out, 4, maximumLeaseMs);
                }
                case CLOSE_PUBLISHER -> {
                    CanonicalProtobuf.uint32(out, 2, keyGeneration);
                    CanonicalProtobuf.bytes(out, 3, reason.canonicalBytes());
                }
                case ACTIVATE_POLICY -> CanonicalProtobuf.bytes(out, 2, snapshot.canonicalBytes());
                case APPROVE_MEMBER -> CanonicalProtobuf.bytes(out, 2, grantDigest);
                case CLOSE_MEMBER -> {
                    CanonicalProtobuf.bytes(out, 2, grantDigest);
                    CanonicalProtobuf.bytes(out, 3, reason.canonicalBytes());
                }
            }
        });
    }

    public static TargetNativePolicyControlRequest decode(final ControlOperationKind kind, final byte[] encoded) {
        final var operation = operation(kind);
        final var fields = TargetCompatibilityCodec.read(
                encoded, MAX_CANONICAL_BYTES, 4, false, "Target Native policy Control request");
        if (fields.isEmpty() || fields.getFirst().number() != 1) {
            throw new IllegalArgumentException("Target Native policy Control request must begin with its scope");
        }
        final TargetNativePolicyControlRequest result;
        final var scope = TargetNativePolicyScope.decode(QueryCodecSupport.bytes(fields.getFirst(), 1));
        switch (operation) {
            case INSTALL_PUBLISHER -> {
                QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4}, "Target Native publisher install");
                result = installPublisher(
                        scope,
                        QueryCodecSupport.uint32Bits(fields.get(1), 2),
                        decodeEd25519PublicKey(QueryCodecSupport.bytes(fields.get(2), 3)),
                        QueryCodecSupport.uint(fields.get(3), 4));
            }
            case CLOSE_PUBLISHER -> {
                QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "Target Native publisher close");
                result = closePublisher(
                        scope,
                        QueryCodecSupport.uint32Bits(fields.get(1), 2),
                        decodeReason(QueryCodecSupport.bytes(fields.get(2), 3)));
            }
            case ACTIVATE_POLICY -> {
                QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "Target Native policy activation");
                result = activate(scope, TargetNativePolicySnapshot.decode(QueryCodecSupport.bytes(fields.get(1), 2)));
            }
            case APPROVE_MEMBER -> {
                QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "Target Native member approval");
                result = approveMember(scope, QueryCodecSupport.fixed(fields.get(1), 2, 32));
            }
            case CLOSE_MEMBER -> {
                QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "Target Native member close");
                result = closeMember(
                        scope,
                        QueryCodecSupport.fixed(fields.get(1), 2, 32),
                        decodeReason(QueryCodecSupport.bytes(fields.get(2), 3)));
            }
            default -> throw new IllegalStateException("unknown Target Native policy Control operation");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target Native policy Control request");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetNativePolicyControlRequest that
                && operation == that.operation
                && scope.equals(that.scope)
                && keyGeneration == that.keyGeneration
                && Arrays.equals(publicKey, that.publicKey)
                && maximumLeaseMs == that.maximumLeaseMs
                && Objects.equals(snapshot, that.snapshot)
                && Arrays.equals(grantDigest, that.grantDigest)
                && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                operation,
                scope,
                keyGeneration,
                Arrays.hashCode(publicKey),
                maximumLeaseMs,
                snapshot,
                Arrays.hashCode(grantDigest),
                reason);
    }

    private static Operation operation(final ControlOperationKind kind) {
        return switch (Objects.requireNonNull(kind, "kind")) {
            case INSTALL_TARGET_NATIVE_PUBLISHER_PERMISSION -> Operation.INSTALL_PUBLISHER;
            case CLOSE_TARGET_NATIVE_PUBLISHER_PERMISSION -> Operation.CLOSE_PUBLISHER;
            case ACTIVATE_TARGET_NATIVE_POLICY -> Operation.ACTIVATE_POLICY;
            case APPROVE_TARGET_NATIVE_MEMBER -> Operation.APPROVE_MEMBER;
            case CLOSE_TARGET_NATIVE_MEMBER -> Operation.CLOSE_MEMBER;
            default -> throw new IllegalArgumentException("not a Target Native policy Control operation");
        };
    }

    private static ControlReason decodeReason(final byte[] encoded) {
        TargetCompatibilityCodec.read(encoded, 70, 3, false, "Target Native Control reason");
        return ControlReason.decode(encoded);
    }

    private static byte[] checkedEd25519PublicKey(final byte[] encoded) {
        if (encoded == null || encoded.length != ED25519_X509_PUBLIC_KEY_BYTES) {
            throw new IllegalArgumentException("Target Native publisher key must be canonical Ed25519 X.509 bytes");
        }
        final PublicKey decoded = decodeEd25519PublicKey(encoded);
        if (!Arrays.equals(encoded, decoded.getEncoded())) {
            throw new IllegalArgumentException("Target Native publisher key is not canonical X.509 encoding");
        }
        return Bytes.copy(encoded);
    }

    private static PublicKey decodeEd25519PublicKey(final byte[] encoded) {
        try {
            return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(encoded));
        } catch (GeneralSecurityException invalid) {
            throw new IllegalArgumentException("Target Native publisher key is not Ed25519", invalid);
        }
    }

    private static byte[] assignedDigest(final byte[] digest, final String name) {
        Bytes.requireLength(digest, 32, name);
        if (Arrays.equals(digest, new byte[32])) {
            throw new IllegalArgumentException(name + " is unassigned");
        }
        return Bytes.copy(digest);
    }
}
