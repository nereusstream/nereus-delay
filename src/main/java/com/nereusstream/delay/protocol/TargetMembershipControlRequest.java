package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Closed Control request branches 16 (grant) and 17 (close first binding). */
public final class TargetMembershipControlRequest implements ControlOperationRequestBranch {
    public static final int MAX_CANONICAL_BYTES =
            4 + TargetMembershipPolicy.MAX_CANONICAL_BYTES + 5 + TargetMembershipGrant.MAX_REGISTRATION_BYTES + 72;
    private final TargetMembershipPolicy policy;
    private final byte[] registrationOrGrantRef;
    private final ControlReason reason;

    private TargetMembershipControlRequest(
            final TargetMembershipPolicy policy, final byte[] value, final ControlReason reason) {
        this.policy = Objects.requireNonNull(policy, "policy");
        if (reason == null) {
            policy.requireRegistration(value);
            this.registrationOrGrantRef = Bytes.copy(value);
        } else {
            this.registrationOrGrantRef = TargetCompatibilityCodec.assigned(value, 32, "membershipGrantRef");
        }
        this.reason = reason;
    }

    public static TargetMembershipControlRequest issue(final TargetMembershipPolicy policy, final byte[] registration) {
        return new TargetMembershipControlRequest(policy, registration, null);
    }

    public static TargetMembershipControlRequest close(
            final TargetMembershipPolicy policy, final byte[] grantRef, final ControlReason reason) {
        return new TargetMembershipControlRequest(policy, grantRef, Objects.requireNonNull(reason, "reason"));
    }

    public TargetMembershipPolicy policy() {
        return policy;
    }

    public boolean isIssue() {
        return reason == null;
    }

    public byte[] value() {
        return Bytes.copy(registrationOrGrantRef);
    }

    public ControlReason reason() {
        return reason;
    }

    public ControlOperationKind operationKind() {
        return isIssue() ? ControlOperationKind.GRANT_TARGET_MEMBERSHIP : ControlOperationKind.CLOSE_TARGET_MEMBERSHIP;
    }

    public int controlKind() {
        return isIssue() ? 15 : 16;
    }

    public ControlOperationRequest operationRequest() {
        return new ControlOperationRequest(operationKind(), this);
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, policy.canonicalBytes());
            CanonicalProtobuf.bytes(out, 2, registrationOrGrantRef);
            if (reason != null) {
                CanonicalProtobuf.bytes(out, 3, reason.canonicalBytes());
            }
        });
    }

    public static TargetMembershipControlRequest decode(final ControlOperationKind kind, final byte[] encoded) {
        if (kind != ControlOperationKind.GRANT_TARGET_MEMBERSHIP
                && kind != ControlOperationKind.CLOSE_TARGET_MEMBERSHIP) {
            throw new IllegalArgumentException("not a membership Control Operation");
        }
        final boolean issue = kind == ControlOperationKind.GRANT_TARGET_MEMBERSHIP;
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 3, false, "membership Control request");
        QueryCodecSupport.requireNumbers(
                fields, issue ? new int[] {1, 2} : new int[] {1, 2, 3}, "membership Control request");
        final var policy = TargetMembershipPolicy.decode(QueryCodecSupport.bytes(fields.getFirst(), 1));
        ControlReason reason = null;
        if (!issue) {
            final byte[] bytes = QueryCodecSupport.bytes(fields.get(2), 3);
            TargetCompatibilityCodec.read(bytes, 70, 3, false, "membership Control reason");
            reason = ControlReason.decode(bytes);
        }
        final var result =
                new TargetMembershipControlRequest(policy, QueryCodecSupport.bytes(fields.get(1), 2), reason);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "membership Control request");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetMembershipControlRequest that
                && policy.equals(that.policy)
                && Arrays.equals(registrationOrGrantRef, that.registrationOrGrantRef)
                && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policy, Arrays.hashCode(registrationOrGrantRef), reason);
    }
}
