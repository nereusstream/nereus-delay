package com.nereusstream.delay.assessment;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Closed lifecycle gate separating implementation authority from deployment authority. */
public final class DeploymentSafetyGate {
    private DeploymentSafetyGate() {}

    public static Decision implementation(
            final GateBStatus gateBStatus,
            final ImplementationSlice requestedSlice,
            final Set<ImplementationSlice> completedSlices) {
        Objects.requireNonNull(gateBStatus, "gateBStatus");
        Objects.requireNonNull(requestedSlice, "requestedSlice");
        Objects.requireNonNull(completedSlices, "completedSlices");
        final EnumSet<ImplementationSlice> completed = EnumSet.noneOf(ImplementationSlice.class);
        completed.addAll(completedSlices);
        if (gateBStatus != GateBStatus.PASS) {
            return Decision.denied(DecisionCode.GATE_B_REQUIRED);
        }
        if (!isOrderedPrefix(completed)) {
            return Decision.denied(DecisionCode.INVALID_IMPLEMENTATION_PROGRESS);
        }
        if (completed.contains(requestedSlice)) {
            return Decision.denied(DecisionCode.SLICE_ALREADY_COMPLETE);
        }
        for (ImplementationSlice slice : ImplementationSlice.values()) {
            if (slice == requestedSlice) {
                break;
            }
            if (!completed.contains(slice)) {
                return Decision.denied(DecisionCode.PREDECESSOR_REQUIRED);
            }
        }
        return Decision.allow();
    }

    public static Decision localDisposable(
            final GateBStatus gateBStatus,
            final String environmentId,
            final EnvironmentClassification environmentClassification,
            final Optional<DisposableEnvironmentAttestation> attestation,
            final LocalOperation operation) {
        Objects.requireNonNull(gateBStatus, "gateBStatus");
        final String canonicalEnvironmentId = AssessmentCanonical.text(environmentId, "environmentId");
        Objects.requireNonNull(environmentClassification, "environmentClassification");
        Objects.requireNonNull(attestation, "attestation");
        Objects.requireNonNull(operation, "operation");
        if (gateBStatus != GateBStatus.PASS) {
            return Decision.denied(DecisionCode.GATE_B_REQUIRED);
        }
        if (environmentClassification == EnvironmentClassification.UNKNOWN) {
            return Decision.denied(DecisionCode.UNKNOWN_ENVIRONMENT);
        }
        if (environmentClassification != EnvironmentClassification.DISPOSABLE_LOCAL) {
            return Decision.denied(DecisionCode.ENVIRONMENT_NOT_DISPOSABLE);
        }
        if (attestation.isEmpty()) {
            return Decision.denied(DecisionCode.DISPOSABLE_ATTESTATION_REQUIRED);
        }
        final DisposableEnvironmentAttestation evidence = attestation.orElseThrow();
        if (!canonicalEnvironmentId.equals(evidence.environmentId())) {
            return Decision.denied(DecisionCode.ATTESTATION_ENVIRONMENT_MISMATCH);
        }
        if (!evidence.complete()) {
            return Decision.denied(DecisionCode.DISPOSABLE_ATTESTATION_INCOMPLETE);
        }
        return Decision.allow();
    }

    public static Decision deployment(
            final GateBStatus gateBStatus,
            final String environmentId,
            final EnvironmentClassification environmentClassification,
            final Optional<GateCAuthorization> gateCAuthorization,
            final DeploymentOperation operation,
            final ShadowReadiness shadowReadiness) {
        Objects.requireNonNull(gateBStatus, "gateBStatus");
        final String canonicalEnvironmentId = AssessmentCanonical.text(environmentId, "environmentId");
        Objects.requireNonNull(environmentClassification, "environmentClassification");
        Objects.requireNonNull(gateCAuthorization, "gateCAuthorization");
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(shadowReadiness, "shadowReadiness");
        if (gateBStatus != GateBStatus.PASS) {
            return Decision.denied(DecisionCode.GATE_B_REQUIRED);
        }
        if (environmentClassification == EnvironmentClassification.UNKNOWN) {
            return Decision.denied(DecisionCode.UNKNOWN_ENVIRONMENT);
        }
        if (!environmentClassification.requiresDeploymentSafetyAssessment()) {
            return Decision.denied(DecisionCode.PERSISTENT_ENVIRONMENT_REQUIRED);
        }
        if (gateCAuthorization.isEmpty()) {
            return Decision.denied(DecisionCode.GATE_C_REQUIRED);
        }
        final GateCAuthorization authority = gateCAuthorization.orElseThrow();
        if (!canonicalEnvironmentId.equals(authority.environmentId())
                || environmentClassification != authority.environmentClassification()) {
            return Decision.denied(DecisionCode.GATE_C_SCOPE_MISMATCH);
        }
        if (operation == DeploymentOperation.ENTER_ENABLED && shadowReadiness != ShadowReadiness.REQUIREMENTS_PASS) {
            return Decision.denied(DecisionCode.SHADOW_REQUIREMENTS_REQUIRED);
        }
        return Decision.allow();
    }

    private static boolean isOrderedPrefix(final Set<ImplementationSlice> completed) {
        boolean missingSeen = false;
        for (ImplementationSlice slice : ImplementationSlice.values()) {
            if (!completed.contains(slice)) {
                missingSeen = true;
            } else if (missingSeen) {
                return false;
            }
        }
        return true;
    }

    public enum GateBStatus {
        PENDING,
        PASS
    }

    public enum ImplementationSlice {
        H1,
        H2,
        H3,
        H4,
        H5,
        H6
    }

    public enum LocalOperation {
        CREATE,
        RESET,
        DESTROY,
        REBUILD,
        INTEGRATION_TEST
    }

    public enum DeploymentOperation {
        ENTER_SHADOW,
        ENTER_ENABLED
    }

    public enum ShadowReadiness {
        NOT_STARTED,
        REQUIREMENTS_PASS
    }

    public enum DecisionCode {
        AUTHORIZED,
        GATE_B_REQUIRED,
        INVALID_IMPLEMENTATION_PROGRESS,
        SLICE_ALREADY_COMPLETE,
        PREDECESSOR_REQUIRED,
        UNKNOWN_ENVIRONMENT,
        ENVIRONMENT_NOT_DISPOSABLE,
        PERSISTENT_ENVIRONMENT_REQUIRED,
        DISPOSABLE_ATTESTATION_REQUIRED,
        DISPOSABLE_ATTESTATION_INCOMPLETE,
        ATTESTATION_ENVIRONMENT_MISMATCH,
        GATE_C_REQUIRED,
        GATE_C_SCOPE_MISMATCH,
        SHADOW_REQUIREMENTS_REQUIRED
    }

    public record Decision(boolean authorized, DecisionCode code) {
        public Decision {
            Objects.requireNonNull(code, "code");
            if (authorized != (code == DecisionCode.AUTHORIZED)) {
                throw new IllegalArgumentException("authorized flag and decision code disagree");
            }
        }

        private static Decision allow() {
            return new Decision(true, DecisionCode.AUTHORIZED);
        }

        private static Decision denied(final DecisionCode code) {
            return new Decision(false, code);
        }
    }
}
