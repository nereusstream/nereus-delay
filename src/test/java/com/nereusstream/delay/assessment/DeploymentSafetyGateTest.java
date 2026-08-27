package com.nereusstream.delay.assessment;

import static com.nereusstream.delay.assessment.DeploymentSafetyGate.DecisionCode.ATTESTATION_ENVIRONMENT_MISMATCH;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.DecisionCode.AUTHORIZED;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.DecisionCode.DISPOSABLE_ATTESTATION_REQUIRED;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.DecisionCode.GATE_B_REQUIRED;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.DecisionCode.GATE_C_REQUIRED;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.DecisionCode.GATE_C_SCOPE_MISMATCH;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.DecisionCode.PREDECESSOR_REQUIRED;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.DecisionCode.SHADOW_REQUIREMENTS_REQUIRED;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.DecisionCode.UNKNOWN_ENVIRONMENT;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.DeploymentOperation.ENTER_ENABLED;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.DeploymentOperation.ENTER_SHADOW;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.GateBStatus.PASS;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.GateBStatus.PENDING;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.ImplementationSlice.H1;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.ImplementationSlice.H2;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.ImplementationSlice.H3;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.ImplementationSlice.H4;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.ImplementationSlice.H5;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.ImplementationSlice.H6;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.LocalOperation.INTEGRATION_TEST;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.LocalOperation.RESET;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.ShadowReadiness.NOT_STARTED;
import static com.nereusstream.delay.assessment.DeploymentSafetyGate.ShadowReadiness.REQUIREMENTS_PASS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.assessment.DeploymentSafetyGate.Decision;
import com.nereusstream.delay.assessment.GateCAuthorization.Resolution;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DeploymentSafetyGateTest {
    @Test
    void gateBPassMakesH1ReadyWithoutGateCAndPreservesSliceOrder() {
        assertAuthorized(DeploymentSafetyGate.implementation(PASS, H1, Set.of()));
        assertDenied(DeploymentSafetyGate.implementation(PASS, H2, Set.of()), PREDECESSOR_REQUIRED);
        assertDenied(DeploymentSafetyGate.implementation(PASS, H3, Set.of()), PREDECESSOR_REQUIRED);
        assertDenied(DeploymentSafetyGate.implementation(PASS, H4, Set.of()), PREDECESSOR_REQUIRED);
        assertDenied(DeploymentSafetyGate.implementation(PASS, H5, Set.of()), PREDECESSOR_REQUIRED);
        assertDenied(DeploymentSafetyGate.implementation(PASS, H6, Set.of()), PREDECESSOR_REQUIRED);
        assertDenied(DeploymentSafetyGate.implementation(PENDING, H1, Set.of()), GATE_B_REQUIRED);

        assertAuthorized(DeploymentSafetyGate.implementation(PASS, H2, Set.of(H1)));
    }

    @Test
    void completeAttestationAllowsDisposableTestingWithoutAssessmentOrGateC() {
        final DisposableEnvironmentAttestation attestation = attestation("local-run");

        assertAuthorized(DeploymentSafetyGate.localDisposable(
                PASS, "local-run", EnvironmentClassification.DISPOSABLE_LOCAL, Optional.of(attestation), RESET));
        assertAuthorized(DeploymentSafetyGate.localDisposable(
                PASS,
                "local-run",
                EnvironmentClassification.DISPOSABLE_LOCAL,
                Optional.of(attestation),
                INTEGRATION_TEST));

        assertDenied(
                DeploymentSafetyGate.localDisposable(
                        PASS, "local-run", EnvironmentClassification.DISPOSABLE_LOCAL, Optional.empty(), RESET),
                DISPOSABLE_ATTESTATION_REQUIRED);
        assertDenied(
                DeploymentSafetyGate.localDisposable(
                        PASS, "other-run", EnvironmentClassification.DISPOSABLE_LOCAL, Optional.of(attestation), RESET),
                ATTESTATION_ENVIRONMENT_MISMATCH);
    }

    @Test
    void existingAndUnknownEnvironmentsRemainFailClosedWithoutExactGateC() {
        assertDenied(
                DeploymentSafetyGate.deployment(
                        PASS,
                        "staging-a",
                        EnvironmentClassification.STAGING,
                        Optional.empty(),
                        ENTER_SHADOW,
                        NOT_STARTED),
                GATE_C_REQUIRED);
        assertDenied(
                DeploymentSafetyGate.deployment(
                        PASS,
                        "unknown-a",
                        EnvironmentClassification.UNKNOWN,
                        Optional.of(gateC("staging-a", EnvironmentClassification.STAGING)),
                        ENTER_SHADOW,
                        NOT_STARTED),
                UNKNOWN_ENVIRONMENT);
        assertDenied(
                DeploymentSafetyGate.deployment(
                        PASS,
                        "production-a",
                        EnvironmentClassification.PRODUCTION,
                        Optional.of(gateC("staging-a", EnvironmentClassification.STAGING)),
                        ENTER_SHADOW,
                        NOT_STARTED),
                GATE_C_SCOPE_MISMATCH);
    }

    @Test
    void gateCAuthorizesShadowButEnabledStillRequiresShadowEvidence() {
        final GateCAuthorization gateC = gateC("staging-a", EnvironmentClassification.STAGING);

        assertAuthorized(DeploymentSafetyGate.deployment(
                PASS, "staging-a", EnvironmentClassification.STAGING, Optional.of(gateC), ENTER_SHADOW, NOT_STARTED));
        assertDenied(
                DeploymentSafetyGate.deployment(
                        PASS,
                        "staging-a",
                        EnvironmentClassification.STAGING,
                        Optional.of(gateC),
                        ENTER_ENABLED,
                        NOT_STARTED),
                SHADOW_REQUIREMENTS_REQUIRED);
        assertAuthorized(DeploymentSafetyGate.deployment(
                PASS,
                "staging-a",
                EnvironmentClassification.STAGING,
                Optional.of(gateC),
                ENTER_ENABLED,
                REQUIREMENTS_PASS));
    }

    @Test
    void positiveGateCAuthorityCannotBeMintedForDisposableOrUnknownEnvironments() {
        assertThrows(
                IllegalArgumentException.class, () -> gateC("local-run", EnvironmentClassification.DISPOSABLE_LOCAL));
        assertThrows(IllegalArgumentException.class, () -> gateC("unknown-run", EnvironmentClassification.UNKNOWN));
    }

    private static DisposableEnvironmentAttestation attestation(final String environmentId) {
        return new DisposableEnvironmentAttestation(
                environmentId, "test-execution-1", true, true, true, true, digest(1));
    }

    private static GateCAuthorization gateC(
            final String environmentId, final EnvironmentClassification environmentClassification) {
        return new GateCAuthorization(
                environmentId, environmentClassification, Resolution.RESET, digest(2), digest(3), digest(4));
    }

    private static void assertAuthorized(final Decision decision) {
        assertTrue(decision.authorized());
        assertEquals(AUTHORIZED, decision.code());
    }

    private static void assertDenied(final Decision decision, final DeploymentSafetyGate.DecisionCode expectedCode) {
        assertFalse(decision.authorized());
        assertEquals(expectedCode, decision.code());
    }

    private static byte[] digest(final int value) {
        final byte[] digest = new byte[32];
        Arrays.fill(digest, (byte) value);
        return digest;
    }
}
