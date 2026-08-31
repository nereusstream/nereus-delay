package com.nereusstream.delay.assessment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class PersistentStagingActivationTest {
    @Test
    void assessmentSidecarMustExactlyMatchTheSignedPayloadIncludingTerminalLf() throws Exception {
        final byte[] persistedSidecar = "{\"outcome\":\"PASS_RETAIN\"}\n".getBytes(StandardCharsets.UTF_8);

        PersistentStagingActivation.requireExactAssessmentBinding(persistedSidecar, persistedSidecar.clone());

        final byte[] legacyPayload = "{\"outcome\":\"PASS_RETAIN\"}".getBytes(StandardCharsets.UTF_8);
        assertThrows(
                IOException.class,
                () -> PersistentStagingActivation.requireExactAssessmentBinding(persistedSidecar, legacyPayload));
    }

    @Test
    void gateResolutionMustMatchTheSignedAssessmentOutcome() throws Exception {
        PersistentStagingActivation.requireResolutionBinding(
                GateCAuthorization.Resolution.RESET, "PASS_DIRECT_REPLACE");
        PersistentStagingActivation.requireResolutionBinding(GateCAuthorization.Resolution.RETAIN, "PASS_RETAIN");

        assertThrows(
                IOException.class,
                () -> PersistentStagingActivation.requireResolutionBinding(
                        GateCAuthorization.Resolution.RETAIN, "PASS_DIRECT_REPLACE"));
        assertThrows(
                IOException.class,
                () -> PersistentStagingActivation.requireResolutionBinding(
                        GateCAuthorization.Resolution.MIGRATED, "PASS_RETAIN"));
    }

    @Test
    void resetDispositionMustRemainInternalNonDestructiveAndReincarnated() throws Exception {
        final JsonObject disposition = new JsonObject();
        disposition.addProperty("decision", "RESET_INTERNAL_ONLY");
        disposition.addProperty("externalUserDataPresent", false);
        disposition.addProperty("existingResourcesAreInternalStagingOnly", true);
        disposition.addProperty("replacementDisposition", "REINCARNATE");
        disposition.addProperty("destructiveOperationsAuthorized", false);

        PersistentStagingActivation.requireDispositionBinding(GateCAuthorization.Resolution.RESET, disposition);

        disposition.addProperty("externalUserDataPresent", true);
        assertThrows(
                IOException.class,
                () -> PersistentStagingActivation.requireDispositionBinding(
                        GateCAuthorization.Resolution.RESET, disposition));
    }

    @Test
    void stagingScopeAndSkipAuditAreClosedExactSets() throws Exception {
        final JsonObject scope = new JsonObject();
        scope.addProperty("environmentId", "staging-local");
        scope.addProperty("environmentClassification", "STAGING");
        scope.addProperty("deploymentId", "deployment-1");
        scope.add("tenantIds", strings("tenant"));
        scope.add("routeIds", strings("route"));
        scope.add("shardIds", strings("route/0"));
        scope.add("eligibleWorkerIds", strings("worker-a"));
        final JsonArray resources = new JsonArray();
        for (DataResetAssessmentScope.ResourceKind kind : DataResetAssessmentScope.ResourceKind.values()) {
            final JsonObject resource = new JsonObject();
            resource.addProperty("kind", kind.name());
            resource.addProperty("identity", "resource/" + kind.name());
            resources.add(resource);
        }
        scope.add("resources", resources);

        assertEquals(
                13, PersistentStagingActivation.decodeScope(scope).resources().size());

        final JsonObject audit = new JsonObject();
        audit.addProperty("schema", "nereus-delay.ndip1-staging-skip-audit");
        audit.addProperty("schemaGeneration", 1);
        audit.addProperty("expectedConditionalSkips", 41);
        final JsonObject counts = new JsonObject();
        counts.addProperty("pass", 41);
        counts.addProperty("failed", 0);
        counts.addProperty("skipped", 0);
        counts.addProperty("notExecuted", 0);
        audit.add("counts", counts);
        final JsonArray rows = new JsonArray();
        for (int index = 0; index < 41; index++) {
            final JsonObject row = new JsonObject();
            row.addProperty("baselineStatus", "CONDITIONAL_SKIP");
            row.addProperty("applicability", "REQUIRED_STAGING");
            row.addProperty("status", "PASS");
            row.addProperty("effectiveRuns", 1);
            rows.add(row);
        }
        audit.add("rows", rows);

        PersistentStagingActivation.requireSkipAudit(audit);
        rows.get(0).getAsJsonObject().addProperty("effectiveRuns", 0);
        assertThrows(IOException.class, () -> PersistentStagingActivation.requireSkipAudit(audit));
    }

    private static JsonArray strings(final String value) {
        final JsonArray result = new JsonArray();
        result.add(value);
        return result;
    }
}
