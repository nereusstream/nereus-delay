package com.nereusstream.delay.assessment;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.assessment.DataResetAssessmentScope.ResourceKind;
import com.nereusstream.delay.assessment.DataResetAssessmentScope.ResourceRef;
import com.nereusstream.delay.assessment.DataResetInventory.AccessStatus;
import com.nereusstream.delay.assessment.DataResetInventory.ExternalRetentionRequirement;
import com.nereusstream.delay.assessment.DataResetInventory.ReplacementDisposition;
import com.nereusstream.delay.assessment.DataResetInventory.ResourceObservation;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.TrustedUtcInterval;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DataResetAssessmentReceiptWriterTest {
    private static final String PACKAGE_DIGEST = "0728ae89515d75858dbe96f8426a18bb5a59fb453d6a930737be37289979532f";
    private static final String SOURCE_BASELINE = "1fcc887f6553f6ed5a5f299109b760402d94573b";

    @Test
    void writesExactlyOneLfAndNeverOverwrites(@TempDir final Path temporary) throws Exception {
        final DataResetAssessmentReceipt receipt = receipt();
        final Path target = temporary.resolve("assessment.json");

        assertEquals(target.toAbsolutePath(), DataResetAssessmentReceiptWriter.writeNew(target, receipt));
        final byte[] expected = Arrays.copyOf(receipt.canonicalJsonBytes(), receipt.canonicalJsonBytes().length + 1);
        expected[expected.length - 1] = '\n';
        assertArrayEquals(expected, Files.readAllBytes(target));
        assertThrows(IOException.class, () -> DataResetAssessmentReceiptWriter.writeNew(target, receipt));
    }

    @Test
    void rejectsSymlinkParent(@TempDir final Path temporary) throws Exception {
        final Path real = Files.createDirectory(temporary.resolve("real"));
        final Path linked = temporary.resolve("linked");
        Files.createSymbolicLink(linked, real);

        assertThrows(
                IOException.class,
                () -> DataResetAssessmentReceiptWriter.writeNew(linked.resolve("assessment.json"), receipt()));
    }

    private static DataResetAssessmentReceipt receipt() {
        final List<ResourceRef> resources = Arrays.stream(ResourceKind.values())
                .map(kind -> new ResourceRef(kind, "resource/" + kind.name()))
                .toList();
        final DataResetAssessmentScope scope = new DataResetAssessmentScope(
                "environment",
                EnvironmentClassification.EXISTING,
                "deployment",
                List.of(),
                List.of(),
                List.of(),
                resources,
                List.of());
        final List<ResourceObservation> observations = scope.resources().stream()
                .map(resource -> new ResourceObservation(
                        resource,
                        AccessStatus.COMPLETE,
                        ExternalRetentionRequirement.NONE,
                        ReplacementDisposition.DISCARDABLE,
                        digest(10 + resource.kind().ordinal())))
                .toList();
        final TrustedUtcIntervalEvidence evidence = new TrustedUtcIntervalEvidence(
                100,
                101,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                new byte[] {1},
                1,
                1,
                1,
                digest(50),
                0,
                null);
        final DataResetInventory inventory = new DataResetInventory(
                scope.scopeDigest(),
                true,
                digest(49),
                new TrustedUtcInterval(100, 101, true, evidence),
                observations,
                true,
                digest(51),
                List.of(),
                true,
                digest(52),
                List.of());
        return DataResetAssessmentEvaluator.evaluate(scope, inventory, PACKAGE_DIGEST, SOURCE_BASELINE);
    }

    private static byte[] digest(final int value) {
        final byte[] result = new byte[32];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
