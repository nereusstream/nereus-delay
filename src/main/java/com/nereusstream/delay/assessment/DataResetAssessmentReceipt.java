package com.nereusstream.delay.assessment;

import com.nereusstream.delay.protocol.Bytes;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/** Closed local G0 receipt. It is assessment evidence, never activation authority. */
public final class DataResetAssessmentReceipt {
    public static final String SCHEMA = "nereus-delay.data-reset-assessment";
    public static final int SCHEMA_GENERATION = 2;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-data-reset-assessment\0");
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");
    private static final Pattern COMMIT = Pattern.compile("[0-9a-f]{40}");

    private final DataResetAssessmentOutcome outcome;
    private final String ndipPackageDigest;
    private final String sourceBaselineCommit;
    private final DataResetAssessmentScope scope;
    private final DataResetInventory inventory;
    private final List<DataResetAssessmentFinding> findings;
    private final byte[] assessmentDigest;

    DataResetAssessmentReceipt(
            final DataResetAssessmentOutcome outcome,
            final String ndipPackageDigest,
            final String sourceBaselineCommit,
            final DataResetAssessmentScope scope,
            final DataResetInventory inventory,
            final List<DataResetAssessmentFinding> findings) {
        this.outcome = Objects.requireNonNull(outcome, "outcome");
        this.ndipPackageDigest = checked(ndipPackageDigest, SHA256, "ndipPackageDigest");
        this.sourceBaselineCommit = checked(sourceBaselineCommit, COMMIT, "sourceBaselineCommit");
        this.scope = Objects.requireNonNull(scope, "scope");
        this.inventory = Objects.requireNonNull(inventory, "inventory");
        Objects.requireNonNull(findings, "findings");
        final List<DataResetAssessmentFinding> sorted = new ArrayList<>(findings);
        sorted.forEach(finding -> Objects.requireNonNull(finding, "finding"));
        sorted.sort(DataResetAssessmentFinding::compareTo);
        for (int index = 1; index < sorted.size(); index++) {
            if (sorted.get(index - 1).equals(sorted.get(index))) {
                throw new IllegalArgumentException("duplicate assessment finding");
            }
        }
        if (outcome.decisionReady() && !sorted.isEmpty()) {
            throw new IllegalArgumentException("a passing assessment cannot carry findings");
        }
        this.findings = List.copyOf(sorted);
        this.assessmentDigest =
                Bytes.sha256(DIGEST_DOMAIN, canonicalPayloadJson().getBytes(StandardCharsets.UTF_8));
    }

    public DataResetAssessmentOutcome outcome() {
        return outcome;
    }

    public String ndipPackageDigest() {
        return ndipPackageDigest;
    }

    public String sourceBaselineCommit() {
        return sourceBaselineCommit;
    }

    public DataResetAssessmentScope scope() {
        return scope;
    }

    public DataResetInventory inventory() {
        return inventory;
    }

    public List<DataResetAssessmentFinding> findings() {
        return findings;
    }

    public byte[] assessmentDigest() {
        return Bytes.copy(assessmentDigest);
    }

    public byte[] canonicalJsonBytes() {
        return canonicalJson().getBytes(StandardCharsets.UTF_8);
    }

    public String canonicalJson() {
        final String payload = canonicalPayloadJson();
        return "{\"assessmentDigest\":" + AssessmentCanonical.quote(AssessmentCanonical.hex(assessmentDigest)) + ','
                + payload.substring(1);
    }

    private String canonicalPayloadJson() {
        final StringBuilder json = new StringBuilder(8192);
        json.append("{\"assessmentSchema\":").append(AssessmentCanonical.quote(SCHEMA));
        json.append(",\"assessmentSchemaGeneration\":").append(SCHEMA_GENERATION);
        json.append(",\"findings\":[");
        for (int index = 0; index < findings.size(); index++) {
            if (index != 0) {
                json.append(',');
            }
            json.append(findings.get(index).canonicalJson());
        }
        json.append(']');
        json.append(",\"inventory\":").append(inventory.canonicalJson());
        json.append(",\"ndipPackageDigest\":").append(AssessmentCanonical.quote(ndipPackageDigest));
        json.append(",\"outcome\":").append(AssessmentCanonical.quote(outcome.name()));
        json.append(",\"scope\":").append(scope.canonicalJson());
        json.append(",\"sourceBaselineCommit\":").append(AssessmentCanonical.quote(sourceBaselineCommit));
        return json.append('}').toString();
    }

    private static String checked(final String value, final Pattern pattern, final String name) {
        final String checked = AssessmentCanonical.text(value, name);
        if (!pattern.matcher(checked).matches()) {
            throw new IllegalArgumentException(name + " has invalid canonical form");
        }
        return checked;
    }
}
