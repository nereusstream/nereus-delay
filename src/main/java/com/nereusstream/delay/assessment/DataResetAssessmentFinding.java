package com.nereusstream.delay.assessment;

import java.util.Objects;

/** Closed, deterministic explanation attached to a G0 assessment outcome. */
public record DataResetAssessmentFinding(Code code, String subject) implements Comparable<DataResetAssessmentFinding> {
    public DataResetAssessmentFinding {
        Objects.requireNonNull(code, "code");
        subject = AssessmentCanonical.text(subject, "finding subject");
    }

    String canonicalJson() {
        return "{\"code\":" + AssessmentCanonical.quote(code.name()) + ",\"subject\":"
                + AssessmentCanonical.quote(subject) + '}';
    }

    @Override
    public int compareTo(final DataResetAssessmentFinding other) {
        final int codeComparison = code.compareTo(other.code);
        return codeComparison != 0 ? codeComparison : AssessmentCanonical.UTF8_ORDER.compare(subject, other.subject);
    }

    public enum Code {
        EXTERNAL_RETENTION_NOT_RETAINABLE(true),
        RESOURCE_MIGRATION_REQUIRED(true),
        OBLIGATION_PRESERVATION_REQUIRED(true),
        SCOPE_DIGEST_MISMATCH(false),
        SCOPE_ENUMERATION_INCOMPLETE(false),
        OBSERVATION_TIME_UNQUALIFIED(false),
        RESOURCE_SET_MISMATCH(false),
        RESOURCE_ACCESS_INCOMPLETE(false),
        EXTERNAL_RETENTION_UNKNOWN(false),
        RESOURCE_DISPOSITION_UNKNOWN(false),
        OBLIGATION_ENUMERATION_INCOMPLETE(false),
        OBLIGATION_DISPOSITION_UNKNOWN(false),
        WORKER_ENUMERATION_INCOMPLETE(false),
        WORKER_SET_MISMATCH(false),
        WORKER_NOT_UPGRADEABLE(false),
        WORKER_STATUS_UNKNOWN(false);

        private final boolean migrationRequired;

        Code(final boolean migrationRequired) {
            this.migrationRequired = migrationRequired;
        }

        public boolean migrationRequired() {
            return migrationRequired;
        }
    }
}
