package com.nereusstream.delay.assessment;

import com.nereusstream.delay.ownership.WorkerAssignment;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ProtocolCapabilityDeclaration;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.ShardSubject;
import java.security.PublicKey;
import java.util.List;
import java.util.Objects;

/**
 * Fail-closed activation barrier for the exact signed reset manifest.
 *
 * <p>The gate is deliberately a pure boundary object. It never creates a
 * Producer, opens a Store, changes an assignment, or writes an activation
 * marker. Callers must invoke it before each corresponding side effect.</p>
 */
public final class DataResetActivationGate {
    private final DataResetManifest manifest;
    private final PublicKey issuerKey;
    private final String environmentId;
    private final ArtifactGenerationSet artifacts;

    public DataResetActivationGate(
            final DataResetManifest manifest,
            final PublicKey issuerKey,
            final String environmentId,
            final ArtifactGenerationSet artifacts) {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.issuerKey = Objects.requireNonNull(issuerKey, "issuerKey");
        this.environmentId = text(environmentId, "environmentId");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        requireSignedCurrent();
        if (!this.environmentId.equals(manifest.scope().environmentId())) {
            throw new IllegalArgumentException("activation environment does not match manifest");
        }
    }

    public DataResetManifest manifest() {
        return manifest;
    }

    public ArtifactGenerationSet artifacts() {
        return artifacts;
    }

    /** Verifies the signed manifest and the exact current generation. */
    public void requireManifest(final long trustedNowEpochMs) {
        requireSignedCurrent();
        manifest.requireWithinWindow(trustedNowEpochMs);
    }

    /** Startup barrier before a Worker opens its Store or consumes a source log. */
    public void requireStartup(final StartupIdentity identity, final long trustedNowEpochMs) {
        Objects.requireNonNull(identity, "identity");
        requireManifest(trustedNowEpochMs);
        manifest.requireExactScope(
                identity.environmentId(),
                identity.deploymentId(),
                identity.tenantScopeDigest(),
                identity.routeSnapshotDigest(),
                identity.shard());
        requireWorker(
                identity.workerId(), identity.workerIdentity(), identity.sessionIdentity(), identity.declaration());
        requireExactResources(identity.resources());
    }

    /** Assignment barrier after the authority publishes a placement projection. */
    public void requireAssignment(
            final WorkerAssignment assignment,
            final ProtocolCapabilityDeclaration declaration,
            final long trustedNowEpochMs) {
        Objects.requireNonNull(assignment, "assignment");
        Objects.requireNonNull(declaration, "declaration");
        requireManifest(trustedNowEpochMs);
        requireAssignmentIdentity(assignment, declaration);
    }

    /** Assignment identity barrier for callers that already checked trusted time. */
    public void requireAssignment(final WorkerAssignment assignment, final ProtocolCapabilityDeclaration declaration) {
        Objects.requireNonNull(assignment, "assignment");
        Objects.requireNonNull(declaration, "declaration");
        requireSignedCurrent();
        requireAssignmentIdentity(assignment, declaration);
    }

    private void requireAssignmentIdentity(
            final WorkerAssignment assignment, final ProtocolCapabilityDeclaration declaration) {
        if (!assignment.routeBound()
                || !Bytes.constantTimeEquals(
                        assignment.routeSnapshotDigest(), manifest.scope().routeSnapshotDigest())) {
            throw new IllegalStateException("Worker assignment is not bound to the manifest Route snapshot");
        }
        final ShardSubject shard = manifest.scope().shard();
        if (!shard.shardId().equals(assignment.sourceAssignment().shardId())) {
            throw new IllegalStateException("Worker assignment belongs to another manifest shard");
        }
        requireWorker(assignment.workerId(), declaration.workerIdentity(), declaration.sessionIdentity(), declaration);
    }

    /** Source-ordered apply barrier; tuple and manifest must be atomically paired. */
    public void requireSourceApply(
            final ProtocolTuple tuple,
            final ArtifactGenerationSet candidateArtifacts,
            final byte[] manifestDigest,
            final long trustedNowEpochMs) {
        Objects.requireNonNull(tuple, "tuple");
        requireManifest(trustedNowEpochMs);
        requireExactArtifacts(candidateArtifacts);
        requireManifestDigest(manifestDigest);
        if (!tuple.equals(artifacts.clientCommandTuple()) && !tuple.equals(artifacts.systemMutationTuple())) {
            throw new IllegalStateException("source apply tuple is outside the manifest generation");
        }
    }

    /** Physical-send barrier; it is intentionally separate from source apply. */
    public void requirePhysicalSend(
            final ArtifactGenerationSet candidateArtifacts, final byte[] manifestDigest, final long trustedNowEpochMs) {
        requireManifest(trustedNowEpochMs);
        requireExactArtifacts(candidateArtifacts);
        requireManifestDigest(manifestDigest);
    }

    /** Compatibility overload for callers that already proved the time window. */
    public void requirePhysicalSend(final ArtifactGenerationSet candidateArtifacts, final byte[] manifestDigest) {
        requireSignedCurrent();
        requireExactArtifacts(candidateArtifacts);
        requireManifestDigest(manifestDigest);
    }

    private void requireSignedCurrent() {
        if (!manifest.isCurrentGeneration()) {
            throw new IllegalStateException("DataResetManifest is not current generation");
        }
        if (!manifest.artifacts().equals(artifacts)) {
            throw new IllegalStateException("activation ArtifactGenerationSet mismatch");
        }
        if (!manifest.verifySignature(issuerKey)) {
            throw new IllegalStateException("DataResetManifest signature is invalid");
        }
    }

    private void requireExactArtifacts(final ArtifactGenerationSet candidateArtifacts) {
        Objects.requireNonNull(candidateArtifacts, "candidateArtifacts");
        if (!artifacts.equals(candidateArtifacts)
                || !Bytes.constantTimeEquals(artifacts.setDigest(), candidateArtifacts.setDigest())) {
            throw new IllegalStateException("candidate ArtifactGenerationSet is stale or mixed");
        }
    }

    private void requireManifestDigest(final byte[] candidateDigest) {
        if (candidateDigest == null || !Bytes.constantTimeEquals(manifest.manifestDigest(), candidateDigest)) {
            throw new IllegalStateException("manifest digest is stale or forged");
        }
    }

    private void requireWorker(
            final String workerId,
            final byte[] workerIdentity,
            final byte[] sessionIdentity,
            final ProtocolCapabilityDeclaration declaration) {
        if (!declaration.isCurrentGeneration() || !declaration.supports(artifacts)) {
            throw new IllegalStateException("Worker does not support the exact manifest generation");
        }
        if (!workerId.equals(declaration.workerId())) {
            throw new IllegalStateException("Worker declaration identity mismatch");
        }
        if (!Bytes.constantTimeEquals(workerIdentity, declaration.workerIdentity())
                || !Bytes.constantTimeEquals(sessionIdentity, declaration.sessionIdentity())) {
            throw new IllegalStateException("Worker declaration session identity mismatch");
        }
        final DataResetManifest.WorkerCapability expected = manifest.workerCapabilities().stream()
                .filter(worker -> worker.workerId().equals(workerId))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Worker is outside the manifest barrier"));
        if (!expected.declaration().equals(declaration)
                || !Bytes.constantTimeEquals(expected.workerIdentity(), workerIdentity)
                || !Bytes.constantTimeEquals(expected.sessionIdentity(), sessionIdentity)) {
            throw new IllegalStateException("Worker capability is stale or mixed generation");
        }
    }

    private void requireExactResources(final List<DataResetManifest.ResourceIncarnation> candidateResources) {
        Objects.requireNonNull(candidateResources, "resources");
        if (!manifest.resources().equals(candidateResources)) {
            throw new IllegalStateException("resource incarnation set is stale or mixed");
        }
    }

    private static String text(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " must be nonblank and contain no NUL");
        }
        return value;
    }

    /** Local identity and fresh resource projection supplied at startup. */
    public record StartupIdentity(
            String environmentId,
            String deploymentId,
            byte[] tenantScopeDigest,
            byte[] routeSnapshotDigest,
            ShardSubject shard,
            String workerId,
            byte[] workerIdentity,
            byte[] sessionIdentity,
            ProtocolCapabilityDeclaration declaration,
            List<DataResetManifest.ResourceIncarnation> resources) {
        public StartupIdentity {
            Objects.requireNonNull(environmentId, "environmentId");
            Objects.requireNonNull(deploymentId, "deploymentId");
            Bytes.requireLength(tenantScopeDigest, 32, "tenantScopeDigest");
            Bytes.requireLength(routeSnapshotDigest, 32, "routeSnapshotDigest");
            Objects.requireNonNull(shard, "shard");
            Objects.requireNonNull(workerId, "workerId");
            Bytes.requireLength(workerIdentity, 32, "workerIdentity");
            Bytes.requireLength(sessionIdentity, 32, "sessionIdentity");
            Objects.requireNonNull(declaration, "declaration");
            resources = List.copyOf(Objects.requireNonNull(resources, "resources"));
            tenantScopeDigest = Bytes.copy(tenantScopeDigest);
            routeSnapshotDigest = Bytes.copy(routeSnapshotDigest);
            workerIdentity = Bytes.copy(workerIdentity);
            sessionIdentity = Bytes.copy(sessionIdentity);
        }

        @Override
        public byte[] tenantScopeDigest() {
            return Bytes.copy(tenantScopeDigest);
        }

        @Override
        public byte[] routeSnapshotDigest() {
            return Bytes.copy(routeSnapshotDigest);
        }

        @Override
        public byte[] workerIdentity() {
            return Bytes.copy(workerIdentity);
        }

        @Override
        public byte[] sessionIdentity() {
            return Bytes.copy(sessionIdentity);
        }
    }
}
