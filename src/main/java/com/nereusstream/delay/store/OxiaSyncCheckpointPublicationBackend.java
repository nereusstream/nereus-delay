package com.nereusstream.delay.store;

import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.RecoveryFloorRef;
import com.nereusstream.delay.protocol.RecoveryPin;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Single-record Oxia authority for checkpoint upload publication.
 *
 * <p>Oxia client 0.9 exposes conditional single-record CAS, not a multi-record
 * transaction. This backend therefore stores the Recovery Catalog snapshot and
 * the shard's upload-intent projections in one canonical record. The provider
 * upload remains an external immutable side effect, while the final
 * PUBLISHED-intent plus catalog-manifest binding is one Oxia version CAS. The
 * session-bound Recovery Pin is a separate ephemeral sibling record and is
 * never encoded into this publication snapshot. The
 * existing separate catalog and upload-intent backends remain available for
 * narrow authority tests and fail closed when asked to claim this boundary.</p>
 *
 * <p>The record is scoped to one shard. All reads validate the exact record
 * key, version, canonical bytes and cross-projection identities before a
 * caller can observe a success.</p>
 */
public final class OxiaSyncCheckpointPublicationBackend implements CheckpointAtomicPublicationAuthority {
    private static final int RECORD_VERSION = 1;
    private static final int MAX_STATE_BYTES = 64 * 1024 * 1024;
    private static final int MAX_INTENTS = 100_000;
    private static final int MAX_CAS_ATTEMPTS = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-oxia-checkpoint-publication\0");
    private static final String RECORD_SUFFIX = "/publication";
    private static final String PIN_SUFFIX = "/recovery-pin";

    private final RecordClient client;
    private final String recordKey;
    private final OxiaSessionBoundRecoveryPinStore pinStore;
    private final CheckpointManifestLimits manifestLimits;
    private final java.util.function.Supplier<byte[]> evidenceStorageNamespace;

    /** Creates an authority over an already configured Oxia client. */
    public OxiaSyncCheckpointPublicationBackend(
            final SyncOxiaClient client, final String keyPrefix, final CheckpointManifestLimits manifestLimits) {
        this(new SyncRecordClient(client, null), keyPrefix, manifestLimits);
    }

    /** Creates a publication authority fenced to the exact ephemeral session of a handle. */
    public OxiaSyncCheckpointPublicationBackend(
            final OxiaSyncOwnerLeaseBackend.ClientHandle handle,
            final String keyPrefix,
            final CheckpointManifestLimits manifestLimits) {
        this(
                new SyncRecordClient(Objects.requireNonNull(handle, "handle").client(), handle.sessionIdentity()),
                keyPrefix,
                manifestLimits,
                handle.backend()::assertConnectedSession,handle.backend()::requireOriginalStorageNamespaceBinding);
    }

    /**
     * Creates an authority whose Recovery Pin operations use the exact
     * session identity supplied by the connected Oxia client owner.
     */
    public OxiaSyncCheckpointPublicationBackend(
            final SyncOxiaClient client,
            final String keyPrefix,
            final CheckpointManifestLimits manifestLimits,
            final byte[] sessionIdentity) {
        this(new SyncRecordClient(client, sessionIdentity), keyPrefix, manifestLimits);
    }

    /** Package-private constructor used by deterministic CAS tests. */
    OxiaSyncCheckpointPublicationBackend(
            final RecordClient client, final String keyPrefix, final CheckpointManifestLimits manifestLimits) {
        this(client, keyPrefix, manifestLimits, () -> {});
    }

    /** Package-private constructor used to exercise the session fence. */
    OxiaSyncCheckpointPublicationBackend(
            final RecordClient client,
            final String keyPrefix,
            final CheckpointManifestLimits manifestLimits,
            final Runnable sessionCheck) {
        this(client,keyPrefix,manifestLimits,sessionCheck,null);
    }
    private OxiaSyncCheckpointPublicationBackend(RecordClient client,String keyPrefix,CheckpointManifestLimits manifestLimits,
            Runnable sessionCheck,java.util.function.Supplier<byte[]> evidenceStorageNamespace){
        this.client = new SessionBoundRecordClient(
                Objects.requireNonNull(client, "client"), Objects.requireNonNull(sessionCheck, "sessionCheck"));
        final String canonicalPrefix = canonicalKeyPrefix(keyPrefix);
        this.recordKey = canonicalPrefix + RECORD_SUFFIX;
        this.pinStore = new OxiaSessionBoundRecoveryPinStore(this.client, canonicalPrefix + PIN_SUFFIX);
        this.manifestLimits = Objects.requireNonNull(manifestLimits, "manifestLimits");
        this.evidenceStorageNamespace=evidenceStorageNamespace;
    }

    @Override
    public RecoveryCatalog.Initialization initializeTargetCatalog(CheckpointUploadIntent pending,
            TargetCheckpointRootVerifier.AuthenticatedImage image, CatalogInitializationAuthority authority) {
        if (client.sessionIdentity() == null) {
            throw new IllegalStateException("Target Catalog initialization requires a connected ephemeral session");
        }
        Objects.requireNonNull(image, "image").authenticate();
        final var marker = CatalogInitialization.from(pending, image.manifest());
        try (var guard = Objects.requireNonNull(Objects.requireNonNull(authority, "authority")
                .acquire(marker, image), "initialization guard")) {
            final Runnable protection = () -> { image.requireCurrent(); guard.requireCurrent(); };
            return mutate(state -> {
                validateScope(state, pending.shard().shardId());
                requireExactPendingOrPublished(state, pending);
                final var catalog = RecoveryCatalog.fromSnapshot(state.catalog());
                final var result = catalog.initializeAuthenticated(marker, protection);
                return changed(result, state.withCatalog(catalog.snapshot()));
            }, protection);
        }
    }

    @Override
    public Optional<CatalogInitialization> currentInitialization() {
        return Optional.ofNullable(read().state().catalog().initialization());
    }

    @Override
    public CheckpointUploadIntent create(final CheckpointUploadIntent pending) {
        return create(pending, () -> {});
    }

    /** Caller-held Owner/namespace/Source/resource protection is rechecked around each metadata CAS step. */
    public CheckpointUploadIntent create(CheckpointUploadIntent pending, Runnable protection) {
        requirePending(pending);
        Objects.requireNonNull(protection, "protection");
        return mutate(state -> {
            validateScope(state, pending.shard().shardId());
            final CheckpointUploadIntent existing = findIntent(state, pending);
            if (existing != null) {
                if (!existing.equals(pending)) {
                    throw new IllegalStateException("checkpoint upload intent CAS conflict");
                }
                return unchanged(existing, state);
            }
            return changed(pending, state.withIntent(pending));
        }, protection);
    }

    @Override
    public CheckpointUploadIntent publish(
            final CheckpointUploadIntent expectedPending, final CheckpointResource resource) {
        requirePending(expectedPending);
        Objects.requireNonNull(resource, "resource");
        return mutate(state -> {
            final CheckpointUploadIntent existing = requireExactPending(state, expectedPending);
            final CheckpointUploadIntent next =
                    nextIntent(expectedPending, CheckpointUploadState.PUBLISHED, resource, null);
            if (existing.equals(next)) {
                return unchanged(existing, state);
            }
            return changed(next, state.withIntent(next));
        });
    }

    @Override
    public Optional<CheckpointUploadIntent> currentPublishedFor(final CheckpointUploadIntent expectedPending) {
        requirePending(expectedPending);
        final CheckpointUploadIntent current = findIntent(read().state(), expectedPending);
        if (current == null || current.state() != CheckpointUploadState.PUBLISHED) {
            return Optional.empty();
        }
        final CheckpointUploadIntent expected =
                nextIntent(expectedPending, CheckpointUploadState.PUBLISHED, current.publishedManifest(), null);
        return current.equals(expected) ? Optional.of(current) : Optional.empty();
    }

    @Override
    public CheckpointUploadIntent beginReaping(
            final CheckpointUploadIntent expectedPending, final TrustedUtcIntervalEvidence evidence) {
        requirePending(expectedPending);
        Objects.requireNonNull(evidence, "evidence");
        evidence.requireEarliestAtLeast(expectedPending.uploadDeadlineEpochMs());
        return mutate(state -> {
            final CheckpointUploadIntent existing = findIntent(state, expectedPending);
            final CheckpointUploadIntent next =
                    nextIntent(expectedPending, CheckpointUploadState.REAPING, null, evidence);
            if (existing != null && existing.equals(next)) {
                return unchanged(existing, state);
            }
            if (existing == null || !existing.equals(expectedPending)) {
                throw new IllegalStateException("checkpoint upload intent expected value does not match current state");
            }
            return changed(next, state.withIntent(next));
        });
    }

    @Override
    public CheckpointUploadIntent beginReaping(
            final CheckpointUploadIntent expectedPending,
            final TrustedUtcIntervalEvidence evidence,
            final RecoveryCatalogAuthority catalog) {
        final CheckpointReapingGuard.Decision decision =
                CheckpointReapingGuard.evaluate(expectedPending, evidence, catalog);
        if (decision != CheckpointReapingGuard.Decision.REAPING_ALLOWED) {
            throw new IllegalStateException("checkpoint reaping guard rejected: " + decision);
        }
        return beginReaping(expectedPending, evidence);
    }

    @Override
    public Optional<CheckpointUploadIntent> current(final CheckpointUploadIntent identity) {
        Objects.requireNonNull(identity, "identity");
        return Optional.ofNullable(findIntent(read().state(), identity));
    }

    /**
     * Commits the provider object identity, PUBLISHED intent and catalog
     * manifest in one canonical Oxia record CAS.
     */
    @Override
    public CheckpointUploadIntent publishUploadedCheckpointAtomically(
            final CheckpointUploadIntent expectedPending,
            final CheckpointResource resource,
            final CheckpointManifest manifest,
            final long expectedCatalogGeneration) {
        return publishUploadedCheckpointAtomically(
                expectedPending, resource, manifest, expectedCatalogGeneration, null);
    }

    @Override
    public CheckpointUploadIntent publishTargetUploadedCheckpointAtomically(CheckpointUploadIntent pending,
            CheckpointResource resource, TargetCheckpointRootVerifier.AuthenticatedImage image, long generation) {
        Objects.requireNonNull(image, "image").authenticate();
        final var result = publishUploadedCheckpointAtomically(pending, resource, image.manifest(), generation, image);
        image.requireCurrent();
        return result;
    }

    private CheckpointUploadIntent publishUploadedCheckpointAtomically(CheckpointUploadIntent expectedPending,
            CheckpointResource resource, CheckpointManifest manifest, long expectedCatalogGeneration,
            TargetCheckpointRootVerifier.AuthenticatedImage image) {
        requirePending(expectedPending);
        Objects.requireNonNull(resource, "resource");
        Objects.requireNonNull(manifest, "manifest");
        validateUploadedObject(expectedPending, resource, manifest);
        if (expectedCatalogGeneration != expectedPending.baseCatalogGeneration()) {
            throw new IllegalStateException("upload intent base catalog generation does not match publication CAS");
        }
        return mutate(state -> {
            if (image != null) { image.requireCurrent(); }
            final CheckpointUploadIntent existing = requireExactPendingOrPublished(state, expectedPending);
            final CheckpointUploadIntent published =
                    nextIntent(expectedPending, CheckpointUploadState.PUBLISHED, resource, null);
            final RecoveryCatalog catalog = RecoveryCatalog.fromSnapshot(state.catalog());
            if (existing.state() == CheckpointUploadState.PUBLISHED) {
                if (!existing.equals(published)) {
                    throw new IllegalStateException("checkpoint published intent identity conflicts with request");
                }
                if (image == null) { catalog.publishUploadedCheckpoint(existing, manifest, expectedCatalogGeneration); }
                else { catalog.publishTargetUploadedCheckpoint(existing, image, expectedCatalogGeneration); }
                return unchanged(existing, state.withCatalog(catalog.snapshot()));
            }
            if (image == null) { catalog.publishUploadedCheckpoint(published, manifest, expectedCatalogGeneration); }
            else { catalog.publishTargetUploadedCheckpoint(published, image, expectedCatalogGeneration); }
            return changed(published, state.withCatalog(catalog.snapshot()).withIntent(published));
        }, image == null ? () -> {} : image::requireCurrent);
    }

    @Override
    public RecoveryCatalog.Publication publish(
            final CheckpointManifest manifest, final long expectedCatalogGeneration) {
        Objects.requireNonNull(manifest, "manifest");
        manifest.validateLimits(manifestLimits);
        return mutate(state -> {
            validateScope(state, manifest.shardId());
            final RecoveryCatalog catalog = RecoveryCatalog.fromSnapshot(state.catalog());
            final RecoveryCatalog.Publication result = catalog.publish(manifest, expectedCatalogGeneration);
            return changed(result, state.withCatalog(catalog.snapshot()));
        });
    }

    @Override
    public RecoveryFloor advanceFloor(
            final byte[] checkpointId, final long expectedCatalogGeneration, final byte[] evidenceCursorDigest) {
        return mutate(state -> {
            final RecoveryCatalog catalog = RecoveryCatalog.fromSnapshot(state.catalog());
            final RecoveryFloor result =
                    catalog.advanceFloor(checkpointId, expectedCatalogGeneration, evidenceCursorDigest);
            return changed(result, state.withCatalog(catalog.snapshot()));
        });
    }

    @Override
    public RecoveryFloorRef advanceFloor(
            final byte[] checkpointId,
            final long expectedCatalogGeneration,
            final List<EvidenceCursor> evidenceCursors) {
        return mutate(state -> {
            final RecoveryCatalog catalog = RecoveryCatalog.fromSnapshot(state.catalog());
            final RecoveryFloorRef result =
                    catalog.advanceFloor(checkpointId, expectedCatalogGeneration, evidenceCursors);
            return changed(result, state.withCatalog(catalog.snapshot()));
        });
    }

    /**
     * Reconciles an already committed atomic publication. A retry is
     * successful only when both the exact PUBLISHED intent and catalog
     * projection are present in the same authority record.
     */
    @Override
    public RecoveryCatalog.Publication publishUploadedCheckpoint(
            final CheckpointUploadIntent publishedIntent,
            final CheckpointManifest manifest,
            final long expectedCatalogGeneration) {
        return publishUploadedCheckpoint(publishedIntent, manifest, expectedCatalogGeneration, null);
    }

    @Override
    public RecoveryCatalog.Publication publishTargetUploadedCheckpoint(CheckpointUploadIntent published,
            TargetCheckpointRootVerifier.AuthenticatedImage image, long generation) {
        Objects.requireNonNull(image, "image").authenticate();
        final var result = publishUploadedCheckpoint(published, image.manifest(), generation, image);
        image.requireCurrent();
        return result;
    }

    private RecoveryCatalog.Publication publishUploadedCheckpoint(CheckpointUploadIntent publishedIntent,
            CheckpointManifest manifest, long expectedCatalogGeneration,
            TargetCheckpointRootVerifier.AuthenticatedImage image) {
        Objects.requireNonNull(publishedIntent, "publishedIntent");
        Objects.requireNonNull(manifest, "manifest");
        if (publishedIntent.state() != CheckpointUploadState.PUBLISHED) {
            throw new IllegalArgumentException("catalog publication requires a PUBLISHED upload intent");
        }
        return mutate(state -> {
            if (image != null) { image.requireCurrent(); }
            final CheckpointUploadIntent current = findIntent(state, publishedIntent);
            if (current == null || !current.equals(publishedIntent)) {
                throw new IllegalStateException("published upload intent is not the exact authority value");
            }
            final RecoveryCatalog catalog = RecoveryCatalog.fromSnapshot(state.catalog());
            final RecoveryCatalog.Publication result =
                    image == null
                            ? catalog.publishUploadedCheckpoint(publishedIntent, manifest, expectedCatalogGeneration)
                            : catalog.publishTargetUploadedCheckpoint(
                                    publishedIntent, image, expectedCatalogGeneration);
            return changed(result, state.withCatalog(catalog.snapshot()));
        }, image == null ? () -> {} : image::requireCurrent);
    }

    @Override
    public Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
        return RecoveryCatalog.fromSnapshot(read().state().catalog()).manifest(checkpointId);
    }

    @Override
    public Optional<RecoveryFloor> currentFloor() {
        return RecoveryCatalog.fromSnapshot(read().state().catalog()).currentFloor();
    }

    @Override
    public Optional<RecoveryFloorRef> currentFloorRef() {
        return RecoveryCatalog.fromSnapshot(read().state().catalog()).currentFloorRef();
    }

    @Override
    public void validatePublishedRestoreCandidate(final CheckpointManifest candidate) {
        RecoveryCatalog.fromSnapshot(read().state().catalog()).validatePublishedRestoreCandidate(candidate);
    }

    @Override
    public Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
            final byte[] candidateCheckpointId,
            final long requiredMutationSequence,
            final SourcePosition... requiredPositions) {
        return RecoveryCatalog.fromSnapshot(read().state().catalog())
                .proveFloorCoverage(candidateCheckpointId, requiredMutationSequence, requiredPositions);
    }

    @Override
    public void validateLocalStoreRecovery(final ShardId shardId, final StoreRecoveryMetadata localMetadata) {
        RecoveryCatalog.fromSnapshot(read().state().catalog()).validateLocalStoreRecovery(shardId, localMetadata);
    }

    @Override
    public RecoveryPin createRecoveryPin(final RecoveryPin pin) {
        final RecoveryPin requested = Objects.requireNonNull(pin, "pin");
        return pinStore.create(
                requested,
                () -> {
                    final PublicationState state = read().state();
                    final RecoveryCatalog catalog = RecoveryCatalog.fromSnapshot(state.catalog());
                    catalog.createRecoveryPin(requested);
                },
                () -> read().state().catalog().catalogGeneration());
    }

    @Override
    public void releaseRecoveryPin(final RecoveryPin pin) {
        pinStore.release(Objects.requireNonNull(pin, "pin"));
    }

    @Override public RecoveryPin replaceRecoveryPin(RecoveryPin expected, RecoveryPin replacement) {
        return pinStore.replace(expected, replacement, () -> {
            final RecoveryCatalog catalog = RecoveryCatalog.fromSnapshot(read().state().catalog());
            catalog.createRecoveryPin(replacement);
        }, () -> read().state().catalog().catalogGeneration());
    }

    @Override
    public Optional<RecoveryPin> activeRecoveryPin() {
        return pinStore.active();
    }

    /** Full image audits plus exact publication/pin projections; this is an input bound, not an RSS certification. */
    public static long recoveryProtectionInputWindowBytes(List<TargetCheckpointRootVerifier.AuthenticatedImage> images){
        if(images==null||images.isEmpty()||images.size()>3)throw new IllegalArgumentException("only Floor, pin candidate and observed-Floor images are accepted");
        long bytes=8L*MAX_STATE_BYTES+64L*1024;
        for(var image:images)bytes=Math.addExact(bytes,Objects.requireNonNull(image).authenticationInputWindowBytes());
        return bytes;
    }

    /**
     * Actual original managed SDK observations and fully authenticated exact Native51 image copies.
     * This does not acquire/replace a Pin, fence future pin admission or authorize payload retirement.
     * The caller must hold the original deployment/admission and image/Source protections separately.
     */
    public EvidenceRecoveryProtection captureEvidenceRecoveryProtection(
            com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedIntent intent,
            List<TargetCheckpointRootVerifier.AuthenticatedImage> images,BoundedReadBudget reads,Runnable current){
        recoveryProtectionInputWindowBytes(images);Objects.requireNonNull(reads);Objects.requireNonNull(current);
        if(reads.maxBytes()==Long.MAX_VALUE)throw new IllegalArgumentException("finite recovery metadata reads required");
        final byte[] namespace=originalEvidenceNamespace();current.run();
        final var before=readEvidenceState(reads);final var catalog=RecoveryCatalog.fromSnapshot(before.state.catalog());
        final var floor=catalog.currentFloorRef().orElseThrow(()->new IllegalStateException("actual typed Recovery Floor is unavailable"));
        final var actual=Objects.requireNonNull(intent).record();final var resource=actual.copy().resource();
        if(!Arrays.equals(floor.recoveryLineageId(),resource.recoveryLineage())||!floor.appliedSourcePosition().shardId().equals(resource.shard()))
            throw new IllegalStateException("Recovery Floor differs from original retirement SHARD/lineage");
        // Proves complete published ancestry, not just a Floor scalar or a supplied Manifest.
        catalog.proveFloorCoverage(floor.checkpointId(),actual.appliedAt().sequence(),actual.appliedAt().source(),actual.body().context().preparedCut().source()).orElseThrow(
                ()->new IllegalStateException("published Floor does not cover the actual Source intent"));
        final var pin=pinStore.readForProtection(reads);current.run();
        final var available=new java.util.HashMap<String,TargetCheckpointRootVerifier.AuthenticatedImage>();
        for(var image:images)if(available.putIfAbsent(Bytes.hex(image.manifest().checkpointId()),image)!=null)
            throw new IllegalArgumentException("ambiguous duplicate recovery protection image");
        final var copies=new java.util.LinkedHashMap<String,TargetCheckpointRootVerifier.RetiringCopy>();
        authenticateRecoveryImage(catalog,floor.checkpointId(),floor.manifestSha256(),intent,available,copies,reads,current);
        if(pin!=null){
            catalog.validateRetainedRecoveryPin(pin.pin());
            if(!pin.pin().shard().shardId().equals(resource.shard()))throw new IllegalStateException("active RecoveryPin belongs to another Source SHARD");
            final var candidate=pin.pin().candidate();
            authenticateRecoveryImage(catalog,candidate.checkpointId(),candidate.manifestSha256(),intent,available,copies,reads,current);
            final var observed=pin.pin().observedFloor();
            authenticateRecoveryImage(catalog,observed.checkpointId(),observed.manifestSha256(),intent,available,copies,reads,current);
        }
        if(copies.size()!=available.size())throw new IllegalArgumentException("unrelated recovery images cannot replace the actual Floor/Pin selection");
        final var protection=new EvidenceRecoveryProtection(this,namespace,before.versionId,Bytes.sha256(before.encoded),floor,pin,List.copyOf(copies.values()));
        protection.requireFor(intent);protection.requireCurrent(reads,current);return protection;
    }
    private void authenticateRecoveryImage(RecoveryCatalog catalog,byte[] id,byte[] digest,
            com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedIntent intent,
            java.util.Map<String,TargetCheckpointRootVerifier.AuthenticatedImage> images,
            java.util.Map<String,TargetCheckpointRootVerifier.RetiringCopy> copies,BoundedReadBudget reads,Runnable current){
        final var published=catalog.manifest(id).orElseThrow(()->new IllegalStateException("protected recovery Manifest is not actually published"));
        if(!Arrays.equals(published.manifestSha256(),digest))throw new IllegalStateException("protected recovery Manifest hash changed");
        final String key=Bytes.hex(id);if(copies.containsKey(key))return;
        final var image=Objects.requireNonNull(images.get(key),"complete actual protected recovery image is unavailable");
        if(!Arrays.equals(image.manifest().canonicalJsonBytes(),published.canonicalJsonBytes()))throw new IllegalStateException("recovery image changes the actual published Manifest");
        if(!reads.beforeTimedWork())throw reads.incomplete();
        current.run();final var copy=image.authenticateRetiringCopy(intent);current.run();
        if(!reads.beforeTimedWork())throw reads.incomplete();copies.put(key,copy);
    }
    private byte[] originalEvidenceNamespace(){
        if(evidenceStorageNamespace==null)throw new IllegalStateException("recovery protection requires its original managed Registry SDK namespace");
        final byte[] namespace=evidenceStorageNamespace.get();Bytes.requireLength(namespace,32,"actual recovery SDK storage namespace");return namespace;
    }
    private StoredState readEvidenceState(BoundedReadBudget reads){
        if(!reads.beforeRead()||!reads.tryCharge(Bytes.utf8(recordKey).length,MAX_STATE_BYTES))throw reads.incomplete();
        final var result=client.get(recordKey);
        if(result==null||!recordKey.equals(result.key())||result.version()==null||result.version().versionId()<0
                ||result.version().sessionId().isPresent()||result.version().clientIdentifier().isPresent())
            throw new IllegalStateException("original persistent checkpoint publication record is missing or changed");
        final byte[] bytes=result.value();if(bytes==null||bytes.length==0||bytes.length>MAX_STATE_BYTES)throw new IllegalStateException("invalid bounded checkpoint publication image");
        return new StoredState(decode(bytes),result.version().versionId(),bytes);
    }
    private static boolean samePin(OxiaSessionBoundRecoveryPinStore.PinRecord a,OxiaSessionBoundRecoveryPinStore.PinRecord b){
        return a==null?b==null:b!=null&&a.version().versionId()==b.version().versionId()&&a.pin().equals(b.pin());
    }
    public static final class EvidenceRecoveryProtection {
        private final OxiaSyncCheckpointPublicationBackend owner;
        private final byte[] storageNamespace,publicationDigest;
        private final long publicationVersion;
        private final RecoveryFloorRef floor;
        private final OxiaSessionBoundRecoveryPinStore.PinRecord pin;
        private final List<TargetCheckpointRootVerifier.RetiringCopy> copies;
        private EvidenceRecoveryProtection(OxiaSyncCheckpointPublicationBackend owner,byte[] namespace,long version,byte[] digest,
                RecoveryFloorRef floor,OxiaSessionBoundRecoveryPinStore.PinRecord pin,List<TargetCheckpointRootVerifier.RetiringCopy> copies){
            this.owner=owner;storageNamespace=Bytes.copy(namespace);publicationVersion=version;publicationDigest=digest;this.floor=floor;this.pin=pin;this.copies=copies;
        }
        public String exactPublicationKey(){return owner.recordKey;}
        public byte[] storageNamespaceBinding(){return Bytes.copy(storageNamespace);}
        public RecoveryFloorRef floor(){return floor;}
        public Optional<RecoveryPin> recoveryPin(){return pin==null?Optional.empty():Optional.of(pin.pin());}
        public void requireAuthority(OxiaSyncCheckpointPublicationBackend expected){if(owner!=expected)throw new IllegalArgumentException("recovery facts belong to another original Catalog authority");}
        public void requireFor(com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedIntent intent){for(var copy:copies)copy.requireFor(intent);}
        /** Actual bounded rereads, not a local boolean and not a substitute for the original admission/Pin protocols. */
        public void requireCurrent(BoundedReadBudget reads,Runnable current){
            Objects.requireNonNull(reads);Objects.requireNonNull(current);
            if(reads.maxBytes()==Long.MAX_VALUE)throw new IllegalArgumentException("finite recovery metadata reads required");
            if(!reads.beforeTimedWork())throw reads.incomplete();
            current.run();if(!Arrays.equals(storageNamespace,owner.originalEvidenceNamespace()))throw new IllegalStateException("original recovery SDK namespace changed");
            final var state=owner.readEvidenceState(reads);
            if(state.versionId!=publicationVersion||!Arrays.equals(publicationDigest,Bytes.sha256(state.encoded)))
                throw new IllegalStateException("original protected Catalog/Floor projection changed");
            if(!samePin(pin,owner.pinStore.readForProtection(reads)))throw new IllegalStateException("original active RecoveryPin projection changed");
            for(var copy:copies)copy.requireCurrent();current.run();
            if(!reads.beforeTimedWork())throw reads.incomplete();
        }
    }

    private <T> T mutate(final Function<PublicationState, Change<T>> operation) {
        return mutate(operation, () -> {});
    }

    private <T> T mutate(final Function<PublicationState, Change<T>> operation, final Runnable protection) {
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            protection.run();
            final StoredState before = read();
            protection.run();
            final Change<T> change = Objects.requireNonNull(operation.apply(before.state()), "publication change");
            final byte[] afterBytes = encode(change.state());
            protection.run();
            if (Arrays.equals(before.encoded(), afterBytes)) {
                return change.result();
            }
            try {
                putExact(afterBytes, before.versionId());
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException conflict) {
                // Another authority writer won the record CAS. Re-read the
                // complete state and apply the exact request again.
                continue;
            } catch (RuntimeException responseFailure) {
                protection.run();
                final StoredState observed = read();
                protection.run();
                if (Arrays.equals(afterBytes, observed.encoded())) {
                    return change.result();
                }
                throw responseFailure;
            }
            protection.run();
            return change.result();
        }
        throw new IllegalStateException("Oxia checkpoint publication CAS did not converge");
    }

    private StoredState read() {
        final GetResult result = client.get(recordKey);
        if (result == null) {
            final PublicationState empty = emptyState();
            return new StoredState(empty, null, encode(empty));
        }
        if (!recordKey.equals(result.key()) || result.version() == null) {
            throw new IllegalStateException("Oxia checkpoint publication response has an invalid key or version");
        }
        final byte[] encoded = result.value();
        if (encoded == null || encoded.length == 0 || encoded.length > MAX_STATE_BYTES) {
            throw new IllegalStateException("Oxia checkpoint publication state has an invalid size");
        }
        return new StoredState(decode(encoded), result.version().versionId(), encoded);
    }

    private void putExact(final byte[] encoded, final Long versionId)
            throws UnexpectedVersionIdException, KeyAlreadyExistsException {
        final Set<PutOption> options = versionId == null
                ? Set.of(PutOption.IfRecordDoesNotExist)
                : Set.of(PutOption.IfVersionIdEquals(versionId));
        final PutResult result = client.put(recordKey, encoded, options);
        if (result == null || !recordKey.equals(result.key()) || result.version() == null) {
            throw new IllegalStateException("Oxia checkpoint publication put returned no exact version");
        }
    }

    private static PublicationState emptyState() {
        return new PublicationState(
                null,
                new RecoveryCatalog.Snapshot(0, null, List.of(), java.util.Map.of(), null, null, null),
                List.of());
    }

    private PublicationState decode(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("Oxia checkpoint publication state is empty");
        }
        int index = 0;
        final CanonicalProtobuf.Reader.Field version = nextField(fields, index++, 1);
        if (uint(version, 1) != RECORD_VERSION) {
            throw new IllegalArgumentException("unsupported Oxia checkpoint publication state version");
        }
        final byte[] catalogBytes = bytes(nextField(fields, index++, 2), 2);
        ShardId shard = null;
        final List<CheckpointUploadIntent> intents = new ArrayList<>();
        byte[] digest = null;
        while (index < fields.size()) {
            final CanonicalProtobuf.Reader.Field field = fields.get(index++);
            switch (field.number()) {
                case 3 -> {
                    if (shard != null) {
                        throw new IllegalArgumentException("duplicate Oxia publication shard field");
                    }
                    shard = ShardSubject.decode(bytes(field, 3)).shardId();
                }
                case 4 -> {
                    if (intents.size() >= MAX_INTENTS) {
                        throw new IllegalArgumentException("Oxia checkpoint publication intent count exceeds bound");
                    }
                    intents.add(CheckpointUploadIntent.decode(bytes(field, 4)));
                }
                case 5 -> {
                    if (digest != null || index != fields.size()) {
                        throw new IllegalArgumentException("Oxia publication digest must be the final field");
                    }
                    digest = bytes(field, 5);
                }
                default ->
                    throw new IllegalArgumentException(
                            "unknown Oxia checkpoint publication state field " + field.number());
            }
        }
        if (digest == null) {
            throw new IllegalArgumentException("Oxia checkpoint publication state has no digest");
        }
        final PublicationState state = new PublicationState(
                shard, OxiaSyncRecoveryCatalogBackend.decodeSnapshot(catalogBytes, manifestLimits), intents);
        final byte[] payload = encodePayload(state);
        if (!Bytes.constantTimeEquals(digest, Bytes.sha256(DIGEST_DOMAIN, payload))
                || !Arrays.equals(encoded, encode(state))) {
            throw new IllegalArgumentException("Oxia checkpoint publication state is not canonical");
        }
        return state;
    }

    private static byte[] encode(final PublicationState state) {
        final byte[] payload = encodePayload(state);
        if (payload.length > MAX_STATE_BYTES - 128) {
            throw new IllegalStateException("Oxia checkpoint publication state exceeds size bound");
        }
        final byte[] digest = Bytes.sha256(DIGEST_DOMAIN, payload);
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(payload);
            CanonicalProtobuf.bytes(output, 5, digest);
        });
    }

    private static byte[] encodePayload(final PublicationState state) {
        final byte[] catalogBytes = OxiaSyncRecoveryCatalogBackend.encodeSnapshot(state.catalog());
        final List<CheckpointUploadIntent> intents = state.intents().stream()
                .sorted(Comparator.comparing(OxiaSyncCheckpointPublicationBackend::intentIdentity))
                .toList();
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECORD_VERSION);
            CanonicalProtobuf.bytes(output, 2, catalogBytes);
            if (state.shard() != null) {
                CanonicalProtobuf.bytes(output, 3, new ShardSubject(state.shard()).canonicalBytes());
            }
            intents.forEach(intent -> CanonicalProtobuf.bytes(output, 4, intent.canonicalBytes()));
        });
    }

    private static String intentIdentity(final CheckpointUploadIntent intent) {
        return Bytes.hex(Bytes.concat(intent.shard().canonicalHashBytes(), intent.checkpointId()));
    }

    private static CheckpointUploadIntent findIntent(
            final PublicationState state, final CheckpointUploadIntent identity) {
        final String requested = intentIdentity(identity);
        for (CheckpointUploadIntent intent : state.intents()) {
            if (requested.equals(intentIdentity(intent))) {
                if (!intent.shard().equals(identity.shard())
                        || !Bytes.constantTimeEquals(intent.recoveryLineageId(), identity.recoveryLineageId())
                        || !Bytes.constantTimeEquals(intent.checkpointId(), identity.checkpointId())) {
                    throw new IllegalStateException("checkpoint publication intent identity collision");
                }
                return intent;
            }
        }
        return null;
    }

    private static CheckpointUploadIntent requireExactPending(
            final PublicationState state, final CheckpointUploadIntent expected) {
        final CheckpointUploadIntent existing = findIntent(state, expected);
        if (existing == null || !existing.equals(expected)) {
            throw new IllegalStateException("checkpoint upload intent expected value does not match current state");
        }
        return existing;
    }

    private static CheckpointUploadIntent requireExactPendingOrPublished(
            final PublicationState state, final CheckpointUploadIntent expected) {
        final CheckpointUploadIntent existing = findIntent(state, expected);
        if (existing == null
                || (existing.state() != CheckpointUploadState.PENDING_UPLOAD
                        && existing.state() != CheckpointUploadState.PUBLISHED)) {
            throw new IllegalStateException("checkpoint upload intent is not pending or published in authority");
        }
        return existing;
    }

    private static void requirePending(final CheckpointUploadIntent pending) {
        Objects.requireNonNull(pending, "pending");
        if (pending.state() != CheckpointUploadState.PENDING_UPLOAD) {
            throw new IllegalArgumentException("checkpoint upload intent must be PENDING_UPLOAD");
        }
    }

    private static void validateScope(final PublicationState state, final ShardId shard) {
        if (state.shard() != null && !state.shard().equals(shard)) {
            throw new IllegalArgumentException("Oxia checkpoint publication record is bound to another shard");
        }
        if (state.catalog().catalogShard() != null
                && !state.catalog().catalogShard().equals(shard)) {
            throw new IllegalArgumentException("Oxia checkpoint publication catalog is bound to another shard");
        }
    }

    private static void validateUploadedObject(
            final CheckpointUploadIntent pending,
            final CheckpointResource resource,
            final CheckpointManifest manifest) {
        if (!resource.objectStoreProfile().equals(pending.objectStoreProfile())
                || !Bytes.constantTimeEquals(resource.recoveryLineageId(), pending.recoveryLineageId())
                || !Bytes.constantTimeEquals(resource.checkpointId(), pending.checkpointId())
                || resource.manifestLength() != manifest.canonicalJsonBytes().length
                || !Bytes.constantTimeEquals(resource.manifestSha256(), manifest.manifestSha256())) {
            throw new IllegalArgumentException("uploaded checkpoint manifest object identity mismatch");
        }
    }

    private static CheckpointUploadIntent nextIntent(
            final CheckpointUploadIntent expected,
            final CheckpointUploadState state,
            final CheckpointResource resource,
            final TrustedUtcIntervalEvidence evidence) {
        if (expected.stateRevision() == -1L) {
            throw new IllegalStateException("checkpoint upload intent state revision exhausted");
        }
        return new CheckpointUploadIntent(
                expected.shard(),
                expected.recoveryLineageId(),
                expected.checkpointId(),
                expected.owner(),
                expected.sourceStoreIncarnation(),
                expected.uploadToken(),
                expected.baseCatalogGeneration(),
                expected.parentCheckpointId(),
                expected.parentManifestSha256(),
                expected.objectStoreProfile(),
                expected.checkpointCreatedAt(),
                expected.uploadDeadlineEpochMs(),
                state,
                expected.stateRevision() + 1,
                resource,
                evidence);
    }

    private static <T> Change<T> changed(final T result, final PublicationState state) {
        return new Change<>(Objects.requireNonNull(result, "result"), state);
    }

    private static <T> Change<T> unchanged(final T result, final PublicationState state) {
        return new Change<>(Objects.requireNonNull(result, "result"), state);
    }

    private static CanonicalProtobuf.Reader.Field nextField(
            final List<CanonicalProtobuf.Reader.Field> fields, final int index, final int number) {
        if (index >= fields.size() || fields.get(index).number() != number) {
            throw new IllegalArgumentException("missing Oxia checkpoint publication state field " + number);
        }
        return fields.get(index);
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid Oxia checkpoint publication bytes field " + number);
        }
        return field.rawValue();
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid Oxia checkpoint publication uint field " + number);
        }
        return field.unsignedValue();
    }

    private static String canonicalKeyPrefix(final String value) {
        Objects.requireNonNull(value, "keyPrefix");
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank()
                || value.endsWith("/")
                || value.indexOf('\0') >= 0
                || !value.equals(new String(encoded, StandardCharsets.UTF_8))
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException("keyPrefix must be a nonblank NFC UTF-8 path without trailing '/'");
        }
        return value;
    }

    interface RecordClient extends OxiaSessionBoundRecoveryPinStore.RecordClient {}

    private static final class SyncRecordClient implements RecordClient {
        private final SyncOxiaClient delegate;
        private final byte[] sessionIdentity;

        private SyncRecordClient(final SyncOxiaClient delegate, final byte[] sessionIdentity) {
            this.delegate = Objects.requireNonNull(delegate, "client");
            this.sessionIdentity = sessionIdentity == null ? null : Bytes.copy(sessionIdentity);
        }

        @Override
        public GetResult get(final String key) {
            return delegate.get(key);
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            return delegate.put(key, value, options);
        }

        @Override
        public boolean delete(final String key, final Set<DeleteOption> options) throws UnexpectedVersionIdException {
            return delegate.delete(key, options);
        }

        @Override
        public byte[] sessionIdentity() {
            return sessionIdentity == null ? null : Bytes.copy(sessionIdentity);
        }
    }

    /**
     * Checks the caller's Oxia session around every publication-record
     * operation. A publication CAS whose response is lost after the marker
     * disappears is therefore never reported as a successful intent/catalog
     * transition.
     */
    private static final class SessionBoundRecordClient implements RecordClient {
        private final RecordClient delegate;
        private final Runnable sessionCheck;

        private SessionBoundRecordClient(final RecordClient delegate, final Runnable sessionCheck) {
            this.delegate = delegate;
            this.sessionCheck = sessionCheck;
        }

        @Override
        public GetResult get(final String key) {
            sessionCheck.run();
            try {
                final GetResult result = delegate.get(key);
                sessionCheck.run();
                return result;
            } catch (RuntimeException failure) {
                sessionCheck.run();
                throw failure;
            }
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            sessionCheck.run();
            try {
                final PutResult result = delegate.put(key, value, options);
                sessionCheck.run();
                return result;
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException expectedCasRace) {
                sessionCheck.run();
                throw expectedCasRace;
            } catch (RuntimeException failure) {
                sessionCheck.run();
                throw failure;
            }
        }

        @Override
        public boolean delete(final String key, final Set<DeleteOption> options) throws UnexpectedVersionIdException {
            sessionCheck.run();
            try {
                final boolean deleted = delegate.delete(key, options);
                sessionCheck.run();
                return deleted;
            } catch (UnexpectedVersionIdException expectedCasRace) {
                sessionCheck.run();
                throw expectedCasRace;
            } catch (RuntimeException failure) {
                sessionCheck.run();
                throw failure;
            }
        }

        @Override
        public byte[] sessionIdentity() {
            final byte[] identity = delegate.sessionIdentity();
            return identity == null ? null : Bytes.copy(identity);
        }
    }

    private record StoredState(PublicationState state, Long versionId, byte[] encoded) {
        private StoredState {
            Objects.requireNonNull(state, "state");
            encoded = Bytes.copy(Objects.requireNonNull(encoded, "encoded"));
        }

        @Override
        public byte[] encoded() {
            return Bytes.copy(encoded);
        }
    }

    private record Change<T>(T result, PublicationState state) {
        private Change {
            Objects.requireNonNull(result, "result");
            Objects.requireNonNull(state, "state");
        }
    }

    private record PublicationState(
            ShardId shard, RecoveryCatalog.Snapshot catalog, List<CheckpointUploadIntent> intents) {
        private PublicationState {
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(intents, "intents");
            if (catalog.activeRecoveryPin() != null) {
                throw new IllegalArgumentException("checkpoint publication record cannot encode RecoveryPin");
            }
            if (shard == null && catalog.catalogShard() != null) {
                shard = catalog.catalogShard();
            }
            if (shard != null && catalog.catalogShard() != null && !shard.equals(catalog.catalogShard())) {
                throw new IllegalArgumentException("checkpoint publication shard differs from catalog shard");
            }
            final Set<String> identities = new HashSet<>();
            final List<CheckpointUploadIntent> copied = new ArrayList<>();
            for (CheckpointUploadIntent intent : intents) {
                Objects.requireNonNull(intent, "intent");
                if (shard != null && !shard.equals(intent.shard().shardId())) {
                    throw new IllegalArgumentException("checkpoint publication intent differs from record shard");
                }
                if (!identities.add(intentIdentity(intent))) {
                    throw new IllegalArgumentException("checkpoint publication has duplicate intent identity");
                }
                copied.add(intent);
            }
            if (!copied.isEmpty() && shard == null) {
                throw new IllegalArgumentException("checkpoint publication intents require a record shard");
            }
            intents = List.copyOf(copied);
        }

        private PublicationState withIntent(final CheckpointUploadIntent intent) {
            final List<CheckpointUploadIntent> next = new ArrayList<>(intents);
            final String identity = intentIdentity(intent);
            for (int index = 0; index < next.size(); index++) {
                if (identity.equals(intentIdentity(next.get(index)))) {
                    next.set(index, intent);
                    return new PublicationState(shard, catalog, next);
                }
            }
            next.add(intent);
            return new PublicationState(shard == null ? intent.shard().shardId() : shard, catalog, next);
        }

        private PublicationState withCatalog(final RecoveryCatalog.Snapshot nextCatalog) {
            return new PublicationState(shard, nextCatalog, intents);
        }
    }
}
