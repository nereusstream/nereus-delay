package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.RecoveryPin;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import java.nio.file.Path;

/** Test-only access to the package-local physical candidate primitive from real Worker fixtures. */
public final class TargetCheckpointCandidateTestBridge {
    private TargetCheckpointCandidateTestBridge() {}

    /** Real Oxia publication codec/CAS operations over a deterministic record transport, not a native service. */
    public static OxiaSyncCheckpointPublicationBackend oxiaPublicationModel(CheckpointManifestLimits limits) {
        return new OxiaSyncCheckpointPublicationBackend(
                new OxiaSyncCheckpointPublicationBackendTest.FakeRecordClient(), "candidate-intent-fixture", limits);
    }

    public static Path create(
            final ShardStore store,
            final Path path,
            final byte[] checkpointId,
            final byte[] lineage,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        return store.createTargetCheckpointCandidate(
                path, checkpointId, lineage, physicalLimits, quotaLimits, ledgerLimits);
    }

    public static Path reuse(
            final ShardStore store,
            final Path path,
            final byte[] checkpointId,
            final byte[] lineage,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        return store.reuseTargetCheckpointCandidate(
                path, checkpointId, lineage, physicalLimits, quotaLimits, ledgerLimits);
    }

    public static Path createSemantic(
            ShardStore store,
            Path path,
            byte[] checkpointId,
            byte[] lineage,
            CheckpointManifestLimits physicalLimits,
            TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            TargetCheckpointSemanticSnapshot snapshot,
            TargetCheckpointSemanticSnapshot.Limits semanticLimits) {
        return store.createTargetSemanticCheckpointCandidate(
                path, checkpointId, lineage, physicalLimits, quotaLimits, ledgerLimits, snapshot, semanticLimits);
    }

    public static ShardStore restoreTarget(
            ShardStoreConfig config,
            ShardId shard,
            SharedRocksDbResources resources,
            Path image,
            CheckpointManifest manifest,
            RecoveryCatalogAuthority catalog,
            RecoveryPin pin,
            CheckpointManifestLimits physicalLimits,
            ShardStore.TargetRestoreAuthentication authentication) {
        return ShardStore.restoreTargetFromCheckpoint(
                config, shard, resources, image, manifest, catalog, pin, physicalLimits, authentication);
    }

    /** Actual WorkClass/coordinator/permit/install path with an explicit local file-download fixture. */
    public static ShardStore restoreTargetThroughWorkClass(
            ShardStoreConfig config,
            ShardId shard,
            SharedRocksDbResources resources,
            Path image,
            CheckpointManifest manifest,
            RecoveryCatalogAuthority catalog,
            RecoveryPin pin,
            CheckpointManifestLimits physicalLimits,
            ShardStore.TargetRestoreAuthentication authentication,
            com.nereusstream.delay.scheduler.WorkClassExecutionRegistry registry) {
        final var profile = new com.nereusstream.delay.protocol.ProfileRef(
                com.nereusstream.delay.protocol.Bytes.utf8("checkpoint-download-fixture"),
                1,
                manifest.manifestSha256(),
                com.nereusstream.delay.protocol.ProfileKind.OBJECT_STORE);
        final var resource = new com.nereusstream.delay.protocol.CheckpointResource(
                manifest.recoveryLineageId(),
                manifest.checkpointId(),
                profile,
                new byte[] {1},
                new byte[] {2},
                new byte[] {3},
                manifest.canonicalJsonBytes().length,
                manifest.manifestSha256());
        final var coordinator = new CheckpointRestoreCoordinator(
                config,
                shard,
                resources,
                (request, target) -> {
                    org.junit.jupiter.api.Assertions.assertThrows(
                            IllegalStateException.class, resources::acquireCheckpointDownloadPermit);
                    try {
                        java.nio.file.Files.createDirectories(target);
                        for (var file : request.manifest().files()) {
                            final var copied = target.resolve(file.name());
                            java.nio.file.Files.createDirectories(copied.getParent());
                            java.nio.file.Files.copy(image.resolve(file.name()), copied);
                        }
                        return target;
                    } catch (java.io.IOException failure) {
                        throw new IllegalStateException(failure);
                    }
                },
                catalog,
                physicalLimits);
        final var executor = new CheckpointRestoreWorkClassExecutor(registry, coordinator);
        final var request = new CheckpointDownloadRequest(manifest, resource);
        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> executor.submit(new CheckpointRestoreWorkClassExecutor.RestoreRequest(request, pin)));
        final var submitted =
                executor.submit(new CheckpointRestoreWorkClassExecutor.RestoreRequest(request, pin, authentication));
        org.junit.jupiter.api.Assertions.assertTrue(submitted.outcome().isEmpty());
        registry.runTurn(new com.nereusstream.delay.scheduler.SchedulerBudget(
                1, submitted.task().bytes(), 1_000_000));
        final var outcome = submitted.outcome().orElseThrow();
        if (outcome.failure() instanceof RuntimeException failure) {
            throw failure;
        }
        if (outcome.failure() != null) {
            throw new AssertionError(outcome.failure());
        }
        return outcome.restored();
    }

    /** Explicit exact Catalog/RecoveryPin fixture; neither publishes format2 nor represents a production Catalog. */
    public static RecoveryCatalogAuthority restoreCatalogFixture(
            CheckpointManifest manifest, java.util.concurrent.atomic.AtomicReference<RecoveryPin> pin) {
        return new RecoveryCatalogAuthority() {
            @Override
            public RecoveryCatalog.Publication publish(CheckpointManifest candidate, long generation) {
                throw new UnsupportedOperationException("fixture cannot publish");
            }

            @Override
            public RecoveryFloor advanceFloor(byte[] checkpoint, long generation, byte[] cursors) {
                throw new UnsupportedOperationException("fixture cannot advance Floor");
            }

            @Override
            public java.util.Optional<CheckpointManifest> manifest(byte[] checkpoint) {
                return java.util.Arrays.equals(checkpoint, manifest.checkpointId())
                        ? java.util.Optional.of(manifest)
                        : java.util.Optional.empty();
            }

            @Override
            public java.util.Optional<RecoveryFloor> currentFloor() {
                return java.util.Optional.empty();
            }

            @Override
            public void validatePublishedRestoreCandidate(CheckpointManifest candidate) {
                if (!java.util.Arrays.equals(manifest.manifestSha256(), candidate.manifestSha256())) {
                    throw new IllegalStateException("fixture has no exact published candidate");
                }
            }

            @Override
            public java.util.Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
                    byte[] checkpoint, long sequence, SourcePosition... positions) {
                throw new UnsupportedOperationException("fixture cannot authorize Floor coverage");
            }

            @Override
            public java.util.Optional<RecoveryPin> activeRecoveryPin() {
                return java.util.Optional.ofNullable(pin.get());
            }
        };
    }

    /** Exact local Catalog baseline fixture; real image/File Object Store/WorkClass publication is exercised. */
    public static void publishTargetFixture(
            SharedRocksDbResources resources,
            com.nereusstream.delay.scheduler.WorkClassExecutionRegistry registry,
            TargetCheckpointRootVerifier.AuthenticatedImage image,
            CheckpointManifestLimits limits,
            Path objectRoot,
            java.util.function.BiConsumer<ShardStore, PublishedRecovery> recover) {
        final var manifest = image.manifest();
        final var catalog = new RecoveryCatalog();
        final var intentStore = new CheckpointUploadIntentStore();
        com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend.ClientHandle nativeSession = null;
        RecoveryCatalogAuthority authority = catalog;
        CheckpointUploadIntentAuthority intents = intentStore;
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        final String prefix = "target-publication-fixture/" + java.util.UUID.randomUUID();
        try {
            if (endpoint != null && !endpoint.isBlank()) {
                try {
                    final String namespace = System.getenv("NEREUS_DELAY_OXIA_NAMESPACE");
                    nativeSession = com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend.connect(
                            endpoint,
                            namespace == null || namespace.isBlank() ? "default" : namespace,
                            "target-image-publication",
                            java.time.Duration.ofSeconds(15),
                            prefix);
                    final var backend = new OxiaSyncCheckpointPublicationBackend(nativeSession, prefix, limits);
                    authority = backend;
                    intents = backend;
                } catch (io.oxia.client.api.exceptions.OxiaException failure) {
                    throw new AssertionError("native publication session", failure);
                }
            }
            final var profile = new com.nereusstream.delay.protocol.ProfileRef(
                    com.nereusstream.delay.protocol.Bytes.utf8("target-checkpoint-object-fixture"),
                    1,
                    manifest.manifestSha256(),
                    com.nereusstream.delay.protocol.ProfileKind.OBJECT_STORE);
            final var owner = new com.nereusstream.delay.protocol.OwnerIdentity(
                    manifest.createdBy().deploymentId(),
                    manifest.createdBy().workerRunId(),
                    manifest.createdBy().ownerEpoch(),
                    manifest.manifestSha256());
            final var sourceIncarnation = java.nio.ByteBuffer.allocate(16)
                    .putLong(manifest.sourceStoreIncarnation().getMostSignificantBits())
                    .putLong(manifest.sourceStoreIncarnation().getLeastSignificantBits())
                    .array();
            final var at = manifest.createdAt();
            final var evidence = new com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence(
                    at.earliestEpochMs(),
                    at.latestEpochMs(),
                    com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence.Source.valueOf(at.source()),
                    at.sourceId(),
                    at.sourceConfigGeneration(),
                    at.sampleSequence(),
                    at.monotonicAnchorNs(),
                    at.sourceEvidenceSha256(),
                    at.sourceKeyVersion(),
                    at.sourceSignature());
            final var pending = new com.nereusstream.delay.protocol.CheckpointUploadIntent(
                    new com.nereusstream.delay.protocol.ShardSubject(manifest.shardId()),
                    manifest.recoveryLineageId(),
                    manifest.checkpointId(),
                    owner,
                    sourceIncarnation,
                    manifest.manifestSha256(),
                    1,
                    null,
                    null,
                    profile,
                    evidence,
                    10_000,
                    com.nereusstream.delay.protocol.CheckpointUploadState.PENDING_UPLOAD,
                    1,
                    null,
                    null);
            intents.create(pending);
            final var initializationAuthority =
                    readerInitializationFixture(authority, pending, image, nativeSession, prefix);
            final var initialization = authority.initializeTargetCatalog(pending, image, initializationAuthority);
            org.junit.jupiter.api.Assertions.assertEquals(1, initialization.catalogGeneration());
            org.junit.jupiter.api.Assertions.assertEquals(
                    1,
                    authority
                            .initializeTargetCatalog(pending, image, initializationAuthority)
                            .catalogGeneration());
            final var exactInitializationAuthority = authority;
            final var deniedInitialization = new IllegalStateException("Catalog genesis permission unavailable");
            org.junit.jupiter.api.Assertions.assertSame(
                    deniedInitialization,
                    org.junit.jupiter.api.Assertions.assertThrows(
                            IllegalStateException.class,
                            () -> exactInitializationAuthority.initializeTargetCatalog(
                                    pending, image, (marker, authenticated) -> {
                                        throw deniedInitialization;
                                    })));
            if (nativeSession != null) {
                final var independent = new OxiaSyncCheckpointPublicationBackend(nativeSession, prefix, limits);
                org.junit.jupiter.api.Assertions.assertArrayEquals(
                        initialization.marker().canonicalBytes(),
                        independent.currentInitialization().orElseThrow().canonicalBytes());
            }
            org.junit.jupiter.api.Assertions.assertTrue(
                    authority.manifest(manifest.checkpointId()).isEmpty());
            org.junit.jupiter.api.Assertions.assertTrue(authority.currentFloor().isEmpty());
            org.junit.jupiter.api.Assertions.assertArrayEquals(
                    initialization.marker().canonicalBytes(),
                    authority.currentInitialization().orElseThrow().canonicalBytes());
            final var coordinator = new CheckpointPublicationCoordinator(resources, intents, limits, authority);
            final var executor = new TargetCheckpointPublicationWorkClassExecutor(registry, coordinator);
            final var adapter = new FilesystemCheckpointUploadAdapter(objectRoot, "fixture", limits);
            final var submitted = executor.submit(pending, image, 1, 1_000, adapter);
            org.junit.jupiter.api.Assertions.assertTrue(submitted.outcome().isEmpty());
            registry.runTurn(new com.nereusstream.delay.scheduler.SchedulerBudget(
                    1, submitted.task().bytes(), 1_000_000));
            final var outcome = submitted.outcome().orElseThrow();
            if (outcome.failure() != null) {
                throw new AssertionError("Target image publication failed", outcome.failure());
            }
            final var published = outcome.published();
            org.junit.jupiter.api.Assertions.assertEquals(
                    com.nereusstream.delay.protocol.CheckpointUploadState.PUBLISHED,
                    published.uploadIntent().state());
            org.junit.jupiter.api.Assertions.assertEquals(
                    2, published.catalogPublication().catalogGeneration());
            final var exactAuthority = authority;
            org.junit.jupiter.api.Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () -> exactAuthority.publishUploadedCheckpoint(published.uploadIntent(), manifest, 1));
            org.junit.jupiter.api.Assertions.assertEquals(
                    2,
                    authority
                            .publishTargetUploadedCheckpoint(published.uploadIntent(), image, 1)
                            .catalogGeneration());
            final Path downloaded = objectRoot.resolveSibling(objectRoot.getFileName() + "-download");
            new FilesystemCheckpointDownloadAdapter(objectRoot, limits)
                    .download(
                            new CheckpointDownloadRequest(
                                    manifest, published.uploadIntent().publishedManifest()),
                            downloaded);
            org.junit.jupiter.api.Assertions.assertEquals(
                    manifest.files().size(),
                    CheckpointFileInventory.collect(downloaded, limits).size());
            final var source = (com.nereusstream.delay.protocol.KafkaSourcePosition) manifest.appliedShardLogPosition();
            final var assignment = new com.nereusstream.delay.ownership.SourceAssignment(
                    manifest.shardId(),
                    com.nereusstream.delay.protocol.Bytes.sha256(manifest.manifestSha256(), new byte[] {1}),
                    1,
                    new com.nereusstream.delay.protocol.KafkaActivationBarrier(
                            manifest.shardId(),
                            source.authenticatedClusterId(),
                            source.nativeTopicUuid(),
                            source.offset() + 2));
            final var leases = nativeSession == null
                    ? new com.nereusstream.delay.ownership.OxiaOwnerLeaseStore(
                            new com.nereusstream.delay.ownership.InMemoryOwnerLeaseStore())
                    : new com.nereusstream.delay.ownership.OxiaOwnerLeaseStore(nativeSession.backend());
            final byte[] recoverySession =
                    nativeSession == null ? manifest.manifestSha256() : nativeSession.sessionIdentity();
            com.nereusstream.delay.ownership.OwnerLease acquiring;
            do {
                acquiring = leases.acquire(
                                assignment,
                                "published-target-recovery",
                                recoverySession,
                                System.currentTimeMillis(),
                                60_000)
                        .orElseThrow();
                if (Long.compareUnsigned(
                                acquiring.ownerEpoch(), manifest.createdBy().ownerEpoch())
                        > 0) {
                    break;
                }
                org.junit.jupiter.api.Assertions.assertTrue(leases.release(acquiring));
            } while (true);
            // This is a test-authorized Floor, not authority to release any UNKNOWN reserve or production protection.
            final var floor = authority.advanceFloor(manifest.checkpointId(), 2, manifest.evidenceCursors());
            final var candidate = new com.nereusstream.delay.protocol.RecoveryCandidateRef(
                    com.nereusstream.delay.protocol.RecoveryCandidateKind.CATALOG_CHECKPOINT,
                    manifest.recoveryLineageId(),
                    manifest.checkpointId(),
                    manifest.manifestSha256(),
                    null);
            final var pin = new RecoveryPin(
                    manifest.checkpointId(),
                    new com.nereusstream.delay.protocol.ShardSubject(manifest.shardId()),
                    new com.nereusstream.delay.protocol.OwnerIdentity(
                            owner.deploymentId(),
                            owner.workerRunId(),
                            acquiring.ownerEpoch(),
                            com.nereusstream.delay.protocol.Bytes.sha256(acquiring.leaseToken())),
                    candidate,
                    floor,
                    floor.catalogGeneration(),
                    recoverySession);
            org.junit.jupiter.api.Assertions.assertEquals(pin, authority.createRecoveryPin(pin));
            final var restoreConfig =
                    ShardStoreConfig.defaults(objectRoot.resolveSibling(objectRoot.getFileName() + "-restore"));
            try (var restoreResources = new SharedRocksDbResources(restoreConfig)) {
                final var restoreRegistry = restorationWorkClasses();
                final var restore = new CheckpointRestoreCoordinator(
                        restoreConfig,
                        manifest.shardId(),
                        restoreResources,
                        new FilesystemCheckpointDownloadAdapter(objectRoot, limits),
                        authority,
                        limits);
                final var handoff = new CheckpointRestoreWorkClassExecutor(restoreRegistry, restore);
                final var attempt = handoff.submit(new CheckpointRestoreWorkClassExecutor.RestoreRequest(
                        new CheckpointDownloadRequest(
                                manifest, published.uploadIntent().publishedManifest()),
                        pin,
                        image.restoreAuthentication(
                                new BoundedReadBudget(8192, 64L << 20, 60_000_000_000L, System::nanoTime))));
                restoreRegistry.runTurn(new com.nereusstream.delay.scheduler.SchedulerBudget(
                        1, attempt.task().bytes(), 1_000_000));
                final var result = attempt.outcome().orElseThrow();
                if (result.failure() != null) {
                    throw new AssertionError("published Target restore failed", result.failure());
                }
                try (var installed = result.restored()) {
                    org.junit.jupiter.api.Assertions.assertNotEquals(
                            manifest.sourceStoreIncarnation(),
                            installed.metadata().storeIncarnationUuid());
                    recover.accept(
                            installed,
                            new PublishedRecovery(authority, pin, leases, acquiring, assignment, restoreRegistry));
                }
                org.junit.jupiter.api.Assertions.assertEquals(0, restoreResources.registeredPhysicalUsageSources());
            }
            org.junit.jupiter.api.Assertions.assertEquals(
                    pin, authority.activeRecoveryPin().orElseThrow());
            authority.releaseRecoveryPin(pin);
            org.junit.jupiter.api.Assertions.assertTrue(
                    authority.activeRecoveryPin().isEmpty());
            if (nativeSession != null) {
                final var reopened = new OxiaSyncCheckpointPublicationBackend(nativeSession, prefix, limits);
                org.junit.jupiter.api.Assertions.assertArrayEquals(
                        manifest.canonicalJsonBytes(),
                        reopened.manifest(manifest.checkpointId()).orElseThrow().canonicalJsonBytes());
                org.junit.jupiter.api.Assertions.assertEquals(
                        published.uploadIntent(),
                        reopened.currentPublishedFor(pending).orElseThrow());
                System.out.println("C5 real Oxia Target PUBLISHED intent/catalog CAS and independent reread; "
                        + ("exact File Object Store image downloaded, remaining permission/bootstrap "
                                + "authority is fixture."));
            }
        } finally {
            if (nativeSession != null) {
                try {
                    nativeSession.close();
                } catch (java.io.IOException failure) {
                    throw new AssertionError("native publication close", failure);
                }
            }
        }
    }

    /** Real capability rows in native mode; eligibility/pin/namespace trust remains a named test fixture. */
    private static CatalogInitializationAuthority readerInitializationFixture(
            RecoveryCatalogAuthority catalog,
            com.nereusstream.delay.protocol.CheckpointUploadIntent pending,
            TargetCheckpointRootVerifier.AuthenticatedImage image,
            com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend.ClientHandle session,
            String prefix) {
        final com.nereusstream.delay.ownership.ProtocolCapabilityAuthority capabilities;
        if (session != null) {
            capabilities = new com.nereusstream.delay.ownership.OxiaSyncProtocolCapabilityBackend(
                    session, prefix + "/capabilities");
        } else {
            capabilities = new com.nereusstream.delay.ownership.ProtocolCapabilityAuthority() {
                private Publication publication;

                @Override
                public Publication publish(
                        com.nereusstream.delay.protocol.ProtocolCapabilityDeclaration value, long revision) {
                    if (revision != (publication == null ? 0 : publication.revision())) {
                        throw new IllegalStateException("capability fixture revision conflict");
                    }
                    publication = new Publication(revision + 1, value);
                    return publication;
                }

                @Override
                public java.util.Optional<Publication> current(String worker) {
                    return java.util.Optional.ofNullable(publication)
                            .filter(value -> value.declaration().workerId().equals(worker));
                }

                @Override
                public boolean withdraw(Publication expected) {
                    throw new UnsupportedOperationException();
                }
            };
        }
        final var artifacts = com.nereusstream.delay.protocol.ArtifactGenerationSet.current(
                1,
                com.nereusstream.delay.protocol.PulsarSourceLock.digest(),
                image.manifest().manifestSha256());
        final byte[] oldSchema = image.manifest().manifestSha256();
        oldSchema[0] ^= 1;
        final var oldArtifacts = com.nereusstream.delay.protocol.ArtifactGenerationSet.current(
                1, com.nereusstream.delay.protocol.PulsarSourceLock.digest(), oldSchema);
        final var reader = "catalog-initialization-reader";
        final var sessionId = session == null ? image.manifest().manifestSha256() : session.sessionIdentity();
        final var tuple = com.nereusstream.delay.protocol.ProtocolTuple.currentSystemMutation();
        final var old = capabilities.publish(
                new com.nereusstream.delay.protocol.ProtocolCapabilityDeclaration(
                        reader,
                        image.manifest().manifestSha256(),
                        java.util.List.of(tuple),
                        oldArtifacts,
                        1,
                        sessionId),
                0);
        final var current = new java.util.concurrent.atomic.AtomicBoolean(true);
        final var closed = new java.util.concurrent.atomic.AtomicInteger();
        final var coordinator =
                new com.nereusstream.delay.ownership.ProtocolActivationAuthorityCoordinator(capabilities);
        final com.nereusstream.delay.ownership.ProtocolCompatibleCatalogInitializationAuthority.ReaderSetProtection
                pins = (initialization, expectedArtifacts, readers) -> {
                    org.junit.jupiter.api.Assertions.assertEquals(artifacts, expectedArtifacts);
                    org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of(reader), readers.workerIds());
                    return fixtureGuard(
                            () -> {
                                if (!current.get()) {
                                    throw new IllegalStateException("Catalog reader protection lost");
                                }
                            },
                            closed::incrementAndGet);
                };
        final var compatible = new com.nereusstream.delay.ownership.ProtocolCompatibleCatalogInitializationAuthority(
                coordinator,
                artifacts,
                java.util.List.of(reader),
                pins,
                (initialization, authenticated) -> fixtureGuard());
        org.junit.jupiter.api.Assertions.assertTrue(org.junit.jupiter.api.Assertions.assertThrows(
                        IllegalStateException.class, () -> catalog.initializeTargetCatalog(pending, image, compatible))
                .getMessage()
                .contains("ArtifactGenerationSet"));
        org.junit.jupiter.api.Assertions.assertTrue(
                catalog.currentInitialization().isEmpty());
        capabilities.publish(
                new com.nereusstream.delay.protocol.ProtocolCapabilityDeclaration(
                        reader, image.manifest().manifestSha256(), java.util.List.of(tuple), artifacts, 2, sessionId),
                old.revision());
        final var missing = new com.nereusstream.delay.ownership.ProtocolCompatibleCatalogInitializationAuthority(
                coordinator,
                artifacts,
                java.util.List.of(reader, "missing-reader"),
                pins,
                (initialization, authenticated) -> fixtureGuard());
        org.junit.jupiter.api.Assertions.assertTrue(org.junit.jupiter.api.Assertions.assertThrows(
                        IllegalStateException.class, () -> catalog.initializeTargetCatalog(pending, image, missing))
                .getMessage()
                .contains("no current protocol capability"));
        final var lost = new com.nereusstream.delay.ownership.ProtocolCompatibleCatalogInitializationAuthority(
                coordinator, artifacts, java.util.List.of(reader), pins, (initialization, authenticated) -> {
                    current.set(false);
                    return fixtureGuard();
                });
        org.junit.jupiter.api.Assertions.assertTrue(org.junit.jupiter.api.Assertions.assertThrows(
                        IllegalStateException.class, () -> catalog.initializeTargetCatalog(pending, image, lost))
                .getMessage()
                .contains("reader protection lost"));
        org.junit.jupiter.api.Assertions.assertEquals(1, closed.get());
        org.junit.jupiter.api.Assertions.assertTrue(
                catalog.currentInitialization().isEmpty());
        current.set(true);
        return compatible;
    }

    private static TargetStoreBackend.CommitGuard fixtureGuard() {
        return fixtureGuard(() -> {}, () -> {});
    }

    private static TargetStoreBackend.CommitGuard fixtureGuard(Runnable current, Runnable cleanup) {
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {
                current.run();
            }

            @Override
            public void close() {
                cleanup.run();
            }
        };
    }

    public record PublishedRecovery(
            RecoveryCatalogAuthority catalog,
            RecoveryPin pin,
            com.nereusstream.delay.ownership.OxiaOwnerLeaseStore leases,
            com.nereusstream.delay.ownership.OwnerLease acquiring,
            com.nereusstream.delay.ownership.SourceAssignment assignment,
            com.nereusstream.delay.scheduler.WorkClassExecutionRegistry workClasses) {}

    private static com.nereusstream.delay.scheduler.WorkClassExecutionRegistry restorationWorkClasses() {
        final var policies = new java.util.EnumMap<
                com.nereusstream.delay.scheduler.WorkClass, com.nereusstream.delay.scheduler.WorkClassPolicy>(
                com.nereusstream.delay.scheduler.WorkClass.class);
        for (var work : com.nereusstream.delay.scheduler.WorkClass.values()) {
            final boolean protectedClass = work != com.nereusstream.delay.scheduler.WorkClass.QUERY
                    && work != com.nereusstream.delay.scheduler.WorkClass.CHECKPOINT;
            policies.put(
                    work,
                    new com.nereusstream.delay.scheduler.WorkClassPolicy(
                            1,
                            1,
                            1_000_000,
                            1,
                            1_000_000,
                            1_000,
                            protectedClass ? 1 : 0,
                            protectedClass ? 1 : 0,
                            work == com.nereusstream.delay.scheduler.WorkClass.LEASE_FENCE));
        }
        return new com.nereusstream.delay.scheduler.WorkClassExecutionRegistry(
                new com.nereusstream.delay.scheduler.WorkClassRuntimeConfig(policies, 100, 100, 16, 2_000_000),
                () -> 0);
    }
}
