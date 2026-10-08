package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.adapter.PulsarJournalResource;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.CredentialUseKind;
import com.nereusstream.delay.protocol.CredentialUseLease;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryContract;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Actual guarded Target Journal append/reopen; Owner/credential/Admission authorities are explicit fixtures. */
public final class PulsarClientArtifactTargetJournalSmoke {
    private PulsarClientArtifactTargetJournalSmoke() {}

    public static void main(final String[] args) throws Exception {
        if (args.length != 3 || !args[2].matches("[a-zA-Z0-9-]+")) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <unique-journal-topic>");
        }
        final String topic = "persistent://public/default/" + args[2];
        final String cluster = PulsarClientArtifactClientBuilder.clusterId();
        final byte[] incarnation = Bytes.sha256(Bytes.utf8(args[2]));
        final long created = 1001;
        final var admin = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();
        final String path = args[1] + "/admin/v2/persistent/public/default/" + args[2];
        create(admin, path, incarnation, created);
        final String destinationPath = path + "-destination";
        create(admin, destinationPath, hash("target-resource"), created);
        try (var client = PulsarClientArtifactClientBuilder.builder(args[0]).build()) {
            final var shard = new ShardId(RouteIncarnation.random(), 0);
            final var resource = new PulsarJournalResource(cluster, incarnation, topic, created, 0);
            final var physical = new CanonicalTargetPartition(BrokerResourceIdentity.pulsar(
                    new PulsarBrokerResourceIdentity(cluster, hash("target-resource"),
                            "persistent://public/default/" + args[2] + "-destination", created)), 0);
            final var channel = channel(shard, physical, 0, 1);
            final var producer = PulsarAttemptJournal.ProducerKey.target(channel, physical);
            final var first = identity(shard, 1);
            final AtomicInteger guardCalls = new AtomicInteger();
            final PulsarClientArtifactAttemptJournal.TargetWriterGuard guard = (s, r) -> {
                require(s.equals(shard) && r.equals(resource), "fixture guard changed Source/resource");
                guardCalls.incrementAndGet();
            };
            final String writer = "target-journal-" + args[2];
            final String subscription = "target-replay-" + args[2];
            final byte[] mappingId;
            try (var live = PulsarClientArtifactAttemptJournal.openTarget(
                    client, shard, resource, writer, subscription, Duration.ofSeconds(15), 100, 1_000_000, guard)) {
                final var mapped = live.journal().appendOrReuseCurrent(producer, first);
                require(mapped.record().mapping().sequenceId() == 0, "first Target sequence differs");
                mappingId = mapped.record().mapping().mappingId();
                live.journal().markOwnershipStarted(mappingId);
                require(live.journal().records().size() == 2, "expected durable MAPPED/OWNERSHIP_STARTED");
            }
            final AtomicInteger sends = new AtomicInteger();
            try (var recovered = PulsarClientArtifactAttemptJournal.openTarget(
                    client, shard, resource, writer, subscription, Duration.ofSeconds(15), 100, 1_000_000, guard)) {
                require(recovered.replayedRecords() == 2, "complete physical replay differs");
                final var restored = recovered.journal().findCurrent(producer, first).orElseThrow();
                require(Arrays.equals(mappingId, restored.mappingId()), "reopen changed exact mapping");
                boolean blocked = false;
                try {
                    recovered.journal().sendAfterOwnershipStarted(restored, ignored -> {
                        sends.incrementAndGet();
                        return CompletableFuture.completedFuture(null);
                    });
                } catch (IllegalStateException expected) {
                    blocked = true;
                }
                require(blocked && sends.get() == 0, "reopened ownership allowed another first SEND");
                final var slot = PulsarAttemptJournal.ProducerKey.target(channel(shard, physical, 1, 1), physical);
                final var other = recovered.journal().appendOrReuseCurrent(slot, identity(shard, 2));
                recovered.journal().retireNotPublished(other.record().mapping().mappingId());
                final var retiredMapping = other.record().mapping();
                final var fencedRetirement = channel(shard, physical, 1, 2);
                final var absent = recovered.journal().targetNotPublishedEvidence(retiredMapping,
                        fencedRetirement.context().evidenceGeneration(), fencedRetirement);
                final var decodedAbsent = com.nereusstream.delay.protocol.PublishEvidence.decode(
                        absent.canonicalBytes());
                require(decodedAbsent.evidenceKind()
                        == com.nereusstream.delay.protocol.PublishEvidenceKind.PULSAR_JOURNAL_ABSENCE,
                        "Target retirement encoded the wrong evidence branch");
                require(recovered.journal().evidenceCursor(retiredMapping.producer(),
                        fencedRetirement.context().evidenceGeneration()).orElseThrow().isTarget(),
                        "Target absence cursor aliased a Lane");
                System.out.println("Target pre-ownership absence encoded: actual durable retirement, "
                        + "independent cursor/Target channel; fencing/retention authority are fixtures");
                final var renewal = PulsarAttemptJournal.ProducerKey.target(channel(shard, physical, 1, 2), physical);
                final var next = recovered.journal().appendOrReuseCurrent(renewal, identity(shard, 3));
                require(next.record().mapping().sequenceId() == 1, "renewal reset the sequence domain");
                require(recovered.journal().records().size() == 5, "guarded Target Journal record count differs");
            }
            boolean incompleteBlocked = false;
            try (var ignored = PulsarClientArtifactAttemptJournal.openTarget(
                    client, shard, resource, writer, subscription, Duration.ofSeconds(15), 1, 1_000_000, guard)) {
                throw new IllegalStateException(
                        "partial replay was exported as complete: " + ignored.replayedRecords());
            } catch (PulsarAttemptJournal.JournalException expected) {
                incompleteBlocked = true;
            }
            require(incompleteBlocked, "small replay budget did not fail closed");
            try (var complete = PulsarClientArtifactAttemptJournal.openTarget(
                    client, shard, resource, writer, subscription, Duration.ofSeconds(15), 100, 1_000_000, guard)) {
                require(complete.replayedRecords() == 5, "failed replay leaked Writer or changed retained records");
                sendBusinessRecord(client, shard, physical, complete.journal());
                verifyAdmissionHistory(client, admin, path, shard, physical, cluster, created);
            }
            System.out.println("Target Journal Broker smoke passed: records=5, reopen=5, "
                    + "renewed-sequence=1, recovered-first-send=0, partial-replay=blocked, guard-calls=" + guardCalls);
        } finally {
            final var destinationDeleted = PulsarClientArtifactAdminHttp.request(
                    admin, destinationPath + "?force=true", "DELETE", "");
            require(destinationDeleted.statusCode() < 300 || destinationDeleted.statusCode() == 404,
                    "owned destination cleanup failed");
            final var deleted = PulsarClientArtifactAdminHttp.request(admin, path + "?force=true", "DELETE", "");
            require(deleted.statusCode() < 300 || deleted.statusCode() == 404, "owned Journal topic cleanup failed");
        }
    }

    /** Actual Target record/SEND/guarded ACK; source Message/Admission/credential/live authorities are fixtures. */
    private static void sendBusinessRecord(
            org.apache.pulsar.client.api.PulsarClient client, ShardId shard, CanonicalTargetPartition physical,
            PulsarAttemptJournal journal) throws Exception {
        final var c = channel(shard, physical, 2, 1);
        final var artifacts = com.nereusstream.delay.protocol.ArtifactGenerationSet.current(
                1, com.nereusstream.delay.protocol.PulsarSourceLock.digest(), hash("send-schema"));
        final var id = DelayMessageId.random(shard);
        final byte[] attempt = hash("business-attempt");
        final byte[] data = Bytes.utf8("target-business-payload");
        final long now = System.currentTimeMillis();
        final var locator = new com.nereusstream.delay.protocol.TargetMessageLocator(
                id, 0, physical.id(), c.context().domain(), c.context().accountingIncarnation(),
                com.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT, null, hash("fixture-schedule-binding"));
        final var destination = c.credentialLease().profile();
        final var capability = new ProfileRef(Bytes.utf8("fixture-capability"), 1, hash("capability"),
                ProfileKind.DELIVERY_CAPABILITY);
        final var metadata = com.nereusstream.delay.protocol.AdapterMetadata.pulsar(
                new com.nereusstream.delay.protocol.PulsarMetadata(null, null, null, java.util.List.of()));
        final var reserved = new com.nereusstream.delay.protocol.ReservedPublishMetadata(
                shard.routeIncarnation(), shard.unsignedPartition(), id, 0, attempt, destination.semanticHash(),
                capability.semanticHash(), now, com.nereusstream.delay.protocol.DeliveryMode.MANAGED);
        final var publication = new com.nereusstream.delay.protocol.TargetOrdinaryPublicationBinding(
                locator, physical, c, attempt, 1, hash("claim"), hash("claimed-message"), 1,
                destination, capability, data.length, Bytes.sha256(data), metadata, reserved,
                now, now + 60_000, now, null, artifacts.setDigest());
        final var obligation = new com.nereusstream.delay.runtime.AttemptObligationRef(
                attempt, 0, com.nereusstream.delay.runtime.AttemptLedgerState.PUBLISHING,
                com.nereusstream.delay.store.KeyCodec.inflight((byte) 2, 1, attempt));
        final var source = new KafkaSourcePosition(shard, "fixture-source", UUID.randomUUID(), 10, null, now);
        final var message = new com.nereusstream.delay.runtime.TargetMessageRecord(
                locator, 2, now, now + 60_000, now, com.nereusstream.delay.protocol.NativeDeliveryPolicy.FORBID,
                source, data, null, new com.nereusstream.delay.runtime.TargetGenerationRuntimeIndex(
                        0, com.nereusstream.delay.runtime.GenerationAggregateState.PUBLISHING,
                        com.nereusstream.delay.runtime.CurrentSendWorkKind.PUBLISHING, null, null, attempt,
                        java.util.List.of(obligation), 1, 0, false, 2));
        final var payload = com.nereusstream.delay.protocol.PayloadForPublish.inline(data);
        final var mapping = journal.appendOrReuseCurrent(PulsarAttemptJournal.ProducerKey.target(c, physical),
                com.nereusstream.delay.adapter.PulsarPreparedRecordFactory.targetJournalIdentity(
                        publication, message, payload, source)).record().mapping();
        final var record = com.nereusstream.delay.adapter.PulsarPreparedRecordFactory.targetManaged(
                publication, message, payload, com.nereusstream.delay.protocol.ResolvedPayload.of(data),
                mapping, artifacts);
        journal.markOwnershipStarted(mapping);
        final var p = physical.resource().pulsar();
        final String producerName = new String(c.context().producerIdentity(), java.nio.charset.StandardCharsets.UTF_8);
        try (var producer = PulsarClientArtifactProducerFactory.create(client, p.authenticatedClusterId(),
                p.resourceIncarnation(), p.physicalTopic(), p.physicalTopicCreationTimestamp(), producerName);
             var transport = new PulsarClientArtifactDestinationTransport(producer, p.authenticatedClusterId(),
                     p.resourceIncarnation(), p.physicalTopic(), p.physicalTopicCreationTimestamp(), 0,
                     Bytes.sha256(c.context().producerIdentity()))) {
            final var gates = new AtomicInteger();
            final var result = journal.sendAfterOwnershipStarted(mapping, ignored ->
                    transport.publishPreparedRecord(record, artifacts, (r, a) -> {
                        gates.incrementAndGet();
                        return null;
                    })).toCompletableFuture().get(15, TimeUnit.SECONDS);
            require(result.disposition()
                    == com.nereusstream.delay.adapter.DestinationPublishResult.Disposition.PUBLISHED,
                    "guarded Target business SEND did not produce PUBLISHED: " + result);
            final var evidence = com.nereusstream.delay.protocol.PublishEvidence.decode(result.evidence());
            evidence.requireOrdinaryTargetPublishedBinding(publication);
            com.nereusstream.delay.adapter.PulsarSendAckEvidence.requireRecordBinding(evidence, record, artifacts);
            journal.markPublished(mapping);
            final long evidenceGeneration = c.context().evidenceGeneration();
            final var journalEvidence = journal.publishedEvidence(mapping, evidenceGeneration, evidence.evidenceId());
            final var cursor = journal.evidenceCursor(mapping.producer(), evidenceGeneration).orElseThrow();
            require(cursor.isTarget(), "Target Journal cursor aliased the Lane namespace");
            require(Arrays.equals(cursor.canonicalBytes(),
                    com.nereusstream.delay.protocol.EvidenceCursor.decode(cursor.canonicalBytes()).canonicalBytes()),
                    "Target Journal cursor roundtrip changed");
            journalEvidence.requireOrdinaryTargetPublishedBinding(publication);
            journal.requireTargetPublishedEvidence(mapping, publication, source, cursor, journalEvidence);
            require(gates.get() == 1, "business SEND ownership gate count differs");
            require(journal.state(mapping.mappingId()) == PulsarAttemptJournal.AttemptState.PUBLISHED,
                    "business Journal publication was not durable");
            System.out.println("Target business SEND passed: payload bytes=" + data.length
                    + ", sequence=" + mapping.sequenceId() + ", guarded generation-2 ACK=true, gate=1");
        }
    }

    /** Actual P1 I/O with an explicitly constructed retained-reference fixture, not a Source Store receipt. */
    private static void verifyAdmissionHistory(
            org.apache.pulsar.client.api.PulsarClient client, HttpClient admin, String basePath,
            ShardId shard, CanonicalTargetPartition physical, String cluster, long created) throws Exception {
        final String sourcePath = basePath + "-admission-source";
        final String topic = "persistent://public/default/" + sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
        final byte[] incarnation = hash("history-source-resource");
        create(admin, sourcePath, incarnation, created);
        final var guard = new org.apache.pulsar.client.api.TopicResourceGuard(cluster, incarnation, created);
        try (var active = PulsarClientArtifactSourceConsumerFactory.create(client, guard, topic,
                "target-history-active-" + UUID.randomUUID())) {
            final var owner = new com.nereusstream.delay.protocol.OwnerIdentity(
                    Arrays.copyOf(hash("history-deployment"), 16), Arrays.copyOf(hash("history-run"), 16),
                    1, hash("history-owner"));
            final var id = DelayMessageId.random(shard);
            final var locator = new com.nereusstream.delay.protocol.TargetMessageLocator(id, 0, physical.id(),
                    new TargetKeyCodec.Domain(0, 1), Arrays.copyOf(hash("history-account"), 16),
                    com.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT, null, hash("history-binding"));
            final byte[] claim = hash("history-claim");
            final byte[] attempt = com.nereusstream.delay.protocol.SystemMutation.computePublishAttemptLogicalIdentity(
                    claim, id, 0, 1);
            final var obligation = new com.nereusstream.delay.runtime.AttemptObligationRef(attempt, 0,
                    com.nereusstream.delay.runtime.AttemptLedgerState.PUBLISHING,
                    com.nereusstream.delay.store.KeyCodec.inflight((byte) 2, owner.ownerEpoch(), attempt));
            final long now = System.currentTimeMillis();
            final var time = new TrustedUtcIntervalEvidence(now, now + 1,
                    TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, hash("history-clock"),
                    1, 1, 1, hash("history-clock-evidence"), 0, null);
            final long[] reserve = new long[com.nereusstream.delay.protocol.CapacityDimension.COUNT];
            reserve[com.nereusstream.delay.protocol.CapacityDimension.RESULT_BYTES.wireValue() - 1] = 128;
            final var commitment = new com.nereusstream.delay.protocol.CapacityVector(reserve);
            final var body = new com.nereusstream.delay.protocol.TargetPublishAdmissionBody(shard, now + 60_000,
                    owner, Arrays.copyOf(hash("history-store"), 16), claim, locator, 1, attempt, obligation,
                    128, commitment, com.nereusstream.delay.protocol.CapacityVector.empty(), time);
            final var keys = java.security.KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final var mutation = com.nereusstream.delay.protocol.SystemMutation.signed(shard,
                    com.nereusstream.delay.protocol.SystemMutationType.TARGET_PUBLISH_ADMISSION, now + 60_000,
                    attempt, body.canonicalBytes(), com.nereusstream.delay.protocol.AuthorIdentity.owner(
                            owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch(), owner.leaseFencingDigest())
                            .canonicalBytes(), 1, keys.getPrivate());
            final com.nereusstream.delay.protocol.PulsarSourcePosition source;
            try (var producer = PulsarClientArtifactProducerFactory.create(client, cluster, incarnation,
                    topic, created, "target-history-source-writer");
                    var appender = new PulsarClientArtifactShardLogMutationAppender(producer, active, shard, cluster,
                            incarnation, topic, created, Duration.ofSeconds(15))) {
                final var appended = appender.append(mutation);
                require(appended.disposition()
                        == com.nereusstream.delay.ownership.ShardLogMutationAppender.AppendDisposition.PERSISTED,
                        "P1 history fixture Admission append was not guarded/persisted");
                source = (com.nereusstream.delay.protocol.PulsarSourcePosition) appended.sourcePosition();
            }
            final var stamp = new com.nereusstream.delay.protocol.TargetQuotaMutation(
                    1, source, Bytes.sha256(mutation.canonicalEnvelope()));
            final var budget = com.nereusstream.delay.protocol.TargetQuotaAttemptBudget.admit(locator,
                    hash("history-tenant"), attempt, mutation.mutationHash(),
                    new com.nereusstream.delay.protocol.TargetQuotaAccounting(hash("history-schema"), 0, 0, 0, 1),
                    128, commitment, com.nereusstream.delay.protocol.CapacityVector.empty(), stamp,
                    Arrays.copyOf(hash("history-lineage"), 16));
            final var first = com.nereusstream.delay.runtime.SystemMutationResult.from(mutation,
                    com.nereusstream.delay.runtime.ApplyStatus.APPLIED, com.nereusstream.delay.protocol.StableCode.OK,
                    source.canonicalBytes());
            // The Reference constructor is private so production cannot substitute a DTO for Store proof.
            // This I/O smoke uses reflection only to build its declared fixture; Store joins have separate real tests.
            final var constructor = com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Reference.class
                    .getDeclaredConstructor(com.nereusstream.delay.protocol.TargetQuotaAttemptBudget.class,
                            com.nereusstream.delay.runtime.SystemMutationResult.class);
            constructor.setAccessible(true);
            final var reference = constructor.newInstance(budget, first);
            final var guards = new AtomicInteger();
            final var found = PulsarClientArtifactTargetAdmissionHistory.read(client, guard, reference,
                    Duration.ofSeconds(15), () -> guards.incrementAndGet());
            require(Arrays.equals(mutation.encodeFrame(), found.encodeFrame()), "P1 history changed original bytes");
            require(guards.get() >= 4, "P1 history omitted authority/lifetime checks");
            final var activeMessage = active.receive(5, TimeUnit.SECONDS);
            require(activeMessage != null && Arrays.equals(activeMessage.getData(), mutation.encodeFrame()),
                    "owned history seek changed the active source subscription");
            final var subscriptions = PulsarClientArtifactAdminHttp.request(admin, sourcePath + "/subscriptions",
                    "GET", "");
            require(subscriptions.statusCode() == 200
                    && !subscriptions.body().contains("nereus-target-admission-history-"),
                    "P1 history left a durable or unclosed query subscription");
            System.out.println("P1 exact Target Admission history passed: guarded append/receive/source, "
                    + "inclusive non-durable seek, exact envelope, active source unchanged, cursor closed; "
                    + "retained reference/Owner/time/retention are fixtures, guard calls=" + guards.get());
        } finally {
            final var deleted = PulsarClientArtifactAdminHttp.request(admin, sourcePath + "?force=true", "DELETE", "");
            require(deleted.statusCode() < 300 || deleted.statusCode() == 404, "owned history source cleanup failed");
        }
    }

    private static PulsarAttemptJournal.CurrentAttemptIdentity identity(ShardId shard, int id) {
        return new PulsarAttemptJournal.CurrentAttemptIdentity(DelayMessageId.random(shard), 0,
                hash("attempt-" + id), hash("prepared-" + id), hash("template-" + id),
                DeliveryContract.NEREUS_MANAGED_NOT_BEFORE,
                new KafkaSourcePosition(shard, "fixture-source", UUID.randomUUID(), id, null, 1000).canonicalBytes(),
                hash("artifact"));
    }

    private static TargetChannelIdentity channel(ShardId shard, CanonicalTargetPartition physical, int slot, long gen) {
        final var context = new TargetChannelIdentity.Context(shard, physical.id(),
                new TargetKeyCodec.Domain(0, 1), Arrays.copyOf(hash("account"), 16), hash("dispatch"),
                hash("controls"), ChannelKind.PULSAR_DEDUP_PRODUCER, slot, gen, 1L, hash("attestation"));
        return new TargetChannelIdentity(context, new CredentialUseLease(
                new ProfileRef(Bytes.utf8("fixture-destination"), 1, hash("profile"), ProfileKind.DESTINATION),
                CredentialUseKind.DESTINATION_CHANNEL, context.credentialHolderScope(), 1, hash("binding"),
                hash("fingerprint"), new TrustedUtcIntervalEvidence(1000, 1001,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("fixture-clock"),
                        1, 1, 1, hash("clock"), 0, null), 100_000, 1));
    }

    private static void create(HttpClient admin, String path, byte[] incarnation, long created) throws Exception {
        final String body = "{\"nereus.resource.guard.version\":\"1\","
                + "\"nereus.resource.incarnation\":\""
                + java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(incarnation)
                + "\",\"nereus.resource.created-at\":\"" + created + "\"}";
        for (int attempt = 0; attempt < 40; attempt++) {
            final var response = PulsarClientArtifactAdminHttp.request(admin, path, "PUT", body);
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return;
            }
            require(response.statusCode() == 409 || response.statusCode() == 412 || response.statusCode() == 503,
                    "guarded Target Journal creation failed: " + response.statusCode() + " " + response.body());
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("Target Journal creation did not converge");
    }

    private static byte[] hash(String value) { return Bytes.sha256(Bytes.utf8(value)); }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
