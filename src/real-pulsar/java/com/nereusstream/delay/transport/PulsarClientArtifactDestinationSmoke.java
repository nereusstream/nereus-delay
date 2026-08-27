package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.DestinationPublishRequest;
import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.adapter.PinnedPulsarDestinationAdapter;
import com.nereusstream.delay.adapter.PulsarDestinationRequest;
import com.nereusstream.delay.adapter.PulsarSendAckEvidence;
import com.nereusstream.delay.adapter.PulsarTargetResource;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.EvidenceVerificationStatus;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishEvidenceKind;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.GuardedMessageId;
import org.apache.pulsar.client.api.GuardedSendSuccessEvidence;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;
import org.apache.pulsar.client.api.TypedMessageBuilder;

/** Real-service smoke for source-bound Pulsar destination publication evidence. */
public final class PulsarClientArtifactDestinationSmoke {
    private static final String CLUSTER = "standalone";
    private static final byte[] INCARNATION = digest(17);
    private static final long CREATION_TIMESTAMP = 1001L;

    private PulsarClientArtifactDestinationSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <topic>");
        }
        final String serviceUrl = arguments[0];
        final String adminUrl = arguments[1];
        final String topic = arguments[2];
        final String physicalTopic = "persistent://public/default/" + topic;
        final HttpClient admin = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        final String freshProcessPhase = System.getenv("NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS_PHASE");
        if (!"READ".equals(freshProcessPhase)) {
            createTopic(admin, adminUrl, topic, INCARNATION, CREATION_TIMESTAMP);
        }

        try {
            if ("WRITE".equals(freshProcessPhase)) {
                writeFreshProcessResponseLoss(serviceUrl, topic, physicalTopic);
                return;
            }
            if ("READ".equals(freshProcessPhase)) {
                readFreshProcessResponseLoss(serviceUrl, topic, physicalTopic);
                return;
            }
            try (PulsarClient client =
                    PulsarClient.builder().serviceUrl(serviceUrl).build()) {
                final String producerName = "nereus-delay-p1-destination-" + topic;
                final byte[] producerNameHash = Bytes.sha256(Bytes.utf8(producerName));
                final boolean responseLoss = hasResponseLoss();
                final AtomicReference<GuardedMessageId> responseLostMessage = new AtomicReference<>();
                final AtomicBoolean responseEvidenceResolved = new AtomicBoolean();
                final Producer<byte[]> producer = PulsarClientArtifactProducerFactory.create(
                        client, CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, producerName);
                final Producer<byte[]> transportProducer =
                        responseLoss ? responseLossProducer(producer, responseLostMessage) : producer;
                final PulsarClientArtifactDestinationTransport.PublishEvidenceProvider evidenceProvider = responseLoss
                        ? (request, preparedHash, failure) -> resolveResponseLoss(
                                request, preparedHash, producerNameHash, responseLostMessage, responseEvidenceResolved)
                        : null;
                try (PulsarClientArtifactDestinationTransport transport = new PulsarClientArtifactDestinationTransport(
                        transportProducer,
                        CLUSTER,
                        INCARNATION,
                        physicalTopic,
                        CREATION_TIMESTAMP,
                        0,
                        producerNameHash,
                        evidenceProvider)) {
                    try (PinnedPulsarDestinationAdapter adapter = new PinnedPulsarDestinationAdapter(
                            new PulsarTargetResource(CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, 0),
                            transport)) {
                        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
                        final DestinationPublishRequest request = request(shard, topic);
                        final PulsarSourcePosition source = new PulsarSourcePosition(
                                shard,
                                digest(19),
                                "persistent://public/default/source-" + topic,
                                7,
                                11,
                                0,
                                1,
                                PulsarSourcePosition.EntryKind.NON_BATCH,
                                1_002);
                        final byte[] preparedHash = Bytes.sha256(Bytes.utf8("prepared-" + topic));
                        final DestinationPublishResult result = adapter.publish(request, source, preparedHash)
                                .toCompletableFuture()
                                .get(20, TimeUnit.SECONDS);
                        requireTypedPublished(result, request, "source-bound Pulsar destination publish");
                        requirePayload(client, physicalTopic, request.payload());
                        if (responseLoss) {
                            if (!responseEvidenceResolved.get()) {
                                throw new IllegalStateException(
                                        "Pulsar response-loss provider did not resolve evidence");
                            }
                            System.out.println("Pulsar committed response-loss smoke passed: real SEND persisted the "
                                    + "exact payload, the local response was discarded, and typed PULSAR_SEND_ACK "
                                    + "evidence resolved PUBLISHED");
                            return;
                        }
                        final PublishEvidence evidence = PublishEvidence.decode(result.evidence());
                        System.out.println("Pulsar destination typed-evidence smoke passed: topic=" + physicalTopic
                                + ", ledger=" + branchNumber(evidence, 3) + ", entry=" + branchNumber(evidence, 4)
                                + ", batchIndex=" + branchNumber(evidence, 5) + ", sequence="
                                + branchNumber(evidence, 8)
                                + ", brokerPersistenceTime=" + result.brokerPersistenceTimeEpochMs());
                    }
                }
            }
        } finally {
            if (!"WRITE".equals(freshProcessPhase)) {
                deleteTopicIfPresent(admin, adminUrl, topic);
            }
        }
    }

    /** Persists the real committed SEND evidence before the response-loss JVM exits. */
    private static void writeFreshProcessResponseLoss(
            final String serviceUrl, final String topic, final String physicalTopic) throws Exception {
        final String statePath =
                required("NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS_STATE_DUMP_DIR") + "/before-process-crash.json";
        final String producerName = "nereus-delay-p1-destination-" + topic;
        final byte[] producerNameHash = Bytes.sha256(Bytes.utf8(producerName));
        final AtomicReference<GuardedMessageId> responseLostMessage = new AtomicReference<>();
        final AtomicBoolean responseEvidenceResolved = new AtomicBoolean();
        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build()) {
            final Producer<byte[]> producer = PulsarClientArtifactProducerFactory.create(
                    client, CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, producerName);
            final Producer<byte[]> transportProducer = responseLossProducer(producer, responseLostMessage);
            final PulsarClientArtifactDestinationTransport.PublishEvidenceProvider evidenceProvider =
                    (request, preparedHash, failure) -> resolveResponseLoss(
                            request, preparedHash, producerNameHash, responseLostMessage, responseEvidenceResolved);
            try (PulsarClientArtifactDestinationTransport transport = new PulsarClientArtifactDestinationTransport(
                            transportProducer,
                            CLUSTER,
                            INCARNATION,
                            physicalTopic,
                            CREATION_TIMESTAMP,
                            0,
                            producerNameHash,
                            evidenceProvider);
                    PinnedPulsarDestinationAdapter adapter = new PinnedPulsarDestinationAdapter(
                            new PulsarTargetResource(CLUSTER, INCARNATION, physicalTopic, CREATION_TIMESTAMP, 0),
                            transport)) {
                final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
                final DestinationPublishRequest request = request(shard, topic);
                final PulsarSourcePosition source = new PulsarSourcePosition(
                        shard,
                        digest(19),
                        "persistent://public/default/source-" + topic,
                        7,
                        11,
                        0,
                        1,
                        PulsarSourcePosition.EntryKind.NON_BATCH,
                        1_002);
                final byte[] preparedHash = Bytes.sha256(Bytes.utf8("prepared-" + topic));
                final DestinationPublishResult result = adapter.publish(request, source, preparedHash)
                        .toCompletableFuture()
                        .get(20, TimeUnit.SECONDS);
                requireTypedPublished(result, request, "fresh-process Pulsar destination response-loss publish");
                final GuardedMessageId messageId = responseLostMessage.get();
                if (!responseEvidenceResolved.get()
                        || messageId == null
                        || !(messageId instanceof MessageIdAdv advanced)) {
                    throw new IllegalStateException("fresh-process response-loss SEND evidence was not captured");
                }
                final GuardedSendSuccessEvidence evidence = messageId.responseEvidence();
                final int rawBatchIndex = advanced.getBatchIndex();
                final int normalizedBatchIndex = rawBatchIndex < 0 ? 0 : rawBatchIndex;
                writeForcedJson(
                        Path.of(statePath),
                        json(
                                "schema", "nereus-delay-chaos-durable-state-dump",
                                "cell", "pulsar-destination-response-loss",
                                "phase", "DESTINATION_RESPONSE_LOSS_READY",
                                "process_pid",
                                        Long.toString(ProcessHandle.current().pid()),
                                "physical_topic", physicalTopic,
                                "authenticated_cluster", CLUSTER,
                                "resource_incarnation_base64", encode(INCARNATION),
                                "topic_creation_timestamp", Long.toString(CREATION_TIMESTAMP),
                                "partition", "0",
                                "lane_id_base64", encode(request.laneId().bytes()),
                                "lane_incarnation_base64", encode(request.laneIncarnation()),
                                "delay_message_id_base64",
                                        encode(request.delayMessageId().bytes()),
                                "generation", Integer.toString(request.generation()),
                                "publish_attempt_id_base64", encode(request.publishAttemptId()),
                                "action_at", Long.toString(request.actionAtEpochMs()),
                                "deliver_at", Long.toString(request.deliverAtEpochMs()),
                                "payload_base64", encode(request.payload()),
                                "adapter_metadata_base64", encode(request.adapterMetadata()),
                                "prepared_hash_base64", encode(preparedHash),
                                "producer_name_hash_base64", encode(producerNameHash),
                                "protocol_version", Integer.toString(evidence.protocolVersion()),
                                "connection_generation", Long.toString(evidence.connectionGeneration()),
                                "producer_id", Long.toString(evidence.producerId()),
                                "sequence_id", Long.toString(evidence.sequenceId()),
                                "ledger_id", Long.toString(evidence.ledgerId()),
                                "entry_id", Long.toString(evidence.entryId()),
                                "batch_index", Integer.toString(normalizedBatchIndex),
                                "broker_entry_timestamp", Long.toString(evidence.brokerEntryTimestamp()),
                                "send_command_sha256_base64", encode(evidence.sendCommandSha256()),
                                "authenticated_response_sha256_base64",
                                        encode(evidence.authenticatedResponseCommandSha256()),
                                "attestation_guard_version",
                                        Integer.toString(evidence.attestation().guardVersion()),
                                "attestation_cluster", evidence.attestation().authenticatedClusterId(),
                                "attestation_incarnation_base64",
                                        encode(evidence.attestation().resourceIncarnation()),
                                "attestation_creation_timestamp",
                                        Long.toString(evidence.attestation().topicCreationTimestamp()),
                                "attestation_topic", evidence.attestation().physicalTopic(),
                                "attestation_partition",
                                        Integer.toString(evidence.attestation().partition()),
                                "physical_send_count", "1",
                                "send_committed", "true",
                                "response_discarded", "true",
                                "durable_broker_read", "true",
                                "dump_forced", "true"));
                System.out.println("Pulsar destination committed response-loss fresh-process WRITE passed: "
                        + "real SEND committed once, response discarded=true, typed evidence durably dumped");
            }
        }
    }

    /** Reopens only the broker evidence and payload in a new JVM; it never sends again. */
    private static void readFreshProcessResponseLoss(
            final String serviceUrl, final String topic, final String physicalTopic) throws Exception {
        final String state = Files.readString(Path.of(
                required("NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS_STATE_DUMP_DIR"), "before-process-crash.json"));
        if (!"nereus-delay-chaos-durable-state-dump".equals(field(state, "schema"))
                || !"pulsar-destination-response-loss".equals(field(state, "cell"))
                || !"DESTINATION_RESPONSE_LOSS_READY".equals(field(state, "phase"))) {
            throw new IllegalStateException("invalid Pulsar destination response-loss pre-process dump");
        }
        if (!physicalTopic.equals(field(state, "physical_topic")) || !topic.equals(topicFromPhysical(physicalTopic))) {
            throw new IllegalStateException("Pulsar destination response-loss topic identity changed");
        }
        final PulsarDestinationRequest request = new PulsarDestinationRequest(
                field(state, "authenticated_cluster"),
                decode(field(state, "resource_incarnation_base64")),
                physicalTopic,
                Long.parseLong(field(state, "topic_creation_timestamp")),
                Integer.parseInt(field(state, "partition")),
                new DestinationLaneId(decode(field(state, "lane_id_base64"))),
                decode(field(state, "lane_incarnation_base64")),
                new DelayMessageId(decode(field(state, "delay_message_id_base64"))),
                Integer.parseInt(field(state, "generation")),
                decode(field(state, "publish_attempt_id_base64")),
                Long.parseLong(field(state, "action_at")),
                Long.parseLong(field(state, "deliver_at")),
                decode(field(state, "payload_base64")),
                decode(field(state, "adapter_metadata_base64")));
        final TopicResourceGuardAttestation attestation = new TopicResourceGuardAttestation(
                Integer.parseInt(field(state, "attestation_guard_version")),
                field(state, "attestation_cluster"),
                decode(field(state, "attestation_incarnation_base64")),
                Long.parseLong(field(state, "attestation_creation_timestamp")),
                field(state, "attestation_topic"),
                Integer.parseInt(field(state, "attestation_partition")));
        final GuardedSendSuccessEvidence responseEvidence = new GuardedSendSuccessEvidence(
                Integer.parseInt(field(state, "protocol_version")),
                Long.parseLong(field(state, "connection_generation")),
                Long.parseLong(field(state, "producer_id")),
                Long.parseLong(field(state, "sequence_id")),
                attestation,
                Long.parseLong(field(state, "ledger_id")),
                Long.parseLong(field(state, "entry_id")),
                Long.parseLong(field(state, "broker_entry_timestamp")),
                decode(field(state, "send_command_sha256_base64")),
                decode(field(state, "authenticated_response_sha256_base64")));
        final PublishEvidence typed = PulsarSendAckEvidence.published(
                request,
                decode(field(state, "prepared_hash_base64")),
                decode(field(state, "producer_name_hash_base64")),
                responseEvidence.ledgerId(),
                responseEvidence.entryId(),
                Integer.parseInt(field(state, "batch_index")),
                responseEvidence.brokerEntryTimestamp(),
                responseEvidence.sequenceId(),
                responseEvidence.authenticatedResponseCommandSha256());
        PulsarSendAckEvidence.requireExactBinding(
                typed,
                request,
                decode(field(state, "prepared_hash_base64")),
                decode(field(state, "producer_name_hash_base64")),
                responseEvidence.brokerEntryTimestamp());
        if (typed.evidenceKind() != PublishEvidenceKind.PULSAR_SEND_ACK
                || typed.verificationStatus() != EvidenceVerificationStatus.VERIFIED_PUBLISHED) {
            throw new IllegalStateException("fresh-process Pulsar SEND evidence did not verify as PUBLISHED");
        }

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build()) {
            final int duplicateCount = requireExactlyOnePayload(client, physicalTopic, request.payload());
            if (duplicateCount != 0) {
                throw new IllegalStateException(
                        "fresh-process response-loss recovery observed duplicate payloads: " + duplicateCount);
            }
            writeForcedJson(
                    Path.of(
                            required("NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS_STATE_DUMP_DIR"),
                            "after-fresh-process.json"),
                    json(
                            "schema", "nereus-delay-chaos-durable-state-dump",
                            "cell", "pulsar-destination-response-loss",
                            "phase", "RECOVERED_AFTER_FRESH_PROCESS",
                            "process_pid", Long.toString(ProcessHandle.current().pid()),
                            "physical_topic", physicalTopic,
                            "authenticated_cluster", field(state, "authenticated_cluster"),
                            "resource_incarnation_base64", field(state, "resource_incarnation_base64"),
                            "topic_creation_timestamp", field(state, "topic_creation_timestamp"),
                            "partition", field(state, "partition"),
                            "publish_attempt_id_base64", field(state, "publish_attempt_id_base64"),
                            "prepared_hash_base64", field(state, "prepared_hash_base64"),
                            "payload_base64", field(state, "payload_base64"),
                            "ledger_id", Long.toString(responseEvidence.ledgerId()),
                            "entry_id", Long.toString(responseEvidence.entryId()),
                            "sequence_id", Long.toString(responseEvidence.sequenceId()),
                            "broker_entry_timestamp", Long.toString(responseEvidence.brokerEntryTimestamp()),
                            "authenticated_response_sha256_base64",
                                    field(state, "authenticated_response_sha256_base64"),
                            "physical_send_count", field(state, "physical_send_count"),
                            "payload_readback_exact", "true",
                            "duplicate_payload_count", Integer.toString(duplicateCount),
                            "evidence_verified", "true",
                            "durable_broker_read", "true",
                            "dump_forced", "true"));
            System.out.println("Pulsar destination committed response-loss fresh-process READ passed: new JVM="
                    + "true, typed PULSAR_SEND_ACK revalidated=true, exact payload count=1, duplicate count=0");
        }
    }

    private static int requireExactlyOnePayload(
            final PulsarClient client, final String physicalTopic, final byte[] expectedPayload) throws Exception {
        final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP);
        final GuardedConsumer<byte[]> guarded = PulsarClientArtifactSourceConsumerFactory.create(
                client, guard, physicalTopic, "nereus-delay-p1-destination-fresh-" + physicalTopic.hashCode());
        try {
            final Message<byte[]> message = guarded.receive(15, TimeUnit.SECONDS);
            if (message == null || !Arrays.equals(expectedPayload, message.getValue())) {
                throw new IllegalStateException("fresh-process destination payload was not read back exactly");
            }
            guarded.acknowledge(message);
            return guarded.receive(2, TimeUnit.SECONDS) == null ? 0 : 1;
        } finally {
            guarded.close();
        }
    }

    private static String topicFromPhysical(final String physicalTopic) {
        final String prefix = "persistent://public/default/";
        if (!physicalTopic.startsWith(prefix)) {
            throw new IllegalArgumentException("unexpected Pulsar physical topic: " + physicalTopic);
        }
        return physicalTopic.substring(prefix.length());
    }

    private static Path statePath(final String fileName) {
        return Path.of(required("NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS_STATE_DUMP_DIR"), fileName)
                .toAbsolutePath()
                .normalize();
    }

    private static String required(final String name) {
        final String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private static String json(final String... fields) {
        if ((fields.length & 1) != 0) {
            throw new IllegalArgumentException("JSON fields must be pairs");
        }
        final StringBuilder result = new StringBuilder("{");
        for (int index = 0; index < fields.length; index += 2) {
            if (index > 0) {
                result.append(',');
            }
            final String value = fields[index + 1];
            result.append('"').append(jsonEscape(fields[index])).append("\":");
            if ("true".equals(value) || "false".equals(value)) {
                result.append(value);
            } else {
                result.append('"').append(jsonEscape(value)).append('"');
            }
        }
        return result.append('}').append('\n').toString();
    }

    private static String field(final String json, final String name) {
        final Matcher matcher = Pattern.compile(
                        "\\\"" + Pattern.quote(name) + "\\\"\\s*:\\s*(?:\\\"([^\\\"]*)\\\"|([^,}\\s]+))")
                .matcher(json);
        if (!matcher.find()) {
            throw new IllegalStateException("missing Pulsar response-loss state field: " + name);
        }
        return matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
    }

    private static String jsonEscape(final String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String encode(final byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static byte[] decode(final String value) {
        return Base64.getUrlDecoder().decode(value);
    }

    private static void writeForcedJson(final Path path, final String json) throws Exception {
        final Path parent = path.getParent();
        Files.createDirectories(parent);
        final Path temporary = Files.createTempFile(parent, path.getFileName().toString(), ".tmp");
        final ByteBuffer bytes = ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8));
        try {
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                while (bytes.hasRemaining()) {
                    channel.write(bytes);
                }
                channel.force(true);
            }
            try {
                Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (java.nio.file.AtomicMoveNotSupportedException unsupported) {
                Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING);
            }
            try (FileChannel directory = FileChannel.open(parent, StandardOpenOption.READ)) {
                directory.force(true);
            } catch (UnsupportedOperationException | java.nio.file.FileSystemException ignored) {
                // The file itself was fsync-forced; directory fsync is not portable on every CI filesystem.
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static boolean hasResponseLoss() {
        return "1".equals(System.getenv("NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS"));
    }

    private static Optional<PulsarClientArtifactDestinationTransport.ResolvedPublish> resolveResponseLoss(
            final PulsarDestinationRequest request,
            final byte[] preparedPublishHash,
            final byte[] producerNameHash,
            final AtomicReference<GuardedMessageId> responseLostMessage,
            final AtomicBoolean responseEvidenceResolved) {
        final GuardedMessageId messageId = responseLostMessage.get();
        if (messageId == null) {
            return Optional.empty();
        }
        final TopicResourceGuard expectedGuard = new TopicResourceGuard(
                request.authenticatedClusterId(),
                request.resourceIncarnation(),
                request.physicalTopicCreationTimestamp());
        if (!expectedGuard.equals(messageId.resourceGuard())
                || !request.physicalTopic().equals(messageId.physicalTopic())
                || request.partition() != messageId.partition()
                || !(messageId instanceof MessageIdAdv advanced)
                || advanced.getLedgerId() < 0
                || advanced.getEntryId() < 0
                || advanced.getPartitionIndex() != request.partition()) {
            return Optional.empty();
        }
        final GuardedSendSuccessEvidence evidence = messageId.responseEvidence();
        final TopicResourceGuardAttestation expectedAttestation =
                new TopicResourceGuardAttestation(expectedGuard, request.physicalTopic(), request.partition());
        if (evidence == null
                || !expectedAttestation.equals(evidence.attestation())
                || evidence.ledgerId() != advanced.getLedgerId()
                || evidence.entryId() != advanced.getEntryId()
                || evidence.brokerEntryTimestamp() != messageId.brokerEntryTimestamp()) {
            return Optional.empty();
        }
        final int rawBatchIndex = advanced.getBatchIndex();
        final int rawBatchSize = advanced.getBatchSize();
        final int normalizedBatchIndex = rawBatchIndex < 0 ? 0 : rawBatchIndex;
        if (rawBatchIndex >= 0 && (rawBatchSize <= 0 || Integer.compareUnsigned(rawBatchIndex, rawBatchSize) >= 0)) {
            return Optional.empty();
        }
        final PublishEvidence typed = PulsarSendAckEvidence.published(
                request,
                preparedPublishHash,
                producerNameHash,
                advanced.getLedgerId(),
                advanced.getEntryId(),
                normalizedBatchIndex,
                evidence.brokerEntryTimestamp(),
                evidence.sequenceId(),
                evidence.authenticatedResponseCommandSha256());
        typed.requireBusinessMutation(request.publishAttemptId(), true);
        responseEvidenceResolved.set(true);
        return Optional.of(
                new PulsarClientArtifactDestinationTransport.ResolvedPublish(typed, evidence.brokerEntryTimestamp()));
    }

    @SuppressWarnings("unchecked")
    private static Producer<byte[]> responseLossProducer(
            final Producer<byte[]> delegate, final AtomicReference<GuardedMessageId> responseLostMessage) {
        return (Producer<byte[]>) Proxy.newProxyInstance(
                PulsarClientArtifactDestinationSmoke.class.getClassLoader(),
                new Class<?>[] {Producer.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("newMessage") && method.getParameterCount() == 0) {
                        final TypedMessageBuilder<byte[]> builder =
                                (TypedMessageBuilder<byte[]>) invoke(delegate, method, arguments);
                        return responseLossBuilder(builder, responseLostMessage);
                    }
                    return invoke(delegate, method, arguments);
                });
    }

    @SuppressWarnings("unchecked")
    private static TypedMessageBuilder<byte[]> responseLossBuilder(
            final TypedMessageBuilder<byte[]> delegate, final AtomicReference<GuardedMessageId> responseLostMessage) {
        return (TypedMessageBuilder<byte[]>) Proxy.newProxyInstance(
                PulsarClientArtifactDestinationSmoke.class.getClassLoader(),
                new Class<?>[] {TypedMessageBuilder.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("value") && method.getParameterCount() == 1) {
                        invoke(delegate, method, arguments);
                        return proxy;
                    }
                    if (method.getName().equals("sendAsync") && method.getParameterCount() == 0) {
                        final CompletableFuture<MessageId> sent =
                                (CompletableFuture<MessageId>) invoke(delegate, method, arguments);
                        return sent.thenCompose(messageId -> {
                            if (!(messageId instanceof GuardedMessageId guarded)) {
                                return CompletableFuture.failedFuture(new IllegalStateException(
                                        "Pulsar response-loss wrapper observed an unguarded MessageId"));
                            }
                            responseLostMessage.set(guarded);
                            return CompletableFuture.failedFuture(
                                    new IllegalStateException("simulated committed Pulsar SEND response loss"));
                        });
                    }
                    return invoke(delegate, method, arguments);
                });
    }

    private static Object invoke(final Object target, final java.lang.reflect.Method method, final Object[] arguments)
            throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    private static DestinationPublishRequest request(final ShardId shard, final String topic) {
        final long now = System.currentTimeMillis();
        return new DestinationPublishRequest(
                DestinationLaneId.derive(Bytes.utf8("destination-lane-" + topic)),
                Arrays.copyOf(digest(23), 16),
                DelayMessageId.random(shard),
                0,
                digest(29),
                now,
                now,
                Bytes.utf8("pulsar-destination-payload-" + topic),
                new byte[0]);
    }

    private static void requireTypedPublished(
            final DestinationPublishResult result, final DestinationPublishRequest request, final String label) {
        if (result.disposition() != DestinationPublishResult.Disposition.PUBLISHED
                || result.evidence() == null
                || result.brokerPersistenceTimeEpochMs() < 0) {
            throw new IllegalStateException(
                    label + " did not return PUBLISHED: " + result.disposition() + "/" + result.stableCode());
        }
        final PublishEvidence evidence = PublishEvidence.decode(result.evidence());
        if (evidence.evidenceKind() != PublishEvidenceKind.PULSAR_SEND_ACK
                || evidence.verificationStatus() != EvidenceVerificationStatus.VERIFIED_PUBLISHED) {
            throw new IllegalStateException(label + " returned the wrong evidence branch");
        }
        evidence.requireBusinessMutation(request.publishAttemptId(), true);
    }

    private static long branchNumber(final PublishEvidence evidence, final int number) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(evidence.branch());
        while (reader.hasRemaining()) {
            final CanonicalProtobuf.Reader.Field field = reader.next();
            if (field.number() == number) {
                return field.unsignedValue();
            }
        }
        throw new IllegalStateException("Pulsar SEND ACK branch is missing field " + number);
    }

    private static void requirePayload(
            final PulsarClient client, final String physicalTopic, final byte[] expectedPayload) throws Exception {
        final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, INCARNATION, CREATION_TIMESTAMP);
        final GuardedConsumer<byte[]> guarded = PulsarClientArtifactSourceConsumerFactory.create(
                client, guard, physicalTopic, "nereus-delay-p1-destination-evidence-" + physicalTopic.hashCode());
        try {
            final Message<byte[]> message = guarded.receive(15, TimeUnit.SECONDS);
            if (message == null || !Arrays.equals(expectedPayload, message.getValue())) {
                throw new IllegalStateException("typed destination publish payload was not read back exactly");
            }
            guarded.acknowledge(message);
        } finally {
            guarded.close();
        }
    }

    private static void createTopic(
            final HttpClient client,
            final String adminUrl,
            final String topic,
            final byte[] incarnation,
            final long creationTimestamp)
            throws Exception {
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + topic;
        final String body = "{\"nereus.resource.guard.version\":\"1\","
                + "\"nereus.resource.incarnation\":\""
                + Base64.getUrlEncoder().withoutPadding().encodeToString(incarnation) + "\","
                + "\"nereus.resource.created-at\":\""
                + Long.toUnsignedString(creationTimestamp) + "\"}";
        for (int attempt = 0; attempt < 40; attempt++) {
            final HttpResponse<String> response = request(client, path, "PUT", body);
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return;
            }
            if (response.statusCode() != 409 && response.statusCode() != 412 && response.statusCode() != 503) {
                throw failure("create topic", response);
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("topic create did not converge: " + topic);
    }

    private static void deleteTopicIfPresent(final HttpClient client, final String adminUrl, final String topic) {
        try {
            final HttpResponse<String> response = request(
                    client, adminUrl + "/admin/v2/persistent/public/default/" + topic + "?force=true", "DELETE", "");
            if (response.statusCode() >= 300 && response.statusCode() != 404) {
                System.err.println("P1 destination cleanup could not delete topic: " + response.statusCode());
            }
        } catch (Exception failure) {
            System.err.println("P1 destination cleanup failed: " + failure.getMessage());
        }
    }

    private static HttpResponse<String> request(
            final HttpClient client, final String path, final String method, final String body) throws Exception {
        final HttpRequest.Builder builder =
                HttpRequest.newBuilder(URI.create(path)).header("Content-Type", "application/json");
        final HttpRequest request = "DELETE".equals(method)
                ? builder.DELETE().build()
                : builder.PUT(HttpRequest.BodyPublishers.ofString(body)).build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static IllegalStateException failure(final String operation, final HttpResponse<String> response) {
        return new IllegalStateException(
                operation + " failed with HTTP " + response.statusCode() + ": " + response.body());
    }

    private static byte[] digest(final int seed) {
        final byte[] result = new byte[32];
        Arrays.fill(result, (byte) seed);
        return result;
    }
}
