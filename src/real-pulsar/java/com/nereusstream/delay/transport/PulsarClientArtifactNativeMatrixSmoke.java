package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.PulsarSendResult;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryContract;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.ExternalDeliveryIdentity;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarKey;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.PulsarRecordTemplate;
import com.nereusstream.delay.protocol.PulsarReservedProperties;
import com.nereusstream.delay.protocol.PulsarSequenceAuthority;
import com.nereusstream.delay.protocol.PulsarSourceLock;
import com.nereusstream.delay.protocol.ReservedPublishMetadata;
import com.nereusstream.delay.protocol.ResolvedPayload;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TopicResourceGuard;

/**
 * One real P1 native-delivery behavior cell.
 *
 * <p>The class deliberately owns no Gate C or DataResetAssessment state. It
 * only emits evidence for an already isolated disposable topic and validates
 * the native P1 behavior that the caller selected.</p>
 */
public final class PulsarClientArtifactNativeMatrixSmoke {
    private static final String CLUSTER = "standalone";
    private static final int TICK_TIME_MILLIS = 1_000;
    private static final long MAX_DELIVERY_DELAY_MILLIS = 60_000;
    private static final long DELIVERY_DELAY_MILLIS = 4_000;
    private static final long STRICT_EARLY_TOLERANCE_MILLIS = 100;
    private static final int MESSAGE_TTL_SECONDS = 1;
    private static final long TTL_EXPIRY_SETTLE_MILLIS = 500;
    private static final long TTL_POST_DELIVER_AT_OBSERVATION_MILLIS = 2_000;
    private static final int RETENTION_TIME_MINUTES = 0;
    private static final int RETENTION_SIZE_MEGABYTES = 0;
    private static final int ADMIN_ATTEMPTS = 60;
    private static final long ADMIN_RETRY_MILLIS = 250;
    private static final int HASH_LENGTH = 32;

    private PulsarClientArtifactNativeMatrixSmoke() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 7) {
            throw new IllegalArgumentException("usage: <service-url> <admin-url> <topic> <subscription-type> "
                    + "<policy-mode> <broker-strictness> <evidence-path>");
        }
        final String serviceUrl = arguments[0];
        final String adminUrl = arguments[1];
        final String topic = requireText(arguments[2], "topic");
        final SubscriptionType subscriptionType = parseSubscriptionType(arguments[3]);
        final PolicyMode policyMode = PolicyMode.parse(arguments[4]);
        if ((policyMode == PolicyMode.TTL_EXPIRY || policyMode == PolicyMode.RETENTION_ZERO)
                && subscriptionType != SubscriptionType.Shared) {
            throw new IllegalArgumentException("native TTL/retention risk cells require Shared subscription");
        }
        final String brokerStrictness = requireText(arguments[5], "broker-strictness");
        final Path evidencePath = Path.of(arguments[6]).toAbsolutePath();
        final long startedAt = System.currentTimeMillis();
        final HttpClient admin = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        final byte[] incarnation = Bytes.sha256(Bytes.utf8("ndip1-disposable-resource\0" + topic));
        final long creationTimestamp = startedAt;
        createTopic(admin, adminUrl, topic, incarnation, creationTimestamp);
        try {
            setDelayedDeliveryPolicy(admin, adminUrl, topic, policyMode.active());
            if (policyMode == PolicyMode.TTL_EXPIRY) {
                setMessageTtl(admin, adminUrl, topic, MESSAGE_TTL_SECONDS);
            } else if (policyMode == PolicyMode.RETENTION_ZERO) {
                setRetention(admin, adminUrl, topic, RETENTION_TIME_MINUTES, RETENTION_SIZE_MEGABYTES);
            }
            final CellResult result = runCell(
                    admin,
                    serviceUrl,
                    adminUrl,
                    topic,
                    subscriptionType,
                    policyMode,
                    brokerStrictness,
                    incarnation,
                    creationTimestamp,
                    startedAt);
            writeEvidence(evidencePath, result);
            System.out.println("Pulsar native matrix cell passed: cell="
                    + result.cellId()
                    + ", subscription="
                    + result.subscriptionType()
                    + ", policy="
                    + result.policyMode()
                    + ", deliverAt="
                    + result.deliverAtEpochMs()
                    + ", receiveAt="
                    + result.receiveAtEpochMs()
                    + ", sequence="
                    + result.actualSequenceId());
        } finally {
            deleteTopicIfPresent(admin, adminUrl, topic);
        }
    }

    private static CellResult runCell(
            final HttpClient admin,
            final String serviceUrl,
            final String adminUrl,
            final String topic,
            final SubscriptionType subscriptionType,
            final PolicyMode policyMode,
            final String brokerStrictness,
            final byte[] incarnation,
            final long creationTimestamp,
            final long startedAt)
            throws Exception {
        final String physicalTopic = "persistent://public/default/" + topic;
        final String cellId = "native." + subscriptionTypeName(subscriptionType) + "." + policyMode.matrixValue();
        final byte[] payload = Bytes.utf8("ndip1-disposable-native-payload-" + cellId);
        final long deliverAtEpochMs = System.currentTimeMillis() + DELIVERY_DELAY_MILLIS;
        final long eventTimeEpochMs = System.currentTimeMillis();
        final RouteIncarnation routeIncarnation = RouteIncarnation.random();
        final DelayMessageId messageId = DelayMessageId.random(new ShardId(routeIncarnation, 0));
        final byte[] publishAttemptId = Bytes.sha256(Bytes.utf8("publish-attempt\0" + topic));
        final byte[] nativeDeliveryId = Bytes.sha256(Bytes.utf8("native-delivery\0" + topic));
        final ArtifactGenerationSet artifacts = ArtifactGenerationSet.current(
                startedAt,
                PulsarSourceLock.digest(),
                Bytes.sha256(Bytes.utf8("ndip1-disposable-native-matrix-schema")));
        final String producerName = "nereus-delay-ndip1-native-" + topic;
        final String subscriptionName = producerName + "-sub";
        final PulsarBrokerResourceIdentity target =
                new PulsarBrokerResourceIdentity(CLUSTER, incarnation, physicalTopic, creationTimestamp);
        final ReservedPublishMetadata reserved = new ReservedPublishMetadata(
                routeIncarnation,
                0,
                messageId,
                1,
                publishAttemptId,
                Bytes.sha256(Bytes.utf8("destination-profile\0" + topic)),
                Bytes.sha256(Bytes.utf8("capability-profile\0" + topic)),
                deliverAtEpochMs,
                DeliveryMode.MANAGED);
        final PulsarKey key = PulsarKey.utf8("ndip1-key-" + topic);
        final byte[] orderingKey = Bytes.utf8("ndip1-ordering-key-" + topic);
        final List<PulsarMetadata.Property> callerProperties = List.of(
                new PulsarMetadata.Property("matrix.alpha", "alpha-" + topic),
                new PulsarMetadata.Property("matrix.zeta", "zeta-" + topic));
        final PulsarRecordTemplate template = new PulsarRecordTemplate(
                BrokerResourceIdentity.pulsar(target),
                0,
                key,
                orderingKey,
                callerProperties,
                eventTimeEpochMs,
                reserved,
                DeliveryContract.PULSAR_NATIVE_DELIVERY,
                deliverAtEpochMs,
                com.nereusstream.delay.protocol.PayloadForPublish.inline(payload),
                artifacts.setDigest());
        final byte[] preparedIdentityHash = Bytes.sha256(Bytes.utf8("prepared-identity\0" + topic));
        final PulsarPreparedRecord record = new PulsarPreparedRecord(
                template,
                template.recordTemplateHash(),
                ResolvedPayload.of(payload),
                PulsarSequenceAuthority.producerAssigned(),
                ExternalDeliveryIdentity.nativeDelivery(nativeDeliveryId),
                preparedIdentityHash,
                PulsarReservedProperties.all(reserved, publishAttemptId, preparedIdentityHash),
                artifacts.setDigest());

        final long sendStartedAt = System.currentTimeMillis();
        final PulsarSendResult sendResult;
        final AckFields ack;
        Message<byte[]> delivered = null;
        long receiveAtEpochMs = 0;
        boolean deliveryObserved = false;
        boolean expiryTriggered = false;
        boolean targetLedgerTrimmed = false;
        String riskObservation = "subscription behavior matched the explicit Pulsar native contract";
        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
                Producer<byte[]> producer = PulsarClientArtifactProducerFactory.create(
                        client, CLUSTER, incarnation, physicalTopic, creationTimestamp, producerName);
                PulsarClientArtifactSendTransport transport = new PulsarClientArtifactSendTransport(
                        producer, CLUSTER, incarnation, physicalTopic, creationTimestamp, 0, true)) {
            final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, incarnation, creationTimestamp);
            if (policyMode == PolicyMode.TTL_EXPIRY) {
                try (GuardedConsumer<byte[]> ignored = PulsarClientArtifactSourceConsumerFactory.create(
                        client, guard, physicalTopic, subscriptionName, subscriptionType)) {
                    // The durable cursor must exist before the target is persisted so TTL can expire it.
                    if (!physicalTopic.equals(ignored.getTopic())) {
                        throw new IllegalStateException("TTL seed consumer is not bound to the exact topic");
                    }
                }
            }
            sendResult = transport
                    .sendPreparedRecord(record, artifacts)
                    .toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
            if (sendResult.disposition() != PulsarSendResult.Disposition.PERSISTED
                    || sendResult.responseEvidenceBytes() == null) {
                throw new IllegalStateException(
                        "native matrix SEND did not return persisted typed evidence: " + sendResult);
            }
            final PublishEvidence evidence = PublishEvidence.decode(sendResult.responseEvidenceBytes());
            com.nereusstream.delay.adapter.PulsarSendAckEvidence.requireExactRecordBinding(
                    evidence,
                    record,
                    artifacts,
                    sendResult.ledgerId(),
                    sendResult.entryId(),
                    sendResult.batchIndex(),
                    sendResult.batchSize(),
                    sendResult.brokerEntryTimestampEpochMs());
            ack = AckFields.from(evidence);
            if (!Arrays.equals(ack.p1SourceLockDigest(), PulsarSourceLock.digest())
                    || !Arrays.equals(ack.artifactGenerationSetDigest(), artifacts.setDigest())
                    || !Arrays.equals(ack.recordTemplateHash(), record.recordTemplateHash())
                    || !Arrays.equals(ack.preparedRecordHash(), record.preparedRecordHash())
                    || ack.ledgerId() != sendResult.ledgerId()
                    || ack.entryId() != sendResult.entryId()
                    || ack.batchIndex() != sendResult.batchIndex()
                    || ack.batchSize() != sendResult.batchSize()
                    || ack.brokerEntryTimestampEpochMs() != sendResult.brokerEntryTimestampEpochMs()) {
                throw new IllegalStateException(
                        "native matrix ACK is not bound to the exact record/position/source lock");
            }

            if (policyMode == PolicyMode.TTL_EXPIRY) {
                waitUntil(sendResult.brokerEntryTimestampEpochMs()
                        + TimeUnit.SECONDS.toMillis(MESSAGE_TTL_SECONDS)
                        + TTL_EXPIRY_SETTLE_MILLIS);
                expireSubscription(admin, adminUrl, topic, subscriptionName, MESSAGE_TTL_SECONDS);
                expiryTriggered = true;
                try (GuardedConsumer<byte[]> consumer = PulsarClientArtifactSourceConsumerFactory.create(
                        client, guard, physicalTopic, subscriptionName, subscriptionType)) {
                    final long observationDeadline = deliverAtEpochMs + TTL_POST_DELIVER_AT_OBSERVATION_MILLIS;
                    while (System.currentTimeMillis() < observationDeadline) {
                        final long remaining = observationDeadline - System.currentTimeMillis();
                        if (remaining <= 0) {
                            break;
                        }
                        final Message<byte[]> unexpected =
                                consumer.receive((int) Math.min(remaining, 250L), TimeUnit.MILLISECONDS);
                        if (unexpected != null) {
                            throw new IllegalStateException(
                                    "TTL-expired native message became visible at or after deliverAt");
                        }
                    }
                }
                riskObservation = "topic TTL and an explicit Pulsar expiry check removed the delayed message before "
                        + "deliverAt; no delivery was observed after deliverAt";
            } else {
                try (GuardedConsumer<byte[]> consumer = PulsarClientArtifactSourceConsumerFactory.create(
                        client, guard, physicalTopic, subscriptionName, subscriptionType)) {
                    final long earlyWindowDeadline = Math.min(deliverAtEpochMs, System.currentTimeMillis() + 1_500L);
                    Message<byte[]> early = null;
                    while (System.currentTimeMillis() < earlyWindowDeadline) {
                        final long remaining = earlyWindowDeadline - System.currentTimeMillis();
                        if (remaining <= 0) {
                            break;
                        }
                        early = consumer.receive((int) Math.min(remaining, 250L), TimeUnit.MILLISECONDS);
                        if (early != null) {
                            break;
                        }
                    }
                    if (early != null) {
                        final long earlyReceivedAt = System.currentTimeMillis();
                        if (policyMode.requiresDelayedBoundary()) {
                            final long earlyByMillis = deliverAtEpochMs - earlyReceivedAt;
                            if (policyMode == PolicyMode.STRICT
                                    || earlyByMillis > TICK_TIME_MILLIS + STRICT_EARLY_TOLERANCE_MILLIS) {
                                throw new IllegalStateException(
                                        "native matrix message was released beyond the allowed native risk boundary: "
                                                + "cell="
                                                + cellId
                                                + ", earlyByMillis="
                                                + earlyByMillis);
                            }
                        } else if (earlyReceivedAt >= deliverAtEpochMs) {
                            throw new IllegalStateException("native matrix immediate cell was not immediate");
                        }
                        delivered = early;
                        receiveAtEpochMs = earlyReceivedAt;
                    } else {
                        delivered = consumer.receive(15, TimeUnit.SECONDS);
                        receiveAtEpochMs = System.currentTimeMillis();
                        if (delivered == null) {
                            throw new IllegalStateException("native matrix message was not delivered: " + cellId);
                        }
                    }
                    validateDeliveredMessage(
                            delivered, physicalTopic, payload, key, orderingKey, eventTimeEpochMs, record, ack);
                    deliveryObserved = true;
                    if (policyMode == PolicyMode.STRICT
                            && receiveAtEpochMs + STRICT_EARLY_TOLERANCE_MILLIS < deliverAtEpochMs) {
                        throw new IllegalStateException("strict native matrix delivery was early by "
                                + (deliverAtEpochMs - receiveAtEpochMs)
                                + " ms");
                    }
                    if (policyMode == PolicyMode.DISABLED
                            || subscriptionType == SubscriptionType.Exclusive
                            || subscriptionType == SubscriptionType.Failover) {
                        if (receiveAtEpochMs >= deliverAtEpochMs && policyMode == PolicyMode.DISABLED) {
                            throw new IllegalStateException("disabled-delivery cell was not immediately visible");
                        }
                        if (receiveAtEpochMs >= deliverAtEpochMs
                                && (subscriptionType == SubscriptionType.Exclusive
                                        || subscriptionType == SubscriptionType.Failover)) {
                            throw new IllegalStateException(
                                    "exclusive/failover native immediate cell was not immediate");
                        }
                    }
                    consumer.acknowledge(delivered);
                }
            }
        }
        if (policyMode == PolicyMode.RETENTION_ZERO) {
            targetLedgerTrimmed = observeZeroRetention(
                    admin,
                    serviceUrl,
                    adminUrl,
                    topic,
                    physicalTopic,
                    subscriptionName,
                    incarnation,
                    creationTimestamp,
                    producerName,
                    sendResult.ledgerId());
            riskObservation = "zero topic retention removed the acknowledged target ledger after rollover and an "
                    + "explicit Pulsar trim";
        }
        final long finishedAt = System.currentTimeMillis();
        final long receiveDelta = deliveryObserved ? receiveAtEpochMs - deliverAtEpochMs : 0;
        return new CellResult(
                cellId,
                subscriptionTypeName(subscriptionType),
                policyMode.value(),
                brokerStrictness,
                policyMode.riskKind(),
                policyMode == PolicyMode.TTL_EXPIRY ? MESSAGE_TTL_SECONDS : 0,
                policyMode == PolicyMode.RETENTION_ZERO ? RETENTION_TIME_MINUTES : -1,
                policyMode == PolicyMode.RETENTION_ZERO ? RETENTION_SIZE_MEGABYTES : -1,
                deliveryObserved,
                expiryTriggered,
                targetLedgerTrimmed,
                riskObservation,
                topic,
                physicalTopic,
                CLUSTER,
                Base64.getUrlEncoder().withoutPadding().encodeToString(incarnation),
                creationTimestamp,
                0,
                Base64.getUrlEncoder().withoutPadding().encodeToString(messageId.bytes()),
                1,
                Base64.getUrlEncoder().withoutPadding().encodeToString(publishAttemptId),
                Base64.getUrlEncoder().withoutPadding().encodeToString(payload),
                Bytes.hex(Bytes.sha256(payload)),
                key.utf8Value(),
                Base64.getUrlEncoder().withoutPadding().encodeToString(orderingKey),
                eventTimeEpochMs,
                deliverAtEpochMs,
                Base64.getUrlEncoder().withoutPadding().encodeToString(record.recordTemplateHash()),
                Base64.getUrlEncoder().withoutPadding().encodeToString(record.preparedRecordHash()),
                Base64.getUrlEncoder().withoutPadding().encodeToString(artifacts.setDigest()),
                sendStartedAt,
                sendResult.ledgerId(),
                sendResult.entryId(),
                sendResult.batchIndex(),
                sendResult.batchSize(),
                sendResult.brokerEntryTimestampEpochMs(),
                ack.actualSequenceId(),
                Base64.getUrlEncoder().withoutPadding().encodeToString(ack.sendCommandSha256()),
                Base64.getUrlEncoder().withoutPadding().encodeToString(ack.authenticatedResponseSha256()),
                receiveAtEpochMs,
                receiveDelta,
                finishedAt);
    }

    private static void waitUntil(final long deadlineEpochMs) throws InterruptedException {
        while (System.currentTimeMillis() < deadlineEpochMs) {
            final long remaining = deadlineEpochMs - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(Math.min(remaining, 100L));
        }
    }

    private static void expireSubscription(
            final HttpClient client,
            final String adminUrl,
            final String topic,
            final String subscriptionName,
            final int expireTimeInSeconds)
            throws Exception {
        final String path = adminUrl
                + "/admin/v2/persistent/public/default/"
                + topic
                + "/subscription/"
                + subscriptionName
                + "/expireMessages/"
                + expireTimeInSeconds;
        requireAdminMutation(client, path, "expire native TTL subscription");
    }

    private static boolean observeZeroRetention(
            final HttpClient admin,
            final String serviceUrl,
            final String adminUrl,
            final String topic,
            final String physicalTopic,
            final String subscriptionName,
            final byte[] incarnation,
            final long creationTimestamp,
            final String producerName,
            final long targetLedgerId)
            throws Exception {
        requireAdminPut(
                admin,
                adminUrl + "/admin/v2/persistent/public/default/" + topic + "/unload",
                "unload native retention topic");
        final byte[] triggerPayload = Bytes.utf8("ndip1-zero-retention-rollover-" + topic);
        final TopicResourceGuard guard = new TopicResourceGuard(CLUSTER, incarnation, creationTimestamp);
        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
                Producer<byte[]> producer = PulsarClientArtifactProducerFactory.create(
                        client,
                        CLUSTER,
                        incarnation,
                        physicalTopic,
                        creationTimestamp,
                        producerName + "-retention-rollover");
                GuardedConsumer<byte[]> consumer = PulsarClientArtifactSourceConsumerFactory.create(
                        client, guard, physicalTopic, subscriptionName, SubscriptionType.Shared)) {
            producer.newMessage().value(triggerPayload).send();
            final Message<byte[]> trigger = consumer.receive(15, TimeUnit.SECONDS);
            if (trigger == null || !Arrays.equals(triggerPayload, trigger.getValue())) {
                throw new IllegalStateException("zero-retention rollover message was not delivered exactly");
            }
            consumer.acknowledge(trigger);
        }
        final String trimPath = adminUrl + "/admin/v2/persistent/public/default/" + topic + "/trim";
        requireAdminMutation(admin, trimPath, "trim zero-retention native topic");
        final String statsPath =
                adminUrl + "/admin/v2/persistent/public/default/" + topic + "/internalStats?metadata=true";
        final Pattern targetLedger = Pattern.compile(
                "\\\"ledgerId\\\"\\s*:\\s*" + Pattern.quote(Long.toString(targetLedgerId)) + "(?:\\D|$)");
        for (int attempt = 0; attempt < ADMIN_ATTEMPTS; attempt++) {
            HttpResponse<String> response = request(admin, statsPath, "GET", "");
            if (response.statusCode() == 404) {
                response = request(admin, statsPath.replace("metadata=true", "metadata=false"), "GET", "");
            }
            if (response.statusCode() >= 200
                    && response.statusCode() < 300
                    && !targetLedger.matcher(response.body()).find()) {
                return true;
            }
            if (attempt > 0 && attempt % 10 == 0) {
                requireAdminMutation(admin, trimPath, "retry zero-retention native topic trim");
            }
            TimeUnit.MILLISECONDS.sleep(ADMIN_RETRY_MILLIS);
        }
        throw new IllegalStateException(
                "zero-retention trim did not remove the acknowledged target ledger: " + targetLedgerId);
    }

    private static void validateDeliveredMessage(
            final Message<byte[]> message,
            final String physicalTopic,
            final byte[] expectedPayload,
            final PulsarKey expectedKey,
            final byte[] expectedOrderingKey,
            final long expectedEventTime,
            final PulsarPreparedRecord record,
            final AckFields ack) {
        if (!physicalTopic.equals(message.getTopicName())
                || !Arrays.equals(expectedPayload, message.getValue())
                || !expectedKey.utf8Value().equals(message.getKey())
                || !Arrays.equals(expectedOrderingKey, message.getOrderingKey())
                || message.getEventTime() != expectedEventTime
                || !ack.actualSequenceIdEquals(message.getSequenceId())
                || !message.getProperties().equals(expectedProperties(record))) {
            throw new IllegalStateException("native matrix message projection was not exact");
        }
        if (!(message.getMessageId() instanceof MessageIdAdv messageId)
                || messageId.getLedgerId() != ack.ledgerId()
                || messageId.getEntryId() != ack.entryId()) {
            throw new IllegalStateException("native matrix message position did not match authenticated ACK");
        }
    }

    private static Map<String, String> expectedProperties(final PulsarPreparedRecord record) {
        final Map<String, String> properties = new LinkedHashMap<>();
        for (PulsarMetadata.Property property : record.template().callerProperties()) {
            properties.put(property.key(), property.value());
        }
        for (PulsarMetadata.Property property : record.finalReservedProperties()) {
            properties.put(property.key(), property.value());
        }
        return properties;
    }

    private static void writeEvidence(final Path path, final CellResult result) throws Exception {
        final Path parent = Objects.requireNonNull(path.getParent(), "evidence path must have a parent");
        Files.createDirectories(parent);
        Files.writeString(path, result.toJson(), StandardCharsets.UTF_8);
    }

    private static void createTopic(
            final HttpClient client,
            final String adminUrl,
            final String topic,
            final byte[] incarnation,
            final long creationTimestamp)
            throws Exception {
        final String body = "{\"nereus.resource.guard.version\":\"1\",\"nereus.resource.incarnation\":\""
                + Base64.getUrlEncoder().withoutPadding().encodeToString(incarnation)
                + "\",\"nereus.resource.created-at\":\""
                + Long.toUnsignedString(creationTimestamp)
                + "\"}";
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + topic;
        for (int attempt = 0; attempt < 60; attempt++) {
            final HttpResponse<String> response = request(client, path, "PUT", body);
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return;
            }
            if (response.statusCode() != 409 && response.statusCode() != 412 && response.statusCode() != 503) {
                throw failure("create native matrix topic", response);
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("native matrix topic creation did not converge: " + topic);
    }

    private static void setDelayedDeliveryPolicy(
            final HttpClient client, final String adminUrl, final String topic, final boolean active) throws Exception {
        final String body = "{\"tickTime\":"
                + TICK_TIME_MILLIS
                + ",\"active\":"
                + active
                + ",\"maxDeliveryDelayInMillis\":"
                + MAX_DELIVERY_DELAY_MILLIS
                + "}";
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + topic + "/delayedDelivery";
        for (int attempt = 0; attempt < 60; attempt++) {
            final HttpResponse<String> response = request(client, path, "POST", body);
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return;
            }
            if (response.statusCode() != 409 && response.statusCode() != 412 && response.statusCode() != 503) {
                throw failure("set native matrix delayed-delivery policy", response);
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new IllegalStateException("native matrix delayed-delivery policy did not converge: " + topic);
    }

    private static void setMessageTtl(
            final HttpClient client, final String adminUrl, final String topic, final int messageTtlSeconds)
            throws Exception {
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + topic + "/messageTTL";
        requireAdminMutation(client, path + "?messageTTL=" + messageTtlSeconds, "set native matrix topic message TTL");
        for (int attempt = 0; attempt < ADMIN_ATTEMPTS; attempt++) {
            final HttpResponse<String> response = request(client, path + "?applied=true", "GET", "");
            if (response.statusCode() >= 200
                    && response.statusCode() < 300
                    && Integer.toString(messageTtlSeconds)
                            .equals(response.body().trim())) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(ADMIN_RETRY_MILLIS);
        }
        throw new IllegalStateException("native matrix topic message TTL did not converge: " + topic);
    }

    private static void setRetention(
            final HttpClient client,
            final String adminUrl,
            final String topic,
            final int retentionTimeMinutes,
            final int retentionSizeMegabytes)
            throws Exception {
        final String path = adminUrl + "/admin/v2/persistent/public/default/" + topic + "/retention";
        final String body = "{\"retentionTimeInMinutes\":"
                + retentionTimeMinutes
                + ",\"retentionSizeInMB\":"
                + retentionSizeMegabytes
                + "}";
        requireAdminMutation(client, path, "set native matrix topic retention", body);
        for (int attempt = 0; attempt < ADMIN_ATTEMPTS; attempt++) {
            final HttpResponse<String> response = request(client, path + "?applied=true", "GET", "");
            final String compact = response.body().replaceAll("\\s+", "");
            if (response.statusCode() >= 200
                    && response.statusCode() < 300
                    && compact.contains("\"retentionTimeInMinutes\":" + retentionTimeMinutes)
                    && compact.contains("\"retentionSizeInMB\":" + retentionSizeMegabytes)) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(ADMIN_RETRY_MILLIS);
        }
        throw new IllegalStateException("native matrix topic retention did not converge: " + topic);
    }

    private static void requireAdminMutation(final HttpClient client, final String path, final String operation)
            throws Exception {
        requireAdminMutation(client, path, operation, "");
    }

    private static void requireAdminPut(final HttpClient client, final String path, final String operation)
            throws Exception {
        requireAdminRequest(client, path, "PUT", operation, "");
    }

    private static void requireAdminMutation(
            final HttpClient client, final String path, final String operation, final String body) throws Exception {
        requireAdminRequest(client, path, "POST", operation, body);
    }

    private static void requireAdminRequest(
            final HttpClient client, final String path, final String method, final String operation, final String body)
            throws Exception {
        HttpResponse<String> lastResponse = null;
        for (int attempt = 0; attempt < ADMIN_ATTEMPTS; attempt++) {
            lastResponse = request(client, path, method, body);
            if (lastResponse.statusCode() >= 200 && lastResponse.statusCode() < 300) {
                return;
            }
            if (lastResponse.statusCode() != 404
                    && lastResponse.statusCode() != 409
                    && lastResponse.statusCode() != 412
                    && lastResponse.statusCode() != 500
                    && lastResponse.statusCode() != 503) {
                throw failure(operation, lastResponse);
            }
            TimeUnit.MILLISECONDS.sleep(ADMIN_RETRY_MILLIS);
        }
        throw failure(operation, Objects.requireNonNull(lastResponse, "admin response"));
    }

    private static void deleteTopicIfPresent(final HttpClient client, final String adminUrl, final String topic) {
        try {
            final HttpResponse<String> response = request(
                    client, adminUrl + "/admin/v2/persistent/public/default/" + topic + "?force=true", "DELETE", "");
            if (response.statusCode() >= 300 && response.statusCode() != 404) {
                System.err.println("native matrix cleanup could not delete topic: " + response.statusCode());
            }
        } catch (Exception failure) {
            System.err.println("native matrix topic cleanup failed: " + failure.getMessage());
        }
    }

    private static HttpResponse<String> request(
            final HttpClient client, final String path, final String method, final String body) throws Exception {
        final HttpRequest.Builder builder =
                HttpRequest.newBuilder(URI.create(path)).header("Content-Type", "application/json");
        final HttpRequest request =
                switch (method) {
                    case "DELETE" -> builder.DELETE().build();
                    case "GET" -> builder.GET().build();
                    case "POST" ->
                        builder.POST(HttpRequest.BodyPublishers.ofString(body)).build();
                    case "PUT" ->
                        builder.PUT(HttpRequest.BodyPublishers.ofString(body)).build();
                    default -> throw new IllegalArgumentException("unsupported HTTP method: " + method);
                };
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static IllegalStateException failure(final String operation, final HttpResponse<String> response) {
        return new IllegalStateException(
                operation + " failed with HTTP " + response.statusCode() + ": " + response.body());
    }

    private static SubscriptionType parseSubscriptionType(final String value) {
        return switch (requireText(value, "subscription-type").toLowerCase()) {
            case "shared" -> SubscriptionType.Shared;
            case "key_shared", "key-shared", "keyshared" -> SubscriptionType.Key_Shared;
            case "exclusive" -> SubscriptionType.Exclusive;
            case "failover" -> SubscriptionType.Failover;
            default -> throw new IllegalArgumentException("unsupported Pulsar subscription type: " + value);
        };
    }

    private static String subscriptionTypeName(final SubscriptionType value) {
        return switch (value) {
            case Shared -> "shared";
            case Key_Shared -> "key_shared";
            case Exclusive -> "exclusive";
            case Failover -> "failover";
            default -> throw new IllegalArgumentException("unsupported Pulsar subscription type: " + value);
        };
    }

    private static String requireText(final String value, final String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private enum PolicyMode {
        STRICT("strict", true, true),
        NON_STRICT("non-strict", true, true),
        DISABLED("disabled", false, false),
        IMMEDIATE("immediate", true, false),
        TTL_EXPIRY("ttl-expiry", true, false),
        RETENTION_ZERO("retention-zero", true, true);

        private final String value;
        private final boolean active;
        private final boolean delayedBoundary;

        PolicyMode(final String value, final boolean active, final boolean delayedBoundary) {
            this.value = value;
            this.active = active;
            this.delayedBoundary = delayedBoundary;
        }

        private static PolicyMode parse(final String value) {
            final String normalized = requireText(value, "policy-mode").toLowerCase();
            for (PolicyMode mode : values()) {
                if (mode.value.equals(normalized)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("unsupported native matrix policy mode: " + value);
        }

        private boolean active() {
            return active;
        }

        private boolean requiresDelayedBoundary() {
            return delayedBoundary;
        }

        private String value() {
            return value;
        }

        private String matrixValue() {
            return value.replace('-', '_');
        }

        private String riskKind() {
            return switch (this) {
                case TTL_EXPIRY -> "message-ttl";
                case RETENTION_ZERO -> "retention";
                default -> "subscription-delivery";
            };
        }
    }

    private record CellResult(
            String cellId,
            String subscriptionType,
            String policyMode,
            String brokerStrictness,
            String riskKind,
            int messageTtlSeconds,
            int retentionTimeMinutes,
            int retentionSizeMegabytes,
            boolean deliveryObserved,
            boolean expiryTriggered,
            boolean targetLedgerTrimmed,
            String riskObservation,
            String topic,
            String physicalTopic,
            String cluster,
            String resourceIncarnationBase64,
            long creationTimestamp,
            int partition,
            String messageIdBase64,
            long generation,
            String publishAttemptIdBase64,
            String payloadBase64,
            String payloadSha256,
            String key,
            String orderingKeyBase64,
            long eventTimeEpochMs,
            long deliverAtEpochMs,
            String recordTemplateHashBase64,
            String preparedRecordHashBase64,
            String artifactGenerationSetDigestBase64,
            long sendStartedAtEpochMs,
            long ledgerId,
            long entryId,
            int batchIndex,
            int batchSize,
            long brokerEntryTimestampEpochMs,
            long actualSequenceId,
            String sendCommandSha256Base64,
            String authenticatedResponseSha256Base64,
            long receiveAtEpochMs,
            long receiveDeltaFromTargetMs,
            long finishedAtEpochMs) {
        private String toJson() {
            return "{\n"
                    + field("schema", "nereus-delay.disposable-local.native-cell-evidence-r2", true)
                    + field("classification", "DISPOSABLE_LOCAL", true)
                    + field("cellId", cellId, true)
                    + field("subscriptionType", subscriptionType, true)
                    + field("policyMode", policyMode, true)
                    + field("brokerStrictness", brokerStrictness, true)
                    + field("riskKind", riskKind, true)
                    + field("messageTtlSeconds", messageTtlSeconds, false)
                    + field("retentionTimeMinutes", retentionTimeMinutes, false)
                    + field("retentionSizeMegabytes", retentionSizeMegabytes, false)
                    + field("deliveryObserved", deliveryObserved, false)
                    + field("expiryTriggered", expiryTriggered, false)
                    + field("targetLedgerTrimmed", targetLedgerTrimmed, false)
                    + field("riskObservation", riskObservation, true)
                    + field("topic", topic, true)
                    + field("physicalTopic", physicalTopic, true)
                    + field("cluster", cluster, true)
                    + field("resourceIncarnationBase64", resourceIncarnationBase64, true)
                    + field("creationTimestamp", creationTimestamp, false)
                    + field("partition", partition, false)
                    + field("messageIdBase64", messageIdBase64, true)
                    + field("generation", generation, false)
                    + field("publishAttemptIdBase64", publishAttemptIdBase64, true)
                    + field("payloadBase64", payloadBase64, true)
                    + field("payloadSha256", payloadSha256, true)
                    + field("key", key, true)
                    + field("orderingKeyBase64", orderingKeyBase64, true)
                    + field("eventTimeEpochMs", eventTimeEpochMs, false)
                    + field("deliverAtEpochMs", deliverAtEpochMs, false)
                    + field("recordTemplateHashBase64", recordTemplateHashBase64, true)
                    + field("preparedRecordHashBase64", preparedRecordHashBase64, true)
                    + field("artifactGenerationSetDigestBase64", artifactGenerationSetDigestBase64, true)
                    + field("sendStartedAtEpochMs", sendStartedAtEpochMs, false)
                    + field("ledgerId", ledgerId, false)
                    + field("entryId", entryId, false)
                    + field("batchIndex", batchIndex, false)
                    + field("batchSize", batchSize, false)
                    + field("brokerEntryTimestampEpochMs", brokerEntryTimestampEpochMs, false)
                    + field("actualSequenceId", actualSequenceId, false)
                    + field("sendCommandSha256Base64", sendCommandSha256Base64, true)
                    + field("authenticatedResponseSha256Base64", authenticatedResponseSha256Base64, true)
                    + field("receiveAtEpochMs", receiveAtEpochMs, false)
                    + field("receiveDeltaFromTargetMs", receiveDeltaFromTargetMs, false)
                    + field(
                            "startedAt",
                            Instant.ofEpochMilli(sendStartedAtEpochMs).toString(),
                            true)
                    + field(
                            "finishedAt",
                            Instant.ofEpochMilli(finishedAtEpochMs).toString(),
                            true)
                    + "  \"verdict\": \"PASS\"\n}\n";
        }

        private static String field(final String key, final Object value, final boolean string) {
            return "  \"" + key + "\": " + (string ? quote(value.toString()) : value) + ",\n";
        }

        private static String quote(final String value) {
            return "\""
                    + value.replace("\\", "\\\\")
                            .replace("\"", "\\\"")
                            .replace("\n", "\\n")
                            .replace("\r", "\\r")
                    + "\"";
        }
    }

    private record AckFields(
            long ledgerId,
            long entryId,
            int batchIndex,
            int batchSize,
            long brokerEntryTimestampEpochMs,
            long actualSequenceId,
            byte[] recordTemplateHash,
            byte[] preparedRecordHash,
            byte[] sendCommandSha256,
            byte[] authenticatedResponseSha256,
            byte[] p1SourceLockDigest,
            byte[] artifactGenerationSetDigest) {
        private static AckFields from(final PublishEvidence evidence) {
            final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(evidence.branch());
            final CanonicalProtobuf.Reader.Field[] fields = new CanonicalProtobuf.Reader.Field[22];
            int count = 0;
            while (reader.hasRemaining()) {
                if (count >= fields.length) {
                    throw new IllegalArgumentException("native matrix ACK has too many fields");
                }
                fields[count++] = reader.next();
            }
            if (count != fields.length) {
                throw new IllegalArgumentException("native matrix ACK has incomplete fields");
            }
            for (int index = 0; index < fields.length; index++) {
                if (fields[index].number() != index + 1) {
                    throw new IllegalArgumentException("native matrix ACK field order is not canonical");
                }
            }
            return new AckFields(
                    fields[3].unsignedValue(),
                    fields[4].unsignedValue(),
                    (int) fields[5].unsignedValue(),
                    (int) fields[6].unsignedValue(),
                    fields[7].unsignedValue(),
                    fields[12].unsignedValue(),
                    fields[15].rawValue(),
                    fields[16].rawValue(),
                    fields[18].rawValue(),
                    fields[19].rawValue(),
                    fields[20].rawValue(),
                    fields[21].rawValue());
        }

        private boolean actualSequenceIdEquals(final long messageSequenceId) {
            return messageSequenceId == actualSequenceId;
        }
    }
}
