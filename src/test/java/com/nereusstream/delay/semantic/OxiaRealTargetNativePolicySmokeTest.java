package com.nereusstream.delay.semantic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlAuthor;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlRole;
import com.nereusstream.delay.protocol.ControlRoleSet;
import com.nereusstream.delay.protocol.HandoffPath;
import com.nereusstream.delay.protocol.HandoffPolicyMode;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetNativePolicySnapshot;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.io.Closeable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Opt-in real Oxia coverage for Target Native current-head notifications and bounded reread after restart. */
@Tag("real-service")
class OxiaRealTargetNativePolicySmokeTest {
    private final Properties vectors = load("target-native-policy-vectors.properties");
    private final Properties bindings = load("target-binding-channel-vectors.properties");
    private final TargetNativePolicyScope scope = TargetNativePolicyScope.decode(hex(vectors, "pulsar.journal.scope"));
    private final SourcePosition source = TargetSourcePosition.decode(hex(bindings, "source"));
    private final KeyPair issuer = issuerKey();
    private final ControlRoleSet roles =
            ControlRoleSet.of(ControlRole.TENANT_POLICY_ADMINISTRATOR, ControlRole.PLATFORM_OPERATOR);
    private final ControlAuthorizationContext actor =
            new ControlAuthorizationContext(bytes(32, 0x91), roles, scope.controlResourceScope());
    private final TargetNativePolicyTrust.PublisherPermission permission =
            new TargetNativePolicyTrust.PublisherPermission(
                    scope,
                    new ControlAuthor(actor.actorIdHash(), roles.digest(), actor.tenantResourceScopeHash()),
                    9,
                    issuer.getPublic(),
                    119_000,
                    at(1));

    @Test
    void currentHeadPublicationNotifiesARealOxiaSubscriber() throws Exception {
        final String endpoint = endpoint();
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String prefix = "nereus-delay-real-native-policy-notification/" + UUID.randomUUID();
        final var changed = new CountDownLatch(1);
        try (var subscriber = connect(endpoint, namespace, prefix, "subscriber");
                var publisher = connect(endpoint, namespace, prefix, "publisher")) {
            final Closeable subscription = subscribe(subscriber, changed);
            try {
                final var publication = publisher.authority().publish(
                        permission, actor, at(10), 0, snapshot(1, HandoffPolicyMode.ENABLED));
                assertTrue(
                        changed.await(15, TimeUnit.SECONDS),
                        "real Oxia current-head notification was not delivered");
                assertEquals(publication, subscriber.authority().current(scope.digest()).orElseThrow());
            } finally {
                subscription.close();
            }
        }
    }

    @Test
    void currentHeadReadRecoversAfterRealOxiaRestartNotificationGap() throws Exception {
        final String gateValue = System.getenv("NEREUS_DELAY_OXIA_ROUTE_RESTART_GATE");
        Assumptions.assumeTrue(
                gateValue != null && !gateValue.isBlank(), "Oxia restart gate is not configured");
        final String endpoint = endpoint();
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String prefix = "nereus-delay-real-native-policy-restart/" + UUID.randomUUID();
        final var firstChange = new CountDownLatch(1);
        try (var subscriber = connect(endpoint, namespace, prefix, "restart-subscriber")) {
            final Closeable subscription = subscribe(subscriber, firstChange);
            try {
                final com.nereusstream.delay.semantic.TargetNativePolicyAuthority.Publication first;
                try (var publisher = connect(endpoint, namespace, prefix, "first-publisher")) {
                    first = publisher.authority().publish(
                            permission, actor, at(10), 0, snapshot(1, HandoffPolicyMode.ENABLED));
                }
                assertTrue(firstChange.await(15, TimeUnit.SECONDS), "pre-restart Oxia notification was not delivered");
                assertEquals(first, subscriber.authority().current(scope.digest()).orElseThrow());
                writeReadyMarker();
                awaitGate(Path.of(gateValue));

                final com.nereusstream.delay.semantic.TargetNativePolicyAuthority.Publication second;
                try (var publisher = connect(endpoint, namespace, prefix, "replacement-publisher")) {
                    second = publisher.authority().publish(
                            permission, actor, at(11), 1, snapshot(2, HandoffPolicyMode.ENABLED));
                }
                assertEquals(second, subscriber.authority().current(scope.digest()).orElseThrow());
                System.out.println("Oxia Target Native current-head reread recovered revision " + second.revision());
            } finally {
                subscription.close();
            }
        }
    }

    private OxiaSyncTargetNativePolicyAuthority.ClientHandle connect(
            final String endpoint, final String namespace, final String prefix, final String role) throws Exception {
        return OxiaSyncTargetNativePolicyAuthority.connect(
                endpoint,
                namespace,
                "nereus-delay-native-policy-" + role + "-" + UUID.randomUUID(),
                Duration.ofSeconds(10),
                prefix,
                trust(),
                (publisher, controlActor, at) -> true);
    }

    private Closeable subscribe(
            final OxiaSyncTargetNativePolicyAuthority.ClientHandle client, final CountDownLatch changed) {
        return client.authority().readAuthority().subscribeCurrentHeadChanges(changed::countDown);
    }

    private TargetNativePolicyTrust trust() {
        return new TargetNativePolicyTrust() {
            @Override
            public Optional<PublisherPermission> publisher(
                    final byte[] scopeDigest, final int keyGeneration, final SourcePosition at) {
                return java.util.Arrays.equals(scope.digest(), scopeDigest)
                        && keyGeneration == permission.keyGeneration()
                        ? Optional.of(permission)
                        : Optional.empty();
            }

            @Override
            public Optional<Activation> activation(final byte[] scopeDigest, final long generation) {
                return Optional.empty();
            }

            @Override
            public Optional<MemberApproval> member(
                    final byte[] grantDigest, final byte[] scopeDigest, final SourcePosition at) {
                return Optional.empty();
            }
        };
    }

    private TargetNativePolicySnapshot snapshot(final long generation, final HandoffPolicyMode mode) {
        final boolean disabled = mode == HandoffPolicyMode.DISABLED;
        return TargetNativePolicySnapshot.create(
                scope,
                generation,
                mode,
                disabled ? 0 : 30_000,
                1_000,
                90_000,
                disabled ? 0 : HandoffPath.MANAGED_HANDOFF,
                issued(),
                permission.keyGeneration(),
                issuer.getPrivate());
    }

    private TrustedUtcIntervalEvidence issued() {
        return new TrustedUtcIntervalEvidence(
                100,
                101,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("native-policy-real-oxia-test-clock"),
                1,
                2,
                3,
                bytes(32, 0x44),
                0,
                null);
    }

    private SourcePosition at(final long offset) {
        final var base = (KafkaSourcePosition) source;
        return new KafkaSourcePosition(
                base.shardId(),
                base.authenticatedClusterId(),
                base.nativeTopicUuid(),
                offset,
                base.leaderEpoch(),
                base.brokerLogAppendTimeEpochMs());
    }

    private KeyPair issuerKey() {
        try {
            final var factory = KeyFactory.getInstance("Ed25519");
            return new KeyPair(
                    factory.generatePublic(new X509EncodedKeySpec(Bytes.concat(
                            HexFormat.of().parseHex("302a300506032b6570032100"), hex(vectors, "issuer.public")))),
                    factory.generatePrivate(new PKCS8EncodedKeySpec(
                            Bytes.concat(
                                    HexFormat.of().parseHex("302e020100300506032b657004220420"),
                                    hex(vectors, "issuer.seed")))));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static void writeReadyMarker() throws Exception {
        final String ready = System.getenv("NEREUS_DELAY_OXIA_ROUTE_RESTART_READY");
        if (ready != null && !ready.isBlank()) {
            Files.writeString(Path.of(ready), "ready\n");
        }
    }

    private static void awaitGate(final Path gate) throws Exception {
        final long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (!Files.exists(gate) && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(50);
        }
        if (!Files.exists(gate)) {
            throw new IllegalStateException("Oxia Native policy restart gate was not released: " + gate);
        }
    }

    private static String endpoint() {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(), "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        return endpoint;
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static Properties load(final String name) {
        try (var input = OxiaRealTargetNativePolicySmokeTest.class.getResourceAsStream("/ndip3/" + name)) {
            final var properties = new Properties();
            properties.load(input);
            return properties;
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static byte[] hex(final Properties properties, final String key) {
        return HexFormat.of().parseHex(properties.getProperty(key));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
