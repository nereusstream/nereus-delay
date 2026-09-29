package com.nereusstream.delay.semantic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlAuthor;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlRole;
import com.nereusstream.delay.protocol.ControlRoleSet;
import com.nereusstream.delay.protocol.HandoffPath;
import com.nereusstream.delay.protocol.HandoffPolicyMode;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetNativePolicyHead;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetNativePolicySnapshot;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.Notification;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import java.io.Closeable;
import java.lang.reflect.Proxy;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class OxiaSyncTargetNativePolicyAuthorityTest {
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
    void authenticatedPublishUsesPersistentRevisionCasAndRetainsDisableLeaseHighWater() {
        final FakeRecords records = new FakeRecords();
        final var authority = authority(records);
        final var first = authority.publish(permission, actor, at(10), 0, snapshot(1, HandoffPolicyMode.ENABLED));
        assertEquals(1, first.revision());
        assertEquals(first, authority.current(scope.digest()).orElseThrow());
        assertEquals(first, authority.readAuthority().current(scope.digest()).orElseThrow());
        assertThrows(
                UnsupportedOperationException.class,
                () -> authority.readAuthority().compareAndSet(scope.digest(), 1, first.head()));
        assertThrows(
                IllegalStateException.class,
                () -> authority.publish(permission, actor, at(10), 0, snapshot(1, HandoffPolicyMode.ENABLED)));
        assertThrows(
                IllegalArgumentException.class,
                () -> authority.publish(permission, actor, at(10), 1, snapshot(3, HandoffPolicyMode.DISABLED)));

        final var disabled = authority.publish(permission, actor, at(11), 1, snapshot(2, HandoffPolicyMode.DISABLED));
        assertEquals(2, disabled.revision());
        assertEquals(90_000, disabled.head().authorizedLeaseUntilEpochMs());
        assertEquals(disabled, authority.readAuthority().current(scope.digest()).orElseThrow());
    }

    @Test
    void responseLossIsRecoveredOnlyWhenTheExactHeadIsStillCurrent() {
        final FakeRecords records = new FakeRecords();
        final var authority = authority(records);
        final var snapshot = snapshot(1, HandoffPolicyMode.ENABLED);

        records.failAfterCommit = true;
        final var published = authority.publish(permission, actor, at(10), 0, snapshot);
        assertEquals(snapshot, published.head().snapshot());

        records.failAfterCommit = true;
        records.replacementAfterFailure = new byte[] {0x08, 0x02};
        assertThrows(
                IllegalStateException.class,
                () -> authority.publish(permission, actor, at(11), 1, snapshot(2, HandoffPolicyMode.DISABLED)));
    }

    @Test
    void concurrentPublicationCannotPassTheOxiaVersionCas() {
        final FakeRecords records = new FakeRecords();
        final var authority = authority(records);
        final var first = authority.publish(permission, actor, at(10), 0, snapshot(1, HandoffPolicyMode.ENABLED));
        final var competing = TargetNativePolicyHead.next(
                first.head(), snapshot(2, HandoffPolicyMode.DISABLED));
        final String key = "/delay/policy/target-native-policy/head/" + HexFormat.of().formatHex(scope.digest());
        records.beforeNextPut = () -> records.values.put(
                key,
                new GetResult(
                        key,
                        competing.canonicalBytes(),
                        new Version(records.nextVersion++, 0, 0, 1, Optional.empty(), Optional.empty())));

        assertThrows(
                IllegalStateException.class,
                () -> authority.publish(permission, actor, at(11), 1, snapshot(2, HandoffPolicyMode.DISABLED)));
        assertEquals(competing, authority.current(scope.digest()).orElseThrow().head());
        assertEquals(2, authority.current(scope.digest()).orElseThrow().revision());
    }

    @Test
    @SuppressWarnings("unchecked")
    void currentHeadNotificationsWakeOnlyForMatchingKeysAndCanDetach() throws Exception {
        final AtomicReference<Consumer<Notification>> notificationConsumer = new AtomicReference<>();
        final AtomicInteger registrations = new AtomicInteger();
        final SyncOxiaClient client = (SyncOxiaClient) Proxy.newProxyInstance(
                SyncOxiaClient.class.getClassLoader(),
                new Class<?>[] {SyncOxiaClient.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("notifications")) {
                        registrations.incrementAndGet();
                        notificationConsumer.set((Consumer<Notification>) arguments[0]);
                        return null;
                    }
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        final var authority = authority(client);
        final String headPrefix = "/delay/policy/target-native-policy/head/";
        final String exactHeadKey = headPrefix + "a".repeat(64);
        final String upperBound = headPrefix.substring(0, headPrefix.length() - 1) + '0';
        final AtomicInteger wakeups = new AtomicInteger();
        final Closeable subscription = authority.readAuthority().subscribeCurrentHeadChanges(wakeups::incrementAndGet);
        assertThrows(
                IllegalStateException.class,
                () -> authority.readAuthority().subscribeCurrentHeadChanges(() -> {}));

        final Consumer<Notification> emit = notificationConsumer.get();
        assertNotNull(emit);
        emit.accept(new Notification.KeyCreated(exactHeadKey, 1));
        emit.accept(new Notification.KeyModified(exactHeadKey, 2));
        emit.accept(new Notification.KeyDeleted(exactHeadKey));
        emit.accept(new Notification.KeyCreated(headPrefix + "A".repeat(64), 3));
        emit.accept(new Notification.KeyCreated(headPrefix + "a".repeat(63), 4));
        emit.accept(new Notification.KeyCreated("/delay/policy/other/" + "a".repeat(64), 5));
        emit.accept(new Notification.KeyRangeDelete("/delay/policy/", upperBound));
        emit.accept(new Notification.KeyRangeDelete("/delay/policy/", headPrefix));
        emit.accept(new Notification.KeyRangeDelete(upperBound, upperBound + "z"));
        assertEquals(4, wakeups.get());

        subscription.close();
        emit.accept(new Notification.KeyModified(exactHeadKey, 6));
        assertEquals(4, wakeups.get());

        final AtomicInteger resumedWakeups = new AtomicInteger();
        final Closeable resumed = authority.readAuthority()
                .subscribeCurrentHeadChanges(resumedWakeups::incrementAndGet);
        emit.accept(new Notification.KeyModified(exactHeadKey, 7));
        assertEquals(1, resumedWakeups.get());
        assertEquals(1, registrations.get());
        resumed.close();
    }

    private OxiaSyncTargetNativePolicyAuthority authority(final FakeRecords records) {
        return new OxiaSyncTargetNativePolicyAuthority(records, "delay/policy", trust(), (p, a, at) -> true);
    }

    private OxiaSyncTargetNativePolicyAuthority authority(final SyncOxiaClient client) {
        return new OxiaSyncTargetNativePolicyAuthority(client, "delay/policy", trust(), (p, a, at) -> true);
    }

    private TargetNativePolicyTrust trust() {
        return new TargetNativePolicyTrust() {
            @Override
            public Optional<PublisherPermission> publisher(byte[] digest, int keyGeneration, SourcePosition at) {
                return java.util.Arrays.equals(scope.digest(), digest) && keyGeneration == permission.keyGeneration()
                        ? Optional.of(permission)
                        : Optional.empty();
            }

            @Override
            public Optional<Activation> activation(byte[] digest, long generation) {
                return Optional.empty();
            }

            @Override
            public Optional<MemberApproval> member(byte[] grant, byte[] digest, SourcePosition at) {
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
                Bytes.utf8("native-policy-test-clock"),
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
                    factory.generatePrivate(new PKCS8EncodedKeySpec(Bytes.concat(
                            HexFormat.of().parseHex("302e020100300506032b657004220420"),
                            hex(vectors, "issuer.seed")))));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static Properties load(final String name) {
        try (var input = OxiaSyncTargetNativePolicyAuthorityTest.class.getResourceAsStream("/ndip3/" + name)) {
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

    private static final class FakeRecords implements OxiaSyncTargetNativePolicyAuthority.RecordClient {
        private final Map<String, GetResult> values = new HashMap<>();
        private long nextVersion;
        private boolean failAfterCommit;
        private byte[] replacementAfterFailure;
        private Runnable beforeNextPut;

        @Override
        public GetResult get(final String key) {
            return values.get(key);
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            if (beforeNextPut != null) {
                final Runnable race = beforeNextPut;
                beforeNextPut = null;
                race.run();
            }
            final GetResult current = values.get(key);
            final OptionVersionId condition = options.stream()
                    .filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast)
                    .findFirst()
                    .orElseThrow();
            if (condition.versionId() == OptionVersionId.KEY_NOT_EXISTS && current != null) {
                throw new KeyAlreadyExistsException(key);
            }
            if (condition.versionId() != OptionVersionId.KEY_NOT_EXISTS
                    && (current == null || current.version().versionId() != condition.versionId())) {
                throw new UnexpectedVersionIdException(
                        key,
                        current == null
                                ? OptionVersionId.KEY_NOT_EXISTS
                                : current.version().versionId());
            }
            final Version version = new Version(nextVersion++, 0, 0, 1, Optional.empty(), Optional.empty());
            values.put(key, new GetResult(key, Bytes.copy(value), version));
            if (failAfterCommit) {
                failAfterCommit = false;
                if (replacementAfterFailure != null) {
                    values.put(key, new GetResult(key, Bytes.copy(replacementAfterFailure), version));
                    replacementAfterFailure = null;
                }
                throw new IllegalStateException("simulated response loss");
            }
            return new PutResult(key, version);
        }
    }
}
