package com.nereusstream.delay.semantic;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
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

    private OxiaSyncTargetNativePolicyAuthority authority(final FakeRecords records) {
        final TargetNativePolicyTrust trust = new TargetNativePolicyTrust() {
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
        return new OxiaSyncTargetNativePolicyAuthority(records, "delay/policy", trust, (p, a, at) -> true);
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
