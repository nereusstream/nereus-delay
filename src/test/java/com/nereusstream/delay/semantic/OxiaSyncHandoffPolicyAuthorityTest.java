package com.nereusstream.delay.semantic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.HandoffPath;
import com.nereusstream.delay.protocol.HandoffPolicyHead;
import com.nereusstream.delay.protocol.HandoffPolicyMode;
import com.nereusstream.delay.protocol.HandoffPolicySnapshot;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class OxiaSyncHandoffPolicyAuthorityTest {
    @Test
    void publishesRereadsAndRejectsStaleCas() throws Exception {
        final FakeRecords records = new FakeRecords();
        final OxiaSyncHandoffPolicyAuthority authority = new OxiaSyncHandoffPolicyAuthority(records, "delay/policy");
        final HandoffPolicyHead first = head(1, HandoffPolicyMode.ENABLED);

        final HandoffPolicyAuthority.Publication published = authority.compareAndSet(first.scopeDigest(), 0, first);
        assertEquals(1, published.oxiaVersion());
        assertEquals(first, authority.requireCurrent(first.scopeDigest()).head());
        assertThrows(IllegalStateException.class, () -> authority.compareAndSet(first.scopeDigest(), 0, first));
        assertThrows(
                IllegalStateException.class,
                () -> authority.compareAndSet(
                        first.scopeDigest(), published.oxiaVersion(), head(1, HandoffPolicyMode.SHADOW)));

        final HandoffPolicyHead disabled = head(2, HandoffPolicyMode.DISABLED);
        final HandoffPolicyAuthority.Publication replacement =
                authority.compareAndSet(first.scopeDigest(), published.oxiaVersion(), disabled);
        assertEquals(2, replacement.oxiaVersion());
        assertEquals(disabled, authority.requireCurrent(first.scopeDigest()).head());
    }

    @Test
    void responseLossSucceedsOnlyAfterExactCanonicalReread() throws Exception {
        final FakeRecords records = new FakeRecords();
        final OxiaSyncHandoffPolicyAuthority authority =
                new OxiaSyncHandoffPolicyAuthority(records, "delay/policy-loss");
        final HandoffPolicyHead head = head(1, HandoffPolicyMode.ENABLED);

        records.failAfterCommit = true;
        assertEquals(head, authority.compareAndSet(head.scopeDigest(), 0, head).head());

        records.replaceAfterFailure = new byte[] {0x08, 0x02};
        records.failAfterCommit = true;
        assertThrows(
                RuntimeException.class,
                () -> authority.compareAndSet(head.scopeDigest(), 1, head(2, HandoffPolicyMode.DISABLED)));
    }

    private static HandoffPolicyHead head(final long generation, final HandoffPolicyMode mode) throws Exception {
        final byte[] scope = bytes(32, 1);
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final int paths = mode == HandoffPolicyMode.DISABLED ? 0 : HandoffPath.MANAGED_HANDOFF;
        final HandoffPolicySnapshot snapshot = HandoffPolicySnapshot.create(
                scope,
                generation,
                mode,
                mode == HandoffPolicyMode.DISABLED ? 0 : 100,
                1_000,
                2_000,
                paths,
                evidence(),
                1,
                bytes(32, 2),
                keys.getPrivate());
        return new HandoffPolicyHead(scope, generation, mode, snapshot, 0);
    }

    private static TrustedUtcIntervalEvidence evidence() {
        return new TrustedUtcIntervalEvidence(
                900,
                901,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("policy-test-clock"),
                1,
                1,
                1,
                bytes(32, 3),
                0,
                null);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static final class FakeRecords implements OxiaSyncHandoffPolicyAuthority.RecordClient {
        private final Map<String, GetResult> values = new HashMap<>();
        private long nextVersion;
        private boolean failAfterCommit;
        private byte[] replaceAfterFailure;

        @Override
        public GetResult get(final String key) {
            return values.get(key);
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
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
                if (replaceAfterFailure != null) {
                    values.put(key, new GetResult(key, Bytes.copy(replaceAfterFailure), version));
                    replaceAfterFailure = null;
                }
                throw new IllegalStateException("simulated response loss");
            }
            return new PutResult(key, version);
        }
    }
}
