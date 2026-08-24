package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlAuthorV1;
import com.nereusstream.delay.protocol.ControlOperationRequestV1;
import com.nereusstream.delay.protocol.ControlReasonKindV1;
import com.nereusstream.delay.protocol.ControlReasonV1;
import com.nereusstream.delay.protocol.ControlTargetKindV1;
import com.nereusstream.delay.protocol.ControlTargetRefV1;
import com.nereusstream.delay.protocol.ForceCheckpointRequestV1;
import com.nereusstream.delay.protocol.PreparedControlOperationV1;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubjectV1;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class OxiaSyncControlTargetRegistrationBackendTest {
    @Test
    void registersExactPreparedBytesIdempotentlyAndReopensTheRecord() throws Exception {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncControlTargetRegistrationBackend backend =
                new OxiaSyncControlTargetRegistrationBackend(records, "delay/targets");
        final PreparedControlOperationV1 prepared = prepared(1);

        assertEquals(ControlTargetRegistrationAuthority.RegistrationResult.RECORDED, backend.register(prepared));
        assertEquals(
                ControlTargetRegistrationAuthority.RegistrationResult.ALREADY_RECORDED,
                backend.register(PreparedControlOperationV1.decode(prepared.canonicalBytes())));
        assertEquals(prepared, backend.find(prepared.operationId()).orElseThrow());
        final OxiaSyncControlTargetRegistrationBackend reopened =
                new OxiaSyncControlTargetRegistrationBackend(records, "delay/targets");
        assertEquals(prepared, reopened.find(prepared.operationId()).orElseThrow());
    }

    @Test
    void responseLossIsAcceptedOnlyAfterExactRereadAndConflictsFail() throws Exception {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncControlTargetRegistrationBackend backend =
                new OxiaSyncControlTargetRegistrationBackend(records, "delay/lost-target");
        final PreparedControlOperationV1 prepared = prepared(2);
        records.failNextPutAfterCommit = true;
        assertEquals(
                ControlTargetRegistrationAuthority.RegistrationResult.ALREADY_RECORDED, backend.register(prepared));

        final PreparedControlOperationV1 conflict = prepared(3);
        final PreparedControlOperationV1 sameIdConflict = PreparedControlOperationV1.prepare(
                prepared.operationId(),
                conflict.kind(),
                conflict.author(),
                conflict.request(),
                conflict.targets(),
                conflict.controlQueryPolicyVersion(),
                conflict.registrationRetryUntil(),
                conflict.signingKeyVersion(),
                KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPrivate());
        assertThrows(IllegalArgumentException.class, () -> backend.register(sameIdConflict));
    }

    @Test
    void sessionFenceRejectsACommittedRegistrationAfterTheMarkerChanges() throws Exception {
        final FakeRecordClient records = new FakeRecordClient();
        final AtomicBoolean sessionAlive = new AtomicBoolean(true);
        final OxiaSyncControlTargetRegistrationBackend backend =
                new OxiaSyncControlTargetRegistrationBackend(records, "delay/fenced-target", () -> {
                    if (!sessionAlive.get()) {
                        throw new IllegalStateException("simulated Oxia session fence");
                    }
                });
        final PreparedControlOperationV1 prepared = prepared(5);
        records.afterPut = () -> sessionAlive.set(false);

        assertThrows(IllegalStateException.class, () -> backend.register(prepared));

        final OxiaSyncControlTargetRegistrationBackend reopened =
                new OxiaSyncControlTargetRegistrationBackend(records, "delay/fenced-target");
        assertEquals(prepared, reopened.find(prepared.operationId()).orElseThrow());
    }

    @Test
    void corruptRecordFailsClosed() throws Exception {
        final FakeRecordClient records = new FakeRecordClient();
        final OxiaSyncControlTargetRegistrationBackend backend =
                new OxiaSyncControlTargetRegistrationBackend(records, "delay/bad-target");
        final PreparedControlOperationV1 prepared = prepared(4);
        records.putRaw("delay/bad-target/operation/" + Bytes.hex(prepared.operationId()), new byte[] {0x08, 0x02});
        assertThrows(IllegalStateException.class, () -> backend.find(prepared.operationId()));
    }

    private static PreparedControlOperationV1 prepared(final int seed) throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlOperationRequestV1 request = ControlOperationRequestV1.forceCheckpoint(
                new ForceCheckpointRequestV1(new ControlReasonV1(ControlReasonKindV1.MAINTENANCE, null, null)));
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, seed + 1)), seed);
        final ControlTargetRefV1 target =
                new ControlTargetRefV1(0, ControlTargetKindV1.SHARD, new ShardSubjectV1(shardId), null, null);
        return PreparedControlOperationV1.prepare(
                bytes(32, seed),
                request.kind(),
                new ControlAuthorV1(bytes(32, seed + 2), bytes(32, seed + 3), bytes(32, seed + 4)),
                request,
                List.of(target),
                1,
                2,
                1,
                keyPair.getPrivate());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static final class FakeRecordClient implements OxiaSyncControlTargetRegistrationBackend.RecordClient {
        private final Map<String, GetResult> records = new HashMap<>();
        private long nextVersion = 1;
        private boolean failNextPutAfterCommit;
        private Runnable afterPut = () -> {};

        @Override
        public GetResult get(final String key) {
            return records.get(key);
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            final GetResult current = records.get(key);
            final OptionVersionId condition = options.stream()
                    .filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast)
                    .findFirst()
                    .orElse(null);
            if (condition != null && condition.versionId() == OptionVersionId.KEY_NOT_EXISTS && current != null) {
                throw new KeyAlreadyExistsException(key);
            }
            if (condition != null
                    && condition.versionId() != OptionVersionId.KEY_NOT_EXISTS
                    && (current == null || current.version().versionId() != condition.versionId())) {
                throw new UnexpectedVersionIdException(
                        key,
                        current == null
                                ? OptionVersionId.KEY_NOT_EXISTS
                                : current.version().versionId());
            }
            final Version version = new Version(nextVersion++, 0, 0, 1, Optional.empty(), Optional.empty());
            records.put(key, new GetResult(key, Bytes.copy(value), version));
            afterPut.run();
            if (failNextPutAfterCommit) {
                failNextPutAfterCommit = false;
                throw new IllegalStateException("simulated response loss");
            }
            return new PutResult(key, version);
        }

        private void putRaw(final String key, final byte[] value) {
            final Version version = new Version(nextVersion++, 0, 0, 1, Optional.empty(), Optional.empty());
            records.put(key, new GetResult(key, Bytes.copy(value), version));
        }
    }
}
