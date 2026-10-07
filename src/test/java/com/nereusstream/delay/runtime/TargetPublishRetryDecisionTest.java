package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DlqExportMode;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.protocol.UncertainPolicy;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TargetPublishRetryDecisionTest {
    @Test
    void typedUnknownMustMatchTheAdmittedAttemptWindowAndPinnedPolicy() {
        final var fixture = fixture();
        assertDoesNotThrow(() -> requireDecision(fixture, outcome(fixture, 105, 500, 1, fixture.policy().ref())));
        assertThrows(IllegalArgumentException.class,
                () -> requireDecision(fixture, outcome(fixture, 105, 500, 2, fixture.policy().ref())));
        assertThrows(IllegalArgumentException.class,
                () -> requireDecision(fixture, outcome(fixture, 106, 500, 1, fixture.policy().ref())));
        assertThrows(IllegalArgumentException.class,
                () -> requireDecision(fixture, outcome(fixture, 105, 499, 1, fixture.policy().ref())));
        assertThrows(IllegalArgumentException.class,
                () -> requireDecision(fixture, outcome(fixture, 105, 500, 1,
                        new RetryPolicyRef(Bytes.utf8("other-policy"), 1, fixture.policy().semanticHash()))));
    }

    private static void requireDecision(Fixture fixture, PublishOutcomeBody outcome) {
        TargetPublishOutcomeStore.requireRetryDecision(
                fixture.message(), fixture.admission(), fixture.admission(), fixture.policy(), outcome);
    }

    private record Fixture(
            TargetMessageRecord message, TargetPublishAdmissionBody admission, RetryPolicySemantic policy) {}

    private static Fixture fixture() {
        final var shard = new ShardId(new RouteIncarnation(repeated(16, 1)), 0);
        final var messageId = DelayMessageId.random(shard);
        final byte[] claimId = repeated(32, 2);
        final byte[] attemptId = SystemMutation.computePublishAttemptLogicalIdentity(claimId, messageId, 0, 1);
        final var owner = new OwnerIdentity(Bytes.utf8("deployment"), Bytes.utf8("run"), 7, repeated(32, 3));
        final var locator = new TargetMessageLocator(
                messageId, 0, new TargetPartitionId(repeated(32, 4)), new TargetKeyCodec.Domain(1, 1),
                repeated(16, 5), OrderingMode.BEST_EFFORT, null, repeated(32, 6));
        final var obligation = new AttemptObligationRef(
                attemptId, 0, AttemptLedgerState.PUBLISHING, KeyCodec.inflight((byte) 2, 7, attemptId));
        final long[] reserve = new long[CapacityDimension.COUNT];
        reserve[CapacityDimension.RESULT_BYTES.wireValue() - 1] = 256;
        final var admission = new TargetPublishAdmissionBody(
                shard, 10_000, owner, repeated(16, 7), claimId, locator, 1, attemptId, obligation,
                7, new CapacityVector(reserve), CapacityVector.empty(), time(100, 105));
        final var source = new KafkaSourcePosition(shard, "cluster", new UUID(1, 2), 0, null, 50);
        final var message = new TargetMessageRecord(
                locator, 2, 100, 500, 100, NativeDeliveryPolicy.FORBID, source,
                Bytes.utf8("payload"), null,
                new TargetGenerationRuntimeIndex(
                        0, GenerationAggregateState.PUBLISHING, CurrentSendWorkKind.PUBLISHING,
                        null, null, attemptId, List.of(obligation), 1, 0, false, 2));
        final var policy = new RetryPolicySemantic(
                Bytes.utf8("policy"), 1, 10, 100, 3, 60_000, UncertainPolicy.HOLD_FOR_EVIDENCE, 0,
                DlqExportMode.NOT_CONFIGURED, 0, 0, 0, 0, false, repeated(32, 8));
        return new Fixture(message, admission, policy);
    }

    private static PublishOutcomeBody outcome(
            Fixture fixture, long firstAttemptAt, long deadline, int completedAttempt, RetryPolicyRef policy) {
        final byte[] decision = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 5);
            CanonicalProtobuf.bytes(output, 2, policy.canonicalBytes());
            CanonicalProtobuf.uint32(output, 3, completedAttempt);
            CanonicalProtobuf.uint64(output, 4, firstAttemptAt);
            CanonicalProtobuf.uint64(output, 5, deadline);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, StableCode.RECOVERY_FIRST_SEND_UNCERTAIN.wireValue());
            CanonicalProtobuf.uint32(output, 9, 1);
        });
        final byte[] transfer = CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, 1, Bytes.utf8("unknown")));
        return PublishOutcomeBody.decode(PublishOutcomeBody.encodeInitial(
                fixture.admission().shard(), 10_000, fixture.admission().publishAttemptId(),
                3, 4, StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, null, transfer, time(110, 115), decision));
    }

    private static TrustedUtcIntervalEvidence time(long earliest, long latest) {
        return new TrustedUtcIntervalEvidence(
                earliest, latest, TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("host-clock"), 1, 1, 1, repeated(32, 9), 0, null);
    }

    private static byte[] repeated(int length, int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
