package io.nereusstream.delay.submission;

import io.nereusstream.delay.adapter.QueuedReceiptQueryPolicy;
import io.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.RetryabilityV1;
import io.nereusstream.delay.protocol.StableCode;

final class SubmissionProjectorSupport {
    private SubmissionProjectorSupport() {
    }

    static PreparedCommand managedCommand(final SubmissionTransportPlan plan) {
        if (!(plan.request() instanceof io.nereusstream.delay.adapter.KafkaProduceRequest
                || plan.request() instanceof io.nereusstream.delay.adapter.PulsarSendRequest)) {
            throw new IllegalArgumentException("managed plan has a non-managed request");
        }
        final byte[] frame = plan.submission().managedFrame();
        final PreparedCommand command = CommandCodec.decodeFrameV1(frame);
        if (!java.util.Arrays.equals(frame, CommandCodec.encodeFrameV1(command))) {
            throw new IllegalArgumentException("managed frame is not canonical");
        }
        return command;
    }

    static QueuedReceiptQueryPolicy queryPolicy(final ManagedRouteAuthority authority) {
        return new QueuedReceiptQueryPolicy(authority.historicalRoute().controlVersion(),
                authority.historicalRoute().queuedReceiptQueryWindowMs());
    }

    static StableCode managedCode(final int wireValue) {
        return WireIngressOutcomeSupport.managedCode(
                WireIngressOutcomeSupport.stableCode(wireValue, StableCode.INTEGRITY_ERROR));
    }

    static StableCode exactNativeRetryCode(final int wireValue) {
        try {
            final StableCode code = StableCode.fromWire(wireValue);
            return RetryabilityV1.forCode(code) == RetryabilityV1.RETRY_EXACT_BYTES
                    ? code : StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN;
        } catch (IllegalArgumentException ignored) {
            return StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN;
        }
    }
}
