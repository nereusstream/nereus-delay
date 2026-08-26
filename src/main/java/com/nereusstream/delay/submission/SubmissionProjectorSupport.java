package com.nereusstream.delay.submission;

import com.nereusstream.delay.adapter.QueuedReceiptQueryPolicy;
import com.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.Retryability;
import com.nereusstream.delay.protocol.StableCode;

final class SubmissionProjectorSupport {
    private SubmissionProjectorSupport() {}

    static PreparedCommand managedCommand(final SubmissionTransportPlan plan) {
        if (!(plan.request() instanceof com.nereusstream.delay.adapter.KafkaProduceRequest
                || plan.request() instanceof com.nereusstream.delay.adapter.PulsarSendRequest)) {
            throw new IllegalArgumentException("managed plan has a non-managed request");
        }
        final byte[] frame = plan.submission().managedFrame();
        final PreparedCommand command = CommandCodec.decodeManagedFrame(frame);
        if (!java.util.Arrays.equals(frame, CommandCodec.encodeManagedFrame(command))) {
            throw new IllegalArgumentException("managed frame is not canonical");
        }
        return command;
    }

    static QueuedReceiptQueryPolicy queryPolicy(final ManagedRouteAuthority authority) {
        return new QueuedReceiptQueryPolicy(
                authority.historicalRoute().controlVersion(),
                authority.historicalRoute().queuedReceiptQueryWindowMs());
    }

    static StableCode managedCode(final int wireValue) {
        return WireIngressOutcomeSupport.managedCode(
                WireIngressOutcomeSupport.stableCode(wireValue, StableCode.INTEGRITY_ERROR));
    }

    static StableCode exactNativeRetryCode(final int wireValue) {
        try {
            final StableCode code = StableCode.fromWire(wireValue);
            return Retryability.forCode(code) == Retryability.RETRY_EXACT_BYTES
                    ? code
                    : StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN;
        } catch (IllegalArgumentException ignored) {
            return StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN;
        }
    }
}
