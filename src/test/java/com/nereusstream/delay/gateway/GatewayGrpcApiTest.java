package com.nereusstream.delay.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.nereusstream.delay.gateway.wire.DelayGatewayGrpc;
import com.nereusstream.delay.gateway.wire.GatewayScheduleRequest;
import io.grpc.MethodDescriptor;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class GatewayGrpcApiTest {
    @Test
    void generatedDescriptorContainsEveryRpc() {
        Set<String> actual = DelayGatewayGrpc.getServiceDescriptor().getMethods().stream()
                .map(MethodDescriptor::getFullMethodName)
                .collect(Collectors.toSet());

        assertEquals(
                Set.of(
                        "nereus.delay.gateway.DelayGateway/Schedule",
                        "nereus.delay.gateway.DelayGateway/PrepareLargeSchedule",
                        "nereus.delay.gateway.DelayGateway/IssuePayloadUploadHandle",
                        "nereus.delay.gateway.DelayGateway/AttestPayloadUpload",
                        "nereus.delay.gateway.DelayGateway/CommitLargeSchedule",
                        "nereus.delay.gateway.DelayGateway/Cancel",
                        "nereus.delay.gateway.DelayGateway/Reschedule",
                        "nereus.delay.gateway.DelayGateway/RetryUncertain",
                        "nereus.delay.gateway.DelayGateway/GetCommandResult",
                        "nereus.delay.gateway.DelayGateway/AwaitApplied",
                        "nereus.delay.gateway.DelayGateway/GetMessage"),
                actual);
    }

    @Test
    void generatedMessageUsesTheFrozenJavaPackageAndProtoName() {
        assertEquals(
                "nereus.delay.gateway.GatewayScheduleRequest",
                GatewayScheduleRequest.getDescriptor().getFullName());
    }
}
