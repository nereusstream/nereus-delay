package com.nereusstream.delay.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.nereusstream.delay.gateway.v1.DelayGatewayV1Grpc;
import com.nereusstream.delay.gateway.v1.GatewayScheduleRequestV1;
import io.grpc.MethodDescriptor;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class GatewayGrpcApiTest {
    @Test
    void generatedDescriptorContainsEveryV1Rpc() {
        Set<String> actual = DelayGatewayV1Grpc.getServiceDescriptor().getMethods().stream()
                .map(MethodDescriptor::getFullMethodName)
                .collect(Collectors.toSet());

        assertEquals(
                Set.of(
                        "nereus.delay.gateway.v1.DelayGatewayV1/Schedule",
                        "nereus.delay.gateway.v1.DelayGatewayV1/PrepareLargeSchedule",
                        "nereus.delay.gateway.v1.DelayGatewayV1/IssuePayloadUploadHandle",
                        "nereus.delay.gateway.v1.DelayGatewayV1/AttestPayloadUpload",
                        "nereus.delay.gateway.v1.DelayGatewayV1/CommitLargeSchedule",
                        "nereus.delay.gateway.v1.DelayGatewayV1/Cancel",
                        "nereus.delay.gateway.v1.DelayGatewayV1/Reschedule",
                        "nereus.delay.gateway.v1.DelayGatewayV1/RetryUncertain",
                        "nereus.delay.gateway.v1.DelayGatewayV1/GetCommandResult",
                        "nereus.delay.gateway.v1.DelayGatewayV1/AwaitApplied",
                        "nereus.delay.gateway.v1.DelayGatewayV1/GetMessage"),
                actual);
    }

    @Test
    void generatedMessageUsesTheFrozenJavaPackageAndProtoName() {
        assertEquals(
                "nereus.delay.gateway.v1.GatewayScheduleRequestV1",
                GatewayScheduleRequestV1.getDescriptor().getFullName());
    }
}
