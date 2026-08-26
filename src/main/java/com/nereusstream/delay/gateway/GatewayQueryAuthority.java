package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.CommandQueryResponse;
import com.nereusstream.delay.protocol.MessageQueryResponse;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Tenant-bound query/application authority consumed by Gateway query RPCs.
 * Implementations own receipt binding, Route/Profile resolution, source/store
 * reads and the await deadline policy; Gateway transports only canonicalize
 * locators and responses.
 */
public interface GatewayQueryAuthority {
    CompletionStage<CommandQueryResponse> getCommandResult(
            AuthenticatedTenantContext tenant, GatewayGetCommandResultRequest request);

    CompletionStage<List<CommandQueryResponse>> awaitApplied(
            AuthenticatedTenantContext tenant, GatewayAwaitAppliedRequest request);

    CompletionStage<MessageQueryResponse> getMessage(
            AuthenticatedTenantContext tenant, GatewayGetMessageRequest request);
}
