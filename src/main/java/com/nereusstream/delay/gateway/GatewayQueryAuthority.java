package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.CommandQueryResponseV1;
import com.nereusstream.delay.protocol.MessageQueryResponseV1;
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
    CompletionStage<CommandQueryResponseV1> getCommandResult(
            AuthenticatedTenantContext tenant, GatewayGetCommandResultRequestV1 request);

    CompletionStage<List<CommandQueryResponseV1>> awaitApplied(
            AuthenticatedTenantContext tenant, GatewayAwaitAppliedRequestV1 request);

    CompletionStage<MessageQueryResponseV1> getMessage(
            AuthenticatedTenantContext tenant, GatewayGetMessageRequestV1 request);
}
