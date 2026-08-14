package io.nereusstream.delay.gateway;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.Objects;

/** Binds transport-owned gRPC metadata and attributes to the current RPC. */
public final class GatewayGrpcContext {
    private static final Context.Key<GatewayPeerContext> PEER_CONTEXT =
            Context.key("nereus-delay-gateway-peer-context");

    private GatewayGrpcContext() {
    }

    /** Returns a provider that fails closed when called outside an intercepted RPC. */
    public static GatewayPeerContextProvider provider() {
        return () -> {
            final GatewayPeerContext peerContext = PEER_CONTEXT.get();
            if (peerContext == null) {
                throw Status.UNAUTHENTICATED.withDescription("Gateway peer context is unavailable")
                        .asRuntimeException();
            }
            return peerContext;
        };
    }

    /** Captures headers/transport attributes without copying them into V1 records. */
    public static ServerInterceptor capturePeerContext() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    final ServerCall<ReqT, RespT> call, final Metadata headers,
                    final ServerCallHandler<ReqT, RespT> next) {
                Objects.requireNonNull(call, "call");
                Objects.requireNonNull(headers, "headers");
                final GatewayPeerContext peerContext = new GatewayPeerContext(headers, call.getAttributes());
                return Contexts.interceptCall(Context.current().withValue(PEER_CONTEXT, peerContext), call, headers,
                        next);
            }
        };
    }
}
