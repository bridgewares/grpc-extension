package io.grpc.constant;

import io.grpc.Metadata;

public interface GrpcConstant {

    /**
     * routeKey
     */
    String ROUTE_KEY = "routeKey";

    /**
     * Metadata key for routeKey
     */
    Metadata.Key<String> ROUTE_KEY_METADATA = Metadata.Key.of(ROUTE_KEY, Metadata.ASCII_STRING_MARSHALLER);

}
