# gRPC Extension: Route-Aware Load Balancer

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.bridgewares/grpc-extension)](https://search.maven.org/artifact/io.github.bridgewares/grpc-extension)
[![GitHub](https://img.shields.io/badge/GitHub-View%20Source-green?logo=github)](https://github.com/bridgewares/grpc-extension)

A Java extension for gRPC that implements route-aware load balancing based on request headers. This extension enhances gRPC's default load balancing capabilities by allowing clients to route requests to specific servers using a `routeKey` header, while maintaining round-robin fallback behavior.

## Features

- **Route-Aware Load Balancing**: Route requests to specific servers based on the `routeKey` header
- **Round-Robin Fallback**: Default to round-robin load balancing when no `routeKey` is specified
- **Graceful Degradation**: Fall back to round-robin when the specified server in `routeKey` is unavailable
- **IPv4/IPv6 Support**: Supports both IPv4 and IPv6 address formats in route keys
- **Seamless Integration**: Works as a drop-in replacement for gRPC's default load balancer

## Architecture

This extension implements a custom gRPC `LoadBalancer` that extends the standard round-robin load balancer with routing capabilities:

- **RouteAwareRoundRobinLoadBalancer**: The main load balancer implementation that handles both route-aware and round-robin selection
- **RouteAwareLoadBalancerProvider**: Load balancer provider for registering and creating load balancer instances
- **ReadyPicker**: Selects subchannels based on route key or round-robin strategy
- **EmptyPicker**: Handles cases where no suitable subchannel is available

## Build and Installation

### Prerequisites

- Java 17 or higher
- Maven 3.6 or higher

### Building from Source

```bash
git clone https://github.com/bridgewares/grpc-extension.git
cd grpc-extension
mvn clean install
```

### Maven Dependency

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.github.bridgewares</groupId>
    <artifactId>grpc-extension</artifactId>
    <version>0.0.1</version>
</dependency>
```

## Usage

### Configuration

To use this load balancer, you need to configure it in your gRPC channel builder:

```java
import io.grpc.LoadBalancerRegistry;
import io.grpc.loadbalance.RouteAwareLoadBalancerProvider;

// Register the custom load balancer (if manual registration is needed)
LoadBalancerRegistry.getDefaultRegistry()
    .register(new RouteAwareLoadBalancerProvider());

// Use it in your channel builder
ManagedChannel channel = ManagedChannelBuilder.forTarget("your-service")
    .defaultLoadBalancingPolicy("round_robin")  // round_robin policy will automatically use our implementation
    .build();
```

### Route Key Format

The `routeKey` header should be in the format `host:port` or `[ipv6]:port`:

- **IPv4**: `192.168.1.1:8080`
- **IPv6**: `[::1]:8080`

### Example Usage

#### With Route Key

```java
// Create headers with route key
Metadata headers = new Metadata();
headers.put(Metadata.Key.of("routeKey", Metadata.ASCII_STRING_MARSHALLER), "192.168.1.1:8080");

// Make request with route key
responseStub.yourMethod(request, headers);
```

#### Without Route Key (Round Robin)

```java
// Make request without route key - uses round-robin
responseStub.yourMethod(request);
```

## Routing Logic

The load balancer follows this routing logic:

1. **Check for Route Key**: If the request contains a valid `routeKey` header:
   - Parse the host and port from the route key
   - Attempt to route to the specified server
   - If the server is available, route the request there
   - If the server is not available, fall back to round-robin

2. **No Route Key**: If no `routeKey` is present:
   - Use standard round-robin load balancing across all available servers

3. **Error Handling**:
   - Invalid route key format returns `INVALID_ARGUMENT` status
   - Non-existent server returns `NOT_FOUND` status
   - Network errors are handled gracefully with fallback to round-robin

## Project Structure

```
grpc-extension/
├── src/main/java/io/grpc/loadbalance/
│   ├── RouteAwareRoundRobinLoadBalancer.java    # Main load balancer implementation
│   └── RouteAwareLoadBalancerProvider.java       # Load balancer provider
├── pom.xml
├── README.md                                   # Chinese version
└── README.en.md                                # English version (this file)
```

## Development

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

### Testing

Run the tests using Maven:

```bash
mvn test
```

## Version History

- **0.0.1** - Initial release with route-aware round-robin load balancing

## License

This project is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for details.


## Acknowledgments

- Built upon the gRPC framework
- Inspired by the need for more flexible load balancing strategies in microservices architectures
- Thanks to the gRPC community for the excellent framework and documentation

---

**[Chinese Version](./README.md)** | English version