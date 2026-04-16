package io.grpc.loadbalance;

/*
 * Copyright 2016 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.grpc.Attributes;
import io.grpc.ConnectivityState;
import io.grpc.ConnectivityStateInfo;
import io.grpc.EquivalentAddressGroup;
import io.grpc.LoadBalancer;
import io.grpc.NameResolver;
import io.grpc.Status;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.grpc.ConnectivityState.CONNECTING;
import static io.grpc.ConnectivityState.IDLE;
import static io.grpc.ConnectivityState.READY;
import static io.grpc.ConnectivityState.SHUTDOWN;
import static io.grpc.ConnectivityState.TRANSIENT_FAILURE;

/**
 * A {@link LoadBalancer} that provides round-robin load-balancing over the {@link
 * EquivalentAddressGroup}s from the {@link NameResolver}.
 */
final class RouteAwareRoundRobinLoadBalancer extends LoadBalancer {
    private static final Logger logger = Logger.getLogger(RouteAwareRoundRobinLoadBalancer.class.getName());
    @VisibleForTesting
    static final Attributes.Key<RouteAwareRoundRobinLoadBalancer.Ref<ConnectivityStateInfo>> STATE_INFO =
            Attributes.Key.create("state-info");

    private final Helper helper;
    private final Map<EquivalentAddressGroup, Subchannel> subchannels = new HashMap<>();
    private final Map<String, Subchannel> addressToSubchannel = new HashMap<>();
    private final Random random;

    private ConnectivityState currentState;
    private RouteAwareRoundRobinLoadBalancer.RoundRobinPicker currentPicker = new RouteAwareRoundRobinLoadBalancer.EmptyPicker(EMPTY_OK);

    RouteAwareRoundRobinLoadBalancer(Helper helper) {
        this.helper = checkNotNull(helper, "helper");
        this.random = new Random();
    }

    @Override
    public boolean acceptResolvedAddresses(ResolvedAddresses resolvedAddresses) {
        if (resolvedAddresses.getAddresses().isEmpty()) {
            handleNameResolutionError(Status.UNAVAILABLE.withDescription(
                    "NameResolver returned no usable address. addrs=" + resolvedAddresses.getAddresses()
                            + ", attrs=" + resolvedAddresses.getAttributes()));
            return false;
        }

        List<EquivalentAddressGroup> servers = resolvedAddresses.getAddresses();
        Set<EquivalentAddressGroup> currentAddrs = subchannels.keySet();
        Map<EquivalentAddressGroup, EquivalentAddressGroup> latestAddrs = stripAttrs(servers);
        Set<EquivalentAddressGroup> removedAddrs = setsDifference(currentAddrs, latestAddrs.keySet());

        // Clear address map and rebuild it
        addressToSubchannel.clear();

        for (Map.Entry<EquivalentAddressGroup, EquivalentAddressGroup> latestEntry : latestAddrs.entrySet()) {
            EquivalentAddressGroup strippedAddressGroup = latestEntry.getKey();
            EquivalentAddressGroup originalAddressGroup = latestEntry.getValue();
            Subchannel existingSubchannel = subchannels.get(strippedAddressGroup);
            if (existingSubchannel != null) {
                // EAG's Attributes may have changed.
                existingSubchannel.updateAddresses(Collections.singletonList(originalAddressGroup));
                // Update address map entries for existing subchannel
                for (SocketAddress address : originalAddressGroup.getAddresses()) {
                    if (address instanceof InetSocketAddress) {
                        InetSocketAddress inetAddr = (InetSocketAddress) address;
                        String addrKey = inetAddr.getHostString() + ":" + inetAddr.getPort();
                        addressToSubchannel.put(addrKey, existingSubchannel);
                    }
                }
                continue;
            }
            // Create new subchannels for new addresses.

            // NB(lukaszx0): we don't merge `attributes` with `subchannelAttr` because subchannel
            // doesn't need them. They're describing the resolved server list but we're not taking
            // any action based on this information.
            Attributes.Builder subchannelAttrs = Attributes.newBuilder()
                    // NB(lukaszx0): because attributes are immutable we can't set new value for the key
                    // after creation but since we can mutate the values we leverage that and set
                    // AtomicReference which will allow mutating state info for given channel.
                    .set(STATE_INFO,
                            new RouteAwareRoundRobinLoadBalancer.Ref<>(ConnectivityStateInfo.forNonError(IDLE)));

            final Subchannel subchannel = checkNotNull(
                    helper.createSubchannel(CreateSubchannelArgs.newBuilder()
                            .setAddresses(originalAddressGroup)
                            .setAttributes(subchannelAttrs.build())
                            .build()),
                    "subchannel");
            subchannel.start(state -> processSubchannelState(subchannel, state));
            subchannels.put(strippedAddressGroup, subchannel);
            // Add to address map
            for (SocketAddress address : originalAddressGroup.getAddresses()) {
                if (address instanceof InetSocketAddress) {
                    InetSocketAddress inetAddr = (InetSocketAddress) address;
                    String addrKey = inetAddr.getHostString() + ":" + inetAddr.getPort();
                    addressToSubchannel.put(addrKey, subchannel);
                }
            }
            subchannel.requestConnection();
        }

        ArrayList<Subchannel> removedSubchannels = new ArrayList<>();
        for (EquivalentAddressGroup addressGroup : removedAddrs) {
            removedSubchannels.add(subchannels.remove(addressGroup));
        }

        // Update the picker before shutting down the subchannels, to reduce the chance of the race
        // between picking a subchannel and shutting it down.
        updateBalancingState();

        // Shutdown removed subchannels
        for (Subchannel removedSubchannel : removedSubchannels) {
            shutdownSubchannel(removedSubchannel);
        }

        return true;
    }

    @Override
    public void handleNameResolutionError(Status error) {
        if (currentState != READY) {
            updateBalancingState(TRANSIENT_FAILURE, new RouteAwareRoundRobinLoadBalancer.EmptyPicker(error));
        }
    }

    private void processSubchannelState(Subchannel subchannel, ConnectivityStateInfo stateInfo) {
        if (subchannels.get(stripAttrs(subchannel.getAddresses())) != subchannel) {
            return;
        }
        if (stateInfo.getState() == TRANSIENT_FAILURE || stateInfo.getState() == IDLE) {
            helper.refreshNameResolution();
        }
        if (stateInfo.getState() == IDLE) {
            subchannel.requestConnection();
        }
        RouteAwareRoundRobinLoadBalancer.Ref<ConnectivityStateInfo> subchannelStateRef = getSubchannelStateInfoRef(subchannel);
        if (subchannelStateRef.value.getState().equals(TRANSIENT_FAILURE)) {
            if (stateInfo.getState().equals(CONNECTING) || stateInfo.getState().equals(IDLE)) {
                return;
            }
        }
        subchannelStateRef.value = stateInfo;
        updateBalancingState();
    }

    private void shutdownSubchannel(Subchannel subchannel) {
        subchannel.shutdown();
        getSubchannelStateInfoRef(subchannel).value =
                ConnectivityStateInfo.forNonError(SHUTDOWN);
    }

    @Override
    public void shutdown() {
        for (Subchannel subchannel : getSubchannels()) {
            shutdownSubchannel(subchannel);
        }
        subchannels.clear();
        addressToSubchannel.clear();
    }

    private static final Status EMPTY_OK = Status.OK.withDescription("no subchannels ready");

    /**
     * Updates picker with the list of active subchannels (state == READY).
     */
    @SuppressWarnings("ReferenceEquality")
    private void updateBalancingState() {
        List<Subchannel> activeList = filterNonFailingSubchannels(getSubchannels());
        if (activeList.isEmpty()) {
            // No READY subchannels, determine aggregate state and error status
            boolean isConnecting = false;
            Status aggStatus = EMPTY_OK;
            for (Subchannel subchannel : getSubchannels()) {
                ConnectivityStateInfo stateInfo = getSubchannelStateInfoRef(subchannel).value;
                // This subchannel IDLE is not because of channel IDLE_TIMEOUT,
                // in which case LB is already shutdown.
                // RRLB will request connection immediately on subchannel IDLE.
                if (stateInfo.getState() == CONNECTING || stateInfo.getState() == IDLE) {
                    isConnecting = true;
                }
                if (aggStatus == EMPTY_OK || !aggStatus.isOk()) {
                    aggStatus = stateInfo.getStatus();
                }
            }
            updateBalancingState(isConnecting ? CONNECTING : TRANSIENT_FAILURE,
                    // If all subchannels are TRANSIENT_FAILURE, return the Status associated with
                    // an arbitrary subchannel, otherwise return OK.
                    new RouteAwareRoundRobinLoadBalancer.EmptyPicker(aggStatus));
        } else {
            // initialize the Picker to a random start index to ensure that a high frequency of Picker
            // churn does not skew subchannel selection.
            int startIndex = random.nextInt(activeList.size());
            updateBalancingState(READY, new RouteAwareRoundRobinLoadBalancer.ReadyPicker(subchannels, addressToSubchannel, activeList, startIndex));
        }
    }

    private void updateBalancingState(ConnectivityState state, RouteAwareRoundRobinLoadBalancer.RoundRobinPicker picker) {
        if (state != currentState || !picker.isEquivalentTo(currentPicker)) {
            helper.updateBalancingState(state, picker);
            currentState = state;
            currentPicker = picker;
        }
    }

    /**
     * Filters out non-ready subchannels.
     */
    private static List<Subchannel> filterNonFailingSubchannels(
            Collection<Subchannel> subchannels) {
        List<Subchannel> readySubchannels = new ArrayList<>(subchannels.size());
        for (Subchannel subchannel : subchannels) {
            if (isReady(subchannel)) {
                readySubchannels.add(subchannel);
            }
        }
        return readySubchannels;
    }

    /**
     * Converts list of {@link EquivalentAddressGroup} to {@link EquivalentAddressGroup} set and
     * remove all attributes. The values are the original EAGs.
     */
    private static Map<EquivalentAddressGroup, EquivalentAddressGroup> stripAttrs(
            List<EquivalentAddressGroup> groupList) {
        Map<EquivalentAddressGroup, EquivalentAddressGroup> addrs = new HashMap<>(groupList.size() * 2);
        for (EquivalentAddressGroup group : groupList) {
            addrs.put(stripAttrs(group), group);
        }
        return addrs;
    }

    private static EquivalentAddressGroup stripAttrs(EquivalentAddressGroup eag) {
        return new EquivalentAddressGroup(eag.getAddresses());
    }

    @VisibleForTesting
    Collection<Subchannel> getSubchannels() {
        return subchannels.values();
    }

    private static RouteAwareRoundRobinLoadBalancer.Ref<ConnectivityStateInfo> getSubchannelStateInfoRef(
            Subchannel subchannel) {
        return checkNotNull(subchannel.getAttributes().get(STATE_INFO), "STATE_INFO");
    }

    // package-private to avoid synthetic access
    static boolean isReady(Subchannel subchannel) {
        return getSubchannelStateInfoRef(subchannel).value.getState() == READY;
    }

    private static <T> Set<T> setsDifference(Set<T> a, Set<T> b) {
        Set<T> aCopy = new HashSet<>(a);
        aCopy.removeAll(b);
        return aCopy;
    }

    // Only subclasses are ReadyPicker or EmptyPicker
    private abstract static class RoundRobinPicker extends SubchannelPicker {
        abstract boolean isEquivalentTo(RouteAwareRoundRobinLoadBalancer.RoundRobinPicker picker);
    }

    @VisibleForTesting
    static final class ReadyPicker extends RouteAwareRoundRobinLoadBalancer.RoundRobinPicker {
        private static final AtomicIntegerFieldUpdater<RouteAwareRoundRobinLoadBalancer.ReadyPicker> indexUpdater =
                AtomicIntegerFieldUpdater.newUpdater(RouteAwareRoundRobinLoadBalancer.ReadyPicker.class, "index");

        private final List<Subchannel> list; // non-empty
        @SuppressWarnings("unused")
        private volatile int index;

        private final Map<EquivalentAddressGroup, Subchannel> subchannels;
        private final Map<String, Subchannel> addressToSubchannel;

        ReadyPicker(Map<EquivalentAddressGroup, Subchannel> subchannels, Map<String, Subchannel> addressToSubchannel, List<Subchannel> list, int startIndex) {
            Preconditions.checkArgument(!list.isEmpty(), "empty list");
            this.list = list;
            this.index = startIndex - 1;
            this.subchannels = subchannels;
            this.addressToSubchannel = addressToSubchannel;
        }

        @Override
        public PickResult pickSubchannel(PickSubchannelArgs args) {
            return nextRouteSubchannel(args);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(RouteAwareRoundRobinLoadBalancer.ReadyPicker.class).add("list", list).toString();
        }

        private PickResult nextRouteSubchannel(PickSubchannelArgs args) {
            if (args == null || args.getHeaders() == null) {
                return nextSubchannel();
            }
            logger.log(Level.INFO, "Channel addresses: {0}", addressToSubchannel.keySet());
            String routeKey = args.getHeaders().get(io.grpc.constant.GrpcConstant.ROUTE_KEY_METADATA);
            logger.log(Level.INFO, "Routing to specific server: {0}", routeKey);
            if (routeKey == null || routeKey.isEmpty()) {
                return nextSubchannel();
            }

            int lastColonIndex = routeKey.lastIndexOf(':');
            if (lastColonIndex == -1) {
                return LoadBalancer.PickResult.withError(Status.INVALID_ARGUMENT.withDescription("Route key must be in format 'host:port' or '[ipv6]:port', but given: " + routeKey));
            }

            String host;
            int port;
            try {
                String portStr;
                // IPv6 [::1]:8080
                if (routeKey.startsWith("[")) {
                    int closingBracketIndex = routeKey.indexOf(']');
                    if (closingBracketIndex == -1 || closingBracketIndex != lastColonIndex - 1) {
                        return LoadBalancer.PickResult.withError(Status.INVALID_ARGUMENT.withDescription("Invalid IPv6 format in route key: " + routeKey));
                    }
                    host = routeKey.substring(1, closingBracketIndex);
                } else {
                    host = routeKey.substring(0, lastColonIndex);
                }
                portStr = routeKey.substring(lastColonIndex + 1);
                port = Integer.parseInt(portStr);
            } catch (Exception e) {
                return LoadBalancer.PickResult.withError(Status.INVALID_ARGUMENT.withDescription("Invalid port in route key: " + routeKey));
            }

            Subchannel subchannel = getSubchannel(host, port);
            if (subchannel == null) {
                return LoadBalancer.PickResult.withError(Status.NOT_FOUND.withDescription("No route server found for: " + routeKey));
            }
            return PickResult.withSubchannel(subchannel);
        }

        private Subchannel getSubchannel(String host, int port) {
            String addrKey = host + ":" + port;
            Subchannel subchannel = addressToSubchannel.get(addrKey);
            if (subchannel != null) {
                return subchannel;
            }

            for (Entry<EquivalentAddressGroup, Subchannel> entry : subchannels.entrySet()) {
                EquivalentAddressGroup addressGroup = entry.getKey();
                List<SocketAddress> addresses = addressGroup.getAddresses();
                for (SocketAddress address : addresses) {
                    if (!(address instanceof InetSocketAddress)) {
                        continue;
                    }
                    InetSocketAddress inetSocketAddress = (InetSocketAddress) address;
                    boolean equalHost = Objects.equal(inetSocketAddress.getHostName(), host) || Objects.equal(inetSocketAddress.getHostString(), host);
                    boolean equalPort = inetSocketAddress.getPort() == port;
                    if (equalHost && equalPort) {
                        return entry.getValue();
                    }
                }
            }
            return null;
        }

        private PickResult nextSubchannel() {
            int size = list.size();
            int i = indexUpdater.incrementAndGet(this);
            if (i >= size) {
                int oldi = i;
                i %= size;
                indexUpdater.compareAndSet(this, oldi, i);
            }
            return PickResult.withSubchannel(list.get(i));
        }

        @VisibleForTesting
        List<Subchannel> getList() {
            return list;
        }

        @Override
        boolean isEquivalentTo(RouteAwareRoundRobinLoadBalancer.RoundRobinPicker picker) {
            if (!(picker instanceof RouteAwareRoundRobinLoadBalancer.ReadyPicker)) {
                return false;
            }
            RouteAwareRoundRobinLoadBalancer.ReadyPicker other = (RouteAwareRoundRobinLoadBalancer.ReadyPicker) picker;
            // the lists cannot contain duplicate subchannels
            return other == this
                    || (list.size() == other.list.size() && new HashSet<>(list).containsAll(other.list));
        }
    }

    @VisibleForTesting
    static final class EmptyPicker extends RouteAwareRoundRobinLoadBalancer.RoundRobinPicker {

        private final Status status;

        EmptyPicker(@Nonnull Status status) {
            this.status = Preconditions.checkNotNull(status, "status");
        }

        @Override
        public PickResult pickSubchannel(PickSubchannelArgs args) {
            return status.isOk() ? PickResult.withNoResult() : PickResult.withError(status);
        }

        @Override
        boolean isEquivalentTo(RouteAwareRoundRobinLoadBalancer.RoundRobinPicker picker) {
            return picker instanceof RouteAwareRoundRobinLoadBalancer.EmptyPicker
                    && (Objects.equal(status, ((RouteAwareRoundRobinLoadBalancer.EmptyPicker) picker).status)
                    || (status.isOk() && ((RouteAwareRoundRobinLoadBalancer.EmptyPicker) picker).status.isOk()));
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(RouteAwareRoundRobinLoadBalancer.EmptyPicker.class).add("status", status).toString();
        }
    }

    /**
     * A lighter weight Reference than AtomicReference.
     */
    @VisibleForTesting
    static final class Ref<T> {
        T value;

        Ref(T value) {
            this.value = value;
        }
    }
}

