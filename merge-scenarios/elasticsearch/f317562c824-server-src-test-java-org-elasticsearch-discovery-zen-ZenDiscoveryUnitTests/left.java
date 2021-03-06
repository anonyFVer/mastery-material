package org.elasticsearch.discovery.zen;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.discovery.zen.PublishClusterStateActionTests.AssertingAckListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseOptions;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.core.IsInstanceOf;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.routing.RoutingTableTests.updateActiveAllocations;
import static org.elasticsearch.cluster.service.MasterServiceTests.discoveryState;
import static org.elasticsearch.discovery.zen.ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING;
import static org.elasticsearch.discovery.zen.ZenDiscovery.shouldIgnoreOrRejectNewClusterState;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;

public class ZenDiscoveryUnitTests extends ESTestCase {

    public void testShouldIgnoreNewClusterState() {
        ClusterName clusterName = new ClusterName("abc");
        DiscoveryNodes.Builder currentNodes = DiscoveryNodes.builder();
        currentNodes.masterNodeId("a").add(new DiscoveryNode("a", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT));
        DiscoveryNodes.Builder newNodes = DiscoveryNodes.builder();
        newNodes.masterNodeId("a").add(new DiscoveryNode("a", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT));
        ClusterState.Builder currentState = ClusterState.builder(clusterName);
        currentState.nodes(currentNodes);
        ClusterState.Builder newState = ClusterState.builder(clusterName);
        newState.nodes(newNodes);
        currentState.version(2);
        newState.version(1);
        assertTrue("should ignore, because new state's version is lower to current state's version", shouldIgnoreOrRejectNewClusterState(logger, currentState.build(), newState.build()));
        currentState.version(1);
        newState.version(1);
        assertTrue("should ignore, because new state's version is equal to current state's version", shouldIgnoreOrRejectNewClusterState(logger, currentState.build(), newState.build()));
        currentState.version(1);
        newState.version(2);
        assertFalse("should not ignore, because new state's version is higher to current state's version", shouldIgnoreOrRejectNewClusterState(logger, currentState.build(), newState.build()));
        currentNodes = DiscoveryNodes.builder();
        currentNodes.masterNodeId("b").add(new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT));
        if (randomBoolean()) {
            currentState.version(2);
            newState.version(1);
        } else {
            currentState.version(1);
            newState.version(2);
        }
        currentState.nodes(currentNodes);
        try {
            shouldIgnoreOrRejectNewClusterState(logger, currentState.build(), newState.build());
            fail("should ignore, because current state's master is not equal to new state's master");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("cluster state from a different master than the current one, rejecting"));
        }
        currentNodes = DiscoveryNodes.builder();
        currentNodes.masterNodeId(null);
        currentState.nodes(currentNodes);
        if (randomBoolean()) {
            currentState.version(2);
            newState.version(1);
        } else {
            currentState.version(1);
            newState.version(2);
        }
        assertFalse("should not ignore, because current state doesn't have a master", shouldIgnoreOrRejectNewClusterState(logger, currentState.build(), newState.build()));
    }

    public void testFilterNonMasterPingResponse() {
        ArrayList<ZenPing.PingResponse> responses = new ArrayList<>();
        ArrayList<DiscoveryNode> masterNodes = new ArrayList<>();
        ArrayList<DiscoveryNode> allNodes = new ArrayList<>();
        for (int i = randomIntBetween(10, 20); i >= 0; i--) {
            Set<Role> roles = new HashSet<>(randomSubsetOf(Arrays.asList(Role.values())));
            DiscoveryNode node = new DiscoveryNode("node_" + i, "id_" + i, buildNewFakeTransportAddress(), Collections.emptyMap(), roles, Version.CURRENT);
            responses.add(new ZenPing.PingResponse(node, randomBoolean() ? null : node, new ClusterName("test"), randomLong()));
            allNodes.add(node);
            if (node.isMasterNode()) {
                masterNodes.add(node);
            }
        }
        boolean ignore = randomBoolean();
        List<ZenPing.PingResponse> filtered = ZenDiscovery.filterPingResponses(responses, ignore, logger);
        final List<DiscoveryNode> filteredNodes = filtered.stream().map(ZenPing.PingResponse::node).collect(Collectors.toList());
        if (ignore) {
            assertThat(filteredNodes, equalTo(masterNodes));
        } else {
            assertThat(filteredNodes, equalTo(allNodes));
        }
    }

    public void testNodesUpdatedAfterClusterStatePublished() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        int minMasterNodes = randomBoolean() ? 3 : 1;
        Settings settings = Settings.builder().put(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), Integer.toString(minMasterNodes)).build();
        ArrayDeque<Closeable> toClose = new ArrayDeque<>();
        try {
            Set<DiscoveryNode> expectedFDNodes = null;
            final MockTransportService masterTransport = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, null);
            masterTransport.start();
            DiscoveryNode masterNode = masterTransport.getLocalNode();
            toClose.addFirst(masterTransport);
            ClusterState state = ClusterStateCreationUtils.state(masterNode, masterNode, masterNode);
            MasterService masterMasterService = ClusterServiceUtils.createMasterService(threadPool, masterNode);
            toClose.addFirst(masterMasterService);
            state = ClusterState.builder(discoveryState(masterMasterService).getClusterName()).nodes(state.nodes()).build();
            Settings settingsWithClusterName = Settings.builder().put(settings).put(ClusterName.CLUSTER_NAME_SETTING.getKey(), discoveryState(masterMasterService).getClusterName().value()).build();
            ZenDiscovery masterZen = buildZenDiscovery(settingsWithClusterName, masterTransport, masterMasterService, threadPool);
            masterZen.setCommittedState(state);
            toClose.addFirst(masterZen);
            masterTransport.acceptIncomingRequests();
            final MockTransportService otherTransport = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, null);
            otherTransport.start();
            toClose.addFirst(otherTransport);
            DiscoveryNode otherNode = otherTransport.getLocalNode();
            final ClusterState otherState = ClusterState.builder(discoveryState(masterMasterService).getClusterName()).nodes(DiscoveryNodes.builder().add(otherNode).localNodeId(otherNode.getId())).build();
            MasterService otherMasterService = ClusterServiceUtils.createMasterService(threadPool, otherNode);
            toClose.addFirst(otherMasterService);
            ZenDiscovery otherZen = buildZenDiscovery(settingsWithClusterName, otherTransport, otherMasterService, threadPool);
            otherZen.setCommittedState(otherState);
            toClose.addFirst(otherZen);
            otherTransport.acceptIncomingRequests();
            masterTransport.connectToNode(otherNode);
            otherTransport.connectToNode(masterNode);
            ClusterState newState = ClusterState.builder(discoveryState(masterMasterService)).incrementVersion().nodes(DiscoveryNodes.builder(state.nodes()).add(otherNode).masterNodeId(masterNode.getId())).build();
            ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent("testing", newState, state);
            AssertingAckListener listener = new AssertingAckListener(newState.nodes().getSize() - 1);
            expectedFDNodes = masterZen.getFaultDetectionNodes();
            AwaitingPublishListener awaitingPublishListener = new AwaitingPublishListener();
            masterZen.publish(clusterChangedEvent, awaitingPublishListener, listener);
            awaitingPublishListener.await();
            if (awaitingPublishListener.getException() == null) {
                listener.await(10, TimeUnit.SECONDS);
                expectedFDNodes = fdNodesForState(newState, masterNode);
            } else {
                assertEquals(3, minMasterNodes);
            }
            assertEquals(expectedFDNodes, masterZen.getFaultDetectionNodes());
        } finally {
            IOUtils.close(toClose);
            terminate(threadPool);
        }
    }

    public void testPendingCSQueueIsClearedWhenClusterStatePublished() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getClass().getName());
        int minMasterNodes = randomBoolean() ? 3 : 1;
        Settings settings = Settings.builder().put(DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), Integer.toString(minMasterNodes)).build();
        ArrayDeque<Closeable> toClose = new ArrayDeque<>();
        try {
            final MockTransportService masterTransport = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, null);
            masterTransport.start();
            DiscoveryNode masterNode = masterTransport.getLocalNode();
            toClose.addFirst(masterTransport);
            ClusterState state = ClusterStateCreationUtils.state(masterNode, null, masterNode);
            MasterService masterMasterService = ClusterServiceUtils.createMasterService(threadPool, masterNode);
            toClose.addFirst(masterMasterService);
            state = ClusterState.builder(discoveryState(masterMasterService).getClusterName()).nodes(state.nodes()).build();
            ZenDiscovery masterZen = buildZenDiscovery(settings, masterTransport, masterMasterService, threadPool);
            masterZen.setCommittedState(state);
            toClose.addFirst(masterZen);
            masterTransport.acceptIncomingRequests();
            masterZen.pendingClusterStatesQueue().addPending(ClusterState.builder(new ClusterName("foreign")).build());
            ClusterState newState = ClusterState.builder(discoveryState(masterMasterService)).incrementVersion().nodes(DiscoveryNodes.builder(discoveryState(masterMasterService).nodes()).masterNodeId(masterNode.getId())).build();
            ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent("testing", newState, state);
            AssertingAckListener listener = new AssertingAckListener(newState.nodes().getSize() - 1);
            AwaitingPublishListener awaitingPublishListener = new AwaitingPublishListener();
            masterZen.publish(clusterChangedEvent, awaitingPublishListener, listener);
            awaitingPublishListener.await();
            if (awaitingPublishListener.getException() == null) {
                listener.await(1, TimeUnit.HOURS);
            }
            assertThat(Arrays.toString(masterZen.pendingClusterStates()), masterZen.pendingClusterStates(), emptyArray());
        } finally {
            IOUtils.close(toClose);
            terminate(threadPool);
        }
    }

    private class AwaitingPublishListener implements ActionListener<Void> {

        private final CountDownLatch countDownLatch = new CountDownLatch(1);

        private FailedToCommitClusterStateException exception;

        @Override
        public synchronized void onResponse(Void aVoid) {
            assertThat(countDownLatch.getCount(), is(1L));
            countDownLatch.countDown();
        }

        @Override
        public synchronized void onFailure(Exception e) {
            assertThat(e, IsInstanceOf.instanceOf(FailedToCommitClusterStateException.class));
            exception = (FailedToCommitClusterStateException) e;
            onResponse(null);
        }

        public void await() throws InterruptedException {
            countDownLatch.await();
        }

        public synchronized FailedToCommitClusterStateException getException() {
            return exception;
        }
    }

    private ZenDiscovery buildZenDiscovery(Settings settings, TransportService service, MasterService masterService, ThreadPool threadPool) {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterApplier clusterApplier = new ClusterApplier() {

            @Override
            public void setInitialState(ClusterState initialState) {
            }

            @Override
            public ClusterState.Builder newClusterStateBuilder() {
                return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings));
            }

            @Override
            public void onNewClusterState(String source, Supplier<ClusterState> clusterStateSupplier, ClusterApplyListener listener) {
                listener.onSuccess(source);
            }
        };
        ZenDiscovery zenDiscovery = new ZenDiscovery(settings, threadPool, service, new NamedWriteableRegistry(ClusterModule.getNamedWriteables()), masterService, clusterApplier, clusterSettings, hostsResolver -> Collections.emptyList(), ESAllocationTestCase.createAllocationService(), Collections.emptyList());
        zenDiscovery.start();
        return zenDiscovery;
    }

    private Set<DiscoveryNode> fdNodesForState(ClusterState clusterState, DiscoveryNode localNode) {
        final Set<DiscoveryNode> discoveryNodes = new HashSet<>();
        clusterState.getNodes().getNodes().valuesIt().forEachRemaining(discoveryNode -> {
            if (discoveryNode.getId().equals(localNode.getId()) == false) {
                discoveryNodes.add(discoveryNode);
            }
        });
        return discoveryNodes;
    }

    public void testValidateOnUnsupportedIndexVersionCreated() throws Exception {
        final int iters = randomIntBetween(3, 10);
        for (int i = 0; i < iters; i++) {
            ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT);
            final DiscoveryNode otherNode = new DiscoveryNode("other_node", buildNewFakeTransportAddress(), emptyMap(), EnumSet.allOf(DiscoveryNode.Role.class), Version.CURRENT);
            final DiscoveryNode localNode = new DiscoveryNode("other_node", buildNewFakeTransportAddress(), emptyMap(), EnumSet.allOf(DiscoveryNode.Role.class), Version.CURRENT);
            MembershipAction.ValidateJoinRequestRequestHandler request = new MembershipAction.ValidateJoinRequestRequestHandler(() -> localNode, ZenDiscovery.addBuiltInJoinValidators(Collections.emptyList()));
            final boolean incompatible = randomBoolean();
            IndexMetaData indexMetaData = IndexMetaData.builder("test").settings(Settings.builder().put(SETTING_VERSION_CREATED, incompatible ? VersionUtils.getPreviousVersion(Version.CURRENT.minimumIndexCompatibilityVersion()) : VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT)).put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).put(SETTING_CREATION_DATE, System.currentTimeMillis())).state(IndexMetaData.State.OPEN).build();
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetaData.getIndex());
            RoutingTable.Builder routing = new RoutingTable.Builder();
            routing.addAsNew(indexMetaData);
            final ShardId shardId = new ShardId("test", "_na_", 0);
            IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
            final DiscoveryNode primaryNode = otherNode;
            indexShardRoutingBuilder.addShard(TestShardRouting.newShardRouting("test", 0, primaryNode.getId(), null, true, ShardRoutingState.INITIALIZING, new UnassignedInfo(UnassignedInfo.Reason.INDEX_REOPENED, "getting there")));
            indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder.build());
            IndexRoutingTable indexRoutingTable = indexRoutingTableBuilder.build();
            IndexMetaData updatedIndexMetaData = updateActiveAllocations(indexRoutingTable, indexMetaData);
            stateBuilder.metaData(MetaData.builder().put(updatedIndexMetaData, false).generateClusterUuidIfNeeded()).routingTable(RoutingTable.builder().add(indexRoutingTable).build());
            if (incompatible) {
                IllegalStateException ex = expectThrows(IllegalStateException.class, () -> request.messageReceived(new MembershipAction.ValidateJoinRequest(stateBuilder.build()), null, null));
                assertEquals("index [test] version not supported: " + VersionUtils.getPreviousVersion(Version.CURRENT.minimumIndexCompatibilityVersion()) + " minimum compatible index version is: " + Version.CURRENT.minimumIndexCompatibilityVersion(), ex.getMessage());
            } else {
                AtomicBoolean sendResponse = new AtomicBoolean(false);
                request.messageReceived(new MembershipAction.ValidateJoinRequest(stateBuilder.build()), new TransportChannel() {

                    @Override
                    public String getProfileName() {
                        return null;
                    }

                    @Override
                    public String getChannelType() {
                        return null;
                    }

                    @Override
                    public void sendResponse(TransportResponse response) throws IOException {
                        sendResponse.set(true);
                    }

                    @Override
                    public void sendResponse(TransportResponse response, TransportResponseOptions options) throws IOException {
                    }

                    @Override
                    public void sendResponse(Exception exception) throws IOException {
                    }
                }, null);
                assertTrue(sendResponse.get());
            }
        }
    }

    public void testIncomingClusterStateValidation() throws Exception {
        ClusterName clusterName = new ClusterName("abc");
        DiscoveryNodes.Builder currentNodes = DiscoveryNodes.builder().add(new DiscoveryNode("a", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT)).localNodeId("a");
        ClusterState previousState = ClusterState.builder(clusterName).nodes(currentNodes).build();
        logger.info("--> testing acceptances of any master when having no master");
        ClusterState state = ClusterState.builder(previousState).nodes(DiscoveryNodes.builder(previousState.nodes()).masterNodeId(randomAlphaOfLength(10))).incrementVersion().build();
        ZenDiscovery.validateIncomingState(logger, state, previousState);
        previousState = state;
        state = ClusterState.builder(previousState).nodes(DiscoveryNodes.builder(previousState.nodes()).masterNodeId("master")).build();
        logger.info("--> testing rejection of another master");
        try {
            ZenDiscovery.validateIncomingState(logger, state, previousState);
            fail("node accepted state from another master");
        } catch (IllegalStateException OK) {
            assertThat(OK.toString(), containsString("cluster state from a different master than the current one, rejecting"));
        }
        logger.info("--> test state from the current master is accepted");
        previousState = state;
        ZenDiscovery.validateIncomingState(logger, ClusterState.builder(previousState).nodes(DiscoveryNodes.builder(previousState.nodes()).masterNodeId("master")).incrementVersion().build(), previousState);
        logger.info("--> testing rejection of another cluster name");
        try {
            ZenDiscovery.validateIncomingState(logger, ClusterState.builder(new ClusterName(randomAlphaOfLength(10))).nodes(previousState.nodes()).build(), previousState);
            fail("node accepted state with another cluster name");
        } catch (IllegalStateException OK) {
            assertThat(OK.toString(), containsString("received state from a node that is not part of the cluster"));
        }
        logger.info("--> testing rejection of a cluster state with wrong local node");
        try {
            state = ClusterState.builder(previousState).nodes(DiscoveryNodes.builder(previousState.nodes()).localNodeId("_non_existing_").build()).incrementVersion().build();
            ZenDiscovery.validateIncomingState(logger, state, previousState);
            fail("node accepted state with non-existence local node");
        } catch (IllegalStateException OK) {
            assertThat(OK.toString(), containsString("received state with a local node that does not match the current local node"));
        }
        try {
            DiscoveryNode otherNode = new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
            state = ClusterState.builder(previousState).nodes(DiscoveryNodes.builder(previousState.nodes()).add(otherNode).localNodeId(otherNode.getId()).build()).incrementVersion().build();
            ZenDiscovery.validateIncomingState(logger, state, previousState);
            fail("node accepted state with existent but wrong local node");
        } catch (IllegalStateException OK) {
            assertThat(OK.toString(), containsString("received state with a local node that does not match the current local node"));
        }
        logger.info("--> testing acceptance of an old cluster state");
        final ClusterState incomingState = previousState;
        previousState = ClusterState.builder(previousState).incrementVersion().build();
        final ClusterState finalPreviousState = previousState;
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> ZenDiscovery.validateIncomingState(logger, incomingState, finalPreviousState));
        final String message = String.format(Locale.ROOT, "rejecting cluster state version [%d] uuid [%s] received from [%s]", incomingState.version(), incomingState.stateUUID(), incomingState.nodes().getMasterNodeId());
        assertThat(e, hasToString("java.lang.IllegalStateException: " + message));
        ClusterState higherVersionState = ClusterState.builder(previousState).incrementVersion().build();
        higherVersionState = ClusterState.builder(higherVersionState).nodes(DiscoveryNodes.builder(higherVersionState.nodes()).masterNodeId(null)).build();
        state = ClusterState.builder(previousState).nodes(DiscoveryNodes.builder(previousState.nodes()).masterNodeId("_new_master_").build()).build();
        ZenDiscovery.validateIncomingState(logger, state, higherVersionState);
    }
}