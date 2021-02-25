package org.elasticsearch.action.support.broadcast.node;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseOptions;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.object.HasToString.hasToString;

public class TransportBroadcastByNodeActionTests extends ESTestCase {

    private static final String TEST_INDEX = "test-index";

    private static final String TEST_CLUSTER = "test-cluster";

    private static ThreadPool THREAD_POOL;

    private ClusterService clusterService;

    private CapturingTransport transport;

    private TestTransportBroadcastByNodeAction action;

    public static class Request extends BroadcastRequest<Request> {

        public Request() {
        }

        public Request(String[] indices) {
            super(indices);
        }
    }

    public static class Response extends BroadcastResponse {

        public Response() {
        }

        public Response(int totalShards, int successfulShards, int failedShards, List<DefaultShardOperationFailedException> shardFailures) {
            super(totalShards, successfulShards, failedShards, shardFailures);
        }
    }

    class TestTransportBroadcastByNodeAction extends TransportBroadcastByNodeAction<Request, Response, TransportBroadcastByNodeAction.EmptyResult> {

        private final Map<ShardRouting, Object> shards = new HashMap<>();

        TestTransportBroadcastByNodeAction(TransportService transportService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request, String executor) {
            super("indices:admin/test", TransportBroadcastByNodeActionTests.this.clusterService, transportService, actionFilters, indexNameExpressionResolver, request, executor);
        }

        @Override
        protected EmptyResult readShardResult(StreamInput in) throws IOException {
            return EmptyResult.readEmptyResultFrom(in);
        }

        @Override
        protected Response newResponse(Request request, int totalShards, int successfulShards, int failedShards, List<EmptyResult> emptyResults, List<DefaultShardOperationFailedException> shardFailures, ClusterState clusterState) {
            return new Response(totalShards, successfulShards, failedShards, shardFailures);
        }

        @Override
        protected Request readRequestFrom(StreamInput in) throws IOException {
            final Request request = new Request();
            request.readFrom(in);
            return request;
        }

        @Override
        protected EmptyResult shardOperation(Request request, ShardRouting shardRouting) {
            if (rarely()) {
                shards.put(shardRouting, Boolean.TRUE);
                return EmptyResult.INSTANCE;
            } else {
                ElasticsearchException e = new ElasticsearchException("operation failed");
                shards.put(shardRouting, e);
                throw e;
            }
        }

        @Override
        protected ShardsIterator shards(ClusterState clusterState, Request request, String[] concreteIndices) {
            return clusterState.routingTable().allShards(new String[] { TEST_INDEX });
        }

        @Override
        protected ClusterBlockException checkGlobalBlock(ClusterState state, Request request) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected ClusterBlockException checkRequestBlock(ClusterState state, Request request, String[] concreteIndices) {
            return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
        }

        public Map<ShardRouting, Object> getResults() {
            return shards;
        }
    }

    class MyResolver extends IndexNameExpressionResolver {

        @Override
        public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
            return request.indices();
        }
    }

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new TestThreadPool(TransportBroadcastByNodeActionTests.class.getSimpleName());
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = createClusterService(THREAD_POOL);
        TransportService transportService = transport.createTransportService(clusterService.getSettings(), THREAD_POOL, TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> clusterService.localNode(), null, Collections.emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();
        setClusterState(clusterService, TEST_INDEX);
        action = new TestTransportBroadcastByNodeAction(transportService, new ActionFilters(new HashSet<>()), new MyResolver(), Request::new, ThreadPool.Names.SAME);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    void setClusterState(ClusterService clusterService, String index) {
        int numberOfNodes = randomIntBetween(3, 5);
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(new Index(index, "_na_"));
        int shardIndex = -1;
        int totalIndexShards = 0;
        for (int i = 0; i < numberOfNodes; i++) {
            final DiscoveryNode node = newNode(i);
            discoBuilder = discoBuilder.add(node);
            int numberOfShards = randomIntBetween(1, 10);
            totalIndexShards += numberOfShards;
            for (int j = 0; j < numberOfShards; j++) {
                final ShardId shardId = new ShardId(index, "_na_", ++shardIndex);
                ShardRouting shard = TestShardRouting.newShardRouting(index, shardId.getId(), node.getId(), true, ShardRoutingState.STARTED);
                IndexShardRoutingTable.Builder indexShard = new IndexShardRoutingTable.Builder(shardId);
                indexShard.addShard(shard);
                indexRoutingTable.addIndexShard(indexShard.build());
            }
        }
        discoBuilder.localNodeId(newNode(0).getId());
        discoBuilder.masterNodeId(newNode(numberOfNodes - 1).getId());
        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName(TEST_CLUSTER));
        stateBuilder.nodes(discoBuilder);
        final IndexMetaData.Builder indexMetaData = IndexMetaData.builder(index).settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)).numberOfReplicas(0).numberOfShards(totalIndexShards);
        stateBuilder.metaData(MetaData.builder().put(indexMetaData));
        stateBuilder.routingTable(RoutingTable.builder().add(indexRoutingTable.build()).build());
        ClusterState clusterState = stateBuilder.build();
        setState(clusterService, clusterState);
    }

    static DiscoveryNode newNode(int nodeId) {
        return new DiscoveryNode("node_" + nodeId, buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
    }

    @AfterClass
    public static void destroyThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        THREAD_POOL = null;
    }

    public void testGlobalBlock() {
        Request request = new Request(new String[] { TEST_INDEX });
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ClusterBlocks.Builder block = ClusterBlocks.builder().addGlobalBlock(new ClusterBlock(1, "test-block", false, true, false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        setState(clusterService, ClusterState.builder(clusterService.state()).blocks(block));
        try {
            action.new AsyncAction(null, request, listener).start();
            fail("expected ClusterBlockException");
        } catch (ClusterBlockException expected) {
            assertEquals("blocked by: [SERVICE_UNAVAILABLE/1/test-block];", expected.getMessage());
        }
    }

    public void testRequestBlock() {
        Request request = new Request(new String[] { TEST_INDEX });
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ClusterBlocks.Builder block = ClusterBlocks.builder().addIndexBlock(TEST_INDEX, new ClusterBlock(1, "test-block", false, true, false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        setState(clusterService, ClusterState.builder(clusterService.state()).blocks(block));
        try {
            action.new AsyncAction(null, request, listener).start();
            fail("expected ClusterBlockException");
        } catch (ClusterBlockException expected) {
            assertEquals("blocked by: [SERVICE_UNAVAILABLE/1/test-block];", expected.getMessage());
        }
    }

    public void testOneRequestIsSentToEachNodeHoldingAShard() {
        Request request = new Request(new String[] { TEST_INDEX });
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        ShardsIterator shardIt = clusterService.state().routingTable().allShards(new String[] { TEST_INDEX });
        Set<String> set = new HashSet<>();
        for (ShardRouting shard : shardIt) {
            set.add(shard.currentNodeId());
        }
        assertEquals(set.size(), capturedRequests.size());
        assertEquals(set, capturedRequests.keySet());
        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            assertEquals(1, entry.getValue().size());
        }
    }

    public void testRequestsAreNotSentToFailedMaster() {
        Request request = new Request(new String[] { TEST_INDEX });
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        DiscoveryNode masterNode = clusterService.state().nodes().getMasterNode();
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder(clusterService.state().getNodes());
        builder.remove(masterNode.getId());
        setState(clusterService, ClusterState.builder(clusterService.state()).nodes(builder));
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        ShardsIterator shardIt = clusterService.state().routingTable().allShards(new String[] { TEST_INDEX });
        Set<String> set = new HashSet<>();
        for (ShardRouting shard : shardIt) {
            if (!shard.currentNodeId().equals(masterNode.getId())) {
                set.add(shard.currentNodeId());
            }
        }
        assertEquals(set.size(), capturedRequests.size());
        assertEquals(set, capturedRequests.keySet());
        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            assertEquals(1, entry.getValue().size());
        }
    }

    public void testOperationExecution() throws Exception {
        ShardsIterator shardIt = clusterService.state().routingTable().allShards(new String[] { TEST_INDEX });
        Set<ShardRouting> shards = new HashSet<>();
        String nodeId = shardIt.iterator().next().currentNodeId();
        for (ShardRouting shard : shardIt) {
            if (nodeId.equals(shard.currentNodeId())) {
                shards.add(shard);
            }
        }
        final TransportBroadcastByNodeAction.BroadcastByNodeTransportRequestHandler handler = action.new BroadcastByNodeTransportRequestHandler();
        TestTransportChannel channel = new TestTransportChannel();
        handler.messageReceived(action.new NodeRequest(nodeId, new Request(), new ArrayList<>(shards)), channel, null);
        assertEquals(shards, action.getResults().keySet());
        TransportResponse response = channel.getCapturedResponse();
        assertTrue(response instanceof TransportBroadcastByNodeAction.NodeResponse);
        TransportBroadcastByNodeAction.NodeResponse nodeResponse = (TransportBroadcastByNodeAction.NodeResponse) response;
        assertEquals("node id", nodeId, nodeResponse.getNodeId());
        int successfulShards = 0;
        int failedShards = 0;
        for (Object result : action.getResults().values()) {
            if (!(result instanceof ElasticsearchException)) {
                successfulShards++;
            } else {
                failedShards++;
            }
        }
        assertEquals("successful shards", successfulShards, nodeResponse.getSuccessfulShards());
        assertEquals("total shards", action.getResults().size(), nodeResponse.getTotalShards());
        assertEquals("failed shards", failedShards, nodeResponse.getExceptions().size());
        List<BroadcastShardOperationFailedException> exceptions = nodeResponse.getExceptions();
        for (BroadcastShardOperationFailedException exception : exceptions) {
            assertThat(exception.getMessage(), is("operation indices:admin/test failed"));
            assertThat(exception, hasToString(containsString("operation failed")));
        }
    }

    public void testResultAggregation() throws ExecutionException, InterruptedException {
        Request request = new Request(new String[] { TEST_INDEX });
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        final boolean simulateFailedMasterNode = rarely();
        DiscoveryNode failedMasterNode = null;
        if (simulateFailedMasterNode) {
            failedMasterNode = clusterService.state().nodes().getMasterNode();
            DiscoveryNodes.Builder builder = DiscoveryNodes.builder(clusterService.state().getNodes());
            builder.remove(failedMasterNode.getId());
            builder.masterNodeId(null);
            setState(clusterService, ClusterState.builder(clusterService.state()).nodes(builder));
        }
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        ShardsIterator shardIt = clusterService.state().getRoutingTable().allShards(new String[] { TEST_INDEX });
        Map<String, List<ShardRouting>> map = new HashMap<>();
        for (ShardRouting shard : shardIt) {
            if (!map.containsKey(shard.currentNodeId())) {
                map.put(shard.currentNodeId(), new ArrayList<>());
            }
            map.get(shard.currentNodeId()).add(shard);
        }
        int totalShards = 0;
        int totalSuccessfulShards = 0;
        int totalFailedShards = 0;
        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            List<BroadcastShardOperationFailedException> exceptions = new ArrayList<>();
            long requestId = entry.getValue().get(0).requestId;
            if (rarely()) {
                totalShards += map.get(entry.getKey()).size();
                totalFailedShards += map.get(entry.getKey()).size();
                transport.handleRemoteError(requestId, new Exception());
            } else {
                List<ShardRouting> shards = map.get(entry.getKey());
                List<TransportBroadcastByNodeAction.EmptyResult> shardResults = new ArrayList<>();
                for (ShardRouting shard : shards) {
                    totalShards++;
                    if (rarely()) {
                        totalFailedShards++;
                        exceptions.add(new BroadcastShardOperationFailedException(shard.shardId(), "operation indices:admin/test failed"));
                    } else {
                        shardResults.add(TransportBroadcastByNodeAction.EmptyResult.INSTANCE);
                    }
                }
                totalSuccessfulShards += shardResults.size();
                TransportBroadcastByNodeAction.NodeResponse nodeResponse = action.new NodeResponse(entry.getKey(), shards.size(), shardResults, exceptions);
                transport.handleResponse(requestId, nodeResponse);
            }
        }
        if (simulateFailedMasterNode) {
            totalShards += map.get(failedMasterNode.getId()).size();
        }
        Response response = listener.get();
        assertEquals("total shards", totalShards, response.getTotalShards());
        assertEquals("successful shards", totalSuccessfulShards, response.getSuccessfulShards());
        assertEquals("failed shards", totalFailedShards, response.getFailedShards());
        assertEquals("accumulated exceptions", totalFailedShards, response.getShardFailures().length);
    }

    public class TestTransportChannel implements TransportChannel {

        private TransportResponse capturedResponse;

        public TransportResponse getCapturedResponse() {
            return capturedResponse;
        }

        @Override
        public String getProfileName() {
            return "";
        }

        @Override
        public void sendResponse(TransportResponse response) throws IOException {
            capturedResponse = response;
        }

        @Override
        public void sendResponse(TransportResponse response, TransportResponseOptions options) throws IOException {
        }

        @Override
        public void sendResponse(Exception exception) throws IOException {
        }

        @Override
        public String getChannelType() {
            return "test";
        }
    }
}