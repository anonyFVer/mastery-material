package org.elasticsearch.action.support.single.instance;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.core.IsEqual.equalTo;

public class TransportInstanceSingleOperationActionTests extends ESTestCase {

    private static ThreadPool THREAD_POOL;

    private ClusterService clusterService;

    private CapturingTransport transport;

    private TransportService transportService;

    private TestTransportInstanceSingleOperationAction action;

    public static class Request extends InstanceShardOperationRequest<Request> {

        public Request() {
        }
    }

    public static class Response extends ActionResponse {

        public Response() {
        }
    }

    class TestTransportInstanceSingleOperationAction extends TransportInstanceSingleOperationAction<Request, Response> {

        private final Map<ShardId, Object> shards = new HashMap<>();

        TestTransportInstanceSingleOperationAction(Settings settings, String actionName, TransportService transportService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request) {
            super(settings, actionName, THREAD_POOL, TransportInstanceSingleOperationActionTests.this.clusterService, transportService, actionFilters, indexNameExpressionResolver, request);
        }

        public Map<ShardId, Object> getResults() {
            return shards;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected void shardOperation(Request request, ActionListener<Response> listener) {
            throw new UnsupportedOperationException("Not implemented in test class");
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected void resolveRequest(ClusterState state, Request request) {
        }

        @Override
        protected ShardIterator shards(ClusterState clusterState, Request request) {
            return clusterState.routingTable().index(request.concreteIndex()).shard(request.shardId.getId()).primaryShardIt();
        }
    }

    class MyResolver extends IndexNameExpressionResolver {

        MyResolver() {
            super(Settings.EMPTY);
        }

        @Override
        public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
            return request.indices();
        }
    }

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new TestThreadPool(TransportInstanceSingleOperationActionTests.class.getSimpleName());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = createClusterService(THREAD_POOL);
        transportService = transport.createTransportService(clusterService.getSettings(), THREAD_POOL, TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> clusterService.localNode(), null, Collections.emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();
        action = new TestTransportInstanceSingleOperationAction(Settings.EMPTY, "indices:admin/test", transportService, new ActionFilters(new HashSet<>()), new MyResolver(), Request::new);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transportService.close();
    }

    @AfterClass
    public static void destroyThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        THREAD_POOL = null;
    }

    public void testGlobalBlock() {
        Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ClusterBlocks.Builder block = ClusterBlocks.builder().addGlobalBlock(new ClusterBlock(1, "", false, true, false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        setState(clusterService, ClusterState.builder(clusterService.state()).blocks(block));
        try {
            action.new AsyncSingleAction(request, listener).start();
            listener.get();
            fail("expected ClusterBlockException");
        } catch (Exception e) {
            if (ExceptionsHelper.unwrap(e, ClusterBlockException.class) == null) {
                logger.info("expected ClusterBlockException  but got ", e);
                fail("expected ClusterBlockException");
            }
        }
    }

    public void testBasicRequestWorks() throws InterruptedException, ExecutionException, TimeoutException {
        Request request = new Request().index("test");
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        setState(clusterService, ClusterStateCreationUtils.state("test", randomBoolean(), ShardRoutingState.STARTED));
        action.new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(1));
        transport.handleResponse(transport.capturedRequests()[0].requestId, new Response());
        listener.get();
    }

    public void testFailureWithoutRetry() throws Exception {
        Request request = new Request().index("test");
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        setState(clusterService, ClusterStateCreationUtils.state("test", randomBoolean(), ShardRoutingState.STARTED));
        action.new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(1));
        long requestId = transport.capturedRequests()[0].requestId;
        transport.clear();
        transport.handleRemoteError(requestId, new TransportException("a generic transport exception", new Exception("generic test exception")));
        try {
            assertTrue(listener.isDone());
            listener.get();
            fail("this should fail with a transport exception");
        } catch (ExecutionException t) {
            if (ExceptionsHelper.unwrap(t, TransportException.class) == null) {
                logger.info("expected TransportException  but got ", t);
                fail("expected and TransportException");
            }
        }
    }

    public void testSuccessAfterRetryWithClusterStateUpdate() throws Exception {
        Request request = new Request().index("test");
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        boolean local = randomBoolean();
        setState(clusterService, ClusterStateCreationUtils.state("test", local, ShardRoutingState.INITIALIZING));
        action.new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(0));
        setState(clusterService, ClusterStateCreationUtils.state("test", local, ShardRoutingState.STARTED));
        assertThat(transport.capturedRequests().length, equalTo(1));
        transport.handleResponse(transport.capturedRequests()[0].requestId, new Response());
        listener.get();
    }

    public void testSuccessAfterRetryWithExceptionFromTransport() throws Exception {
        Request request = new Request().index("test");
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        boolean local = randomBoolean();
        setState(clusterService, ClusterStateCreationUtils.state("test", local, ShardRoutingState.STARTED));
        action.new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(1));
        long requestId = transport.capturedRequests()[0].requestId;
        transport.clear();
        DiscoveryNode node = clusterService.state().getNodes().getLocalNode();
        transport.handleLocalError(requestId, new ConnectTransportException(node, "test exception"));
        setState(clusterService, ClusterStateCreationUtils.state("test", local, ShardRoutingState.STARTED));
        assertThat(transport.capturedRequests().length, equalTo(1));
        transport.handleResponse(transport.capturedRequests()[0].requestId, new Response());
        listener.get();
    }

    public void testRetryOfAnAlreadyTimedOutRequest() throws Exception {
        Request request = new Request().index("test").timeout(new TimeValue(0, TimeUnit.MILLISECONDS));
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        setState(clusterService, ClusterStateCreationUtils.state("test", randomBoolean(), ShardRoutingState.STARTED));
        action.new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(1));
        long requestId = transport.capturedRequests()[0].requestId;
        transport.clear();
        DiscoveryNode node = clusterService.state().getNodes().getLocalNode();
        transport.handleLocalError(requestId, new ConnectTransportException(node, "test exception"));
        assertBusy(() -> assertThat(transport.capturedRequests().length, equalTo(1)));
        requestId = transport.capturedRequests()[0].requestId;
        transport.handleLocalError(requestId, new ConnectTransportException(node, "test exception"));
        try {
            assertTrue(listener.isDone());
            listener.get();
            fail("this should fail with a transport exception");
        } catch (ExecutionException t) {
            if (ExceptionsHelper.unwrap(t, ConnectTransportException.class) == null) {
                logger.info("expected ConnectTransportException  but got ", t);
                fail("expected and ConnectTransportException");
            }
        }
    }

    public void testUnresolvableRequestDoesNotHang() throws InterruptedException, ExecutionException, TimeoutException {
        action = new TestTransportInstanceSingleOperationAction(Settings.EMPTY, "indices:admin/test_unresolvable", transportService, new ActionFilters(new HashSet<>()), new MyResolver(), Request::new) {

            @Override
            protected void resolveRequest(ClusterState state, Request request) {
                throw new IllegalStateException("request cannot be resolved");
            }
        };
        Request request = new Request().index("test");
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        setState(clusterService, ClusterStateCreationUtils.state("test", randomBoolean(), ShardRoutingState.STARTED));
        action.new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(0));
        try {
            listener.get();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrap(e, IllegalStateException.class) == null) {
                logger.info("expected IllegalStateException  but got ", e);
                fail("expected and IllegalStateException");
            }
        }
    }
}