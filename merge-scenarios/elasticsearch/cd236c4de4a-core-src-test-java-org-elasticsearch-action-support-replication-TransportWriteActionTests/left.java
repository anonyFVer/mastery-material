package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import java.util.HashSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class TransportWriteActionTests extends ESTestCase {

    private IndexShard indexShard;

    private Translog.Location location;

    @Before
    public void initCommonMocks() {
        indexShard = mock(IndexShard.class);
        location = mock(Translog.Location.class);
    }

    public void testPrimaryNoRefreshCall() throws Exception {
        noRefreshCall(TestAction::shardOperationOnPrimary, TestAction.WritePrimaryResult::respond);
    }

    public void testReplicaNoRefreshCall() throws Exception {
        noRefreshCall(TestAction::shardOperationOnReplica, TestAction.WriteReplicaResult::respond);
    }

    private <Result, Response> void noRefreshCall(ThrowingTriFunction<TestAction, TestRequest, IndexShard, Result> action, BiConsumer<Result, CapturingActionListener<Response>> responder) throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.NONE);
        Result result = action.apply(new TestAction(), request, indexShard);
        CapturingActionListener<Response> listener = new CapturingActionListener<>();
        responder.accept(result, listener);
        assertNotNull(listener.response);
        assertNull(listener.failure);
        verify(indexShard, never()).refresh(any());
        verify(indexShard, never()).addRefreshListener(any(), any());
    }

    public void testPrimaryImmediateRefresh() throws Exception {
        immediateRefresh(TestAction::shardOperationOnPrimary, TestAction.WritePrimaryResult::respond, r -> assertTrue(r.forcedRefresh));
    }

    public void testReplicaImmediateRefresh() throws Exception {
        immediateRefresh(TestAction::shardOperationOnReplica, TestAction.WriteReplicaResult::respond, r -> {
        });
    }

    private <Result, Response> void immediateRefresh(ThrowingTriFunction<TestAction, TestRequest, IndexShard, Result> action, BiConsumer<Result, CapturingActionListener<Response>> responder, Consumer<Response> responseChecker) throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        Result result = action.apply(new TestAction(), request, indexShard);
        CapturingActionListener<Response> listener = new CapturingActionListener<>();
        responder.accept(result, listener);
        assertNotNull(listener.response);
        assertNull(listener.failure);
        responseChecker.accept(listener.response);
        verify(indexShard).refresh("refresh_flag_index");
        verify(indexShard, never()).addRefreshListener(any(), any());
    }

    public void testPrimaryWaitForRefresh() throws Exception {
        waitForRefresh(TestAction::shardOperationOnPrimary, TestAction.WritePrimaryResult::respond, (r, forcedRefresh) -> assertEquals(forcedRefresh, r.forcedRefresh));
    }

    public void testReplicaWaitForRefresh() throws Exception {
        waitForRefresh(TestAction::shardOperationOnReplica, TestAction.WriteReplicaResult::respond, (r, forcedRefresh) -> {
        });
    }

    private <Result, Response> void waitForRefresh(ThrowingTriFunction<TestAction, TestRequest, IndexShard, Result> action, BiConsumer<Result, CapturingActionListener<Response>> responder, BiConsumer<Response, Boolean> resultChecker) throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        Result result = action.apply(new TestAction(), request, indexShard);
        CapturingActionListener<Response> listener = new CapturingActionListener<>();
        responder.accept(result, listener);
        assertNull(listener.response);
        @SuppressWarnings({ "unchecked", "rawtypes" })
        ArgumentCaptor<Consumer<Boolean>> refreshListener = ArgumentCaptor.forClass((Class) Consumer.class);
        verify(indexShard, never()).refresh(any());
        verify(indexShard).addRefreshListener(any(), refreshListener.capture());
        boolean forcedRefresh = randomBoolean();
        refreshListener.getValue().accept(forcedRefresh);
        assertNotNull(listener.response);
        assertNull(listener.failure);
        resultChecker.accept(listener.response, forcedRefresh);
    }

    public void testDocumentFailureInShardOperationOnPrimary() throws Exception {
        TestRequest request = new TestRequest();
        TestAction testAction = new TestAction(true, true);
        TransportWriteAction<TestRequest, TestRequest, TestResponse>.WritePrimaryResult writePrimaryResult = testAction.shardOperationOnPrimary(request, indexShard);
        CapturingActionListener<TestResponse> listener = new CapturingActionListener<>();
        writePrimaryResult.respond(listener);
        assertNull(listener.response);
        assertNotNull(listener.failure);
    }

    public void testDocumentFailureInShardOperationOnReplica() throws Exception {
        TestRequest request = new TestRequest();
        TestAction testAction = new TestAction(randomBoolean(), true);
        TransportWriteAction<TestRequest, TestRequest, TestResponse>.WriteReplicaResult writeReplicaResult = testAction.shardOperationOnReplica(request, indexShard);
        CapturingActionListener<TransportResponse.Empty> listener = new CapturingActionListener<>();
        writeReplicaResult.respond(listener);
        assertNull(listener.response);
        assertNotNull(listener.failure);
    }

    private class TestAction extends TransportWriteAction<TestRequest, TestRequest, TestResponse> {

        private final boolean withDocumentFailureOnPrimary;

        private final boolean withDocumentFailureOnReplica;

        protected TestAction() {
            this(false, false);
        }

        protected TestAction(boolean withDocumentFailureOnPrimary, boolean withDocumentFailureOnReplica) {
            super(Settings.EMPTY, "test", new TransportService(Settings.EMPTY, null, null, TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null), null, null, null, null, new ActionFilters(new HashSet<>()), new IndexNameExpressionResolver(Settings.EMPTY), TestRequest::new, TestRequest::new, ThreadPool.Names.SAME);
            this.withDocumentFailureOnPrimary = withDocumentFailureOnPrimary;
            this.withDocumentFailureOnReplica = withDocumentFailureOnReplica;
        }

        @Override
        protected TestResponse newResponseInstance() {
            return new TestResponse();
        }

        @Override
        protected WritePrimaryResult shardOperationOnPrimary(TestRequest request, IndexShard primary) throws Exception {
            final WritePrimaryResult primaryResult;
            if (withDocumentFailureOnPrimary) {
                primaryResult = new WritePrimaryResult(request, null, null, new RuntimeException("simulated"), primary);
            } else {
                primaryResult = new WritePrimaryResult(request, new TestResponse(), location, null, primary);
            }
            return primaryResult;
        }

        @Override
        protected WriteReplicaResult shardOperationOnReplica(TestRequest request, IndexShard replica) throws Exception {
            final WriteReplicaResult replicaResult;
            if (withDocumentFailureOnReplica) {
                replicaResult = new WriteReplicaResult(request, null, new RuntimeException("simulated"), replica);
            } else {
                replicaResult = new WriteReplicaResult(request, location, null, replica);
            }
            return replicaResult;
        }
    }

    private static class TestRequest extends ReplicatedWriteRequest<TestRequest> {

        public TestRequest() {
            setShardId(new ShardId("test", "test", 1));
        }

        @Override
        public String toString() {
            return "TestRequest{}";
        }
    }

    private static class TestResponse extends ReplicationResponse implements WriteResponse {

        boolean forcedRefresh;

        @Override
        public void setForcedRefresh(boolean forcedRefresh) {
            this.forcedRefresh = forcedRefresh;
        }
    }

    private static class CapturingActionListener<R> implements ActionListener<R> {

        private R response;

        private Exception failure;

        @Override
        public void onResponse(R response) {
            this.response = response;
        }

        @Override
        public void onFailure(Exception failure) {
            this.failure = failure;
        }
    }

    private interface ThrowingTriFunction<A, B, C, R> {

        R apply(A a, B b, C c) throws Exception;
    }
}