package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.reindex.ScrollableHitSource.Hit;
import org.elasticsearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.lucene.util.TestUtil.randomSimpleString;
import static org.elasticsearch.action.bulk.BackoffPolicy.constantBackoff;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueNanos;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AsyncBulkByScrollActionTests extends ESTestCase {

    private MyMockClient client;

    private ThreadPool threadPool;

    private DummyAbstractBulkByScrollRequest testRequest;

    private SearchRequest firstSearchRequest;

    private PlainActionFuture<BulkIndexByScrollResponse> listener;

    private String scrollId;

    private TaskManager taskManager;

    private WorkingBulkByScrollTask testTask;

    private Map<String, String> expectedHeaders = new HashMap<>();

    private DiscoveryNode localNode;

    private TaskId taskId;

    @Before
    public void setupForTest() {
        client = new MyMockClient(new NoOpClient(getTestName()));
        threadPool = new TestThreadPool(getTestName());
        firstSearchRequest = new SearchRequest();
        testRequest = new DummyAbstractBulkByScrollRequest(firstSearchRequest);
        listener = new PlainActionFuture<>();
        scrollId = null;
        taskManager = new TaskManager(Settings.EMPTY);
        testTask = (WorkingBulkByScrollTask) taskManager.register("don'tcare", "hereeither", testRequest);
        expectedHeaders.clear();
        expectedHeaders.put(randomSimpleString(random()), randomSimpleString(random()));
        threadPool.getThreadContext().newStoredContext();
        threadPool.getThreadContext().putHeader(expectedHeaders);
        localNode = new DiscoveryNode("thenode", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        taskId = new TaskId(localNode.getId(), testTask.getId());
    }

    @After
    public void tearDownAndVerifyCommonStuff() {
        client.close();
        threadPool.shutdown();
    }

    private String scrollId() {
        scrollId = randomSimpleString(random(), 1, 10);
        return scrollId;
    }

    public void testStartRetriesOnRejectionAndSucceeds() throws Exception {
        client.searchesToReject = randomIntBetween(0, testRequest.getMaxRetries() - 1);
        DummyAbstractAsyncBulkByScrollAction action = new DummyActionWithoutBackoff();
        action.start();
        assertBusy(() -> assertEquals(client.searchesToReject + 1, client.searchAttempts.get()));
        if (listener.isDone()) {
            Object result = listener.get();
            fail("Expected listener not to be done but it was and had " + result);
        }
        assertBusy(() -> assertNotNull("There should be a search attempt pending that we didn't reject", client.lastSearch.get()));
        assertEquals(client.searchesToReject, testTask.getStatus().getSearchRetries());
    }

    public void testStartRetriesOnRejectionButFailsOnTooManyRejections() throws Exception {
        client.searchesToReject = testRequest.getMaxRetries() + randomIntBetween(1, 100);
        DummyAbstractAsyncBulkByScrollAction action = new DummyActionWithoutBackoff();
        action.start();
        assertBusy(() -> assertEquals(testRequest.getMaxRetries() + 1, client.searchAttempts.get()));
        assertBusy(() -> assertTrue(listener.isDone()));
        ExecutionException e = expectThrows(ExecutionException.class, () -> listener.get());
        assertThat(ExceptionsHelper.stackTrace(e), containsString(EsRejectedExecutionException.class.getSimpleName()));
        assertNull("There shouldn't be a search attempt pending that we didn't reject", client.lastSearch.get());
        assertEquals(testRequest.getMaxRetries(), testTask.getStatus().getSearchRetries());
    }

    public void testStartNextScrollRetriesOnRejectionAndSucceeds() throws Exception {
        client.scrollsToReject = randomIntBetween(0, testRequest.getMaxRetries() - 1);
        DummyAbstractAsyncBulkByScrollAction action = new DummyActionWithoutBackoff();
        action.setScroll(scrollId());
        action.startNextScroll(timeValueNanos(System.nanoTime()), 0);
        assertBusy(() -> assertEquals(client.scrollsToReject + 1, client.scrollAttempts.get()));
        if (listener.isDone()) {
            Object result = listener.get();
            fail("Expected listener not to be done but it was and had " + result);
        }
        assertBusy(() -> assertNotNull("There should be a scroll attempt pending that we didn't reject", client.lastScroll.get()));
        assertEquals(client.scrollsToReject, testTask.getStatus().getSearchRetries());
    }

    public void testStartNextScrollRetriesOnRejectionButFailsOnTooManyRejections() throws Exception {
        client.scrollsToReject = testRequest.getMaxRetries() + randomIntBetween(1, 100);
        DummyAbstractAsyncBulkByScrollAction action = new DummyActionWithoutBackoff();
        action.setScroll(scrollId());
        action.startNextScroll(timeValueNanos(System.nanoTime()), 0);
        assertBusy(() -> assertEquals(testRequest.getMaxRetries() + 1, client.scrollAttempts.get()));
        assertBusy(() -> assertTrue(listener.isDone()));
        ExecutionException e = expectThrows(ExecutionException.class, () -> listener.get());
        assertThat(ExceptionsHelper.stackTrace(e), containsString(EsRejectedExecutionException.class.getSimpleName()));
        assertNull("There shouldn't be a scroll attempt pending that we didn't reject", client.lastScroll.get());
        assertEquals(testRequest.getMaxRetries(), testTask.getStatus().getSearchRetries());
    }

    public void testScrollResponseSetsTotal() {
        assertEquals(0, testTask.getStatus().getTotal());
        long total = randomIntBetween(0, Integer.MAX_VALUE);
        ScrollableHitSource.Response response = new ScrollableHitSource.Response(false, emptyList(), total, emptyList(), null);
        simulateScrollResponse(new DummyAbstractAsyncBulkByScrollAction(), timeValueSeconds(0), 0, response);
        assertEquals(total, testTask.getStatus().getTotal());
    }

    public void testScrollResponseBatchingBehavior() throws Exception {
        int maxBatches = randomIntBetween(0, 100);
        for (int batches = 1; batches < maxBatches; batches++) {
            Hit hit = new ScrollableHitSource.BasicHit("index", "type", "id", 0);
            ScrollableHitSource.Response response = new ScrollableHitSource.Response(false, emptyList(), 1, singletonList(hit), null);
            DummyAbstractAsyncBulkByScrollAction action = new DummyAbstractAsyncBulkByScrollAction();
            simulateScrollResponse(action, timeValueNanos(System.nanoTime()), 0, response);
            final int expectedBatches = batches;
            assertBusy(() -> assertEquals(expectedBatches, testTask.getStatus().getBatches()));
            assertBusy(() -> assertEquals(expectedHeaders, client.lastHeaders.get()));
        }
    }

    public void testBulkResponseSetsLotsOfStatus() {
        testRequest.setAbortOnVersionConflict(false);
        int maxBatches = randomIntBetween(0, 100);
        long versionConflicts = 0;
        long created = 0;
        long updated = 0;
        long deleted = 0;
        for (int batches = 0; batches < maxBatches; batches++) {
            BulkItemResponse[] responses = new BulkItemResponse[randomIntBetween(0, 100)];
            for (int i = 0; i < responses.length; i++) {
                ShardId shardId = new ShardId(new Index("name", "uid"), 0);
                if (rarely()) {
                    versionConflicts++;
                    responses[i] = new BulkItemResponse(i, randomFrom(DocWriteRequest.OpType.values()), new Failure(shardId.getIndexName(), "type", "id" + i, new VersionConflictEngineException(shardId, "type", "id", "test")));
                    continue;
                }
                boolean createdResponse;
                DocWriteRequest.OpType opType;
                switch(randomIntBetween(0, 2)) {
                    case 0:
                        createdResponse = true;
                        opType = DocWriteRequest.OpType.CREATE;
                        created++;
                        break;
                    case 1:
                        createdResponse = false;
                        opType = randomFrom(DocWriteRequest.OpType.INDEX, DocWriteRequest.OpType.UPDATE);
                        updated++;
                        break;
                    case 2:
                        createdResponse = false;
                        opType = DocWriteRequest.OpType.DELETE;
                        deleted++;
                        break;
                    default:
                        throw new RuntimeException("Bad scenario");
                }
                responses[i] = new BulkItemResponse(i, opType, new IndexResponse(shardId, "type", "id" + i, randomInt(20), randomInt(), createdResponse));
            }
            new DummyAbstractAsyncBulkByScrollAction().onBulkResponse(timeValueNanos(System.nanoTime()), new BulkResponse(responses, 0));
            assertEquals(versionConflicts, testTask.getStatus().getVersionConflicts());
            assertEquals(updated, testTask.getStatus().getUpdated());
            assertEquals(created, testTask.getStatus().getCreated());
            assertEquals(deleted, testTask.getStatus().getDeleted());
            assertEquals(versionConflicts, testTask.getStatus().getVersionConflicts());
        }
    }

    public void testThreadPoolRejectionsAbortRequest() throws Exception {
        testTask.rethrottle(1);
        threadPool.shutdown();
        threadPool = new TestThreadPool(getTestName()) {

            @Override
            public ScheduledFuture<?> schedule(TimeValue delay, String name, Runnable command) {
                assertThat(delay.nanos(), greaterThan(0L));
                assertThat(delay.seconds(), lessThanOrEqualTo(10L));
                ((AbstractRunnable) command).onRejection(new EsRejectedExecutionException("test"));
                return null;
            }
        };
        ScrollableHitSource.Response response = new ScrollableHitSource.Response(false, emptyList(), 0, emptyList(), null);
        simulateScrollResponse(new DummyAbstractAsyncBulkByScrollAction(), timeValueNanos(System.nanoTime()), 10, response);
        ExecutionException e = expectThrows(ExecutionException.class, () -> listener.get());
        assertThat(e.getMessage(), equalTo("EsRejectedExecutionException[test]"));
        assertThat(client.scrollsCleared, contains(scrollId));
        assertEquals(timeValueMillis(0), testTask.getStatus().getThrottled());
    }

    public void testShardFailuresAbortRequest() throws Exception {
        SearchFailure shardFailure = new SearchFailure(new RuntimeException("test"));
        ScrollableHitSource.Response scrollResponse = new ScrollableHitSource.Response(false, singletonList(shardFailure), 0, emptyList(), null);
        simulateScrollResponse(new DummyAbstractAsyncBulkByScrollAction(), timeValueNanos(System.nanoTime()), 0, scrollResponse);
        BulkIndexByScrollResponse response = listener.get();
        assertThat(response.getBulkFailures(), empty());
        assertThat(response.getSearchFailures(), contains(shardFailure));
        assertFalse(response.isTimedOut());
        assertNull(response.getReasonCancelled());
        assertThat(client.scrollsCleared, contains(scrollId));
    }

    public void testSearchTimeoutsAbortRequest() throws Exception {
        ScrollableHitSource.Response scrollResponse = new ScrollableHitSource.Response(true, emptyList(), 0, emptyList(), null);
        simulateScrollResponse(new DummyAbstractAsyncBulkByScrollAction(), timeValueNanos(System.nanoTime()), 0, scrollResponse);
        BulkIndexByScrollResponse response = listener.get();
        assertThat(response.getBulkFailures(), empty());
        assertThat(response.getSearchFailures(), empty());
        assertTrue(response.isTimedOut());
        assertNull(response.getReasonCancelled());
        assertThat(client.scrollsCleared, contains(scrollId));
    }

    public void testBulkFailuresAbortRequest() throws Exception {
        Failure failure = new Failure("index", "type", "id", new RuntimeException("test"));
        DummyAbstractAsyncBulkByScrollAction action = new DummyAbstractAsyncBulkByScrollAction();
        BulkResponse bulkResponse = new BulkResponse(new BulkItemResponse[] { new BulkItemResponse(0, DocWriteRequest.OpType.CREATE, failure) }, randomLong());
        action.onBulkResponse(timeValueNanos(System.nanoTime()), bulkResponse);
        BulkIndexByScrollResponse response = listener.get();
        assertThat(response.getBulkFailures(), contains(failure));
        assertThat(response.getSearchFailures(), empty());
        assertNull(response.getReasonCancelled());
    }

    public void testListenerReceiveBuildBulkExceptions() throws Exception {
        DummyAbstractAsyncBulkByScrollAction action = new DummyAbstractAsyncBulkByScrollAction() {

            @Override
            protected BulkRequest buildBulk(Iterable<? extends ScrollableHitSource.Hit> docs) {
                throw new RuntimeException("surprise");
            }
        };
        Hit hit = new ScrollableHitSource.BasicHit("index", "type", "id", 0);
        ScrollableHitSource.Response response = new ScrollableHitSource.Response(false, emptyList(), 1, singletonList(hit), null);
        simulateScrollResponse(action, timeValueNanos(System.nanoTime()), 0, response);
        ExecutionException e = expectThrows(ExecutionException.class, () -> listener.get());
        assertThat(e.getCause(), instanceOf(RuntimeException.class));
        assertThat(e.getCause().getMessage(), equalTo("surprise"));
    }

    public void testBulkRejectionsRetryWithEnoughRetries() throws Exception {
        int bulksToTry = randomIntBetween(1, 10);
        long retryAttempts = 0;
        for (int i = 0; i < bulksToTry; i++) {
            bulkRetryTestCase(false);
            retryAttempts += testRequest.getMaxRetries();
            assertEquals(retryAttempts, testTask.getStatus().getBulkRetries());
        }
    }

    public void testBulkRejectionsRetryAndFailAnyway() throws Exception {
        bulkRetryTestCase(true);
        assertEquals(testRequest.getMaxRetries(), testTask.getStatus().getBulkRetries());
    }

    public void testScrollDelay() throws Exception {
        AtomicReference<TimeValue> capturedDelay = new AtomicReference<>();
        AtomicReference<Runnable> capturedCommand = new AtomicReference<>();
        threadPool.shutdown();
        threadPool = new TestThreadPool(getTestName()) {

            @Override
            public ScheduledFuture<?> schedule(TimeValue delay, String name, Runnable command) {
                capturedDelay.set(delay);
                capturedCommand.set(command);
                return null;
            }
        };
        DummyAbstractAsyncBulkByScrollAction action = new DummyAbstractAsyncBulkByScrollAction();
        action.setScroll(scrollId());
        firstSearchRequest.scroll(timeValueSeconds(10));
        testTask.rethrottle(1f);
        action.startNextScroll(timeValueNanos(System.nanoTime()), 100);
        assertThat(client.lastScroll.get().request.scroll().keepAlive().seconds(), either(equalTo(110L)).or(equalTo(109L)));
        InternalSearchHit hit = new InternalSearchHit(0, "id", new Text("type"), emptyMap());
        InternalSearchHits hits = new InternalSearchHits(new InternalSearchHit[] { hit }, 0, 0);
        InternalSearchResponse internalResponse = new InternalSearchResponse(hits, null, null, null, false, false);
        SearchResponse searchResponse = new SearchResponse(internalResponse, scrollId(), 5, 4, randomLong(), null);
        if (randomBoolean()) {
            client.lastScroll.get().listener.onResponse(searchResponse);
            assertThat(capturedDelay.get().seconds(), either(equalTo(100L)).or(equalTo(99L)));
        } else {
            testTask.rethrottle(10f);
            client.lastScroll.get().listener.onResponse(searchResponse);
            assertThat(capturedDelay.get().seconds(), either(equalTo(10L)).or(equalTo(9L)));
        }
        capturedCommand.get().run();
        assertEquals(capturedDelay.get(), testTask.getStatus().getThrottled());
    }

    private void bulkRetryTestCase(boolean failWithRejection) throws Exception {
        int totalFailures = randomIntBetween(1, testRequest.getMaxRetries());
        int size = randomIntBetween(1, 100);
        testRequest.setMaxRetries(totalFailures - (failWithRejection ? 1 : 0));
        client.bulksToReject = client.bulksAttempts.get() + totalFailures;
        CountDownLatch successLatch = new CountDownLatch(1);
        DummyAbstractAsyncBulkByScrollAction action = new DummyActionWithoutBackoff() {

            @Override
            void startNextScroll(TimeValue lastBatchStartTime, int lastBatchSize) {
                successLatch.countDown();
            }
        };
        BulkRequest request = new BulkRequest();
        for (int i = 0; i < size + 1; i++) {
            request.add(new IndexRequest("index", "type", "id" + i));
        }
        action.sendBulkRequest(timeValueNanos(System.nanoTime()), request);
        if (failWithRejection) {
            BulkIndexByScrollResponse response = listener.get();
            assertThat(response.getBulkFailures(), hasSize(1));
            assertEquals(response.getBulkFailures().get(0).getStatus(), RestStatus.TOO_MANY_REQUESTS);
            assertThat(response.getSearchFailures(), empty());
            assertNull(response.getReasonCancelled());
        } else {
            successLatch.await(10, TimeUnit.SECONDS);
        }
    }

    public void testDefaultRetryTimes() {
        Iterator<TimeValue> policy = new DummyAbstractAsyncBulkByScrollAction().buildBackoffPolicy().iterator();
        long millis = 0;
        while (policy.hasNext()) {
            millis += policy.next().millis();
        }
        int defaultBackoffBeforeFailing = 59460;
        assertEquals(defaultBackoffBeforeFailing, millis);
    }

    public void testRefreshIsFalseByDefault() throws Exception {
        refreshTestCase(null, true, false);
    }

    public void testRefreshFalseDoesntExecuteRefresh() throws Exception {
        refreshTestCase(false, true, false);
    }

    public void testRefreshTrueExecutesRefresh() throws Exception {
        refreshTestCase(true, true, true);
    }

    public void testRefreshTrueSkipsRefreshIfNoDestinationIndexes() throws Exception {
        refreshTestCase(true, false, false);
    }

    private void refreshTestCase(Boolean refresh, boolean addDestinationIndexes, boolean shouldRefresh) {
        if (refresh != null) {
            testRequest.setRefresh(refresh);
        }
        DummyAbstractAsyncBulkByScrollAction action = new DummyAbstractAsyncBulkByScrollAction();
        if (addDestinationIndexes) {
            action.addDestinationIndices(singleton("foo"));
        }
        action.refreshAndFinish(emptyList(), emptyList(), false);
        if (shouldRefresh) {
            assertArrayEquals(new String[] { "foo" }, client.lastRefreshRequest.get().indices());
        } else {
            assertNull("No refresh was attempted", client.lastRefreshRequest.get());
        }
    }

    public void testCancelBeforeInitialSearch() throws Exception {
        cancelTaskCase((DummyAbstractAsyncBulkByScrollAction action) -> action.start());
    }

    public void testCancelBeforeScrollResponse() throws Exception {
        cancelTaskCase((DummyAbstractAsyncBulkByScrollAction action) -> simulateScrollResponse(action, timeValueNanos(System.nanoTime()), 1, null));
    }

    public void testCancelBeforeSendBulkRequest() throws Exception {
        cancelTaskCase((DummyAbstractAsyncBulkByScrollAction action) -> action.sendBulkRequest(timeValueNanos(System.nanoTime()), null));
    }

    public void testCancelBeforeOnBulkResponse() throws Exception {
        cancelTaskCase((DummyAbstractAsyncBulkByScrollAction action) -> action.onBulkResponse(timeValueNanos(System.nanoTime()), new BulkResponse(new BulkItemResponse[0], 0)));
    }

    public void testCancelBeforeStartNextScroll() throws Exception {
        cancelTaskCase((DummyAbstractAsyncBulkByScrollAction action) -> action.startNextScroll(timeValueNanos(System.nanoTime()), 0));
    }

    public void testCancelBeforeRefreshAndFinish() throws Exception {
        testRequest.setRefresh(usually());
        cancelTaskCase((DummyAbstractAsyncBulkByScrollAction action) -> action.refreshAndFinish(emptyList(), emptyList(), false));
        assertNull("No refresh was attempted", client.lastRefreshRequest.get());
    }

    public void testCancelWhileDelayedAfterScrollResponse() throws Exception {
        String reason = randomSimpleString(random());
        threadPool.shutdown();
        threadPool = new TestThreadPool(getTestName()) {

            @Override
            public ScheduledFuture<?> schedule(TimeValue delay, String name, Runnable command) {
                if (delay.nanos() > 0) {
                    generic().execute(() -> taskManager.cancel(testTask, reason, (Set<String> s) -> {
                    }));
                }
                return super.schedule(delay, name, command);
            }
        };
        DummyAbstractAsyncBulkByScrollAction action = new DummyAbstractAsyncBulkByScrollAction();
        boolean previousScrollSet = usually();
        if (previousScrollSet) {
            action.setScroll(scrollId());
        }
        long total = randomIntBetween(0, Integer.MAX_VALUE);
        ScrollableHitSource.Response response = new ScrollableHitSource.Response(false, emptyList(), total, emptyList(), null);
        testTask.rethrottle(1);
        simulateScrollResponse(action, timeValueNanos(System.nanoTime()), 1000, response);
        assertEquals(reason, listener.get(10, TimeUnit.SECONDS).getReasonCancelled());
        if (previousScrollSet) {
            assertThat(client.scrollsCleared, contains(scrollId));
        }
    }

    private void cancelTaskCase(Consumer<DummyAbstractAsyncBulkByScrollAction> testMe) throws Exception {
        DummyAbstractAsyncBulkByScrollAction action = new DummyAbstractAsyncBulkByScrollAction();
        boolean previousScrollSet = usually();
        if (previousScrollSet) {
            action.setScroll(scrollId());
        }
        String reason = randomSimpleString(random());
        taskManager.cancel(testTask, reason, (Set<String> s) -> {
        });
        testMe.accept(action);
        assertEquals(reason, listener.get().getReasonCancelled());
        if (previousScrollSet) {
            assertThat(client.scrollsCleared, contains(scrollId));
        }
    }

    private void simulateScrollResponse(DummyAbstractAsyncBulkByScrollAction action, TimeValue lastBatchTime, int lastBatchSize, ScrollableHitSource.Response response) {
        action.setScroll(scrollId());
        action.onScrollResponse(lastBatchTime, lastBatchSize, response);
    }

    private class DummyAbstractAsyncBulkByScrollAction extends AbstractAsyncBulkByScrollAction<DummyAbstractBulkByScrollRequest> {

        public DummyAbstractAsyncBulkByScrollAction() {
            super(testTask, AsyncBulkByScrollActionTests.this.logger, new ParentTaskAssigningClient(client, localNode, testTask), AsyncBulkByScrollActionTests.this.threadPool, testRequest, listener);
        }

        @Override
        protected boolean needsSourceDocumentVersions() {
            return randomBoolean();
        }

        @Override
        protected BulkRequest buildBulk(Iterable<? extends ScrollableHitSource.Hit> docs) {
            return new BulkRequest();
        }
    }

    private class DummyActionWithoutBackoff extends DummyAbstractAsyncBulkByScrollAction {

        @Override
        BackoffPolicy buildBackoffPolicy() {
            return constantBackoff(timeValueMillis(0), testRequest.getMaxRetries());
        }
    }

    private static class DummyAbstractBulkByScrollRequest extends AbstractBulkByScrollRequest<DummyAbstractBulkByScrollRequest> {

        public DummyAbstractBulkByScrollRequest(SearchRequest searchRequest) {
            super(searchRequest, true);
        }

        @Override
        DummyAbstractBulkByScrollRequest forSlice(TaskId slicingTask, SearchRequest slice) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected DummyAbstractBulkByScrollRequest self() {
            return this;
        }
    }

    private class MyMockClient extends FilterClient {

        private final List<String> scrollsCleared = new ArrayList<>();

        private final AtomicInteger bulksAttempts = new AtomicInteger();

        private final AtomicInteger searchAttempts = new AtomicInteger();

        private final AtomicInteger scrollAttempts = new AtomicInteger();

        private final AtomicReference<Map<String, String>> lastHeaders = new AtomicReference<>();

        private final AtomicReference<RefreshRequest> lastRefreshRequest = new AtomicReference<>();

        private final AtomicReference<RequestAndListener<SearchRequest, SearchResponse>> lastSearch = new AtomicReference<>();

        private final AtomicReference<RequestAndListener<SearchScrollRequest, SearchResponse>> lastScroll = new AtomicReference<>();

        private int bulksToReject = 0;

        private int searchesToReject = 0;

        private int scrollsToReject = 0;

        public MyMockClient(Client in) {
            super(in);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
            lastHeaders.set(threadPool.getThreadContext().getHeaders());
            if (request instanceof ClearScrollRequest) {
                assertEquals(TaskId.EMPTY_TASK_ID, request.getParentTask());
            } else {
                assertEquals(taskId, request.getParentTask());
            }
            if (request instanceof RefreshRequest) {
                lastRefreshRequest.set((RefreshRequest) request);
                listener.onResponse(null);
                return;
            }
            if (request instanceof SearchRequest) {
                if (searchAttempts.incrementAndGet() <= searchesToReject) {
                    listener.onFailure(wrappedRejectedException());
                    return;
                }
                lastSearch.set(new RequestAndListener<>((SearchRequest) request, (ActionListener<SearchResponse>) listener));
                return;
            }
            if (request instanceof SearchScrollRequest) {
                if (scrollAttempts.incrementAndGet() <= scrollsToReject) {
                    listener.onFailure(wrappedRejectedException());
                    return;
                }
                lastScroll.set(new RequestAndListener<>((SearchScrollRequest) request, (ActionListener<SearchResponse>) listener));
                return;
            }
            if (request instanceof ClearScrollRequest) {
                ClearScrollRequest clearScroll = (ClearScrollRequest) request;
                scrollsCleared.addAll(clearScroll.getScrollIds());
                listener.onResponse((Response) new ClearScrollResponse(true, clearScroll.getScrollIds().size()));
                return;
            }
            if (request instanceof BulkRequest) {
                BulkRequest bulk = (BulkRequest) request;
                int toReject;
                if (bulksAttempts.incrementAndGet() > bulksToReject) {
                    toReject = -1;
                } else {
                    toReject = randomIntBetween(0, bulk.requests().size() - 1);
                }
                BulkItemResponse[] responses = new BulkItemResponse[bulk.requests().size()];
                for (int i = 0; i < bulk.requests().size(); i++) {
                    DocWriteRequest<?> item = bulk.requests().get(i);
                    DocWriteResponse response;
                    ShardId shardId = new ShardId(new Index(item.index(), "uuid"), 0);
                    if (item instanceof IndexRequest) {
                        IndexRequest index = (IndexRequest) item;
                        response = new IndexResponse(shardId, index.type(), index.id(), randomInt(20), randomIntBetween(0, Integer.MAX_VALUE), true);
                    } else if (item instanceof UpdateRequest) {
                        UpdateRequest update = (UpdateRequest) item;
                        response = new UpdateResponse(shardId, update.type(), update.id(), randomIntBetween(0, Integer.MAX_VALUE), Result.CREATED);
                    } else if (item instanceof DeleteRequest) {
                        DeleteRequest delete = (DeleteRequest) item;
                        response = new DeleteResponse(shardId, delete.type(), delete.id(), randomInt(20), randomIntBetween(0, Integer.MAX_VALUE), true);
                    } else {
                        throw new RuntimeException("Unknown request:  " + item);
                    }
                    if (i == toReject) {
                        responses[i] = new BulkItemResponse(i, item.opType(), new Failure(response.getIndex(), response.getType(), response.getId(), new EsRejectedExecutionException()));
                    } else {
                        responses[i] = new BulkItemResponse(i, item.opType(), response);
                    }
                }
                listener.onResponse((Response) new BulkResponse(responses, 1));
                return;
            }
            super.doExecute(action, request, listener);
        }

        private Exception wrappedRejectedException() {
            Exception e = new EsRejectedExecutionException();
            int wraps = randomIntBetween(0, 4);
            for (int i = 0; i < wraps; i++) {
                switch(randomIntBetween(0, 2)) {
                    case 0:
                        e = new SearchPhaseExecutionException("test", "test failure", e, new ShardSearchFailure[0]);
                        continue;
                    case 1:
                        e = new ReduceSearchPhaseException("test", "test failure", e, new ShardSearchFailure[0]);
                        continue;
                    case 2:
                        e = new ElasticsearchException(e);
                        continue;
                }
            }
            return e;
        }
    }

    private static class RequestAndListener<Request extends ActionRequest, Response> {

        private final Request request;

        private final ActionListener<Response> listener;

        public RequestAndListener(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
        }
    }
}