package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.TransportBulkActionTookTests.Resolver;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;

public class TransportBulkActionTests extends ESTestCase {

    private TransportService transportService;

    private ClusterService clusterService;

    private ThreadPool threadPool;

    private TestTransportBulkAction bulkAction;

    class TestTransportBulkAction extends TransportBulkAction {

        boolean indexCreated = false;

        TestTransportBulkAction() {
            super(TransportBulkActionTests.this.threadPool, transportService, clusterService, null, null, null, new ActionFilters(Collections.emptySet()), new Resolver(), new AutoCreateIndex(Settings.EMPTY, clusterService.getClusterSettings(), new Resolver()));
        }

        @Override
        protected boolean needToCheck() {
            return true;
        }

        @Override
        void createIndex(String index, TimeValue timeout, ActionListener<CreateIndexResponse> listener) {
            indexCreated = true;
            listener.onResponse(null);
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("TransportBulkActionTookTests");
        clusterService = createClusterService(threadPool);
        CapturingTransport capturingTransport = new CapturingTransport();
        transportService = capturingTransport.createCapturingTransportService(clusterService.getSettings(), threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundAddress -> clusterService.localNode(), null, Collections.emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();
        bulkAction = new TestTransportBulkAction();
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        clusterService.close();
        super.tearDown();
    }

    public void testDeleteNonExistingDocDoesNotCreateIndex() throws Exception {
        BulkRequest bulkRequest = new BulkRequest().add(new DeleteRequest("index", "type", "id"));
        bulkAction.execute(null, bulkRequest, ActionListener.wrap(response -> {
            assertFalse(bulkAction.indexCreated);
            BulkItemResponse[] bulkResponses = ((BulkResponse) response).getItems();
            assertEquals(bulkResponses.length, 1);
            assertTrue(bulkResponses[0].isFailed());
            assertTrue(bulkResponses[0].getFailure().getCause() instanceof IndexNotFoundException);
            assertEquals("index", bulkResponses[0].getFailure().getIndex());
        }, exception -> {
            throw new AssertionError(exception);
        }));
    }

    public void testDeleteNonExistingDocExternalVersionCreatesIndex() throws Exception {
        BulkRequest bulkRequest = new BulkRequest().add(new DeleteRequest("index", "type", "id").versionType(VersionType.EXTERNAL).version(0));
        bulkAction.execute(null, bulkRequest, ActionListener.wrap(response -> {
            assertTrue(bulkAction.indexCreated);
        }, exception -> {
            throw new AssertionError(exception);
        }));
    }

    public void testDeleteNonExistingDocExternalGteVersionCreatesIndex() throws Exception {
        BulkRequest bulkRequest = new BulkRequest().add(new DeleteRequest("index2", "type", "id").versionType(VersionType.EXTERNAL_GTE).version(0));
        bulkAction.execute(null, bulkRequest, ActionListener.wrap(response -> {
            assertTrue(bulkAction.indexCreated);
        }, exception -> {
            throw new AssertionError(exception);
        }));
    }
}