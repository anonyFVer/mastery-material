package org.elasticsearch.index.mapper;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.MockTcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.CoreMatchers.instanceOf;

public class DynamicMappingDisabledTests extends ESSingleNodeTestCase {

    private static ThreadPool THREAD_POOL;

    private ClusterService clusterService;

    private TransportService transportService;

    private TransportIndexAction transportIndexAction;

    @BeforeClass
    public static void createThreadPool() {
        THREAD_POOL = new TestThreadPool("DynamicMappingDisabledTests");
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder().put(MapperService.INDEX_MAPPER_DYNAMIC_SETTING.getKey(), false).build();
        clusterService = createClusterService(THREAD_POOL);
        Transport transport = new MockTcpTransport(settings, THREAD_POOL, BigArrays.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService(), new NamedWriteableRegistry(Collections.emptyList()), new NetworkService(settings, Collections.emptyList()));
        transportService = new TransportService(clusterService.getSettings(), transport, THREAD_POOL, TransportService.NOOP_TRANSPORT_INTERCEPTOR, null);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        ShardStateAction shardStateAction = new ShardStateAction(settings, clusterService, transportService, null, null, THREAD_POOL);
        ActionFilters actionFilters = new ActionFilters(Collections.emptySet());
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(settings);
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), indexNameExpressionResolver);
        UpdateHelper updateHelper = new UpdateHelper(settings, null);
        TransportShardBulkAction shardBulkAction = new TransportShardBulkAction(settings, transportService, clusterService, indicesService, THREAD_POOL, shardStateAction, null, updateHelper, actionFilters, indexNameExpressionResolver);
        TransportBulkAction bulkAction = new TransportBulkAction(settings, THREAD_POOL, transportService, clusterService, shardBulkAction, null, actionFilters, indexNameExpressionResolver, autoCreateIndex, System::currentTimeMillis);
        transportIndexAction = new TransportIndexAction(settings, transportService, clusterService, indicesService, THREAD_POOL, shardStateAction, actionFilters, indexNameExpressionResolver, bulkAction, shardBulkAction);
    }

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

    public void testDynamicDisabled() {
        IndexRequest request = new IndexRequest("index", "type", "1");
        request.source("foo", 3);
        final AtomicBoolean onFailureCalled = new AtomicBoolean();
        transportIndexAction.execute(request, new ActionListener<IndexResponse>() {

            @Override
            public void onResponse(IndexResponse indexResponse) {
                fail("Indexing request should have failed");
            }

            @Override
            public void onFailure(Exception e) {
                onFailureCalled.set(true);
                assertThat(e, instanceOf(IndexNotFoundException.class));
                assertEquals(e.getMessage(), "no such index");
            }
        });
        assertTrue(onFailureCalled.get());
    }
}