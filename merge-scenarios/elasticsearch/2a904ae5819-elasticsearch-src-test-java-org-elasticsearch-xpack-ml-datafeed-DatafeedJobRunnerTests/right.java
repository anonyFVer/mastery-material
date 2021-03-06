package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.MlPlugin;
import org.elasticsearch.xpack.ml.action.FlushJobAction;
import org.elasticsearch.xpack.ml.action.PostDataAction;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.aggregation.AggregationDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.chunked.ChunkedDataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.scroll.ScrollDataExtractorFactory;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.junit.Before;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatafeedJobRunnerTests extends ESTestCase {

    private Client client;

    private ActionFuture<PostDataAction.Response> jobDataFuture;

    private ActionFuture<FlushJobAction.Response> flushJobFuture;

    private ClusterService clusterService;

    private ThreadPool threadPool;

    private DataExtractorFactory dataExtractorFactory;

    private DatafeedJobRunner datafeedJobRunner;

    private long currentTime = 120000;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpTests() {
        client = mock(Client.class);
        jobDataFuture = mock(ActionFuture.class);
        flushJobFuture = mock(ActionFuture.class);
        clusterService = mock(ClusterService.class);
        JobProvider jobProvider = mock(JobProvider.class);
        Mockito.doAnswer(invocationOnMock -> {
            String jobId = (String) invocationOnMock.getArguments()[0];
            @SuppressWarnings("unchecked")
            Consumer<DataCounts> handler = (Consumer<DataCounts>) invocationOnMock.getArguments()[1];
            handler.accept(new DataCounts(jobId));
            return null;
        }).when(jobProvider).dataCounts(any(), any(), any());
        dataExtractorFactory = mock(DataExtractorFactory.class);
        Auditor auditor = mock(Auditor.class);
        threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).submit(any(Runnable.class));
        when(threadPool.executor(MlPlugin.DATAFEED_RUNNER_THREAD_POOL_NAME)).thenReturn(executorService);
        when(client.execute(same(PostDataAction.INSTANCE), any())).thenReturn(jobDataFuture);
        when(client.execute(same(FlushJobAction.INSTANCE), any())).thenReturn(flushJobFuture);
        datafeedJobRunner = new DatafeedJobRunner(threadPool, client, clusterService, jobProvider, () -> currentTime) {

            @Override
            DataExtractorFactory createDataExtractorFactory(DatafeedConfig datafeedConfig, Job job) {
                return dataExtractorFactory;
            }
        };
        when(jobProvider.audit(anyString())).thenReturn(auditor);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("rawtypes")
            Consumer consumer = (Consumer) invocationOnMock.getArguments()[3];
            consumer.accept(new ResourceNotFoundException("dummy"));
            return null;
        }).when(jobProvider).buckets(any(), any(), any(), any());
    }

    public void testStart_GivenNewlyCreatedJobLoopBack() throws Exception {
        Job.Builder jobBuilder = createDatafeedJob();
        DatafeedConfig datafeedConfig = createDatafeedConfig("datafeed1", "foo").build();
        DataCounts dataCounts = new DataCounts("foo", 1, 0, 0, 0, 0, 0, 0, new Date(0), new Date(0));
        Job job = jobBuilder.build();
        MlMetadata mlMetadata = new MlMetadata.Builder().putJob(job, false).putDatafeed(datafeedConfig).build();
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("_name")).metaData(MetaData.builder().putCustom(MlMetadata.TYPE, mlMetadata)).build());
        DataExtractor dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(0L, 60000L)).thenReturn(dataExtractor);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        InputStream in = new ByteArrayInputStream("".getBytes(Charset.forName("utf-8")));
        when(dataExtractor.next()).thenReturn(Optional.of(in));
        when(jobDataFuture.get()).thenReturn(new PostDataAction.Response(dataCounts));
        Consumer<Exception> handler = mockConsumer();
        StartDatafeedAction.DatafeedTask task = mock(StartDatafeedAction.DatafeedTask.class);
        datafeedJobRunner.run("datafeed1", 0L, 60000L, task, handler);
        verify(threadPool, times(1)).executor(MlPlugin.DATAFEED_RUNNER_THREAD_POOL_NAME);
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(client).execute(same(PostDataAction.INSTANCE), eq(createExpectedPostDataRequest(job)));
        verify(client).execute(same(FlushJobAction.INSTANCE), any());
    }

    private static PostDataAction.Request createExpectedPostDataRequest(Job job) {
        DataDescription.Builder expectedDataDescription = new DataDescription.Builder();
        expectedDataDescription.setTimeFormat("epoch_ms");
        expectedDataDescription.setFormat(DataDescription.DataFormat.JSON);
        PostDataAction.Request expectedPostDataRequest = new PostDataAction.Request(job.getId());
        expectedPostDataRequest.setDataDescription(expectedDataDescription.build());
        return expectedPostDataRequest;
    }

    public void testStart_extractionProblem() throws Exception {
        Job.Builder jobBuilder = createDatafeedJob();
        DatafeedConfig datafeedConfig = createDatafeedConfig("datafeed1", "foo").build();
        DataCounts dataCounts = new DataCounts("foo", 1, 0, 0, 0, 0, 0, 0, new Date(0), new Date(0));
        Job job = jobBuilder.build();
        MlMetadata mlMetadata = new MlMetadata.Builder().putJob(job, false).putDatafeed(datafeedConfig).build();
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("_name")).metaData(MetaData.builder().putCustom(MlMetadata.TYPE, mlMetadata)).build());
        DataExtractor dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(0L, 60000L)).thenReturn(dataExtractor);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        when(dataExtractor.next()).thenThrow(new RuntimeException("dummy"));
        when(jobDataFuture.get()).thenReturn(new PostDataAction.Response(dataCounts));
        Consumer<Exception> handler = mockConsumer();
        StartDatafeedAction.DatafeedTask task = mock(StartDatafeedAction.DatafeedTask.class);
        datafeedJobRunner.run("datafeed1", 0L, 60000L, task, handler);
        verify(threadPool, times(1)).executor(MlPlugin.DATAFEED_RUNNER_THREAD_POOL_NAME);
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(client, never()).execute(same(PostDataAction.INSTANCE), eq(new PostDataAction.Request("foo")));
        verify(client, never()).execute(same(FlushJobAction.INSTANCE), any());
    }

    public void testStart_GivenNewlyCreatedJobLoopBackAndRealtime() throws Exception {
        Job.Builder jobBuilder = createDatafeedJob();
        DatafeedConfig datafeedConfig = createDatafeedConfig("datafeed1", "foo").build();
        DataCounts dataCounts = new DataCounts("foo", 1, 0, 0, 0, 0, 0, 0, new Date(0), new Date(0));
        Job job = jobBuilder.build();
        MlMetadata mlMetadata = new MlMetadata.Builder().putJob(job, false).putDatafeed(datafeedConfig).build();
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("_name")).metaData(MetaData.builder().putCustom(MlMetadata.TYPE, mlMetadata)).build());
        DataExtractor dataExtractor = mock(DataExtractor.class);
        when(dataExtractorFactory.newExtractor(0L, 60000L)).thenReturn(dataExtractor);
        when(dataExtractor.hasNext()).thenReturn(true).thenReturn(false);
        InputStream in = new ByteArrayInputStream("".getBytes(Charset.forName("utf-8")));
        when(dataExtractor.next()).thenReturn(Optional.of(in));
        when(jobDataFuture.get()).thenReturn(new PostDataAction.Response(dataCounts));
        Consumer<Exception> handler = mockConsumer();
        boolean cancelled = randomBoolean();
        StartDatafeedAction.DatafeedTask task = new StartDatafeedAction.DatafeedTask(1, "type", "action", null, "datafeed1");
        datafeedJobRunner.run("datafeed1", 0L, null, task, handler);
        verify(threadPool, times(1)).executor(MlPlugin.DATAFEED_RUNNER_THREAD_POOL_NAME);
        if (cancelled) {
            task.stop();
            verify(handler).accept(null);
        } else {
            verify(client).execute(same(PostDataAction.INSTANCE), eq(createExpectedPostDataRequest(job)));
            verify(client).execute(same(FlushJobAction.INSTANCE), any());
            verify(threadPool, times(1)).schedule(eq(new TimeValue(480100)), eq(MlPlugin.DATAFEED_RUNNER_THREAD_POOL_NAME), any());
        }
    }

    public void testCreateDataExtractorFactoryGivenDefaultScroll() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        DatafeedConfig datafeedConfig = createDatafeedConfig("datafeed1", "foo").build();
        DatafeedJobRunner runner = new DatafeedJobRunner(threadPool, client, clusterService, mock(JobProvider.class), () -> currentTime);
        DataExtractorFactory dataExtractorFactory = runner.createDataExtractorFactory(datafeedConfig, jobBuilder.build());
        assertThat(dataExtractorFactory, instanceOf(ChunkedDataExtractorFactory.class));
    }

    public void testCreateDataExtractorFactoryGivenScrollWithAutoChunk() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        DatafeedConfig.Builder datafeedConfig = createDatafeedConfig("datafeed1", "foo");
        datafeedConfig.setChunkingConfig(ChunkingConfig.newAuto());
        DatafeedJobRunner runner = new DatafeedJobRunner(threadPool, client, clusterService, mock(JobProvider.class), () -> currentTime);
        DataExtractorFactory dataExtractorFactory = runner.createDataExtractorFactory(datafeedConfig.build(), jobBuilder.build());
        assertThat(dataExtractorFactory, instanceOf(ChunkedDataExtractorFactory.class));
    }

    public void testCreateDataExtractorFactoryGivenScrollWithOffChunk() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        DatafeedConfig.Builder datafeedConfig = createDatafeedConfig("datafeed1", "foo");
        datafeedConfig.setChunkingConfig(ChunkingConfig.newOff());
        DatafeedJobRunner runner = new DatafeedJobRunner(threadPool, client, clusterService, mock(JobProvider.class), () -> currentTime);
        DataExtractorFactory dataExtractorFactory = runner.createDataExtractorFactory(datafeedConfig.build(), jobBuilder.build());
        assertThat(dataExtractorFactory, instanceOf(ScrollDataExtractorFactory.class));
    }

    public void testCreateDataExtractorFactoryGivenAggregation() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        DatafeedConfig.Builder datafeedConfig = createDatafeedConfig("datafeed1", "foo");
        datafeedConfig.setAggregations(AggregatorFactories.builder().addAggregator(AggregationBuilders.avg("a")));
        DatafeedJobRunner runner = new DatafeedJobRunner(threadPool, client, clusterService, mock(JobProvider.class), () -> currentTime);
        DataExtractorFactory dataExtractorFactory = runner.createDataExtractorFactory(datafeedConfig.build(), jobBuilder.build());
        assertThat(dataExtractorFactory, instanceOf(AggregationDataExtractorFactory.class));
    }

    public void testCreateDataExtractorFactoryGivenDefaultAggregationWithAutoChunk() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        DatafeedConfig.Builder datafeedConfig = createDatafeedConfig("datafeed1", "foo");
        datafeedConfig.setAggregations(AggregatorFactories.builder());
        datafeedConfig.setChunkingConfig(ChunkingConfig.newAuto());
        DatafeedJobRunner runner = new DatafeedJobRunner(threadPool, client, clusterService, mock(JobProvider.class), () -> currentTime);
        DataExtractorFactory dataExtractorFactory = runner.createDataExtractorFactory(datafeedConfig.build(), jobBuilder.build());
        assertThat(dataExtractorFactory, instanceOf(ChunkedDataExtractorFactory.class));
    }

    public static DatafeedConfig.Builder createDatafeedConfig(String datafeedId, String jobId) {
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, jobId);
        datafeedConfig.setIndexes(Arrays.asList("myIndex"));
        datafeedConfig.setTypes(Arrays.asList("myType"));
        return datafeedConfig;
    }

    public static Job.Builder createDatafeedJob() {
        AnalysisConfig.Builder acBuilder = new AnalysisConfig.Builder(Arrays.asList(new Detector.Builder("metric", "field").build()));
        acBuilder.setBucketSpan(3600L);
        acBuilder.setDetectors(Arrays.asList(new Detector.Builder("metric", "field").build()));
        Job.Builder builder = new Job.Builder("foo");
        builder.setAnalysisConfig(acBuilder);
        return builder;
    }

    @SuppressWarnings("unchecked")
    private Consumer<Exception> mockConsumer() {
        return mock(Consumer.class);
    }
}