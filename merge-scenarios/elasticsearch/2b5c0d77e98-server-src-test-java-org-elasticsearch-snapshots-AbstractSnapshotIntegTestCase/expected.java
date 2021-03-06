package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractSnapshotIntegTestCase extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockRepository.Plugin.class);
    }

    @After
    public void assertConsistentHistoryInLuceneIndex() throws Exception {
        internalCluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
    }

    public static long getFailureCount(String repository) {
        long failureCount = 0;
        for (RepositoriesService repositoriesService : internalCluster().getDataOrMasterNodeInstances(RepositoriesService.class)) {
            MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
            failureCount += mockRepository.getFailureCount();
        }
        return failureCount;
    }

    public static int numberOfFiles(Path dir) throws IOException {
        final AtomicInteger count = new AtomicInteger();
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                count.incrementAndGet();
                return FileVisitResult.CONTINUE;
            }
        });
        return count.get();
    }

    public static void stopNode(final String node) throws IOException {
        internalCluster().stopRandomNode(settings -> settings.get("node.name").equals(node));
    }

    public void waitForBlock(String node, String repository, TimeValue timeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        RepositoriesService repositoriesService = internalCluster().getInstance(RepositoriesService.class, node);
        MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
        while (System.currentTimeMillis() - start < timeout.millis()) {
            if (mockRepository.blocked()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Timeout waiting for node [" + node + "] to be blocked");
    }

    public SnapshotInfo waitForCompletion(String repository, String snapshotName, TimeValue timeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeout.millis()) {
            List<SnapshotInfo> snapshotInfos = client().admin().cluster().prepareGetSnapshots(repository).setSnapshots(snapshotName).get().getSnapshots();
            assertThat(snapshotInfos.size(), equalTo(1));
            if (snapshotInfos.get(0).state().completed()) {
                ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
                SnapshotsInProgress snapshotsInProgress = stateResponse.getState().custom(SnapshotsInProgress.TYPE);
                if (snapshotsInProgress == null) {
                    return snapshotInfos.get(0);
                } else {
                    boolean found = false;
                    for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
                        final Snapshot curr = entry.snapshot();
                        if (curr.getRepository().equals(repository) && curr.getSnapshotId().getName().equals(snapshotName)) {
                            found = true;
                            break;
                        }
                    }
                    if (found == false) {
                        return snapshotInfos.get(0);
                    }
                }
            }
            Thread.sleep(100);
        }
        fail("Timeout!!!");
        return null;
    }

    public static String blockMasterFromFinalizingSnapshotOnIndexFile(final String repositoryName) {
        final String masterName = internalCluster().getMasterName();
        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, masterName).repository(repositoryName)).setBlockOnWriteIndexFile(true);
        return masterName;
    }

    public static String blockMasterFromFinalizingSnapshotOnSnapFile(final String repositoryName) {
        final String masterName = internalCluster().getMasterName();
        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, masterName).repository(repositoryName)).setBlockAndFailOnWriteSnapFiles(true);
        return masterName;
    }

    public static String blockNodeWithIndex(final String repositoryName, final String indexName) {
        for (String node : internalCluster().nodesInclude(indexName)) {
            ((MockRepository) internalCluster().getInstance(RepositoriesService.class, node).repository(repositoryName)).blockOnDataFiles(true);
            return node;
        }
        fail("No nodes for the index " + indexName + " found");
        return null;
    }

    public static void blockAllDataNodes(String repository) {
        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            ((MockRepository) repositoriesService.repository(repository)).blockOnDataFiles(true);
        }
    }

    public static void unblockAllDataNodes(String repository) {
        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            ((MockRepository) repositoriesService.repository(repository)).unblock();
        }
    }

    public void waitForBlockOnAnyDataNode(String repository, TimeValue timeout) throws InterruptedException {
        if (false == awaitBusy(() -> {
            for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
                MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
                if (mockRepository.blocked()) {
                    return true;
                }
            }
            return false;
        }, timeout.millis(), TimeUnit.MILLISECONDS)) {
            fail("Timeout waiting for repository block on any data node!!!");
        }
    }

    public static void unblockNode(final String repository, final String node) {
        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, node).repository(repository)).unblock();
    }
}