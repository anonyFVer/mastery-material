package org.elasticsearch.gateway;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.MetaDataUpgrader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

public class GatewayMetaState implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(GatewayMetaState.class);

    private final NodeEnvironment nodeEnv;

    private final MetaStateService metaStateService;

    private final Settings settings;

    @Nullable
    private Manifest previousManifest;

    private MetaData previousMetaData;

    public GatewayMetaState(Settings settings, NodeEnvironment nodeEnv, MetaStateService metaStateService, MetaDataIndexUpgradeService metaDataIndexUpgradeService, MetaDataUpgrader metaDataUpgrader) throws IOException {
        this.settings = settings;
        this.nodeEnv = nodeEnv;
        this.metaStateService = metaStateService;
        ensureNoPre019State();
        ensureAtomicMoveSupported();
        upgradeMetaData(metaDataIndexUpgradeService, metaDataUpgrader);
        profileLoadMetaData();
    }

    private void profileLoadMetaData() throws IOException {
        if (isMasterOrDataNode()) {
            long startNS = System.nanoTime();
            metaStateService.loadFullState();
            logger.debug("took {} to load state", TimeValue.timeValueMillis(TimeValue.nsecToMSec(System.nanoTime() - startNS)));
        }
    }

    private void upgradeMetaData(MetaDataIndexUpgradeService metaDataIndexUpgradeService, MetaDataUpgrader metaDataUpgrader) throws IOException {
        if (isMasterOrDataNode()) {
            try {
                final Tuple<Manifest, MetaData> metaStateAndData = metaStateService.loadFullState();
                final Manifest manifest = metaStateAndData.v1();
                final MetaData metaData = metaStateAndData.v2();
                final AtomicClusterStateWriter writer = new AtomicClusterStateWriter(metaStateService, manifest);
                final MetaData upgradedMetaData = upgradeMetaData(metaData, metaDataIndexUpgradeService, metaDataUpgrader);
                final long globalStateGeneration;
                if (MetaData.isGlobalStateEquals(metaData, upgradedMetaData) == false) {
                    globalStateGeneration = writer.writeGlobalState("upgrade", upgradedMetaData);
                } else {
                    globalStateGeneration = manifest.getGlobalGeneration();
                }
                Map<Index, Long> indices = new HashMap<>(manifest.getIndexGenerations());
                for (IndexMetaData indexMetaData : upgradedMetaData) {
                    if (metaData.hasIndexMetaData(indexMetaData) == false) {
                        final long generation = writer.writeIndex("upgrade", indexMetaData);
                        indices.put(indexMetaData.getIndex(), generation);
                    }
                }
                final Manifest newManifest = new Manifest(globalStateGeneration, indices);
                writer.writeManifestAndCleanup("startup", newManifest);
            } catch (Exception e) {
                logger.error("failed to read or upgrade local state, exiting...", e);
                throw e;
            }
        }
    }

    private boolean isMasterOrDataNode() {
        return DiscoveryNode.isMasterNode(settings) || DiscoveryNode.isDataNode(settings);
    }

    private void ensureAtomicMoveSupported() throws IOException {
        if (isMasterOrDataNode()) {
            nodeEnv.ensureAtomicMoveSupported();
        }
    }

    public MetaData loadMetaData() throws IOException {
        return metaStateService.loadFullState().v2();
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        if (isMasterOrDataNode() == false) {
            return;
        }
        if (event.state().blocks().disableStatePersistence()) {
            previousMetaData = null;
            previousManifest = null;
            return;
        }
        try {
            if (previousManifest == null) {
                previousManifest = metaStateService.loadManifestOrEmpty();
            }
            updateMetaData(event);
        } catch (Exception e) {
            logger.warn("Exception occurred when storing new meta data", e);
        }
    }

    static class AtomicClusterStateWriter {

        private static final String FINISHED_MSG = "AtomicClusterStateWriter is finished";

        private final List<Runnable> commitCleanupActions;

        private final List<Runnable> rollbackCleanupActions;

        private final Manifest previousManifest;

        private final MetaStateService metaStateService;

        private boolean finished;

        AtomicClusterStateWriter(MetaStateService metaStateService, Manifest previousManifest) {
            this.metaStateService = metaStateService;
            assert previousManifest != null;
            this.previousManifest = previousManifest;
            this.commitCleanupActions = new ArrayList<>();
            this.rollbackCleanupActions = new ArrayList<>();
            this.finished = false;
        }

        long writeGlobalState(String reason, MetaData metaData) throws WriteStateException {
            assert finished == false : FINISHED_MSG;
            try {
                rollbackCleanupActions.add(() -> metaStateService.cleanupGlobalState(previousManifest.getGlobalGeneration()));
                long generation = metaStateService.writeGlobalState(reason, metaData);
                commitCleanupActions.add(() -> metaStateService.cleanupGlobalState(generation));
                return generation;
            } catch (WriteStateException e) {
                rollback();
                throw e;
            }
        }

        long writeIndex(String reason, IndexMetaData metaData) throws WriteStateException {
            assert finished == false : FINISHED_MSG;
            try {
                Index index = metaData.getIndex();
                Long previousGeneration = previousManifest.getIndexGenerations().get(index);
                if (previousGeneration != null) {
                    rollbackCleanupActions.add(() -> metaStateService.cleanupIndex(index, previousGeneration));
                }
                long generation = metaStateService.writeIndex(reason, metaData);
                commitCleanupActions.add(() -> metaStateService.cleanupIndex(index, generation));
                return generation;
            } catch (WriteStateException e) {
                rollback();
                throw e;
            }
        }

        long writeManifestAndCleanup(String reason, Manifest manifest) throws WriteStateException {
            assert finished == false : FINISHED_MSG;
            try {
                long generation = metaStateService.writeManifestAndCleanup(reason, manifest);
                commitCleanupActions.forEach(Runnable::run);
                finished = true;
                return generation;
            } catch (WriteStateException e) {
                rollback();
                throw e;
            }
        }

        void rollback() {
            rollbackCleanupActions.forEach(Runnable::run);
            finished = true;
        }
    }

    private void updateMetaData(ClusterChangedEvent event) throws IOException {
        ClusterState newState = event.state();
        ClusterState previousState = event.previousState();
        MetaData newMetaData = newState.metaData();
        final AtomicClusterStateWriter writer = new AtomicClusterStateWriter(metaStateService, previousManifest);
        long globalStateGeneration = writeGlobalState(writer, newMetaData);
        Map<Index, Long> indexGenerations = writeIndicesMetadata(writer, newState, previousState);
        Manifest manifest = new Manifest(globalStateGeneration, indexGenerations);
        writeManifest(writer, manifest);
        previousMetaData = newMetaData;
        previousManifest = manifest;
    }

    private void writeManifest(AtomicClusterStateWriter writer, Manifest manifest) throws IOException {
        if (manifest.equals(previousManifest) == false) {
            writer.writeManifestAndCleanup("changed", manifest);
        }
    }

    private Map<Index, Long> writeIndicesMetadata(AtomicClusterStateWriter writer, ClusterState newState, ClusterState previousState) throws IOException {
        Map<Index, Long> previouslyWrittenIndices = previousManifest.getIndexGenerations();
        Set<Index> relevantIndices = getRelevantIndices(newState, previousState, previouslyWrittenIndices.keySet());
        Map<Index, Long> newIndices = new HashMap<>();
        Iterable<IndexMetaDataAction> actions = resolveIndexMetaDataActions(previouslyWrittenIndices, relevantIndices, previousMetaData, newState.metaData());
        for (IndexMetaDataAction action : actions) {
            long generation = action.execute(writer);
            newIndices.put(action.getIndex(), generation);
        }
        return newIndices;
    }

    private long writeGlobalState(AtomicClusterStateWriter writer, MetaData newMetaData) throws IOException {
        if (previousMetaData == null || MetaData.isGlobalStateEquals(previousMetaData, newMetaData) == false) {
            return writer.writeGlobalState("changed", newMetaData);
        }
        return previousManifest.getGlobalGeneration();
    }

    public static Set<Index> getRelevantIndices(ClusterState state, ClusterState previousState, Set<Index> previouslyWrittenIndices) {
        Set<Index> relevantIndices;
        if (isDataOnlyNode(state)) {
            relevantIndices = getRelevantIndicesOnDataOnlyNode(state, previousState, previouslyWrittenIndices);
        } else if (state.nodes().getLocalNode().isMasterNode()) {
            relevantIndices = getRelevantIndicesForMasterEligibleNode(state);
        } else {
            relevantIndices = Collections.emptySet();
        }
        return relevantIndices;
    }

    private static boolean isDataOnlyNode(ClusterState state) {
        return ((state.nodes().getLocalNode().isMasterNode() == false) && state.nodes().getLocalNode().isDataNode());
    }

    private void ensureNoPre019State() throws IOException {
        if (DiscoveryNode.isDataNode(settings)) {
            ensureNoPre019ShardState();
        }
        if (isMasterOrDataNode()) {
            ensureNoPre019MetadataFiles();
        }
    }

    private void ensureNoPre019MetadataFiles() throws IOException {
        for (Path dataLocation : nodeEnv.nodeDataPaths()) {
            final Path stateLocation = dataLocation.resolve(MetaDataStateFormat.STATE_DIR_NAME);
            if (!Files.exists(stateLocation)) {
                continue;
            }
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(stateLocation)) {
                for (Path stateFile : stream) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("[upgrade]: processing [{}]", stateFile.getFileName());
                    }
                    final String name = stateFile.getFileName().toString();
                    if (name.startsWith("metadata-")) {
                        throw new IllegalStateException("Detected pre 0.19 metadata file please upgrade to a version before " + Version.CURRENT.minimumIndexCompatibilityVersion() + " first to upgrade state structures - metadata found: [" + stateFile.getParent().toAbsolutePath());
                    }
                }
            }
        }
    }

    private void ensureNoPre019ShardState() throws IOException {
        for (Path dataLocation : nodeEnv.nodeDataPaths()) {
            final Path stateLocation = dataLocation.resolve(MetaDataStateFormat.STATE_DIR_NAME);
            if (Files.exists(stateLocation)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(stateLocation, "shards-*")) {
                    for (Path stateFile : stream) {
                        throw new IllegalStateException("Detected pre 0.19 shard state file please upgrade to a version before " + Version.CURRENT.minimumIndexCompatibilityVersion() + " first to upgrade state structures - shard state found: [" + stateFile.getParent().toAbsolutePath());
                    }
                }
            }
        }
    }

    static MetaData upgradeMetaData(MetaData metaData, MetaDataIndexUpgradeService metaDataIndexUpgradeService, MetaDataUpgrader metaDataUpgrader) {
        boolean changed = false;
        final MetaData.Builder upgradedMetaData = MetaData.builder(metaData);
        for (IndexMetaData indexMetaData : metaData) {
            IndexMetaData newMetaData = metaDataIndexUpgradeService.upgradeIndexMetaData(indexMetaData, Version.CURRENT.minimumIndexCompatibilityVersion());
            changed |= indexMetaData != newMetaData;
            upgradedMetaData.put(newMetaData, false);
        }
        if (applyPluginUpgraders(metaData.getCustoms(), metaDataUpgrader.customMetaDataUpgraders, upgradedMetaData::removeCustom, upgradedMetaData::putCustom)) {
            changed = true;
        }
        if (applyPluginUpgraders(metaData.getTemplates(), metaDataUpgrader.indexTemplateMetaDataUpgraders, upgradedMetaData::removeTemplate, (s, indexTemplateMetaData) -> upgradedMetaData.put(indexTemplateMetaData))) {
            changed = true;
        }
        return changed ? upgradedMetaData.build() : metaData;
    }

    private static <Data> boolean applyPluginUpgraders(ImmutableOpenMap<String, Data> existingData, UnaryOperator<Map<String, Data>> upgrader, Consumer<String> removeData, BiConsumer<String, Data> putData) {
        Map<String, Data> existingMap = new HashMap<>();
        for (ObjectObjectCursor<String, Data> customCursor : existingData) {
            existingMap.put(customCursor.key, customCursor.value);
        }
        Map<String, Data> upgradedCustoms = upgrader.apply(existingMap);
        if (upgradedCustoms.equals(existingMap) == false) {
            existingMap.keySet().forEach(removeData);
            for (Map.Entry<String, Data> upgradedCustomEntry : upgradedCustoms.entrySet()) {
                putData.accept(upgradedCustomEntry.getKey(), upgradedCustomEntry.getValue());
            }
            return true;
        }
        return false;
    }

    public static List<IndexMetaDataAction> resolveIndexMetaDataActions(Map<Index, Long> previouslyWrittenIndices, Set<Index> relevantIndices, MetaData previousMetaData, MetaData newMetaData) {
        List<IndexMetaDataAction> actions = new ArrayList<>();
        for (Index index : relevantIndices) {
            IndexMetaData newIndexMetaData = newMetaData.getIndexSafe(index);
            IndexMetaData previousIndexMetaData = previousMetaData == null ? null : previousMetaData.index(index);
            if (previouslyWrittenIndices.containsKey(index) == false || previousIndexMetaData == null) {
                actions.add(new WriteNewIndexMetaData(newIndexMetaData));
            } else if (previousIndexMetaData.getVersion() != newIndexMetaData.getVersion()) {
                actions.add(new WriteChangedIndexMetaData(previousIndexMetaData, newIndexMetaData));
            } else {
                actions.add(new KeepPreviousGeneration(index, previouslyWrittenIndices.get(index)));
            }
        }
        return actions;
    }

    private static Set<Index> getRelevantIndicesOnDataOnlyNode(ClusterState state, ClusterState previousState, Set<Index> previouslyWrittenIndices) {
        RoutingNode newRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (newRoutingNode == null) {
            throw new IllegalStateException("cluster state does not contain this node - cannot write index meta state");
        }
        Set<Index> indices = new HashSet<>();
        for (ShardRouting routing : newRoutingNode) {
            indices.add(routing.index());
        }
        for (IndexMetaData indexMetaData : state.metaData()) {
            boolean isOrWasClosed = indexMetaData.getState().equals(IndexMetaData.State.CLOSE);
            IndexMetaData previousMetaData = previousState.metaData().index(indexMetaData.getIndex());
            if (previousMetaData != null) {
                isOrWasClosed = isOrWasClosed || previousMetaData.getState().equals(IndexMetaData.State.CLOSE);
            }
            if (previouslyWrittenIndices.contains(indexMetaData.getIndex()) && isOrWasClosed) {
                indices.add(indexMetaData.getIndex());
            }
        }
        return indices;
    }

    private static Set<Index> getRelevantIndicesForMasterEligibleNode(ClusterState state) {
        Set<Index> relevantIndices;
        relevantIndices = new HashSet<>();
        for (IndexMetaData indexMetaData : state.metaData()) {
            relevantIndices.add(indexMetaData.getIndex());
        }
        return relevantIndices;
    }

    public interface IndexMetaDataAction {

        Index getIndex();

        long execute(AtomicClusterStateWriter writer) throws WriteStateException;
    }

    public static class KeepPreviousGeneration implements IndexMetaDataAction {

        private final Index index;

        private final long generation;

        KeepPreviousGeneration(Index index, long generation) {
            this.index = index;
            this.generation = generation;
        }

        @Override
        public Index getIndex() {
            return index;
        }

        @Override
        public long execute(AtomicClusterStateWriter writer) {
            return generation;
        }
    }

    public static class WriteNewIndexMetaData implements IndexMetaDataAction {

        private final IndexMetaData indexMetaData;

        WriteNewIndexMetaData(IndexMetaData indexMetaData) {
            this.indexMetaData = indexMetaData;
        }

        @Override
        public Index getIndex() {
            return indexMetaData.getIndex();
        }

        @Override
        public long execute(AtomicClusterStateWriter writer) throws WriteStateException {
            return writer.writeIndex("freshly created", indexMetaData);
        }
    }

    public static class WriteChangedIndexMetaData implements IndexMetaDataAction {

        private final IndexMetaData newIndexMetaData;

        private final IndexMetaData oldIndexMetaData;

        WriteChangedIndexMetaData(IndexMetaData oldIndexMetaData, IndexMetaData newIndexMetaData) {
            this.oldIndexMetaData = oldIndexMetaData;
            this.newIndexMetaData = newIndexMetaData;
        }

        @Override
        public Index getIndex() {
            return newIndexMetaData.getIndex();
        }

        @Override
        public long execute(AtomicClusterStateWriter writer) throws WriteStateException {
            return writer.writeIndex("version changed from [" + oldIndexMetaData.getVersion() + "] to [" + newIndexMetaData.getVersion() + "]", newIndexMetaData);
        }
    }
}