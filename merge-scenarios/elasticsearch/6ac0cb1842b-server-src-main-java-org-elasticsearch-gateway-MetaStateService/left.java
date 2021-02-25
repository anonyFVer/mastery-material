package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class MetaStateService {

    private static final Logger logger = LogManager.getLogger(MetaStateService.class);

    private final NodeEnvironment nodeEnv;

    private final NamedXContentRegistry namedXContentRegistry;

    protected MetaDataStateFormat<MetaData> META_DATA_FORMAT = MetaData.FORMAT;

    protected MetaDataStateFormat<IndexMetaData> INDEX_META_DATA_FORMAT = IndexMetaData.FORMAT;

    protected MetaDataStateFormat<Manifest> MANIFEST_FORMAT = Manifest.FORMAT;

    public MetaStateService(NodeEnvironment nodeEnv, NamedXContentRegistry namedXContentRegistry) {
        this.nodeEnv = nodeEnv;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    Tuple<Manifest, MetaData> loadFullState() throws IOException {
        final Manifest manifest = MANIFEST_FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
        if (manifest == null) {
            return loadFullStateBWC();
        }
        final MetaData.Builder metaDataBuilder;
        if (manifest.isGlobalGenerationMissing()) {
            metaDataBuilder = MetaData.builder();
        } else {
            final MetaData globalMetaData = META_DATA_FORMAT.loadGeneration(logger, namedXContentRegistry, manifest.getGlobalGeneration(), nodeEnv.nodeDataPaths());
            if (globalMetaData != null) {
                metaDataBuilder = MetaData.builder(globalMetaData);
            } else {
                throw new IOException("failed to find global metadata [generation: " + manifest.getGlobalGeneration() + "]");
            }
        }
        for (Map.Entry<Index, Long> entry : manifest.getIndexGenerations().entrySet()) {
            final Index index = entry.getKey();
            final long generation = entry.getValue();
            final String indexFolderName = index.getUUID();
            final IndexMetaData indexMetaData = INDEX_META_DATA_FORMAT.loadGeneration(logger, namedXContentRegistry, generation, nodeEnv.resolveIndexFolder(indexFolderName));
            if (indexMetaData != null) {
                metaDataBuilder.put(indexMetaData, false);
            } else {
                throw new IOException("failed to find metadata for existing index " + index.getName() + " [location: " + indexFolderName + ", generation: " + generation + "]");
            }
        }
        return new Tuple<>(manifest, metaDataBuilder.build());
    }

    private Tuple<Manifest, MetaData> loadFullStateBWC() throws IOException {
        Map<Index, Long> indices = new HashMap<>();
        MetaData.Builder metaDataBuilder;
        Tuple<MetaData, Long> metaDataAndGeneration = META_DATA_FORMAT.loadLatestStateWithGeneration(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
        MetaData globalMetaData = metaDataAndGeneration.v1();
        long globalStateGeneration = metaDataAndGeneration.v2();
        if (globalMetaData != null) {
            metaDataBuilder = MetaData.builder(globalMetaData);
            assert Version.CURRENT.major < 8 : "failed to find manifest file, which is mandatory staring with Elasticsearch version 8.0";
        } else {
            metaDataBuilder = MetaData.builder();
        }
        for (String indexFolderName : nodeEnv.availableIndexFolders()) {
            Tuple<IndexMetaData, Long> indexMetaDataAndGeneration = INDEX_META_DATA_FORMAT.loadLatestStateWithGeneration(logger, namedXContentRegistry, nodeEnv.resolveIndexFolder(indexFolderName));
            assert Version.CURRENT.major < 8 : "failed to find manifest file, which is mandatory staring with Elasticsearch version 8.0";
            IndexMetaData indexMetaData = indexMetaDataAndGeneration.v1();
            long generation = indexMetaDataAndGeneration.v2();
            if (indexMetaData != null) {
                indices.put(indexMetaData.getIndex(), generation);
                metaDataBuilder.put(indexMetaData, false);
            } else {
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }
        Manifest manifest = new Manifest(globalStateGeneration, indices);
        return new Tuple<>(manifest, metaDataBuilder.build());
    }

    @Nullable
    public IndexMetaData loadIndexState(Index index) throws IOException {
        return INDEX_META_DATA_FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.indexPaths(index));
    }

    List<IndexMetaData> loadIndicesStates(Predicate<String> excludeIndexPathIdsPredicate) throws IOException {
        List<IndexMetaData> indexMetaDataList = new ArrayList<>();
        for (String indexFolderName : nodeEnv.availableIndexFolders(excludeIndexPathIdsPredicate)) {
            assert excludeIndexPathIdsPredicate.test(indexFolderName) == false : "unexpected folder " + indexFolderName + " which should have been excluded";
            IndexMetaData indexMetaData = INDEX_META_DATA_FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.resolveIndexFolder(indexFolderName));
            if (indexMetaData != null) {
                final String indexPathId = indexMetaData.getIndex().getUUID();
                if (indexFolderName.equals(indexPathId)) {
                    indexMetaDataList.add(indexMetaData);
                } else {
                    throw new IllegalStateException("[" + indexFolderName + "] invalid index folder name, rename to [" + indexPathId + "]");
                }
            } else {
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }
        return indexMetaDataList;
    }

    public Manifest loadManifestOrEmpty() throws IOException {
        Manifest manifest = MANIFEST_FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
        if (manifest == null) {
            manifest = Manifest.empty();
        }
        return manifest;
    }

    MetaData loadGlobalState() throws IOException {
        return META_DATA_FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
    }

    public long writeManifestAndCleanup(String reason, Manifest manifest) throws WriteStateException {
        logger.trace("[_meta] writing state, reason [{}]", reason);
        try {
            long generation = MANIFEST_FORMAT.writeAndCleanup(manifest, nodeEnv.nodeDataPaths());
            logger.trace("[_meta] state written (generation: {})", generation);
            return generation;
        } catch (WriteStateException ex) {
            throw new WriteStateException(ex.isDirty(), "[_meta]: failed to write meta state", ex);
        }
    }

    public long writeIndex(String reason, IndexMetaData indexMetaData) throws WriteStateException {
        final Index index = indexMetaData.getIndex();
        logger.trace("[{}] writing state, reason [{}]", index, reason);
        try {
            long generation = INDEX_META_DATA_FORMAT.write(indexMetaData, nodeEnv.indexPaths(indexMetaData.getIndex()));
            logger.trace("[{}] state written", index);
            return generation;
        } catch (WriteStateException ex) {
            throw new WriteStateException(false, "[" + index + "]: failed to write index state", ex);
        }
    }

    long writeGlobalState(String reason, MetaData metaData) throws WriteStateException {
        logger.trace("[_global] writing state, reason [{}]", reason);
        try {
            long generation = META_DATA_FORMAT.write(metaData, nodeEnv.nodeDataPaths());
            logger.trace("[_global] state written");
            return generation;
        } catch (WriteStateException ex) {
            throw new WriteStateException(false, "[_global]: failed to write global state", ex);
        }
    }

    void cleanupGlobalState(long currentGeneration) {
        META_DATA_FORMAT.cleanupOldFiles(currentGeneration, nodeEnv.nodeDataPaths());
    }

    public void cleanupIndex(Index index, long currentGeneration) {
        INDEX_META_DATA_FORMAT.cleanupOldFiles(currentGeneration, nodeEnv.indexPaths(index));
    }

    public void writeIndexAndUpdateManifest(String reason, IndexMetaData metaData) throws IOException {
        long generation = writeIndex(reason, metaData);
        Manifest manifest = loadManifestOrEmpty();
        Map<Index, Long> indices = new HashMap<>(manifest.getIndexGenerations());
        indices.put(metaData.getIndex(), generation);
        manifest = new Manifest(manifest.getGlobalGeneration(), indices);
        writeManifestAndCleanup(reason, manifest);
        cleanupIndex(metaData.getIndex(), generation);
    }

    public void writeGlobalStateAndUpdateManifest(String reason, MetaData metaData) throws IOException {
        long generation = writeGlobalState(reason, metaData);
        Manifest manifest = loadManifestOrEmpty();
        manifest = new Manifest(generation, manifest.getIndexGenerations());
        writeManifestAndCleanup(reason, manifest);
        cleanupGlobalState(generation);
    }
}