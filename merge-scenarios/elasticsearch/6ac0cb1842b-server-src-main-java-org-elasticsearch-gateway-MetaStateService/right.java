package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class MetaStateService {

    private static final Logger logger = LogManager.getLogger(MetaStateService.class);

    private final NodeEnvironment nodeEnv;

    private final NamedXContentRegistry namedXContentRegistry;

    public MetaStateService(NodeEnvironment nodeEnv, NamedXContentRegistry namedXContentRegistry) {
        this.nodeEnv = nodeEnv;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    MetaData loadFullState() throws IOException {
        MetaData globalMetaData = loadGlobalState();
        MetaData.Builder metaDataBuilder;
        if (globalMetaData != null) {
            metaDataBuilder = MetaData.builder(globalMetaData);
        } else {
            metaDataBuilder = MetaData.builder();
        }
        for (String indexFolderName : nodeEnv.availableIndexFolders()) {
            IndexMetaData indexMetaData = IndexMetaData.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.resolveIndexFolder(indexFolderName));
            if (indexMetaData != null) {
                metaDataBuilder.put(indexMetaData, false);
            } else {
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }
        return metaDataBuilder.build();
    }

    @Nullable
    public IndexMetaData loadIndexState(Index index) throws IOException {
        return IndexMetaData.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.indexPaths(index));
    }

    List<IndexMetaData> loadIndicesStates(Predicate<String> excludeIndexPathIdsPredicate) throws IOException {
        List<IndexMetaData> indexMetaDataList = new ArrayList<>();
        for (String indexFolderName : nodeEnv.availableIndexFolders(excludeIndexPathIdsPredicate)) {
            assert excludeIndexPathIdsPredicate.test(indexFolderName) == false : "unexpected folder " + indexFolderName + " which should have been excluded";
            IndexMetaData indexMetaData = IndexMetaData.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.resolveIndexFolder(indexFolderName));
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

    MetaData loadGlobalState() throws IOException {
        return MetaData.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
    }

    public void writeIndex(String reason, IndexMetaData indexMetaData) throws IOException {
        final Index index = indexMetaData.getIndex();
        logger.trace("[{}] writing state, reason [{}]", index, reason);
        try {
            IndexMetaData.FORMAT.write(indexMetaData, nodeEnv.indexPaths(indexMetaData.getIndex()));
            logger.trace("[{}] state written", index);
        } catch (Exception ex) {
            logger.warn(() -> new ParameterizedMessage("[{}]: failed to write index state", index), ex);
            throw new IOException("failed to write state for [" + index + "]", ex);
        }
    }

    void writeGlobalState(String reason, MetaData metaData) throws IOException {
        logger.trace("[_global] writing state, reason [{}]", reason);
        try {
            MetaData.FORMAT.write(metaData, nodeEnv.nodeDataPaths());
            logger.trace("[_global] state written");
        } catch (Exception ex) {
            logger.warn("[_global]: failed to write global state", ex);
            throw new IOException("failed to write global state", ex);
        }
    }
}