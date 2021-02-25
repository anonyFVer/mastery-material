package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import java.io.IOException;
import java.util.List;

public interface RecoveryTargetHandler {

    void prepareForTranslogOperations(int totalTranslogOps, long maxUnsafeAutoIdTimestamp) throws IOException;

    void finalizeRecovery();

    void ensureClusterStateVersion(long clusterStateVersion);

    void indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps);

    void receiveFileInfo(List<String> phase1FileNames, List<Long> phase1FileSizes, List<String> phase1ExistingFileNames, List<Long> phase1ExistingFileSizes, int totalTranslogOps);

    void cleanFiles(int totalTranslogOps, Store.MetadataSnapshot sourceMetaData) throws IOException;

    void writeFileChunk(StoreFileMetaData fileMetaData, long position, BytesReference content, boolean lastChunk, int totalTranslogOps) throws IOException;
}