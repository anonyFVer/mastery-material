package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.transport.TransportResponse;
import java.io.IOException;
import java.util.List;

public interface RecoveryTargetHandler {

    void prepareForTranslogOperations(int totalTranslogOps, long maxUnsafeAutoIdTimestamp) throws IOException;

    FinalizeResponse finalizeRecovery();

    void ensureClusterStateVersion(long clusterStateVersion);

    void indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps);

    void receiveFileInfo(List<String> phase1FileNames, List<Long> phase1FileSizes, List<String> phase1ExistingFileNames, List<Long> phase1ExistingFileSizes, int totalTranslogOps);

    void cleanFiles(int totalTranslogOps, Store.MetadataSnapshot sourceMetaData) throws IOException;

    void writeFileChunk(StoreFileMetaData fileMetaData, long position, BytesReference content, boolean lastChunk, int totalTranslogOps) throws IOException;

    class FinalizeResponse extends TransportResponse {

        private long localCheckpoint;

        private String allocationId;

        public FinalizeResponse(String allocationId, long localCheckpoint) {
            this.localCheckpoint = localCheckpoint;
            this.allocationId = allocationId;
        }

        FinalizeResponse() {
        }

        public long getLocalCheckpoint() {
            return localCheckpoint;
        }

        public String getAllocationId() {
            return allocationId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeZLong(localCheckpoint);
            out.writeString(allocationId);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            localCheckpoint = in.readZLong();
            allocationId = in.readString();
        }
    }
}