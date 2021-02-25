package org.elasticsearch.indices.recovery;

import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class RemoteRecoveryTargetHandler implements RecoveryTargetHandler {

    private final TransportService transportService;

    private final long recoveryId;

    private final ShardId shardId;

    private final DiscoveryNode targetNode;

    private final RecoverySettings recoverySettings;

    private final TransportRequestOptions translogOpsRequestOptions;

    private final TransportRequestOptions fileChunkRequestOptions;

    private final AtomicLong bytesSinceLastPause = new AtomicLong();

    private final Consumer<Long> onSourceThrottle;

    public RemoteRecoveryTargetHandler(long recoveryId, ShardId shardId, TransportService transportService, DiscoveryNode targetNode, RecoverySettings recoverySettings, Consumer<Long> onSourceThrottle) {
        this.transportService = transportService;
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.targetNode = targetNode;
        this.recoverySettings = recoverySettings;
        this.onSourceThrottle = onSourceThrottle;
        this.translogOpsRequestOptions = TransportRequestOptions.builder().withCompress(true).withType(TransportRequestOptions.Type.RECOVERY).withTimeout(recoverySettings.internalActionLongTimeout()).build();
        this.fileChunkRequestOptions = TransportRequestOptions.builder().withCompress(false).withType(TransportRequestOptions.Type.RECOVERY).withTimeout(recoverySettings.internalActionTimeout()).build();
    }

    @Override
    public void prepareForTranslogOperations(int totalTranslogOps) throws IOException {
        transportService.submitRequest(targetNode, RecoveryTargetService.Actions.PREPARE_TRANSLOG, new RecoveryPrepareForTranslogOperationsRequest(recoveryId, shardId, totalTranslogOps), TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(), EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
    }

    @Override
    public void finalizeRecovery() {
        transportService.submitRequest(targetNode, RecoveryTargetService.Actions.FINALIZE, new RecoveryFinalizeRecoveryRequest(recoveryId, shardId), TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionLongTimeout()).build(), EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
    }

    @Override
    public void indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps) {
        final RecoveryTranslogOperationsRequest translogOperationsRequest = new RecoveryTranslogOperationsRequest(recoveryId, shardId, operations, totalTranslogOps);
        transportService.submitRequest(targetNode, RecoveryTargetService.Actions.TRANSLOG_OPS, translogOperationsRequest, translogOpsRequestOptions, EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
    }

    @Override
    public void receiveFileInfo(List<String> phase1FileNames, List<Long> phase1FileSizes, List<String> phase1ExistingFileNames, List<Long> phase1ExistingFileSizes, int totalTranslogOps) {
        RecoveryFilesInfoRequest recoveryInfoFilesRequest = new RecoveryFilesInfoRequest(recoveryId, shardId, phase1FileNames, phase1FileSizes, phase1ExistingFileNames, phase1ExistingFileSizes, totalTranslogOps);
        transportService.submitRequest(targetNode, RecoveryTargetService.Actions.FILES_INFO, recoveryInfoFilesRequest, TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(), EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
    }

    @Override
    public void cleanFiles(int totalTranslogOps, Store.MetadataSnapshot sourceMetaData) throws IOException {
        transportService.submitRequest(targetNode, RecoveryTargetService.Actions.CLEAN_FILES, new RecoveryCleanFilesRequest(recoveryId, shardId, sourceMetaData, totalTranslogOps), TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(), EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
    }

    @Override
    public void writeFileChunk(StoreFileMetaData fileMetaData, long position, BytesReference content, boolean lastChunk, int totalTranslogOps) throws IOException {
        final long throttleTimeInNanos;
        final RateLimiter rl = recoverySettings.rateLimiter();
        if (rl != null) {
            long bytes = bytesSinceLastPause.addAndGet(content.length());
            if (bytes > rl.getMinPauseCheckBytes()) {
                bytesSinceLastPause.addAndGet(-bytes);
                try {
                    throttleTimeInNanos = rl.pause(bytes);
                    onSourceThrottle.accept(throttleTimeInNanos);
                } catch (IOException e) {
                    throw new ElasticsearchException("failed to pause recovery", e);
                }
            } else {
                throttleTimeInNanos = 0;
            }
        } else {
            throttleTimeInNanos = 0;
        }
        transportService.submitRequest(targetNode, RecoveryTargetService.Actions.FILE_CHUNK, new RecoveryFileChunkRequest(recoveryId, shardId, fileMetaData, position, content, lastChunk, totalTranslogOps, throttleTimeInNanos), fileChunkRequestOptions, EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
    }
}