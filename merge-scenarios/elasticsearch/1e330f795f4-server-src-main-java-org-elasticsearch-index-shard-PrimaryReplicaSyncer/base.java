package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.resync.ResyncReplicationRequest;
import org.elasticsearch.action.resync.ResyncReplicationResponse;
import org.elasticsearch.action.resync.TransportResyncReplicationAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.TransportService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static java.util.Objects.requireNonNull;

public class PrimaryReplicaSyncer extends AbstractComponent {

    private final TaskManager taskManager;

    private final SyncAction syncAction;

    public static final ByteSizeValue DEFAULT_CHUNK_SIZE = new ByteSizeValue(512, ByteSizeUnit.KB);

    private volatile ByteSizeValue chunkSize = DEFAULT_CHUNK_SIZE;

    @Inject
    public PrimaryReplicaSyncer(Settings settings, TransportService transportService, TransportResyncReplicationAction syncAction) {
        this(settings, transportService.getTaskManager(), syncAction);
    }

    public PrimaryReplicaSyncer(Settings settings, TaskManager taskManager, SyncAction syncAction) {
        super(settings);
        this.taskManager = taskManager;
        this.syncAction = syncAction;
    }

    void setChunkSize(ByteSizeValue chunkSize) {
        if (chunkSize.bytesAsInt() <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0");
        }
        this.chunkSize = chunkSize;
    }

    public void resync(final IndexShard indexShard, final ActionListener<ResyncTask> listener) {
        ActionListener<ResyncTask> resyncListener = null;
        try {
            final long startingSeqNo = indexShard.getGlobalCheckpoint() + 1;
            Translog.Snapshot snapshot = indexShard.getTranslog().newSnapshotFromMinSeqNo(startingSeqNo);
            resyncListener = new ActionListener<ResyncTask>() {

                @Override
                public void onResponse(final ResyncTask resyncTask) {
                    try {
                        snapshot.close();
                        listener.onResponse(resyncTask);
                    } catch (final Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(final Exception e) {
                    try {
                        snapshot.close();
                    } catch (final Exception inner) {
                        e.addSuppressed(inner);
                    } finally {
                        listener.onFailure(e);
                    }
                }
            };
            ShardId shardId = indexShard.shardId();
            Translog.Snapshot wrappedSnapshot = new Translog.Snapshot() {

                @Override
                public synchronized void close() throws IOException {
                    snapshot.close();
                }

                @Override
                public synchronized int totalOperations() {
                    return snapshot.totalOperations();
                }

                @Override
                public synchronized Translog.Operation next() throws IOException {
                    IndexShardState state = indexShard.state();
                    if (state == IndexShardState.CLOSED) {
                        throw new IndexShardClosedException(shardId);
                    } else {
                        assert state == IndexShardState.STARTED : "resync should only happen on a started shard, but state was: " + state;
                    }
                    return snapshot.next();
                }
            };
            resync(shardId, indexShard.routingEntry().allocationId().getId(), indexShard.getPrimaryTerm(), wrappedSnapshot, startingSeqNo, resyncListener);
        } catch (Exception e) {
            if (resyncListener != null) {
                resyncListener.onFailure(e);
            } else {
                listener.onFailure(e);
            }
        }
    }

    private void resync(final ShardId shardId, final String primaryAllocationId, final long primaryTerm, final Translog.Snapshot snapshot, long startingSeqNo, ActionListener<ResyncTask> listener) {
        ResyncRequest request = new ResyncRequest(shardId, primaryAllocationId);
        ResyncTask resyncTask = (ResyncTask) taskManager.register("transport", "resync", request);
        ActionListener<Void> wrappedListener = new ActionListener<Void>() {

            @Override
            public void onResponse(Void ignore) {
                resyncTask.setPhase("finished");
                taskManager.unregister(resyncTask);
                listener.onResponse(resyncTask);
            }

            @Override
            public void onFailure(Exception e) {
                resyncTask.setPhase("finished");
                taskManager.unregister(resyncTask);
                listener.onFailure(e);
            }
        };
        try {
            new SnapshotSender(logger, syncAction, resyncTask, shardId, primaryAllocationId, primaryTerm, snapshot, chunkSize.bytesAsInt(), startingSeqNo, wrappedListener).run();
        } catch (Exception e) {
            wrappedListener.onFailure(e);
        }
    }

    public interface SyncAction {

        void sync(ResyncReplicationRequest request, Task parentTask, String primaryAllocationId, long primaryTerm, ActionListener<ResyncReplicationResponse> listener);
    }

    static class SnapshotSender extends AbstractRunnable implements ActionListener<ResyncReplicationResponse> {

        private final Logger logger;

        private final SyncAction syncAction;

        private final ResyncTask task;

        private final String primaryAllocationId;

        private final long primaryTerm;

        private final ShardId shardId;

        private final Translog.Snapshot snapshot;

        private final long startingSeqNo;

        private final int chunkSizeInBytes;

        private final ActionListener<Void> listener;

        private final AtomicInteger totalSentOps = new AtomicInteger();

        private final AtomicInteger totalSkippedOps = new AtomicInteger();

        private AtomicBoolean closed = new AtomicBoolean();

        SnapshotSender(Logger logger, SyncAction syncAction, ResyncTask task, ShardId shardId, String primaryAllocationId, long primaryTerm, Translog.Snapshot snapshot, int chunkSizeInBytes, long startingSeqNo, ActionListener<Void> listener) {
            this.logger = logger;
            this.syncAction = syncAction;
            this.task = task;
            this.shardId = shardId;
            this.primaryAllocationId = primaryAllocationId;
            this.primaryTerm = primaryTerm;
            this.snapshot = snapshot;
            this.chunkSizeInBytes = chunkSizeInBytes;
            this.startingSeqNo = startingSeqNo;
            this.listener = listener;
            task.setTotalOperations(snapshot.totalOperations());
        }

        @Override
        public void onResponse(ResyncReplicationResponse response) {
            run();
        }

        @Override
        public void onFailure(Exception e) {
            if (closed.compareAndSet(false, true)) {
                listener.onFailure(e);
            }
        }

        private static Translog.Operation[] EMPTY_ARRAY = new Translog.Operation[0];

        @Override
        protected void doRun() throws Exception {
            long size = 0;
            final List<Translog.Operation> operations = new ArrayList<>();
            task.setPhase("collecting_ops");
            task.setResyncedOperations(totalSentOps.get());
            task.setSkippedOperations(totalSkippedOps.get());
            Translog.Operation operation;
            while ((operation = snapshot.next()) != null) {
                final long seqNo = operation.seqNo();
                if (startingSeqNo >= 0 && (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || seqNo < startingSeqNo)) {
                    totalSkippedOps.incrementAndGet();
                    continue;
                }
                operations.add(operation);
                size += operation.estimateSize();
                totalSentOps.incrementAndGet();
                if (size >= chunkSizeInBytes) {
                    break;
                }
            }
            if (!operations.isEmpty()) {
                task.setPhase("sending_ops");
                ResyncReplicationRequest request = new ResyncReplicationRequest(shardId, operations.toArray(EMPTY_ARRAY));
                logger.trace("{} sending batch of [{}][{}] (total sent: [{}], skipped: [{}])", shardId, operations.size(), new ByteSizeValue(size), totalSentOps.get(), totalSkippedOps.get());
                syncAction.sync(request, task, primaryAllocationId, primaryTerm, this);
            } else if (closed.compareAndSet(false, true)) {
                logger.trace("{} resync completed (total sent: [{}], skipped: [{}])", shardId, totalSentOps.get(), totalSkippedOps.get());
                listener.onResponse(null);
            }
        }
    }

    public static class ResyncRequest extends ActionRequest {

        private final ShardId shardId;

        private final String allocationId;

        public ResyncRequest(ShardId shardId, String allocationId) {
            this.shardId = shardId;
            this.allocationId = allocationId;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId) {
            return new ResyncTask(id, type, action, getDescription(), parentTaskId);
        }

        @Override
        public String getDescription() {
            return toString();
        }

        @Override
        public String toString() {
            return "ResyncRequest{ " + shardId + ", " + allocationId + " }";
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class ResyncTask extends Task {

        private volatile String phase = "starting";

        private volatile int totalOperations;

        private volatile int resyncedOperations;

        private volatile int skippedOperations;

        public ResyncTask(long id, String type, String action, String description, TaskId parentTaskId) {
            super(id, type, action, description, parentTaskId);
        }

        public void setPhase(String phase) {
            this.phase = phase;
        }

        public String getPhase() {
            return phase;
        }

        public int getTotalOperations() {
            return totalOperations;
        }

        public void setTotalOperations(int totalOperations) {
            this.totalOperations = totalOperations;
        }

        public int getResyncedOperations() {
            return resyncedOperations;
        }

        public void setResyncedOperations(int resyncedOperations) {
            this.resyncedOperations = resyncedOperations;
        }

        public int getSkippedOperations() {
            return skippedOperations;
        }

        public void setSkippedOperations(int skippedOperations) {
            this.skippedOperations = skippedOperations;
        }

        @Override
        public ResyncTask.Status getStatus() {
            return new ResyncTask.Status(phase, totalOperations, resyncedOperations, skippedOperations);
        }

        public static class Status implements Task.Status {

            public static final String NAME = "resync";

            private final String phase;

            private final int totalOperations;

            private final int resyncedOperations;

            private final int skippedOperations;

            public Status(StreamInput in) throws IOException {
                phase = in.readString();
                totalOperations = in.readVInt();
                resyncedOperations = in.readVInt();
                skippedOperations = in.readVInt();
            }

            public Status(String phase, int totalOperations, int resyncedOperations, int skippedOperations) {
                this.phase = requireNonNull(phase, "Phase cannot be null");
                this.totalOperations = totalOperations;
                this.resyncedOperations = resyncedOperations;
                this.skippedOperations = skippedOperations;
            }

            @Override
            public String getWriteableName() {
                return NAME;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("phase", phase);
                builder.field("totalOperations", totalOperations);
                builder.field("resyncedOperations", resyncedOperations);
                builder.field("skippedOperations", skippedOperations);
                builder.endObject();
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(phase);
                out.writeVLong(totalOperations);
                out.writeVLong(resyncedOperations);
                out.writeVLong(skippedOperations);
            }

            @Override
            public String toString() {
                return Strings.toString(this);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o)
                    return true;
                if (o == null || getClass() != o.getClass())
                    return false;
                Status status = (Status) o;
                if (totalOperations != status.totalOperations)
                    return false;
                if (resyncedOperations != status.resyncedOperations)
                    return false;
                if (skippedOperations != status.skippedOperations)
                    return false;
                return phase.equals(status.phase);
            }

            @Override
            public int hashCode() {
                int result = phase.hashCode();
                result = 31 * result + totalOperations;
                result = 31 * result + resyncedOperations;
                result = 31 * result + skippedOperations;
                return result;
            }
        }
    }
}