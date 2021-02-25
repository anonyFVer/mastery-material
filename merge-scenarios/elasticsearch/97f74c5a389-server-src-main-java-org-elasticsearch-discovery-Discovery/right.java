package org.elasticsearch.discovery;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import java.io.IOException;

public interface Discovery extends LifecycleComponent {

    void publish(ClusterChangedEvent clusterChangedEvent, AckListener ackListener);

    interface AckListener {

        void onCommit(TimeValue commitTime);

        void onNodeAck(DiscoveryNode node, @Nullable Exception e);
    }

    class FailedToCommitClusterStateException extends ElasticsearchException {

        public FailedToCommitClusterStateException(StreamInput in) throws IOException {
            super(in);
        }

        public FailedToCommitClusterStateException(String msg, Object... args) {
            super(msg, args);
        }

        public FailedToCommitClusterStateException(String msg, Throwable cause, Object... args) {
            super(msg, cause, args);
        }
    }

    DiscoveryStats stats();

    void startInitialJoin();
}