package org.elasticsearch.discovery.single;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterApplier.ClusterApplyListener;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.transport.TransportService;
import java.util.Objects;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public class SingleNodeDiscovery extends AbstractLifecycleComponent implements Discovery {

    protected final TransportService transportService;

    private final ClusterApplier clusterApplier;

    private volatile ClusterState clusterState;

    public SingleNodeDiscovery(final Settings settings, final TransportService transportService, final MasterService masterService, final ClusterApplier clusterApplier) {
        super(Objects.requireNonNull(settings));
        this.transportService = Objects.requireNonNull(transportService);
        masterService.setClusterStateSupplier(() -> clusterState);
        this.clusterApplier = clusterApplier;
    }

    @Override
    public synchronized void publish(final ClusterChangedEvent event, ActionListener<Void> publishListener, final AckListener ackListener) {
        clusterState = event.state();
        ackListener.onCommit(TimeValue.ZERO);
        clusterApplier.onNewClusterState("apply-locally-on-node[" + event.source() + "]", () -> clusterState, new ClusterApplyListener() {

            @Override
            public void onSuccess(String source) {
                publishListener.onResponse(null);
                ackListener.onNodeAck(transportService.getLocalNode(), null);
            }

            @Override
            public void onFailure(String source, Exception e) {
                publishListener.onFailure(e);
                ackListener.onNodeAck(transportService.getLocalNode(), e);
                logger.warn(() -> new ParameterizedMessage("failed while applying cluster state locally [{}]", event.source()), e);
            }
        });
    }

    @Override
    public DiscoveryStats stats() {
        return new DiscoveryStats(null, null);
    }

    @Override
    public synchronized void startInitialJoin() {
        if (lifecycle.started() == false) {
            throw new IllegalStateException("can't start initial join when not started");
        }
        clusterState = ClusterState.builder(clusterState).build();
        clusterApplier.onNewClusterState("single-node-start-initial-join", () -> clusterState, (source, e) -> {
        });
    }

    @Override
    protected synchronized void doStart() {
        DiscoveryNode localNode = transportService.getLocalNode();
        clusterState = createInitialState(localNode);
        clusterApplier.setInitialState(clusterState);
    }

    protected ClusterState createInitialState(DiscoveryNode localNode) {
        ClusterState.Builder builder = clusterApplier.newClusterStateBuilder();
        return builder.nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()).build()).blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK)).build();
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
    }
}