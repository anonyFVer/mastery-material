package org.elasticsearch.discovery;

import org.elasticsearch.cluster.coordination.ClusterStatePublisher;
import org.elasticsearch.common.component.LifecycleComponent;

public interface Discovery extends LifecycleComponent, ClusterStatePublisher {

    DiscoveryStats stats();

    void startInitialJoin();
}