package org.elasticsearch.test.disruption;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportService;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import static org.junit.Assert.assertFalse;

public class NetworkDisruption implements ServiceDisruptionScheme {

    private final Logger logger = Loggers.getLogger(NetworkDisruption.class);

    private final DisruptedLinks disruptedLinks;

    private final NetworkLinkDisruptionType networkLinkDisruptionType;

    protected volatile InternalTestCluster cluster;

    protected volatile boolean activeDisruption = false;

    public NetworkDisruption(DisruptedLinks disruptedLinks, NetworkLinkDisruptionType networkLinkDisruptionType) {
        this.disruptedLinks = disruptedLinks;
        this.networkLinkDisruptionType = networkLinkDisruptionType;
    }

    @Override
    public void applyToCluster(InternalTestCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public void removeFromCluster(InternalTestCluster cluster) {
        stopDisrupting();
    }

    @Override
    public void removeAndEnsureHealthy(InternalTestCluster cluster) {
        removeFromCluster(cluster);
        ensureNodeCount(cluster);
    }

    protected void ensureNodeCount(InternalTestCluster cluster) {
        assertFalse("cluster failed to form after disruption was healed", cluster.client().admin().cluster().prepareHealth().setWaitForNodes("" + cluster.size()).setWaitForRelocatingShards(0).get().isTimedOut());
    }

    @Override
    public synchronized void applyToNode(String node, InternalTestCluster cluster) {
    }

    @Override
    public synchronized void removeFromNode(String node1, InternalTestCluster cluster) {
        logger.info("stop disrupting node (disruption type: {}, disrupted links: {})", networkLinkDisruptionType, disruptedLinks);
        applyToNodes(new String[] { node1 }, cluster.getNodeNames(), networkLinkDisruptionType::removeDisruption);
        applyToNodes(cluster.getNodeNames(), new String[] { node1 }, networkLinkDisruptionType::removeDisruption);
    }

    @Override
    public synchronized void testClusterClosed() {
    }

    @Override
    public synchronized void startDisrupting() {
        logger.info("start disrupting (disruption type: {}, disrupted links: {})", networkLinkDisruptionType, disruptedLinks);
        applyToNodes(cluster.getNodeNames(), cluster.getNodeNames(), networkLinkDisruptionType::applyDisruption);
        activeDisruption = true;
    }

    @Override
    public synchronized void stopDisrupting() {
        if (!activeDisruption) {
            return;
        }
        logger.info("stop disrupting (disruption scheme: {}, disrupted links: {})", networkLinkDisruptionType, disruptedLinks);
        applyToNodes(cluster.getNodeNames(), cluster.getNodeNames(), networkLinkDisruptionType::removeDisruption);
        activeDisruption = false;
    }

    private void applyToNodes(String[] nodes1, String[] nodes2, BiConsumer<MockTransportService, MockTransportService> consumer) {
        for (String node1 : nodes1) {
            if (disruptedLinks.nodes().contains(node1)) {
                for (String node2 : nodes2) {
                    if (disruptedLinks.nodes().contains(node2)) {
                        if (node1.equals(node2) == false) {
                            if (disruptedLinks.disrupt(node1, node2)) {
                                consumer.accept(transport(node1), transport(node2));
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public TimeValue expectedTimeToHeal() {
        return networkLinkDisruptionType.expectedTimeToHeal();
    }

    private MockTransportService transport(String node) {
        return (MockTransportService) cluster.getInstance(TransportService.class, node);
    }

    public abstract static class DisruptedLinks {

        private final Set<String> nodes;

        protected DisruptedLinks(Set<String>... nodeSets) {
            Set<String> allNodes = new HashSet<>();
            for (Set<String> nodeSet : nodeSets) {
                allNodes.addAll(nodeSet);
            }
            this.nodes = allNodes;
        }

        public Set<String> nodes() {
            return nodes;
        }

        public abstract boolean disrupt(String node1, String node2);
    }

    public static class TwoPartitions extends DisruptedLinks {

        protected final Set<String> nodesSideOne;

        protected final Set<String> nodesSideTwo;

        public TwoPartitions(String node1, String node2) {
            this(Collections.singleton(node1), Collections.singleton(node2));
        }

        public TwoPartitions(Set<String> nodesSideOne, Set<String> nodesSideTwo) {
            super(nodesSideOne, nodesSideTwo);
            this.nodesSideOne = nodesSideOne;
            this.nodesSideTwo = nodesSideTwo;
            assert nodesSideOne.isEmpty() == false;
            assert nodesSideTwo.isEmpty() == false;
            assert Sets.haveEmptyIntersection(nodesSideOne, nodesSideTwo);
        }

        public static TwoPartitions random(Random random, String... nodes) {
            return random(random, Sets.newHashSet(nodes));
        }

        public static TwoPartitions random(Random random, Set<String> nodes) {
            assert nodes.size() >= 2 : "two partitions topology requires at least 2 nodes";
            Set<String> nodesSideOne = new HashSet<>();
            Set<String> nodesSideTwo = new HashSet<>();
            for (String node : nodes) {
                if (nodesSideOne.isEmpty()) {
                    nodesSideOne.add(node);
                } else if (nodesSideTwo.isEmpty()) {
                    nodesSideTwo.add(node);
                } else if (random.nextBoolean()) {
                    nodesSideOne.add(node);
                } else {
                    nodesSideTwo.add(node);
                }
            }
            return new TwoPartitions(nodesSideOne, nodesSideTwo);
        }

        @Override
        public boolean disrupt(String node1, String node2) {
            if (nodesSideOne.contains(node1) && nodesSideTwo.contains(node2)) {
                return true;
            }
            if (nodesSideOne.contains(node2) && nodesSideTwo.contains(node1)) {
                return true;
            }
            return false;
        }

        public Set<String> getNodesSideOne() {
            return Collections.unmodifiableSet(nodesSideOne);
        }

        public Set<String> getNodesSideTwo() {
            return Collections.unmodifiableSet(nodesSideTwo);
        }

        public Collection<String> getMajoritySide() {
            if (nodesSideOne.size() >= nodesSideTwo.size()) {
                return getNodesSideOne();
            } else {
                return getNodesSideTwo();
            }
        }

        public Collection<String> getMinoritySide() {
            if (nodesSideOne.size() >= nodesSideTwo.size()) {
                return getNodesSideTwo();
            } else {
                return getNodesSideOne();
            }
        }

        @Override
        public String toString() {
            return "two partitions (partition 1: " + nodesSideOne + " and partition 2: " + nodesSideTwo + ")";
        }
    }

    public static class Bridge extends DisruptedLinks {

        private final String bridgeNode;

        private final Set<String> nodesSideOne;

        private final Set<String> nodesSideTwo;

        public Bridge(String bridgeNode, Set<String> nodesSideOne, Set<String> nodesSideTwo) {
            super(Collections.singleton(bridgeNode), nodesSideOne, nodesSideTwo);
            this.bridgeNode = bridgeNode;
            this.nodesSideOne = nodesSideOne;
            this.nodesSideTwo = nodesSideTwo;
            assert nodesSideOne.isEmpty() == false;
            assert nodesSideTwo.isEmpty() == false;
            assert Sets.haveEmptyIntersection(nodesSideOne, nodesSideTwo);
            assert nodesSideOne.contains(bridgeNode) == false && nodesSideTwo.contains(bridgeNode) == false;
        }

        public static Bridge random(Random random, String... nodes) {
            return random(random, Sets.newHashSet(nodes));
        }

        public static Bridge random(Random random, Set<String> nodes) {
            assert nodes.size() >= 3 : "bridge topology requires at least 3 nodes";
            String bridgeNode = RandomPicks.randomFrom(random, nodes);
            Set<String> nodesSideOne = new HashSet<>();
            Set<String> nodesSideTwo = new HashSet<>();
            for (String node : nodes) {
                if (node.equals(bridgeNode) == false) {
                    if (nodesSideOne.isEmpty()) {
                        nodesSideOne.add(node);
                    } else if (nodesSideTwo.isEmpty()) {
                        nodesSideTwo.add(node);
                    } else if (random.nextBoolean()) {
                        nodesSideOne.add(node);
                    } else {
                        nodesSideTwo.add(node);
                    }
                }
            }
            return new Bridge(bridgeNode, nodesSideOne, nodesSideTwo);
        }

        @Override
        public boolean disrupt(String node1, String node2) {
            if (nodesSideOne.contains(node1) && nodesSideTwo.contains(node2)) {
                return true;
            }
            if (nodesSideOne.contains(node2) && nodesSideTwo.contains(node1)) {
                return true;
            }
            return false;
        }

        public String getBridgeNode() {
            return bridgeNode;
        }

        public Set<String> getNodesSideOne() {
            return nodesSideOne;
        }

        public Set<String> getNodesSideTwo() {
            return nodesSideTwo;
        }

        public String toString() {
            return "bridge partition (super connected node: [" + bridgeNode + "], partition 1: " + nodesSideOne + " and partition 2: " + nodesSideTwo + ")";
        }
    }

    public abstract static class NetworkLinkDisruptionType {

        public abstract void applyDisruption(MockTransportService sourceTransportService, MockTransportService targetTransportService);

        public void removeDisruption(MockTransportService sourceTransportService, MockTransportService targetTransportService) {
            sourceTransportService.clearRule(targetTransportService);
        }

        public TimeValue expectedTimeToHeal() {
            return TimeValue.timeValueMillis(0);
        }
    }

    public static class NetworkDisconnect extends NetworkLinkDisruptionType {

        @Override
        public void applyDisruption(MockTransportService sourceTransportService, MockTransportService targetTransportService) {
            sourceTransportService.addFailToSendNoConnectRule(targetTransportService);
        }

        @Override
        public String toString() {
            return "network disconnects";
        }
    }

    public static class NetworkUnresponsive extends NetworkLinkDisruptionType {

        @Override
        public void applyDisruption(MockTransportService sourceTransportService, MockTransportService targetTransportService) {
            sourceTransportService.addUnresponsiveRule(targetTransportService);
        }

        @Override
        public String toString() {
            return "network unresponsive";
        }
    }

    public static class NetworkDelay extends NetworkLinkDisruptionType {

        public static TimeValue DEFAULT_DELAY_MIN = TimeValue.timeValueSeconds(10);

        public static TimeValue DEFAULT_DELAY_MAX = TimeValue.timeValueSeconds(90);

        private final TimeValue delay;

        public NetworkDelay(TimeValue delay) {
            this.delay = delay;
        }

        public static NetworkDelay random(Random random) {
            return random(random, DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX);
        }

        public static NetworkDelay random(Random random, TimeValue delayMin, TimeValue delayMax) {
            return new NetworkDelay(TimeValue.timeValueMillis(delayMin.millis() == delayMax.millis() ? delayMin.millis() : delayMin.millis() + random.nextInt((int) (delayMax.millis() - delayMin.millis()))));
        }

        @Override
        public void applyDisruption(MockTransportService sourceTransportService, MockTransportService targetTransportService) {
            sourceTransportService.addUnresponsiveRule(targetTransportService, delay);
        }

        @Override
        public TimeValue expectedTimeToHeal() {
            return delay;
        }

        @Override
        public String toString() {
            return "network delays for [" + delay + "]";
        }
    }
}