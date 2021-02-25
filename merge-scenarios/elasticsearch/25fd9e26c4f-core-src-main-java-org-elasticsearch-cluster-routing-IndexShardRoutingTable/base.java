package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import static java.util.Collections.emptyMap;

public class IndexShardRoutingTable implements Iterable<ShardRouting> {

    final ShardShuffler shuffler;

    final ShardId shardId;

    final ShardRouting primary;

    final List<ShardRouting> primaryAsList;

    final List<ShardRouting> replicas;

    final List<ShardRouting> shards;

    final List<ShardRouting> activeShards;

    final List<ShardRouting> assignedShards;

    static final List<ShardRouting> NO_SHARDS = Collections.emptyList();

    final boolean allShardsStarted;

    private volatile Map<AttributesKey, AttributesRoutings> activeShardsByAttributes = emptyMap();

    private volatile Map<AttributesKey, AttributesRoutings> initializingShardsByAttributes = emptyMap();

    private final Object shardsByAttributeMutex = new Object();

    final List<ShardRouting> allInitializingShards;

    IndexShardRoutingTable(ShardId shardId, List<ShardRouting> shards) {
        this.shardId = shardId;
        this.shuffler = new RotationShardShuffler(Randomness.get().nextInt());
        this.shards = Collections.unmodifiableList(shards);
        ShardRouting primary = null;
        List<ShardRouting> replicas = new ArrayList<>();
        List<ShardRouting> activeShards = new ArrayList<>();
        List<ShardRouting> assignedShards = new ArrayList<>();
        List<ShardRouting> allInitializingShards = new ArrayList<>();
        boolean allShardsStarted = true;
        for (ShardRouting shard : shards) {
            if (shard.primary()) {
                primary = shard;
            } else {
                replicas.add(shard);
            }
            if (shard.active()) {
                activeShards.add(shard);
            }
            if (shard.initializing()) {
                allInitializingShards.add(shard);
            }
            if (shard.relocating()) {
                allInitializingShards.add(shard.buildTargetRelocatingShard());
            }
            if (shard.assignedToNode()) {
                assignedShards.add(shard);
            }
            if (shard.state() != ShardRoutingState.STARTED) {
                allShardsStarted = false;
            }
        }
        this.allShardsStarted = allShardsStarted;
        this.primary = primary;
        if (primary != null) {
            this.primaryAsList = Collections.singletonList(primary);
        } else {
            this.primaryAsList = Collections.emptyList();
        }
        this.replicas = Collections.unmodifiableList(replicas);
        this.activeShards = Collections.unmodifiableList(activeShards);
        this.assignedShards = Collections.unmodifiableList(assignedShards);
        this.allInitializingShards = Collections.unmodifiableList(allInitializingShards);
    }

    public ShardId shardId() {
        return shardId;
    }

    public ShardId getShardId() {
        return shardId();
    }

    @Override
    public Iterator<ShardRouting> iterator() {
        return shards.iterator();
    }

    public int size() {
        return shards.size();
    }

    public int getSize() {
        return size();
    }

    public List<ShardRouting> shards() {
        return this.shards;
    }

    public List<ShardRouting> getShards() {
        return shards();
    }

    public List<ShardRouting> activeShards() {
        return this.activeShards;
    }

    public List<ShardRouting> getActiveShards() {
        return activeShards();
    }

    public List<ShardRouting> assignedShards() {
        return this.assignedShards;
    }

    public List<ShardRouting> getAssignedShards() {
        return this.assignedShards;
    }

    public ShardIterator shardsRandomIt() {
        return new PlainShardIterator(shardId, shuffler.shuffle(shards));
    }

    public ShardIterator shardsIt() {
        return new PlainShardIterator(shardId, shards);
    }

    public ShardIterator shardsIt(int seed) {
        return new PlainShardIterator(shardId, shuffler.shuffle(shards, seed));
    }

    public ShardIterator activeInitializingShardsRandomIt() {
        return activeInitializingShardsIt(shuffler.nextSeed());
    }

    public ShardIterator activeInitializingShardsIt(int seed) {
        if (allInitializingShards.isEmpty()) {
            return new PlainShardIterator(shardId, shuffler.shuffle(activeShards, seed));
        }
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        ordered.addAll(shuffler.shuffle(activeShards, seed));
        ordered.addAll(allInitializingShards);
        return new PlainShardIterator(shardId, ordered);
    }

    private boolean noPrimariesActive() {
        if (!primaryAsList.isEmpty() && !primaryAsList.get(0).active() && !primaryAsList.get(0).initializing()) {
            return true;
        }
        return false;
    }

    public ShardIterator primaryShardIt() {
        return new PlainShardIterator(shardId, primaryAsList);
    }

    public ShardIterator primaryActiveInitializingShardIt() {
        if (noPrimariesActive()) {
            return new PlainShardIterator(shardId, NO_SHARDS);
        }
        return primaryShardIt();
    }

    public ShardIterator primaryFirstActiveInitializingShardsIt() {
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        for (ShardRouting shardRouting : shuffler.shuffle(activeShards)) {
            ordered.add(shardRouting);
            if (shardRouting.primary()) {
                ordered.set(ordered.size() - 1, ordered.get(0));
                ordered.set(0, shardRouting);
            }
        }
        if (!allInitializingShards.isEmpty()) {
            ordered.addAll(allInitializingShards);
        }
        return new PlainShardIterator(shardId, ordered);
    }

    public ShardIterator replicaActiveInitializingShardIt() {
        if (noPrimariesActive()) {
            return new PlainShardIterator(shardId, NO_SHARDS);
        }
        LinkedList<ShardRouting> ordered = new LinkedList<>();
        for (ShardRouting replica : shuffler.shuffle(replicas)) {
            if (replica.active()) {
                ordered.addFirst(replica);
            } else if (replica.initializing()) {
                ordered.addLast(replica);
            }
        }
        return new PlainShardIterator(shardId, ordered);
    }

    public ShardIterator replicaFirstActiveInitializingShardsIt() {
        if (noPrimariesActive()) {
            return new PlainShardIterator(shardId, NO_SHARDS);
        }
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        for (ShardRouting replica : shuffler.shuffle(replicas)) {
            if (replica.active()) {
                ordered.add(replica);
            }
        }
        ordered.add(primary);
        if (!allInitializingShards.isEmpty()) {
            ordered.addAll(allInitializingShards);
        }
        return new PlainShardIterator(shardId, ordered);
    }

    public ShardIterator onlyNodeActiveInitializingShardsIt(String nodeId) {
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        int seed = shuffler.nextSeed();
        for (ShardRouting shardRouting : shuffler.shuffle(activeShards, seed)) {
            if (nodeId.equals(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        for (ShardRouting shardRouting : shuffler.shuffle(allInitializingShards, seed)) {
            if (nodeId.equals(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        return new PlainShardIterator(shardId, ordered);
    }

    public ShardIterator onlyNodeSelectorActiveInitializingShardsIt(String nodeAttributes, DiscoveryNodes discoveryNodes) {
        return onlyNodeSelectorActiveInitializingShardsIt(new String[] { nodeAttributes }, discoveryNodes);
    }

    public ShardIterator onlyNodeSelectorActiveInitializingShardsIt(String[] nodeAttributes, DiscoveryNodes discoveryNodes) {
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        Set<String> selectedNodes = Sets.newHashSet(discoveryNodes.resolveNodes(nodeAttributes));
        int seed = shuffler.nextSeed();
        for (ShardRouting shardRouting : shuffler.shuffle(activeShards, seed)) {
            if (selectedNodes.contains(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        for (ShardRouting shardRouting : shuffler.shuffle(allInitializingShards, seed)) {
            if (selectedNodes.contains(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        if (ordered.isEmpty()) {
            final String message = String.format(Locale.ROOT, "no data nodes with %s [%s] found for shard: %s", nodeAttributes.length == 1 ? "criteria" : "criterion", String.join(",", nodeAttributes), shardId());
            throw new IllegalArgumentException(message);
        }
        return new PlainShardIterator(shardId, ordered);
    }

    public ShardIterator preferNodeActiveInitializingShardsIt(Set<String> nodeIds) {
        ArrayList<ShardRouting> preferred = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        ArrayList<ShardRouting> notPreferred = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        for (ShardRouting shardRouting : shuffler.shuffle(activeShards)) {
            if (nodeIds.contains(shardRouting.currentNodeId())) {
                preferred.add(shardRouting);
            } else {
                notPreferred.add(shardRouting);
            }
        }
        preferred.addAll(notPreferred);
        if (!allInitializingShards.isEmpty()) {
            preferred.addAll(allInitializingShards);
        }
        return new PlainShardIterator(shardId, preferred);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        IndexShardRoutingTable that = (IndexShardRoutingTable) o;
        if (!shardId.equals(that.shardId))
            return false;
        if (!shards.equals(that.shards))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = shardId.hashCode();
        result = 31 * result + shards.hashCode();
        return result;
    }

    public boolean allShardsStarted() {
        return allShardsStarted;
    }

    static class AttributesKey {

        final String[] attributes;

        AttributesKey(String[] attributes) {
            this.attributes = attributes;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(attributes);
        }

        @Override
        public boolean equals(Object obj) {
            return Arrays.equals(attributes, ((AttributesKey) obj).attributes);
        }
    }

    static class AttributesRoutings {

        public final List<ShardRouting> withSameAttribute;

        public final List<ShardRouting> withoutSameAttribute;

        public final int totalSize;

        AttributesRoutings(List<ShardRouting> withSameAttribute, List<ShardRouting> withoutSameAttribute) {
            this.withSameAttribute = withSameAttribute;
            this.withoutSameAttribute = withoutSameAttribute;
            this.totalSize = withoutSameAttribute.size() + withSameAttribute.size();
        }
    }

    private AttributesRoutings getActiveAttribute(AttributesKey key, DiscoveryNodes nodes) {
        AttributesRoutings shardRoutings = activeShardsByAttributes.get(key);
        if (shardRoutings == null) {
            synchronized (shardsByAttributeMutex) {
                ArrayList<ShardRouting> from = new ArrayList<>(activeShards);
                List<ShardRouting> to = collectAttributeShards(key, nodes, from);
                shardRoutings = new AttributesRoutings(to, Collections.unmodifiableList(from));
                activeShardsByAttributes = MapBuilder.newMapBuilder(activeShardsByAttributes).put(key, shardRoutings).immutableMap();
            }
        }
        return shardRoutings;
    }

    private AttributesRoutings getInitializingAttribute(AttributesKey key, DiscoveryNodes nodes) {
        AttributesRoutings shardRoutings = initializingShardsByAttributes.get(key);
        if (shardRoutings == null) {
            synchronized (shardsByAttributeMutex) {
                ArrayList<ShardRouting> from = new ArrayList<>(allInitializingShards);
                List<ShardRouting> to = collectAttributeShards(key, nodes, from);
                shardRoutings = new AttributesRoutings(to, Collections.unmodifiableList(from));
                initializingShardsByAttributes = MapBuilder.newMapBuilder(initializingShardsByAttributes).put(key, shardRoutings).immutableMap();
            }
        }
        return shardRoutings;
    }

    private static List<ShardRouting> collectAttributeShards(AttributesKey key, DiscoveryNodes nodes, ArrayList<ShardRouting> from) {
        final ArrayList<ShardRouting> to = new ArrayList<>();
        for (final String attribute : key.attributes) {
            final String localAttributeValue = nodes.getLocalNode().getAttributes().get(attribute);
            if (localAttributeValue != null) {
                for (Iterator<ShardRouting> iterator = from.iterator(); iterator.hasNext(); ) {
                    ShardRouting fromShard = iterator.next();
                    final DiscoveryNode discoveryNode = nodes.get(fromShard.currentNodeId());
                    if (discoveryNode == null) {
                        iterator.remove();
                    } else if (localAttributeValue.equals(discoveryNode.getAttributes().get(attribute))) {
                        iterator.remove();
                        to.add(fromShard);
                    }
                }
            }
        }
        return Collections.unmodifiableList(to);
    }

    public ShardIterator preferAttributesActiveInitializingShardsIt(String[] attributes, DiscoveryNodes nodes) {
        return preferAttributesActiveInitializingShardsIt(attributes, nodes, shuffler.nextSeed());
    }

    public ShardIterator preferAttributesActiveInitializingShardsIt(String[] attributes, DiscoveryNodes nodes, int seed) {
        AttributesKey key = new AttributesKey(attributes);
        AttributesRoutings activeRoutings = getActiveAttribute(key, nodes);
        AttributesRoutings initializingRoutings = getInitializingAttribute(key, nodes);
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeRoutings.totalSize + initializingRoutings.totalSize);
        ordered.addAll(shuffler.shuffle(activeRoutings.withSameAttribute, seed));
        ordered.addAll(shuffler.shuffle(activeRoutings.withoutSameAttribute, seed));
        ordered.addAll(shuffler.shuffle(initializingRoutings.withSameAttribute, seed));
        ordered.addAll(shuffler.shuffle(initializingRoutings.withoutSameAttribute, seed));
        return new PlainShardIterator(shardId, ordered);
    }

    public ShardRouting primaryShard() {
        return primary;
    }

    public List<ShardRouting> replicaShards() {
        return this.replicas;
    }

    public List<ShardRouting> replicaShardsWithState(ShardRoutingState... states) {
        List<ShardRouting> shards = new ArrayList<>();
        for (ShardRouting shardEntry : replicas) {
            for (ShardRoutingState state : states) {
                if (shardEntry.state() == state) {
                    shards.add(shardEntry);
                }
            }
        }
        return shards;
    }

    public List<ShardRouting> shardsWithState(ShardRoutingState state) {
        if (state == ShardRoutingState.INITIALIZING) {
            return allInitializingShards;
        }
        List<ShardRouting> shards = new ArrayList<>();
        for (ShardRouting shardEntry : this) {
            if (shardEntry.state() == state) {
                shards.add(shardEntry);
            }
        }
        return shards;
    }

    public static class Builder {

        private ShardId shardId;

        private final List<ShardRouting> shards;

        public Builder(IndexShardRoutingTable indexShard) {
            this.shardId = indexShard.shardId;
            this.shards = new ArrayList<>(indexShard.shards);
        }

        public Builder(ShardId shardId) {
            this.shardId = shardId;
            this.shards = new ArrayList<>();
        }

        public Builder addShard(ShardRouting shardEntry) {
            for (ShardRouting shard : shards) {
                if (shard.assignedToNode() && shardEntry.assignedToNode() && shard.currentNodeId().equals(shardEntry.currentNodeId())) {
                    return this;
                }
            }
            shards.add(shardEntry);
            return this;
        }

        public Builder removeShard(ShardRouting shardEntry) {
            shards.remove(shardEntry);
            return this;
        }

        public IndexShardRoutingTable build() {
            return new IndexShardRoutingTable(shardId, Collections.unmodifiableList(new ArrayList<>(shards)));
        }

        public static IndexShardRoutingTable readFrom(StreamInput in) throws IOException {
            Index index = new Index(in);
            return readFromThin(in, index);
        }

        public static IndexShardRoutingTable readFromThin(StreamInput in, Index index) throws IOException {
            int iShardId = in.readVInt();
            ShardId shardId = new ShardId(index, iShardId);
            Builder builder = new Builder(shardId);
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                ShardRouting shard = new ShardRouting(shardId, in);
                builder.addShard(shard);
            }
            return builder.build();
        }

        public static void writeTo(IndexShardRoutingTable indexShard, StreamOutput out) throws IOException {
            out.writeString(indexShard.shardId().getIndex().getName());
            writeToThin(indexShard, out);
        }

        public static void writeToThin(IndexShardRoutingTable indexShard, StreamOutput out) throws IOException {
            out.writeVInt(indexShard.shardId.id());
            out.writeVInt(indexShard.shards.size());
            for (ShardRouting entry : indexShard) {
                entry.writeToThin(out);
            }
        }
    }
}