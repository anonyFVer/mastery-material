package org.elasticsearch.action.admin.cluster.shards;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ClusterSearchShardsResponse extends ActionResponse implements ToXContent {

    private ClusterSearchShardsGroup[] groups;

    private DiscoveryNode[] nodes;

    private Map<String, AliasFilter> indicesAndFilters;

    public ClusterSearchShardsResponse() {
    }

    ClusterSearchShardsResponse(ClusterSearchShardsGroup[] groups, DiscoveryNode[] nodes, Map<String, AliasFilter> indicesAndFilters) {
        this.groups = groups;
        this.nodes = nodes;
        this.indicesAndFilters = indicesAndFilters;
    }

    public ClusterSearchShardsGroup[] getGroups() {
        return groups;
    }

    public DiscoveryNode[] getNodes() {
        return nodes;
    }

    public Map<String, AliasFilter> getIndicesAndFilters() {
        return indicesAndFilters;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        groups = new ClusterSearchShardsGroup[in.readVInt()];
        for (int i = 0; i < groups.length; i++) {
            groups[i] = ClusterSearchShardsGroup.readSearchShardsGroupResponse(in);
        }
        nodes = new DiscoveryNode[in.readVInt()];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new DiscoveryNode(in);
        }
        if (in.getVersion().onOrAfter(Version.V_5_1_1_UNRELEASED)) {
            int size = in.readVInt();
            indicesAndFilters = new HashMap<>();
            for (int i = 0; i < size; i++) {
                String index = in.readString();
                AliasFilter aliasFilter = new AliasFilter(in);
                indicesAndFilters.put(index, aliasFilter);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(groups.length);
        for (ClusterSearchShardsGroup response : groups) {
            response.writeTo(out);
        }
        out.writeVInt(nodes.length);
        for (DiscoveryNode node : nodes) {
            node.writeTo(out);
        }
        if (out.getVersion().onOrAfter(Version.V_5_1_1_UNRELEASED)) {
            out.writeVInt(indicesAndFilters.size());
            for (Map.Entry<String, AliasFilter> entry : indicesAndFilters.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (DiscoveryNode node : nodes) {
            node.toXContent(builder, params);
        }
        builder.endObject();
        if (indicesAndFilters != null) {
            builder.startObject("indices");
            for (Map.Entry<String, AliasFilter> entry : indicesAndFilters.entrySet()) {
                String index = entry.getKey();
                builder.startObject(index);
                AliasFilter aliasFilter = entry.getValue();
                if (aliasFilter.getAliases().length > 0) {
                    builder.array("aliases", aliasFilter.getAliases());
                    builder.field("filter");
                    aliasFilter.getQueryBuilder().toXContent(builder, params);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.startArray("shards");
        for (ClusterSearchShardsGroup group : groups) {
            group.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }
}