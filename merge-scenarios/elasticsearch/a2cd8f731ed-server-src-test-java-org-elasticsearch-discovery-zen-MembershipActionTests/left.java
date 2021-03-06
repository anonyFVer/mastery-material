package org.elasticsearch.discovery.zen;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.coordination.JoinTaskExecutor;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import java.util.stream.Collectors;
import static org.elasticsearch.test.VersionUtils.allVersions;
import static org.elasticsearch.test.VersionUtils.getPreviousVersion;
import static org.elasticsearch.test.VersionUtils.incompatibleFutureVersion;
import static org.elasticsearch.test.VersionUtils.maxCompatibleVersion;
import static org.elasticsearch.test.VersionUtils.randomCompatibleVersion;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.elasticsearch.test.VersionUtils.randomVersionBetween;

public class MembershipActionTests extends ESTestCase {

    public void testPreventJoinClusterWithNewerIndices() {
        Settings.builder().build();
        MetaData.Builder metaBuilder = MetaData.builder();
        IndexMetaData indexMetaData = IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1).build();
        metaBuilder.put(indexMetaData, false);
        MetaData metaData = metaBuilder.build();
        JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT, metaData);
        expectThrows(IllegalStateException.class, () -> JoinTaskExecutor.ensureIndexCompatibility(VersionUtils.getPreviousVersion(Version.CURRENT), metaData));
    }

    public void testPreventJoinClusterWithUnsupportedIndices() {
        Settings.builder().build();
        MetaData.Builder metaBuilder = MetaData.builder();
        IndexMetaData indexMetaData = IndexMetaData.builder("test").settings(settings(VersionUtils.getPreviousVersion(Version.CURRENT.minimumIndexCompatibilityVersion()))).numberOfShards(1).numberOfReplicas(1).build();
        metaBuilder.put(indexMetaData, false);
        MetaData metaData = metaBuilder.build();
        expectThrows(IllegalStateException.class, () -> JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT, metaData));
    }

    public void testPreventJoinClusterWithUnsupportedNodeVersions() {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        final Version version = randomVersion(random());
        builder.add(new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), version));
        builder.add(new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), randomCompatibleVersion(random(), version)));
        DiscoveryNodes nodes = builder.build();
        final Version maxNodeVersion = nodes.getMaxNodeVersion();
        final Version minNodeVersion = nodes.getMinNodeVersion();
        if (maxNodeVersion.onOrAfter(Version.V_6_0_0_alpha1)) {
            final Version tooLow = getPreviousVersion(maxNodeVersion.minimumCompatibilityVersion());
            expectThrows(IllegalStateException.class, () -> {
                if (randomBoolean()) {
                    JoinTaskExecutor.ensureNodesCompatibility(tooLow, nodes);
                } else {
                    JoinTaskExecutor.ensureNodesCompatibility(tooLow, minNodeVersion, maxNodeVersion);
                }
            });
        }
        if (minNodeVersion.before(Version.V_5_5_0)) {
            Version tooHigh = incompatibleFutureVersion(minNodeVersion);
            expectThrows(IllegalStateException.class, () -> {
                if (randomBoolean()) {
                    JoinTaskExecutor.ensureNodesCompatibility(tooHigh, nodes);
                } else {
                    JoinTaskExecutor.ensureNodesCompatibility(tooHigh, minNodeVersion, maxNodeVersion);
                }
            });
        }
        if (minNodeVersion.onOrAfter(Version.V_6_0_0_alpha1)) {
            Version oldMajor = randomFrom(allVersions().stream().filter(v -> v.major < 6).collect(Collectors.toList()));
            expectThrows(IllegalStateException.class, () -> JoinTaskExecutor.ensureMajorVersionBarrier(oldMajor, minNodeVersion));
        }
        final Version minGoodVersion = maxNodeVersion.major == minNodeVersion.major ? minNodeVersion : maxNodeVersion.minimumCompatibilityVersion();
        final Version justGood = randomVersionBetween(random(), minGoodVersion, maxCompatibleVersion(minNodeVersion));
        if (randomBoolean()) {
            JoinTaskExecutor.ensureNodesCompatibility(justGood, nodes);
        } else {
            JoinTaskExecutor.ensureNodesCompatibility(justGood, minNodeVersion, maxNodeVersion);
        }
    }

    public void testSuccess() {
        Settings.builder().build();
        MetaData.Builder metaBuilder = MetaData.builder();
        IndexMetaData indexMetaData = IndexMetaData.builder("test").settings(settings(VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT))).numberOfShards(1).numberOfReplicas(1).build();
        metaBuilder.put(indexMetaData, false);
        indexMetaData = IndexMetaData.builder("test1").settings(settings(VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT))).numberOfShards(1).numberOfReplicas(1).build();
        metaBuilder.put(indexMetaData, false);
        MetaData metaData = metaBuilder.build();
        JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT, metaData);
    }
}