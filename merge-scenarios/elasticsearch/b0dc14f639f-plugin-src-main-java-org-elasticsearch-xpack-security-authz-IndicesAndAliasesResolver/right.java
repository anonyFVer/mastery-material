package org.elasticsearch.xpack.security.authz;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;
import org.elasticsearch.action.AliasesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.graph.action.GraphExploreRequest;

public class IndicesAndAliasesResolver {

    public static final String NO_INDEX_PLACEHOLDER = "-*";

    private static final ResolvedIndices NO_INDEX_PLACEHOLDER_RESOLVED = ResolvedIndices.local(NO_INDEX_PLACEHOLDER);

    private static final String[] NO_INDICES_ARRAY = new String[] { "*", "-*" };

    static final List<String> NO_INDICES_LIST = Arrays.asList(NO_INDICES_ARRAY);

    private final IndexNameExpressionResolver nameExpressionResolver;

    private final RemoteClusterResolver remoteClusterResolver;

    public IndicesAndAliasesResolver(Settings settings, ClusterService clusterService) {
        this.nameExpressionResolver = new IndexNameExpressionResolver(settings);
        this.remoteClusterResolver = new RemoteClusterResolver(settings, clusterService.getClusterSettings());
    }

    public ResolvedIndices resolve(TransportRequest request, MetaData metaData, AuthorizedIndices authorizedIndices) {
        if (request instanceof IndicesAliasesRequest) {
            ResolvedIndices indices = ResolvedIndices.empty();
            IndicesAliasesRequest indicesAliasesRequest = (IndicesAliasesRequest) request;
            for (IndicesRequest indicesRequest : indicesAliasesRequest.getAliasActions()) {
                indices = ResolvedIndices.add(indices, resolveIndicesAndAliases(indicesRequest, metaData, authorizedIndices));
            }
            return indices;
        }
        if (request instanceof IndicesRequest == false) {
            throw new IllegalStateException("Request [" + request + "] is not an Indices request, but should be.");
        }
        return resolveIndicesAndAliases((IndicesRequest) request, metaData, authorizedIndices);
    }

    ResolvedIndices resolveIndicesAndAliases(IndicesRequest indicesRequest, MetaData metaData, AuthorizedIndices authorizedIndices) {
        boolean indicesReplacedWithNoIndices = false;
        final ResolvedIndices indices;
        if (indicesRequest instanceof PutMappingRequest && ((PutMappingRequest) indicesRequest).getConcreteIndex() != null) {
            assert indicesRequest.indices() == null || indicesRequest.indices().length == 0 : "indices are: " + Arrays.toString(indicesRequest.indices());
            return ResolvedIndices.local(((PutMappingRequest) indicesRequest).getConcreteIndex().getName());
        } else if (indicesRequest instanceof IndicesRequest.Replaceable) {
            IndicesRequest.Replaceable replaceable = (IndicesRequest.Replaceable) indicesRequest;
            final boolean replaceWildcards = indicesRequest.indicesOptions().expandWildcardsOpen() || indicesRequest.indicesOptions().expandWildcardsClosed();
            IndicesOptions indicesOptions = indicesRequest.indicesOptions();
            if (indicesRequest instanceof IndicesExistsRequest) {
                indicesOptions = IndicesOptions.fromOptions(true, true, indicesOptions.expandWildcardsOpen(), indicesOptions.expandWildcardsClosed());
            }
            ResolvedIndices result = ResolvedIndices.empty();
            if (IndexNameExpressionResolver.isAllIndices(indicesList(indicesRequest.indices()))) {
                if (replaceWildcards) {
                    for (String authorizedIndex : authorizedIndices.get()) {
                        if (isIndexVisible(authorizedIndex, indicesOptions, metaData)) {
                            result = ResolvedIndices.add(result, ResolvedIndices.local(authorizedIndex));
                        }
                    }
                }
            } else {
                final ResolvedIndices split;
                if (allowsRemoteIndices(indicesRequest)) {
                    split = remoteClusterResolver.splitLocalAndRemoteIndexNames(indicesRequest.indices());
                } else {
                    split = ResolvedIndices.local(indicesRequest.indices());
                }
                List<String> replaced = replaceWildcardsWithAuthorizedIndices(split.getLocal(), indicesOptions, metaData, authorizedIndices.get(), replaceWildcards);
                if (indicesOptions.ignoreUnavailable()) {
                    replaced = replaced.stream().filter(authorizedIndices.get()::contains).collect(Collectors.toList());
                }
                result = new ResolvedIndices(new ArrayList<>(replaced), split.getRemote());
            }
            if (result.isEmpty()) {
                if (indicesOptions.allowNoIndices()) {
                    replaceable.indices(NO_INDICES_ARRAY);
                    indicesReplacedWithNoIndices = true;
                    indices = NO_INDEX_PLACEHOLDER_RESOLVED;
                } else {
                    throw new IndexNotFoundException(Arrays.toString(indicesRequest.indices()));
                }
            } else {
                replaceable.indices(result.toArray());
                indices = result;
            }
        } else {
            if (containsWildcards(indicesRequest)) {
                throw new IllegalStateException("There are no external requests known to support wildcards that don't support replacing " + "their indices");
            }
            List<String> resolvedNames = new ArrayList<>();
            for (String name : indicesRequest.indices()) {
                resolvedNames.add(nameExpressionResolver.resolveDateMathExpression(name));
            }
            indices = new ResolvedIndices(resolvedNames, new ArrayList<>());
        }
        if (indicesRequest instanceof AliasesRequest) {
            AliasesRequest aliasesRequest = (AliasesRequest) indicesRequest;
            if (aliasesRequest.expandAliasesWildcards()) {
                List<String> aliases = replaceWildcardsWithAuthorizedAliases(aliasesRequest.aliases(), loadAuthorizedAliases(authorizedIndices.get(), metaData));
                aliasesRequest.aliases(aliases.toArray(new String[aliases.size()]));
            }
            if (indicesReplacedWithNoIndices) {
                if (indicesRequest instanceof GetAliasesRequest == false) {
                    throw new IllegalStateException(GetAliasesRequest.class.getSimpleName() + " is the only known " + "request implementing " + AliasesRequest.class.getSimpleName() + " that may allow no indices. Found [" + indicesRequest.getClass().getName() + "] which ended up with an empty set of indices.");
                }
            } else {
                return ResolvedIndices.add(indices, ResolvedIndices.local(aliasesRequest.aliases()));
            }
        }
        return indices;
    }

    public static boolean allowsRemoteIndices(IndicesRequest request) {
        return request instanceof SearchRequest || request instanceof FieldCapabilitiesRequest || request instanceof GraphExploreRequest;
    }

    private List<String> loadAuthorizedAliases(List<String> authorizedIndices, MetaData metaData) {
        List<String> authorizedAliases = new ArrayList<>();
        SortedMap<String, AliasOrIndex> existingAliases = metaData.getAliasAndIndexLookup();
        for (String authorizedIndex : authorizedIndices) {
            AliasOrIndex aliasOrIndex = existingAliases.get(authorizedIndex);
            if (aliasOrIndex != null && aliasOrIndex.isAlias()) {
                authorizedAliases.add(authorizedIndex);
            }
        }
        return authorizedAliases;
    }

    private List<String> replaceWildcardsWithAuthorizedAliases(String[] aliases, List<String> authorizedAliases) {
        List<String> finalAliases = new ArrayList<>();
        boolean matchAllAliases = aliases.length == 0;
        if (matchAllAliases) {
            finalAliases.addAll(authorizedAliases);
        }
        for (String aliasPattern : aliases) {
            if (aliasPattern.equals(MetaData.ALL)) {
                matchAllAliases = true;
                finalAliases.addAll(authorizedAliases);
            } else if (Regex.isSimpleMatchPattern(aliasPattern)) {
                for (String authorizedAlias : authorizedAliases) {
                    if (Regex.simpleMatch(aliasPattern, authorizedAlias)) {
                        finalAliases.add(authorizedAlias);
                    }
                }
            } else {
                finalAliases.add(aliasPattern);
            }
        }
        if (finalAliases.isEmpty()) {
            String indexName = matchAllAliases ? MetaData.ALL : Arrays.toString(aliases);
            throw new IndexNotFoundException(indexName);
        }
        return finalAliases;
    }

    private boolean containsWildcards(IndicesRequest indicesRequest) {
        if (IndexNameExpressionResolver.isAllIndices(indicesList(indicesRequest.indices()))) {
            return true;
        }
        for (String index : indicesRequest.indices()) {
            if (Regex.isSimpleMatchPattern(index)) {
                return true;
            }
        }
        return false;
    }

    private List<String> replaceWildcardsWithAuthorizedIndices(Iterable<String> indices, IndicesOptions indicesOptions, MetaData metaData, List<String> authorizedIndices, boolean replaceWildcards) {
        List<String> finalIndices = new ArrayList<>();
        boolean wildcardSeen = false;
        for (String index : indices) {
            String aliasOrIndex;
            boolean minus = false;
            if (index.charAt(0) == '-' && wildcardSeen) {
                aliasOrIndex = index.substring(1);
                minus = true;
            } else {
                aliasOrIndex = index;
            }
            final String dateMathName = nameExpressionResolver.resolveDateMathExpression(aliasOrIndex);
            if (dateMathName != aliasOrIndex) {
                assert dateMathName.equals(aliasOrIndex) == false;
                if (replaceWildcards && Regex.isSimpleMatchPattern(dateMathName)) {
                    aliasOrIndex = dateMathName;
                } else if (authorizedIndices.contains(dateMathName) && isIndexVisible(dateMathName, indicesOptions, metaData, true)) {
                    if (minus) {
                        finalIndices.remove(dateMathName);
                    } else {
                        finalIndices.add(dateMathName);
                    }
                } else {
                    if (indicesOptions.ignoreUnavailable() == false) {
                        throw new IndexNotFoundException(dateMathName);
                    }
                }
            }
            if (replaceWildcards && Regex.isSimpleMatchPattern(aliasOrIndex)) {
                wildcardSeen = true;
                Set<String> resolvedIndices = new HashSet<>();
                for (String authorizedIndex : authorizedIndices) {
                    if (Regex.simpleMatch(aliasOrIndex, authorizedIndex) && isIndexVisible(authorizedIndex, indicesOptions, metaData)) {
                        resolvedIndices.add(authorizedIndex);
                    }
                }
                if (resolvedIndices.isEmpty()) {
                    if (indicesOptions.allowNoIndices() == false) {
                        throw new IndexNotFoundException(aliasOrIndex);
                    }
                } else {
                    if (minus) {
                        finalIndices.removeAll(resolvedIndices);
                    } else {
                        finalIndices.addAll(resolvedIndices);
                    }
                }
            } else if (dateMathName == aliasOrIndex) {
                assert dateMathName.equals(aliasOrIndex);
                if (minus) {
                    finalIndices.remove(aliasOrIndex);
                } else {
                    finalIndices.add(aliasOrIndex);
                }
            }
        }
        return finalIndices;
    }

    private static boolean isIndexVisible(String index, IndicesOptions indicesOptions, MetaData metaData) {
        return isIndexVisible(index, indicesOptions, metaData, false);
    }

    private static boolean isIndexVisible(String index, IndicesOptions indicesOptions, MetaData metaData, boolean dateMathExpression) {
        AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(index);
        if (aliasOrIndex.isAlias()) {
            return indicesOptions.ignoreAliases() == false;
        }
        assert aliasOrIndex.getIndices().size() == 1 : "concrete index must point to a single index";
        IndexMetaData indexMetaData = aliasOrIndex.getIndices().get(0);
        if (indexMetaData.getState() == IndexMetaData.State.CLOSE && (indicesOptions.expandWildcardsClosed() || dateMathExpression)) {
            return true;
        }
        if (indexMetaData.getState() == IndexMetaData.State.OPEN && (indicesOptions.expandWildcardsOpen() || dateMathExpression)) {
            return true;
        }
        return false;
    }

    private static List<String> indicesList(String[] list) {
        return (list == null) ? null : Arrays.asList(list);
    }

    private static class RemoteClusterResolver extends RemoteClusterAware {

        private final CopyOnWriteArraySet<String> clusters;

        private RemoteClusterResolver(Settings settings, ClusterSettings clusterSettings) {
            super(settings);
            clusters = new CopyOnWriteArraySet<>(buildRemoteClustersSeeds(settings).keySet());
            listenForUpdates(clusterSettings);
        }

        @Override
        protected Set<String> getRemoteClusterNames() {
            return clusters;
        }

        @Override
        protected void updateRemoteCluster(String clusterAlias, List<InetSocketAddress> addresses) {
            if (addresses.isEmpty()) {
                clusters.remove(clusterAlias);
            } else {
                clusters.add(clusterAlias);
            }
        }

        ResolvedIndices splitLocalAndRemoteIndexNames(String... indices) {
            final Map<String, List<String>> map = super.groupClusterIndices(indices, exists -> false);
            final List<String> local = map.remove(LOCAL_CLUSTER_GROUP_KEY);
            final List<String> remote = map.entrySet().stream().flatMap(e -> e.getValue().stream().map(v -> e.getKey() + REMOTE_CLUSTER_INDEX_SEPARATOR + v)).collect(Collectors.toList());
            return new ResolvedIndices(local == null ? Collections.emptyList() : local, remote);
        }
    }

    public static class ResolvedIndices {

        private final List<String> local;

        private final List<String> remote;

        ResolvedIndices(List<String> local, List<String> remote) {
            this.local = local;
            this.remote = remote;
        }

        private static ResolvedIndices empty() {
            return new ResolvedIndices(Collections.emptyList(), Collections.emptyList());
        }

        private static ResolvedIndices local(String... names) {
            return new ResolvedIndices(Arrays.asList(names), Collections.emptyList());
        }

        public List<String> getLocal() {
            return Collections.unmodifiableList(local);
        }

        public List<String> getRemote() {
            return Collections.unmodifiableList(remote);
        }

        public boolean isEmpty() {
            return local.isEmpty() && remote.isEmpty();
        }

        public boolean isNoIndicesPlaceholder() {
            return remote.isEmpty() && local.size() == 1 && local.contains(IndicesAndAliasesResolver.NO_INDEX_PLACEHOLDER);
        }

        private String[] toArray() {
            final String[] array = new String[local.size() + remote.size()];
            int i = 0;
            for (String index : local) {
                array[i++] = index;
            }
            for (String index : remote) {
                array[i++] = index;
            }
            return array;
        }

        private static ResolvedIndices add(ResolvedIndices a, ResolvedIndices b) {
            List<String> local = new ArrayList<>(a.local.size() + b.local.size());
            local.addAll(a.local);
            local.addAll(b.local);
            List<String> remote = new ArrayList<>(a.remote.size() + b.remote.size());
            remote.addAll(a.remote);
            remote.addAll(b.remote);
            return new ResolvedIndices(local, remote);
        }
    }
}