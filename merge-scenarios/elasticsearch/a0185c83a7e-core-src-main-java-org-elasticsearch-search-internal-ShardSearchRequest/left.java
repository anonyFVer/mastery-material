package org.elasticsearch.search.internal;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.AliasFilterParsingException;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import java.io.IOException;
import java.util.function.Function;

public interface ShardSearchRequest {

    ShardId shardId();

    String[] types();

    SearchSourceBuilder source();

    void source(SearchSourceBuilder source);

    int numberOfShards();

    SearchType searchType();

    QueryBuilder filteringAliases();

    long nowInMillis();

    Boolean requestCache();

    Scroll scroll();

    void setProfile(boolean profile);

    boolean isProfile();

    BytesReference cacheKey() throws IOException;

    void rewrite(QueryShardContext context) throws IOException;

    static QueryBuilder parseAliasFilter(Function<XContentParser, QueryParseContext> contextFactory, IndexMetaData metaData, String... aliasNames) {
        if (aliasNames == null || aliasNames.length == 0) {
            return null;
        }
        Index index = metaData.getIndex();
        ImmutableOpenMap<String, AliasMetaData> aliases = metaData.getAliases();
        Function<AliasMetaData, QueryBuilder> parserFunction = (alias) -> {
            if (alias.filter() == null) {
                return null;
            }
            try {
                byte[] filterSource = alias.filter().uncompressed();
                try (XContentParser parser = XContentFactory.xContent(filterSource).createParser(filterSource)) {
                    return contextFactory.apply(parser).parseInnerQueryBuilder();
                }
            } catch (IOException ex) {
                throw new AliasFilterParsingException(index, alias.getAlias(), "Invalid alias filter", ex);
            }
        };
        if (aliasNames.length == 1) {
            AliasMetaData alias = aliases.get(aliasNames[0]);
            if (alias == null) {
                throw new InvalidAliasNameException(index, aliasNames[0], "Unknown alias name was passed to alias Filter");
            }
            return parserFunction.apply(alias);
        } else {
            BoolQueryBuilder combined = new BoolQueryBuilder();
            for (String aliasName : aliasNames) {
                AliasMetaData alias = aliases.get(aliasName);
                if (alias == null) {
                    throw new InvalidAliasNameException(index, aliasNames[0], "Unknown alias name was passed to alias Filter");
                }
                QueryBuilder parsedFilter = parserFunction.apply(alias);
                if (parsedFilter != null) {
                    combined.should(parsedFilter);
                } else {
                    return null;
                }
            }
            return combined;
        }
    }
}