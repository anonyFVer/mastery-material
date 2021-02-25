package org.elasticsearch.search.internal;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public final class AliasFilter implements Writeable {

    private final String[] aliases;

    private final QueryBuilder filter;

    private final boolean reparseAliases;

    public AliasFilter(QueryBuilder filter, String... aliases) {
        this.aliases = aliases == null ? Strings.EMPTY_ARRAY : aliases;
        this.filter = filter;
        reparseAliases = false;
    }

    public AliasFilter(StreamInput input) throws IOException {
        aliases = input.readStringArray();
        if (input.getVersion().onOrAfter(Version.V_5_1_1_UNRELEASED)) {
            filter = input.readOptionalNamedWriteable(QueryBuilder.class);
            reparseAliases = false;
        } else {
            reparseAliases = true;
            filter = null;
        }
    }

    private QueryBuilder reparseFilter(QueryRewriteContext context) {
        if (reparseAliases) {
            final IndexMetaData indexMetaData = context.getIndexSettings().getIndexMetaData();
            return ShardSearchRequest.parseAliasFilter(context::newParseContext, indexMetaData, aliases);
        }
        return filter;
    }

    AliasFilter rewrite(QueryRewriteContext context) throws IOException {
        QueryBuilder queryBuilder = reparseFilter(context);
        if (queryBuilder != null) {
            return new AliasFilter(QueryBuilder.rewriteQuery(queryBuilder, context), aliases);
        }
        return new AliasFilter(filter, aliases);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(aliases);
        if (out.getVersion().onOrAfter(Version.V_5_1_1_UNRELEASED)) {
            out.writeOptionalNamedWriteable(filter);
        }
    }

    public String[] getAliases() {
        return aliases;
    }

    public QueryBuilder getQueryBuilder() {
        if (reparseAliases) {
            throw new IllegalStateException("alias filter for aliases: " + Arrays.toString(aliases) + " must be rewritten first");
        }
        return filter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AliasFilter that = (AliasFilter) o;
        return reparseAliases == that.reparseAliases && Arrays.equals(aliases, that.aliases) && Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aliases, filter, reparseAliases);
    }

    @Override
    public String toString() {
        return "AliasFilter{" + "aliases=" + Arrays.toString(aliases) + ", filter=" + filter + ", reparseAliases=" + reparseAliases + '}';
    }
}