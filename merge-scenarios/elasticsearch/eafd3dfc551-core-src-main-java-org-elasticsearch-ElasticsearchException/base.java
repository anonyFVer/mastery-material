package org.elasticsearch;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TcpTransport;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_UUID_NA_VALUE;

public class ElasticsearchException extends RuntimeException implements ToXContent, Writeable {

    public static final String REST_EXCEPTION_SKIP_CAUSE = "rest.exception.cause.skip";

    public static final String REST_EXCEPTION_SKIP_STACK_TRACE = "rest.exception.stacktrace.skip";

    public static final boolean REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT = true;

    public static final boolean REST_EXCEPTION_SKIP_CAUSE_DEFAULT = false;

    private static final String INDEX_HEADER_KEY = "es.index";

    private static final String INDEX_HEADER_KEY_UUID = "es.index_uuid";

    private static final String SHARD_HEADER_KEY = "es.shard";

    private static final String RESOURCE_HEADER_TYPE_KEY = "es.resource.type";

    private static final String RESOURCE_HEADER_ID_KEY = "es.resource.id";

    private static final Map<Integer, FunctionThatThrowsIOException<StreamInput, ? extends ElasticsearchException>> ID_TO_SUPPLIER;

    private static final Map<Class<? extends ElasticsearchException>, ElasticsearchExceptionHandle> CLASS_TO_ELASTICSEARCH_EXCEPTION_HANDLE;

    private final Map<String, List<String>> headers = new HashMap<>();

    public ElasticsearchException(Throwable cause) {
        super(cause);
    }

    public ElasticsearchException(String msg, Object... args) {
        super(LoggerMessageFormat.format(msg, args));
    }

    public ElasticsearchException(String msg, Throwable cause, Object... args) {
        super(LoggerMessageFormat.format(msg, args), cause);
    }

    public ElasticsearchException(StreamInput in) throws IOException {
        super(in.readOptionalString(), in.readException());
        readStackTrace(this, in);
        headers.putAll(in.readMapOfLists(StreamInput::readString, StreamInput::readString));
    }

    public void addHeader(String key, String... value) {
        this.headers.put(key, Arrays.asList(value));
    }

    public void addHeader(String key, List<String> value) {
        this.headers.put(key, value);
    }

    public Set<String> getHeaderKeys() {
        return headers.keySet();
    }

    public List<String> getHeader(String key) {
        return headers.get(key);
    }

    public RestStatus status() {
        Throwable cause = unwrapCause();
        if (cause == this) {
            return RestStatus.INTERNAL_SERVER_ERROR;
        } else {
            return ExceptionsHelper.status(cause);
        }
    }

    public Throwable unwrapCause() {
        return ExceptionsHelper.unwrapCause(this);
    }

    public String getDetailedMessage() {
        if (getCause() != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(toString()).append("; ");
            if (getCause() instanceof ElasticsearchException) {
                sb.append(((ElasticsearchException) getCause()).getDetailedMessage());
            } else {
                sb.append(getCause());
            }
            return sb.toString();
        } else {
            return super.toString();
        }
    }

    public Throwable getRootCause() {
        Throwable rootCause = this;
        Throwable cause = getCause();
        while (cause != null && cause != rootCause) {
            rootCause = cause;
            cause = cause.getCause();
        }
        return rootCause;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(this.getMessage());
        out.writeException(this.getCause());
        writeStackTraces(this, out);
        out.writeMapOfLists(headers, StreamOutput::writeString, StreamOutput::writeString);
    }

    public static ElasticsearchException readException(StreamInput input, int id) throws IOException {
        FunctionThatThrowsIOException<StreamInput, ? extends ElasticsearchException> elasticsearchException = ID_TO_SUPPLIER.get(id);
        if (elasticsearchException == null) {
            throw new IllegalStateException("unknown exception for id: " + id);
        }
        return elasticsearchException.apply(input);
    }

    public static boolean isRegistered(Class<? extends Throwable> exception) {
        return CLASS_TO_ELASTICSEARCH_EXCEPTION_HANDLE.containsKey(exception);
    }

    static Set<Class<? extends ElasticsearchException>> getRegisteredKeys() {
        return CLASS_TO_ELASTICSEARCH_EXCEPTION_HANDLE.keySet();
    }

    public static int getId(Class<? extends ElasticsearchException> exception) {
        return CLASS_TO_ELASTICSEARCH_EXCEPTION_HANDLE.get(exception).id;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Throwable ex = ExceptionsHelper.unwrapCause(this);
        if (ex != this) {
            toXContent(builder, params, this);
        } else {
            builder.field("type", getExceptionName());
            builder.field("reason", getMessage());
            for (String key : headers.keySet()) {
                if (key.startsWith("es.")) {
                    List<String> values = headers.get(key);
                    xContentHeader(builder, key.substring("es.".length()), values);
                }
            }
            innerToXContent(builder, params);
            renderHeader(builder, params);
            if (params.paramAsBoolean(REST_EXCEPTION_SKIP_STACK_TRACE, REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT) == false) {
                builder.field("stack_trace", ExceptionsHelper.stackTrace(this));
            }
        }
        return builder;
    }

    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        causeToXContent(builder, params);
    }

    protected void causeToXContent(XContentBuilder builder, Params params) throws IOException {
        final Throwable cause = getCause();
        if (cause != null && params.paramAsBoolean(REST_EXCEPTION_SKIP_CAUSE, REST_EXCEPTION_SKIP_CAUSE_DEFAULT) == false) {
            builder.field("caused_by");
            builder.startObject();
            toXContent(builder, params, cause);
            builder.endObject();
        }
    }

    protected final void renderHeader(XContentBuilder builder, Params params) throws IOException {
        boolean hasHeader = false;
        for (String key : headers.keySet()) {
            if (key.startsWith("es.")) {
                continue;
            }
            if (hasHeader == false) {
                builder.startObject("header");
                hasHeader = true;
            }
            List<String> values = headers.get(key);
            xContentHeader(builder, key, values);
        }
        if (hasHeader) {
            builder.endObject();
        }
    }

    private void xContentHeader(XContentBuilder builder, String key, List<String> values) throws IOException {
        if (values != null && values.isEmpty() == false) {
            if (values.size() == 1) {
                builder.field(key, values.get(0));
            } else {
                builder.startArray(key);
                for (String value : values) {
                    builder.value(value);
                }
                builder.endArray();
            }
        }
    }

    public static void toXContent(XContentBuilder builder, Params params, Throwable ex) throws IOException {
        ex = ExceptionsHelper.unwrapCause(ex);
        if (ex instanceof ElasticsearchException) {
            ((ElasticsearchException) ex).toXContent(builder, params);
        } else {
            builder.field("type", getExceptionName(ex));
            builder.field("reason", ex.getMessage());
            if (ex.getCause() != null) {
                builder.field("caused_by");
                builder.startObject();
                toXContent(builder, params, ex.getCause());
                builder.endObject();
            }
            if (params.paramAsBoolean(REST_EXCEPTION_SKIP_STACK_TRACE, REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT) == false) {
                builder.field("stack_trace", ExceptionsHelper.stackTrace(ex));
            }
        }
    }

    public ElasticsearchException[] guessRootCauses() {
        final Throwable cause = getCause();
        if (cause != null && cause instanceof ElasticsearchException) {
            return ((ElasticsearchException) cause).guessRootCauses();
        }
        return new ElasticsearchException[] { this };
    }

    public static ElasticsearchException[] guessRootCauses(Throwable t) {
        Throwable ex = ExceptionsHelper.unwrapCause(t);
        if (ex instanceof ElasticsearchException) {
            return ((ElasticsearchException) ex).guessRootCauses();
        }
        return new ElasticsearchException[] { new ElasticsearchException(t.getMessage(), t) {

            @Override
            protected String getExceptionName() {
                return getExceptionName(getCause());
            }
        } };
    }

    protected String getExceptionName() {
        return getExceptionName(this);
    }

    public static String getExceptionName(Throwable ex) {
        String simpleName = ex.getClass().getSimpleName();
        if (simpleName.startsWith("Elasticsearch")) {
            simpleName = simpleName.substring("Elasticsearch".length());
        }
        return toUnderscoreCase(simpleName);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (headers.containsKey(INDEX_HEADER_KEY)) {
            builder.append(getIndex());
            if (headers.containsKey(SHARD_HEADER_KEY)) {
                builder.append('[').append(getShardId()).append(']');
            }
            builder.append(' ');
        }
        return builder.append(ExceptionsHelper.detailedMessage(this).trim()).toString();
    }

    public static <T extends Throwable> T readStackTrace(T throwable, StreamInput in) throws IOException {
        final int stackTraceElements = in.readVInt();
        StackTraceElement[] stackTrace = new StackTraceElement[stackTraceElements];
        for (int i = 0; i < stackTraceElements; i++) {
            final String declaringClasss = in.readString();
            final String fileName = in.readOptionalString();
            final String methodName = in.readString();
            final int lineNumber = in.readVInt();
            stackTrace[i] = new StackTraceElement(declaringClasss, methodName, fileName, lineNumber);
        }
        throwable.setStackTrace(stackTrace);
        int numSuppressed = in.readVInt();
        for (int i = 0; i < numSuppressed; i++) {
            throwable.addSuppressed(in.readException());
        }
        return throwable;
    }

    public static <T extends Throwable> T writeStackTraces(T throwable, StreamOutput out) throws IOException {
        StackTraceElement[] stackTrace = throwable.getStackTrace();
        out.writeVInt(stackTrace.length);
        for (StackTraceElement element : stackTrace) {
            out.writeString(element.getClassName());
            out.writeOptionalString(element.getFileName());
            out.writeString(element.getMethodName());
            out.writeVInt(element.getLineNumber());
        }
        Throwable[] suppressed = throwable.getSuppressed();
        out.writeVInt(suppressed.length);
        for (Throwable t : suppressed) {
            out.writeException(t);
        }
        return throwable;
    }

    enum ElasticsearchExceptionHandle {

        INDEX_SHARD_SNAPSHOT_FAILED_EXCEPTION(org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException.class, org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException::new, 0),
        DFS_PHASE_EXECUTION_EXCEPTION(org.elasticsearch.search.dfs.DfsPhaseExecutionException.class, org.elasticsearch.search.dfs.DfsPhaseExecutionException::new, 1),
        EXECUTION_CANCELLED_EXCEPTION(org.elasticsearch.common.util.CancellableThreads.ExecutionCancelledException.class, org.elasticsearch.common.util.CancellableThreads.ExecutionCancelledException::new, 2),
        MASTER_NOT_DISCOVERED_EXCEPTION(org.elasticsearch.discovery.MasterNotDiscoveredException.class, org.elasticsearch.discovery.MasterNotDiscoveredException::new, 3),
        ELASTICSEARCH_SECURITY_EXCEPTION(org.elasticsearch.ElasticsearchSecurityException.class, org.elasticsearch.ElasticsearchSecurityException::new, 4),
        INDEX_SHARD_RESTORE_EXCEPTION(org.elasticsearch.index.snapshots.IndexShardRestoreException.class, org.elasticsearch.index.snapshots.IndexShardRestoreException::new, 5),
        INDEX_CLOSED_EXCEPTION(org.elasticsearch.indices.IndexClosedException.class, org.elasticsearch.indices.IndexClosedException::new, 6),
        BIND_HTTP_EXCEPTION(org.elasticsearch.http.BindHttpException.class, org.elasticsearch.http.BindHttpException::new, 7),
        REDUCE_SEARCH_PHASE_EXCEPTION(org.elasticsearch.action.search.ReduceSearchPhaseException.class, org.elasticsearch.action.search.ReduceSearchPhaseException::new, 8),
        NODE_CLOSED_EXCEPTION(org.elasticsearch.node.NodeClosedException.class, org.elasticsearch.node.NodeClosedException::new, 9),
        SNAPSHOT_FAILED_ENGINE_EXCEPTION(org.elasticsearch.index.engine.SnapshotFailedEngineException.class, org.elasticsearch.index.engine.SnapshotFailedEngineException::new, 10),
        SHARD_NOT_FOUND_EXCEPTION(org.elasticsearch.index.shard.ShardNotFoundException.class, org.elasticsearch.index.shard.ShardNotFoundException::new, 11),
        CONNECT_TRANSPORT_EXCEPTION(org.elasticsearch.transport.ConnectTransportException.class, org.elasticsearch.transport.ConnectTransportException::new, 12),
        NOT_SERIALIZABLE_TRANSPORT_EXCEPTION(org.elasticsearch.transport.NotSerializableTransportException.class, org.elasticsearch.transport.NotSerializableTransportException::new, 13),
        RESPONSE_HANDLER_FAILURE_TRANSPORT_EXCEPTION(org.elasticsearch.transport.ResponseHandlerFailureTransportException.class, org.elasticsearch.transport.ResponseHandlerFailureTransportException::new, 14),
        INDEX_CREATION_EXCEPTION(org.elasticsearch.indices.IndexCreationException.class, org.elasticsearch.indices.IndexCreationException::new, 15),
        INDEX_NOT_FOUND_EXCEPTION(org.elasticsearch.index.IndexNotFoundException.class, org.elasticsearch.index.IndexNotFoundException::new, 16),
        ILLEGAL_SHARD_ROUTING_STATE_EXCEPTION(org.elasticsearch.cluster.routing.IllegalShardRoutingStateException.class, org.elasticsearch.cluster.routing.IllegalShardRoutingStateException::new, 17),
        BROADCAST_SHARD_OPERATION_FAILED_EXCEPTION(org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException.class, org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException::new, 18),
        RESOURCE_NOT_FOUND_EXCEPTION(org.elasticsearch.ResourceNotFoundException.class, org.elasticsearch.ResourceNotFoundException::new, 19),
        ACTION_TRANSPORT_EXCEPTION(org.elasticsearch.transport.ActionTransportException.class, org.elasticsearch.transport.ActionTransportException::new, 20),
        ELASTICSEARCH_GENERATION_EXCEPTION(org.elasticsearch.ElasticsearchGenerationException.class, org.elasticsearch.ElasticsearchGenerationException::new, 21),
        INDEX_SHARD_STARTED_EXCEPTION(org.elasticsearch.index.shard.IndexShardStartedException.class, org.elasticsearch.index.shard.IndexShardStartedException::new, 23),
        SEARCH_CONTEXT_MISSING_EXCEPTION(org.elasticsearch.search.SearchContextMissingException.class, org.elasticsearch.search.SearchContextMissingException::new, 24),
        GENERAL_SCRIPT_EXCEPTION(org.elasticsearch.script.GeneralScriptException.class, org.elasticsearch.script.GeneralScriptException::new, 25),
        BATCH_OPERATION_EXCEPTION(org.elasticsearch.index.shard.TranslogRecoveryPerformer.BatchOperationException.class, org.elasticsearch.index.shard.TranslogRecoveryPerformer.BatchOperationException::new, 26),
        SNAPSHOT_CREATION_EXCEPTION(org.elasticsearch.snapshots.SnapshotCreationException.class, org.elasticsearch.snapshots.SnapshotCreationException::new, 27),
        DELETE_FAILED_ENGINE_EXCEPTION(org.elasticsearch.index.engine.DeleteFailedEngineException.class, org.elasticsearch.index.engine.DeleteFailedEngineException::new, 28),
        DOCUMENT_MISSING_EXCEPTION(org.elasticsearch.index.engine.DocumentMissingException.class, org.elasticsearch.index.engine.DocumentMissingException::new, 29),
        SNAPSHOT_EXCEPTION(org.elasticsearch.snapshots.SnapshotException.class, org.elasticsearch.snapshots.SnapshotException::new, 30),
        INVALID_ALIAS_NAME_EXCEPTION(org.elasticsearch.indices.InvalidAliasNameException.class, org.elasticsearch.indices.InvalidAliasNameException::new, 31),
        INVALID_INDEX_NAME_EXCEPTION(org.elasticsearch.indices.InvalidIndexNameException.class, org.elasticsearch.indices.InvalidIndexNameException::new, 32),
        INDEX_PRIMARY_SHARD_NOT_ALLOCATED_EXCEPTION(org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException.class, org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException::new, 33),
        TRANSPORT_EXCEPTION(org.elasticsearch.transport.TransportException.class, org.elasticsearch.transport.TransportException::new, 34),
        ELASTICSEARCH_PARSE_EXCEPTION(org.elasticsearch.ElasticsearchParseException.class, org.elasticsearch.ElasticsearchParseException::new, 35),
        SEARCH_EXCEPTION(org.elasticsearch.search.SearchException.class, org.elasticsearch.search.SearchException::new, 36),
        MAPPER_EXCEPTION(org.elasticsearch.index.mapper.MapperException.class, org.elasticsearch.index.mapper.MapperException::new, 37),
        INVALID_TYPE_NAME_EXCEPTION(org.elasticsearch.indices.InvalidTypeNameException.class, org.elasticsearch.indices.InvalidTypeNameException::new, 38),
        SNAPSHOT_RESTORE_EXCEPTION(org.elasticsearch.snapshots.SnapshotRestoreException.class, org.elasticsearch.snapshots.SnapshotRestoreException::new, 39),
        PARSING_EXCEPTION(org.elasticsearch.common.ParsingException.class, org.elasticsearch.common.ParsingException::new, 40),
        INDEX_SHARD_CLOSED_EXCEPTION(org.elasticsearch.index.shard.IndexShardClosedException.class, org.elasticsearch.index.shard.IndexShardClosedException::new, 41),
        RECOVER_FILES_RECOVERY_EXCEPTION(org.elasticsearch.indices.recovery.RecoverFilesRecoveryException.class, org.elasticsearch.indices.recovery.RecoverFilesRecoveryException::new, 42),
        TRUNCATED_TRANSLOG_EXCEPTION(org.elasticsearch.index.translog.TruncatedTranslogException.class, org.elasticsearch.index.translog.TruncatedTranslogException::new, 43),
        RECOVERY_FAILED_EXCEPTION(org.elasticsearch.indices.recovery.RecoveryFailedException.class, org.elasticsearch.indices.recovery.RecoveryFailedException::new, 44),
        INDEX_SHARD_RELOCATED_EXCEPTION(org.elasticsearch.index.shard.IndexShardRelocatedException.class, org.elasticsearch.index.shard.IndexShardRelocatedException::new, 45),
        NODE_SHOULD_NOT_CONNECT_EXCEPTION(org.elasticsearch.transport.NodeShouldNotConnectException.class, org.elasticsearch.transport.NodeShouldNotConnectException::new, 46),
        INDEX_TEMPLATE_ALREADY_EXISTS_EXCEPTION(org.elasticsearch.indices.IndexTemplateAlreadyExistsException.class, org.elasticsearch.indices.IndexTemplateAlreadyExistsException::new, 47),
        TRANSLOG_CORRUPTED_EXCEPTION(org.elasticsearch.index.translog.TranslogCorruptedException.class, org.elasticsearch.index.translog.TranslogCorruptedException::new, 48),
        CLUSTER_BLOCK_EXCEPTION(org.elasticsearch.cluster.block.ClusterBlockException.class, org.elasticsearch.cluster.block.ClusterBlockException::new, 49),
        FETCH_PHASE_EXECUTION_EXCEPTION(org.elasticsearch.search.fetch.FetchPhaseExecutionException.class, org.elasticsearch.search.fetch.FetchPhaseExecutionException::new, 50),
        INDEX_SHARD_ALREADY_EXISTS_EXCEPTION(org.elasticsearch.index.IndexShardAlreadyExistsException.class, org.elasticsearch.index.IndexShardAlreadyExistsException::new, 51),
        VERSION_CONFLICT_ENGINE_EXCEPTION(org.elasticsearch.index.engine.VersionConflictEngineException.class, org.elasticsearch.index.engine.VersionConflictEngineException::new, 52),
        ENGINE_EXCEPTION(org.elasticsearch.index.engine.EngineException.class, org.elasticsearch.index.engine.EngineException::new, 53),
        NO_SUCH_NODE_EXCEPTION(org.elasticsearch.action.NoSuchNodeException.class, org.elasticsearch.action.NoSuchNodeException::new, 55),
        SETTINGS_EXCEPTION(org.elasticsearch.common.settings.SettingsException.class, org.elasticsearch.common.settings.SettingsException::new, 56),
        INDEX_TEMPLATE_MISSING_EXCEPTION(org.elasticsearch.indices.IndexTemplateMissingException.class, org.elasticsearch.indices.IndexTemplateMissingException::new, 57),
        SEND_REQUEST_TRANSPORT_EXCEPTION(org.elasticsearch.transport.SendRequestTransportException.class, org.elasticsearch.transport.SendRequestTransportException::new, 58),
        ES_REJECTED_EXECUTION_EXCEPTION(org.elasticsearch.common.util.concurrent.EsRejectedExecutionException.class, org.elasticsearch.common.util.concurrent.EsRejectedExecutionException::new, 59),
        EARLY_TERMINATION_EXCEPTION(org.elasticsearch.common.lucene.Lucene.EarlyTerminationException.class, org.elasticsearch.common.lucene.Lucene.EarlyTerminationException::new, 60),
        NOT_SERIALIZABLE_EXCEPTION_WRAPPER(org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper.class, org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper::new, 62),
        ALIAS_FILTER_PARSING_EXCEPTION(org.elasticsearch.indices.AliasFilterParsingException.class, org.elasticsearch.indices.AliasFilterParsingException::new, 63),
        GATEWAY_EXCEPTION(org.elasticsearch.gateway.GatewayException.class, org.elasticsearch.gateway.GatewayException::new, 65),
        INDEX_SHARD_NOT_RECOVERING_EXCEPTION(org.elasticsearch.index.shard.IndexShardNotRecoveringException.class, org.elasticsearch.index.shard.IndexShardNotRecoveringException::new, 66),
        HTTP_EXCEPTION(org.elasticsearch.http.HttpException.class, org.elasticsearch.http.HttpException::new, 67),
        ELASTICSEARCH_EXCEPTION(org.elasticsearch.ElasticsearchException.class, org.elasticsearch.ElasticsearchException::new, 68),
        SNAPSHOT_MISSING_EXCEPTION(org.elasticsearch.snapshots.SnapshotMissingException.class, org.elasticsearch.snapshots.SnapshotMissingException::new, 69),
        PRIMARY_MISSING_ACTION_EXCEPTION(org.elasticsearch.action.PrimaryMissingActionException.class, org.elasticsearch.action.PrimaryMissingActionException::new, 70),
        FAILED_NODE_EXCEPTION(org.elasticsearch.action.FailedNodeException.class, org.elasticsearch.action.FailedNodeException::new, 71),
        SEARCH_PARSE_EXCEPTION(org.elasticsearch.search.SearchParseException.class, org.elasticsearch.search.SearchParseException::new, 72),
        CONCURRENT_SNAPSHOT_EXECUTION_EXCEPTION(org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException.class, org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException::new, 73),
        BLOB_STORE_EXCEPTION(org.elasticsearch.common.blobstore.BlobStoreException.class, org.elasticsearch.common.blobstore.BlobStoreException::new, 74),
        INCOMPATIBLE_CLUSTER_STATE_VERSION_EXCEPTION(org.elasticsearch.cluster.IncompatibleClusterStateVersionException.class, org.elasticsearch.cluster.IncompatibleClusterStateVersionException::new, 75),
        RECOVERY_ENGINE_EXCEPTION(org.elasticsearch.index.engine.RecoveryEngineException.class, org.elasticsearch.index.engine.RecoveryEngineException::new, 76),
        UNCATEGORIZED_EXECUTION_EXCEPTION(org.elasticsearch.common.util.concurrent.UncategorizedExecutionException.class, org.elasticsearch.common.util.concurrent.UncategorizedExecutionException::new, 77),
        TIMESTAMP_PARSING_EXCEPTION(org.elasticsearch.action.TimestampParsingException.class, org.elasticsearch.action.TimestampParsingException::new, 78),
        ROUTING_MISSING_EXCEPTION(org.elasticsearch.action.RoutingMissingException.class, org.elasticsearch.action.RoutingMissingException::new, 79),
        INDEX_FAILED_ENGINE_EXCEPTION(org.elasticsearch.index.engine.IndexFailedEngineException.class, org.elasticsearch.index.engine.IndexFailedEngineException::new, 80),
        INDEX_SHARD_RESTORE_FAILED_EXCEPTION(org.elasticsearch.index.snapshots.IndexShardRestoreFailedException.class, org.elasticsearch.index.snapshots.IndexShardRestoreFailedException::new, 81),
        REPOSITORY_EXCEPTION(org.elasticsearch.repositories.RepositoryException.class, org.elasticsearch.repositories.RepositoryException::new, 82),
        RECEIVE_TIMEOUT_TRANSPORT_EXCEPTION(org.elasticsearch.transport.ReceiveTimeoutTransportException.class, org.elasticsearch.transport.ReceiveTimeoutTransportException::new, 83),
        NODE_DISCONNECTED_EXCEPTION(org.elasticsearch.transport.NodeDisconnectedException.class, org.elasticsearch.transport.NodeDisconnectedException::new, 84),
        ALREADY_EXPIRED_EXCEPTION(org.elasticsearch.index.AlreadyExpiredException.class, org.elasticsearch.index.AlreadyExpiredException::new, 85),
        AGGREGATION_EXECUTION_EXCEPTION(org.elasticsearch.search.aggregations.AggregationExecutionException.class, org.elasticsearch.search.aggregations.AggregationExecutionException::new, 86),
        INVALID_INDEX_TEMPLATE_EXCEPTION(org.elasticsearch.indices.InvalidIndexTemplateException.class, org.elasticsearch.indices.InvalidIndexTemplateException::new, 88),
        REFRESH_FAILED_ENGINE_EXCEPTION(org.elasticsearch.index.engine.RefreshFailedEngineException.class, org.elasticsearch.index.engine.RefreshFailedEngineException::new, 90),
        AGGREGATION_INITIALIZATION_EXCEPTION(org.elasticsearch.search.aggregations.AggregationInitializationException.class, org.elasticsearch.search.aggregations.AggregationInitializationException::new, 91),
        DELAY_RECOVERY_EXCEPTION(org.elasticsearch.indices.recovery.DelayRecoveryException.class, org.elasticsearch.indices.recovery.DelayRecoveryException::new, 92),
        NO_NODE_AVAILABLE_EXCEPTION(org.elasticsearch.client.transport.NoNodeAvailableException.class, org.elasticsearch.client.transport.NoNodeAvailableException::new, 94),
        INVALID_SNAPSHOT_NAME_EXCEPTION(org.elasticsearch.snapshots.InvalidSnapshotNameException.class, org.elasticsearch.snapshots.InvalidSnapshotNameException::new, 96),
        ILLEGAL_INDEX_SHARD_STATE_EXCEPTION(org.elasticsearch.index.shard.IllegalIndexShardStateException.class, org.elasticsearch.index.shard.IllegalIndexShardStateException::new, 97),
        INDEX_SHARD_SNAPSHOT_EXCEPTION(org.elasticsearch.index.snapshots.IndexShardSnapshotException.class, org.elasticsearch.index.snapshots.IndexShardSnapshotException::new, 98),
        INDEX_SHARD_NOT_STARTED_EXCEPTION(org.elasticsearch.index.shard.IndexShardNotStartedException.class, org.elasticsearch.index.shard.IndexShardNotStartedException::new, 99),
        SEARCH_PHASE_EXECUTION_EXCEPTION(org.elasticsearch.action.search.SearchPhaseExecutionException.class, org.elasticsearch.action.search.SearchPhaseExecutionException::new, 100),
        ACTION_NOT_FOUND_TRANSPORT_EXCEPTION(org.elasticsearch.transport.ActionNotFoundTransportException.class, org.elasticsearch.transport.ActionNotFoundTransportException::new, 101),
        TRANSPORT_SERIALIZATION_EXCEPTION(org.elasticsearch.transport.TransportSerializationException.class, org.elasticsearch.transport.TransportSerializationException::new, 102),
        REMOTE_TRANSPORT_EXCEPTION(org.elasticsearch.transport.RemoteTransportException.class, org.elasticsearch.transport.RemoteTransportException::new, 103),
        ENGINE_CREATION_FAILURE_EXCEPTION(org.elasticsearch.index.engine.EngineCreationFailureException.class, org.elasticsearch.index.engine.EngineCreationFailureException::new, 104),
        ROUTING_EXCEPTION(org.elasticsearch.cluster.routing.RoutingException.class, org.elasticsearch.cluster.routing.RoutingException::new, 105),
        INDEX_SHARD_RECOVERY_EXCEPTION(org.elasticsearch.index.shard.IndexShardRecoveryException.class, org.elasticsearch.index.shard.IndexShardRecoveryException::new, 106),
        REPOSITORY_MISSING_EXCEPTION(org.elasticsearch.repositories.RepositoryMissingException.class, org.elasticsearch.repositories.RepositoryMissingException::new, 107),
        DOCUMENT_SOURCE_MISSING_EXCEPTION(org.elasticsearch.index.engine.DocumentSourceMissingException.class, org.elasticsearch.index.engine.DocumentSourceMissingException::new, 109),
        NO_CLASS_SETTINGS_EXCEPTION(org.elasticsearch.common.settings.NoClassSettingsException.class, org.elasticsearch.common.settings.NoClassSettingsException::new, 111),
        BIND_TRANSPORT_EXCEPTION(org.elasticsearch.transport.BindTransportException.class, org.elasticsearch.transport.BindTransportException::new, 112),
        ALIASES_NOT_FOUND_EXCEPTION(org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException.class, org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException::new, 113),
        INDEX_SHARD_RECOVERING_EXCEPTION(org.elasticsearch.index.shard.IndexShardRecoveringException.class, org.elasticsearch.index.shard.IndexShardRecoveringException::new, 114),
        TRANSLOG_EXCEPTION(org.elasticsearch.index.translog.TranslogException.class, org.elasticsearch.index.translog.TranslogException::new, 115),
        PROCESS_CLUSTER_EVENT_TIMEOUT_EXCEPTION(org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException.class, org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException::new, 116),
        RETRY_ON_PRIMARY_EXCEPTION(ReplicationOperation.RetryOnPrimaryException.class, ReplicationOperation.RetryOnPrimaryException::new, 117),
        ELASTICSEARCH_TIMEOUT_EXCEPTION(org.elasticsearch.ElasticsearchTimeoutException.class, org.elasticsearch.ElasticsearchTimeoutException::new, 118),
        QUERY_PHASE_EXECUTION_EXCEPTION(org.elasticsearch.search.query.QueryPhaseExecutionException.class, org.elasticsearch.search.query.QueryPhaseExecutionException::new, 119),
        REPOSITORY_VERIFICATION_EXCEPTION(org.elasticsearch.repositories.RepositoryVerificationException.class, org.elasticsearch.repositories.RepositoryVerificationException::new, 120),
        INVALID_AGGREGATION_PATH_EXCEPTION(org.elasticsearch.search.aggregations.InvalidAggregationPathException.class, org.elasticsearch.search.aggregations.InvalidAggregationPathException::new, 121),
        INDEX_ALREADY_EXISTS_EXCEPTION(org.elasticsearch.indices.IndexAlreadyExistsException.class, org.elasticsearch.indices.IndexAlreadyExistsException::new, 123),
        HTTP_ON_TRANSPORT_EXCEPTION(TcpTransport.HttpOnTransportException.class, TcpTransport.HttpOnTransportException::new, 125),
        MAPPER_PARSING_EXCEPTION(org.elasticsearch.index.mapper.MapperParsingException.class, org.elasticsearch.index.mapper.MapperParsingException::new, 126),
        SEARCH_CONTEXT_EXCEPTION(org.elasticsearch.search.SearchContextException.class, org.elasticsearch.search.SearchContextException::new, 127),
        SEARCH_SOURCE_BUILDER_EXCEPTION(org.elasticsearch.search.builder.SearchSourceBuilderException.class, org.elasticsearch.search.builder.SearchSourceBuilderException::new, 128),
        ENGINE_CLOSED_EXCEPTION(org.elasticsearch.index.engine.EngineClosedException.class, org.elasticsearch.index.engine.EngineClosedException::new, 129),
        NO_SHARD_AVAILABLE_ACTION_EXCEPTION(org.elasticsearch.action.NoShardAvailableActionException.class, org.elasticsearch.action.NoShardAvailableActionException::new, 130),
        UNAVAILABLE_SHARDS_EXCEPTION(org.elasticsearch.action.UnavailableShardsException.class, org.elasticsearch.action.UnavailableShardsException::new, 131),
        FLUSH_FAILED_ENGINE_EXCEPTION(org.elasticsearch.index.engine.FlushFailedEngineException.class, org.elasticsearch.index.engine.FlushFailedEngineException::new, 132),
        CIRCUIT_BREAKING_EXCEPTION(org.elasticsearch.common.breaker.CircuitBreakingException.class, org.elasticsearch.common.breaker.CircuitBreakingException::new, 133),
        NODE_NOT_CONNECTED_EXCEPTION(org.elasticsearch.transport.NodeNotConnectedException.class, org.elasticsearch.transport.NodeNotConnectedException::new, 134),
        STRICT_DYNAMIC_MAPPING_EXCEPTION(org.elasticsearch.index.mapper.StrictDynamicMappingException.class, org.elasticsearch.index.mapper.StrictDynamicMappingException::new, 135),
        RETRY_ON_REPLICA_EXCEPTION(org.elasticsearch.action.support.replication.TransportReplicationAction.RetryOnReplicaException.class, org.elasticsearch.action.support.replication.TransportReplicationAction.RetryOnReplicaException::new, 136),
        TYPE_MISSING_EXCEPTION(org.elasticsearch.indices.TypeMissingException.class, org.elasticsearch.indices.TypeMissingException::new, 137),
        FAILED_TO_COMMIT_CLUSTER_STATE_EXCEPTION(org.elasticsearch.discovery.Discovery.FailedToCommitClusterStateException.class, org.elasticsearch.discovery.Discovery.FailedToCommitClusterStateException::new, 140),
        QUERY_SHARD_EXCEPTION(org.elasticsearch.index.query.QueryShardException.class, org.elasticsearch.index.query.QueryShardException::new, 141),
        NO_LONGER_PRIMARY_SHARD_EXCEPTION(ShardStateAction.NoLongerPrimaryShardException.class, ShardStateAction.NoLongerPrimaryShardException::new, 142),
        SCRIPT_EXCEPTION(org.elasticsearch.script.ScriptException.class, org.elasticsearch.script.ScriptException::new, 143),
        NOT_MASTER_EXCEPTION(org.elasticsearch.cluster.NotMasterException.class, org.elasticsearch.cluster.NotMasterException::new, 144),
        STATUS_EXCEPTION(org.elasticsearch.ElasticsearchStatusException.class, org.elasticsearch.ElasticsearchStatusException::new, 145);

        final Class<? extends ElasticsearchException> exceptionClass;

        final FunctionThatThrowsIOException<StreamInput, ? extends ElasticsearchException> constructor;

        final int id;

        <E extends ElasticsearchException> ElasticsearchExceptionHandle(Class<E> exceptionClass, FunctionThatThrowsIOException<StreamInput, E> constructor, int id) {
            this.exceptionClass = exceptionClass;
            this.constructor = constructor;
            this.id = id;
        }
    }

    static {
        ID_TO_SUPPLIER = unmodifiableMap(Arrays.stream(ElasticsearchExceptionHandle.values()).collect(Collectors.toMap(e -> e.id, e -> e.constructor)));
        CLASS_TO_ELASTICSEARCH_EXCEPTION_HANDLE = unmodifiableMap(Arrays.stream(ElasticsearchExceptionHandle.values()).collect(Collectors.toMap(e -> e.exceptionClass, e -> e)));
    }

    public Index getIndex() {
        List<String> index = getHeader(INDEX_HEADER_KEY);
        if (index != null && index.isEmpty() == false) {
            List<String> index_uuid = getHeader(INDEX_HEADER_KEY_UUID);
            return new Index(index.get(0), index_uuid.get(0));
        }
        return null;
    }

    public ShardId getShardId() {
        List<String> shard = getHeader(SHARD_HEADER_KEY);
        if (shard != null && shard.isEmpty() == false) {
            return new ShardId(getIndex(), Integer.parseInt(shard.get(0)));
        }
        return null;
    }

    public void setIndex(Index index) {
        if (index != null) {
            addHeader(INDEX_HEADER_KEY, index.getName());
            addHeader(INDEX_HEADER_KEY_UUID, index.getUUID());
        }
    }

    public void setIndex(String index) {
        if (index != null) {
            setIndex(new Index(index, INDEX_UUID_NA_VALUE));
        }
    }

    public void setShard(ShardId shardId) {
        if (shardId != null) {
            setIndex(shardId.getIndex());
            addHeader(SHARD_HEADER_KEY, Integer.toString(shardId.id()));
        }
    }

    public void setShard(String index, int shardId) {
        setIndex(index);
        addHeader(SHARD_HEADER_KEY, Integer.toString(shardId));
    }

    public void setResources(String type, String... id) {
        assert type != null;
        addHeader(RESOURCE_HEADER_ID_KEY, id);
        addHeader(RESOURCE_HEADER_TYPE_KEY, type);
    }

    public List<String> getResourceId() {
        return getHeader(RESOURCE_HEADER_ID_KEY);
    }

    public String getResourceType() {
        List<String> header = getHeader(RESOURCE_HEADER_TYPE_KEY);
        if (header != null && header.isEmpty() == false) {
            assert header.size() == 1;
            return header.get(0);
        }
        return null;
    }

    public static void renderException(XContentBuilder builder, Params params, Exception e) throws IOException {
        builder.startObject("error");
        final ElasticsearchException[] rootCauses = ElasticsearchException.guessRootCauses(e);
        builder.field("root_cause");
        builder.startArray();
        for (ElasticsearchException rootCause : rootCauses) {
            builder.startObject();
            rootCause.toXContent(builder, new ToXContent.DelegatingMapParams(Collections.singletonMap(ElasticsearchException.REST_EXCEPTION_SKIP_CAUSE, "true"), params));
            builder.endObject();
        }
        builder.endArray();
        ElasticsearchException.toXContent(builder, params, e);
        builder.endObject();
    }

    interface FunctionThatThrowsIOException<T, R> {

        R apply(T t) throws IOException;
    }

    private static String toUnderscoreCase(String value) {
        StringBuilder sb = new StringBuilder();
        boolean changed = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isUpperCase(c)) {
                if (!changed) {
                    for (int j = 0; j < i; j++) {
                        sb.append(value.charAt(j));
                    }
                    changed = true;
                    if (i == 0) {
                        sb.append(Character.toLowerCase(c));
                    } else {
                        sb.append('_');
                        sb.append(Character.toLowerCase(c));
                    }
                } else {
                    sb.append('_');
                    sb.append(Character.toLowerCase(c));
                }
            } else {
                if (changed) {
                    sb.append(c);
                }
            }
        }
        if (!changed) {
            return value;
        }
        return sb.toString();
    }
}