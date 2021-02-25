package org.elasticsearch.action.support.master;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.MasterNodeChangePredicate;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;
import java.util.function.Predicate;
import java.util.function.Supplier;

public abstract class TransportMasterNodeAction<Request extends MasterNodeRequest<Request>, Response extends ActionResponse> extends HandledTransportAction<Request, Response> {

    protected final ThreadPool threadPool;

    protected final TransportService transportService;

    protected final ClusterService clusterService;

    protected final IndexNameExpressionResolver indexNameExpressionResolver;

    private final String executor;

    protected TransportMasterNodeAction(Settings settings, String actionName, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request) {
        this(settings, actionName, true, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, request);
    }

    protected TransportMasterNodeAction(Settings settings, String actionName, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters, Writeable.Reader<Request> request, IndexNameExpressionResolver indexNameExpressionResolver) {
        this(settings, actionName, true, transportService, clusterService, threadPool, actionFilters, request, indexNameExpressionResolver);
    }

    protected TransportMasterNodeAction(Settings settings, String actionName, boolean canTripCircuitBreaker, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request) {
        super(settings, actionName, canTripCircuitBreaker, transportService, actionFilters, request);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.executor = executor();
    }

    protected TransportMasterNodeAction(Settings settings, String actionName, boolean canTripCircuitBreaker, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters, Writeable.Reader<Request> request, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, actionName, canTripCircuitBreaker, transportService, actionFilters, request);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.executor = executor();
    }

    protected abstract String executor();

    protected abstract Response newResponse();

    protected abstract void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception;

    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        masterOperation(request, state, listener);
    }

    protected boolean localExecute(Request request) {
        return false;
    }

    protected abstract ClusterBlockException checkBlock(Request request, ClusterState state);

    @Override
    protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(task, request, listener).start();
    }

    class AsyncSingleAction {

        private final ActionListener<Response> listener;

        private final Request request;

        private volatile ClusterStateObserver observer;

        private final Task task;

        AsyncSingleAction(Task task, Request request, ActionListener<Response> listener) {
            this.task = task;
            this.request = request;
            if (task != null) {
                request.setParentTask(clusterService.localNode().getId(), task.getId());
            }
            this.listener = listener;
        }

        public void start() {
            ClusterState state = clusterService.state();
            this.observer = new ClusterStateObserver(state, clusterService, request.masterNodeTimeout(), logger, threadPool.getThreadContext());
            doStart(state);
        }

        protected void doStart(ClusterState clusterState) {
            try {
                final Predicate<ClusterState> masterChangePredicate = MasterNodeChangePredicate.build(clusterState);
                final DiscoveryNodes nodes = clusterState.nodes();
                if (nodes.isLocalNodeElectedMaster() || localExecute(request)) {
                    final ClusterBlockException blockException = checkBlock(request, clusterState);
                    if (blockException != null) {
                        if (!blockException.retryable()) {
                            listener.onFailure(blockException);
                        } else {
                            logger.trace("can't execute due to a cluster block, retrying", blockException);
                            retry(blockException, newState -> {
                                try {
                                    ClusterBlockException newException = checkBlock(request, newState);
                                    return (newException == null || !newException.retryable());
                                } catch (Exception e) {
                                    logger.trace("exception occurred during cluster block checking, accepting state", e);
                                    return true;
                                }
                            });
                        }
                    } else {
                        ActionListener<Response> delegate = new ActionListener<Response>() {

                            @Override
                            public void onResponse(Response response) {
                                listener.onResponse(response);
                            }

                            @Override
                            public void onFailure(Exception t) {
                                if (t instanceof FailedToCommitClusterStateException || t instanceof NotMasterException) {
                                    logger.debug(() -> new ParameterizedMessage("master could not publish cluster state or " + "stepped down before publishing action [{}], scheduling a retry", actionName), t);
                                    retry(t, masterChangePredicate);
                                } else {
                                    listener.onFailure(t);
                                }
                            }
                        };
                        threadPool.executor(executor).execute(new ActionRunnable<Response>(delegate) {

                            @Override
                            protected void doRun() throws Exception {
                                masterOperation(task, request, clusterState, delegate);
                            }
                        });
                    }
                } else {
                    if (nodes.getMasterNode() == null) {
                        logger.debug("no known master node, scheduling a retry");
                        retry(null, masterChangePredicate);
                    } else {
                        DiscoveryNode masterNode = nodes.getMasterNode();
                        final String actionName = getMasterActionName(masterNode);
                        transportService.sendRequest(masterNode, actionName, request, new ActionListenerResponseHandler<Response>(listener, TransportMasterNodeAction.this::newResponse) {

                            @Override
                            public void handleException(final TransportException exp) {
                                Throwable cause = exp.unwrapCause();
                                if (cause instanceof ConnectTransportException) {
                                    logger.debug("connection exception while trying to forward request with action name [{}] to " + "master node [{}], scheduling a retry. Error: [{}]", actionName, nodes.getMasterNode(), exp.getDetailedMessage());
                                    retry(cause, masterChangePredicate);
                                } else {
                                    listener.onFailure(exp);
                                }
                            }
                        });
                    }
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        private void retry(final Throwable failure, final Predicate<ClusterState> statePredicate) {
            observer.waitForNextChange(new ClusterStateObserver.Listener() {

                @Override
                public void onNewClusterState(ClusterState state) {
                    doStart(state);
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    logger.debug(() -> new ParameterizedMessage("timed out while retrying [{}] after failure (timeout [{}])", actionName, timeout), failure);
                    listener.onFailure(new MasterNotDiscoveredException(failure));
                }
            }, statePredicate);
        }
    }

    protected String getMasterActionName(DiscoveryNode node) {
        return actionName;
    }
}