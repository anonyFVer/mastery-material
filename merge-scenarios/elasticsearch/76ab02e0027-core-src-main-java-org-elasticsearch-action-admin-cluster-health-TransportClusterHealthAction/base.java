package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportClusterHealthAction extends TransportMasterNodeReadAction<ClusterHealthRequest, ClusterHealthResponse> {

    private final GatewayAllocator gatewayAllocator;

    @Inject
    public TransportClusterHealthAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, GatewayAllocator gatewayAllocator) {
        super(settings, ClusterHealthAction.NAME, false, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, ClusterHealthRequest::new);
        this.gatewayAllocator = gatewayAllocator;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterHealthRequest request, ClusterState state) {
        return null;
    }

    @Override
    protected ClusterHealthResponse newResponse() {
        return new ClusterHealthResponse();
    }

    @Override
    protected final void masterOperation(ClusterHealthRequest request, ClusterState state, ActionListener<ClusterHealthResponse> listener) throws Exception {
        logger.warn("attempt to execute a cluster health operation without a task");
        throw new UnsupportedOperationException("task parameter is required for this operation");
    }

    @Override
    protected void masterOperation(Task task, final ClusterHealthRequest request, final ClusterState unusedState, final ActionListener<ClusterHealthResponse> listener) {
        if (request.waitForEvents() != null) {
            final long endTimeMS = TimeValue.nsecToMSec(System.nanoTime()) + request.timeout().millis();
            clusterService.submitStateUpdateTask("cluster_health (wait_for_events [" + request.waitForEvents() + "])", new ClusterStateUpdateTask(request.waitForEvents()) {

                @Override
                public ClusterState execute(ClusterState currentState) {
                    return currentState;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    final long timeoutInMillis = Math.max(0, endTimeMS - TimeValue.nsecToMSec(System.nanoTime()));
                    final TimeValue newTimeout = TimeValue.timeValueMillis(timeoutInMillis);
                    request.timeout(newTimeout);
                    executeHealth(request, listener);
                }

                @Override
                public void onNoLongerMaster(String source) {
                    logger.trace("stopped being master while waiting for events with priority [{}]. retrying.", request.waitForEvents());
                    doExecute(task, request, listener);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error("unexpected failure during [{}]", e, source);
                    listener.onFailure(e);
                }

                @Override
                public boolean runOnlyOnMaster() {
                    return !request.local();
                }
            });
        } else {
            executeHealth(request, listener);
        }
    }

    private void executeHealth(final ClusterHealthRequest request, final ActionListener<ClusterHealthResponse> listener) {
        int waitFor = 5;
        if (request.waitForStatus() == null) {
            waitFor--;
        }
        if (request.waitForRelocatingShards() == -1) {
            waitFor--;
        }
        if (request.waitForActiveShards() == -1) {
            waitFor--;
        }
        if (request.waitForNodes().isEmpty()) {
            waitFor--;
        }
        if (request.indices() == null || request.indices().length == 0) {
            waitFor--;
        }
        assert waitFor >= 0;
        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, logger, threadPool.getThreadContext());
        final ClusterState state = observer.observedState();
        if (request.timeout().millis() == 0) {
            listener.onResponse(getResponse(request, state, waitFor, request.timeout().millis() == 0));
            return;
        }
        final int concreteWaitFor = waitFor;
        final ClusterStateObserver.ChangePredicate validationPredicate = new ClusterStateObserver.ValidationPredicate() {

            @Override
            protected boolean validate(ClusterState newState) {
                return newState.status() == ClusterState.ClusterStateStatus.APPLIED && validateRequest(request, newState, concreteWaitFor);
            }
        };
        final ClusterStateObserver.Listener stateListener = new ClusterStateObserver.Listener() {

            @Override
            public void onNewClusterState(ClusterState clusterState) {
                listener.onResponse(getResponse(request, clusterState, concreteWaitFor, false));
            }

            @Override
            public void onClusterServiceClose() {
                listener.onFailure(new IllegalStateException("ClusterService was close during health call"));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                final ClusterState clusterState = clusterService.state();
                final ClusterHealthResponse response = getResponse(request, clusterState, concreteWaitFor, true);
                listener.onResponse(response);
            }
        };
        if (state.status() == ClusterState.ClusterStateStatus.APPLIED && validateRequest(request, state, concreteWaitFor)) {
            stateListener.onNewClusterState(state);
        } else {
            observer.waitForNextChange(stateListener, validationPredicate, request.timeout());
        }
    }

    private boolean validateRequest(final ClusterHealthRequest request, ClusterState clusterState, final int waitFor) {
        ClusterHealthResponse response = clusterHealth(request, clusterState, clusterService.numberOfPendingTasks(), gatewayAllocator.getNumberOfInFlightFetch(), clusterService.getMaxTaskWaitTime());
        return prepareResponse(request, response, clusterState, waitFor);
    }

    private ClusterHealthResponse getResponse(final ClusterHealthRequest request, ClusterState clusterState, final int waitFor, boolean timedOut) {
        ClusterHealthResponse response = clusterHealth(request, clusterState, clusterService.numberOfPendingTasks(), gatewayAllocator.getNumberOfInFlightFetch(), clusterService.getMaxTaskWaitTime());
        boolean valid = prepareResponse(request, response, clusterState, waitFor);
        assert valid || timedOut;
        response.setTimedOut(timedOut && valid == false);
        return response;
    }

    private boolean prepareResponse(final ClusterHealthRequest request, final ClusterHealthResponse response, ClusterState clusterState, final int waitFor) {
        int waitForCounter = 0;
        if (request.waitForStatus() != null && response.getStatus().value() <= request.waitForStatus().value()) {
            waitForCounter++;
        }
        if (request.waitForRelocatingShards() != -1 && response.getRelocatingShards() <= request.waitForRelocatingShards()) {
            waitForCounter++;
        }
        if (request.waitForActiveShards() != -1 && response.getActiveShards() >= request.waitForActiveShards()) {
            waitForCounter++;
        }
        if (request.indices() != null && request.indices().length > 0) {
            try {
                indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), request.indices());
                waitForCounter++;
            } catch (IndexNotFoundException e) {
                response.setStatus(ClusterHealthStatus.RED);
            }
        }
        if (!request.waitForNodes().isEmpty()) {
            if (request.waitForNodes().startsWith(">=")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(2));
                if (response.getNumberOfNodes() >= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("ge(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() >= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("<=")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(2));
                if (response.getNumberOfNodes() <= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("le(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() <= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith(">")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(1));
                if (response.getNumberOfNodes() > expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("gt(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() > expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("<")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(1));
                if (response.getNumberOfNodes() < expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("lt(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() < expected) {
                    waitForCounter++;
                }
            } else {
                int expected = Integer.parseInt(request.waitForNodes());
                if (response.getNumberOfNodes() == expected) {
                    waitForCounter++;
                }
            }
        }
        return waitForCounter == waitFor;
    }

    private ClusterHealthResponse clusterHealth(ClusterHealthRequest request, ClusterState clusterState, int numberOfPendingTasks, int numberOfInFlightFetch, TimeValue pendingTaskTimeInQueue) {
        if (logger.isTraceEnabled()) {
            logger.trace("Calculating health based on state version [{}]", clusterState.version());
        }
        String[] concreteIndices;
        try {
            concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        } catch (IndexNotFoundException e) {
            ClusterHealthResponse response = new ClusterHealthResponse(clusterState.getClusterName().value(), Strings.EMPTY_ARRAY, clusterState, numberOfPendingTasks, numberOfInFlightFetch, UnassignedInfo.getNumberOfDelayedUnassigned(clusterState), pendingTaskTimeInQueue);
            response.setStatus(ClusterHealthStatus.RED);
            return response;
        }
        return new ClusterHealthResponse(clusterState.getClusterName().value(), concreteIndices, clusterState, numberOfPendingTasks, numberOfInFlightFetch, UnassignedInfo.getNumberOfDelayedUnassigned(clusterState), pendingTaskTimeInQueue);
    }
}