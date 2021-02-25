package org.elasticsearch.test.transport;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportStats;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import static org.apache.lucene.util.LuceneTestCase.rarely;

public class CapturingTransport implements Transport {

    private volatile Map<String, RequestHandlerRegistry> requestHandlers = Collections.emptyMap();

    final Object requestHandlerMutex = new Object();

    private final ResponseHandlers responseHandlers = new ResponseHandlers();

    private TransportConnectionListener listener;

    public static class CapturedRequest {

        public final DiscoveryNode node;

        public final long requestId;

        public final String action;

        public final TransportRequest request;

        public CapturedRequest(DiscoveryNode node, long requestId, String action, TransportRequest request) {
            this.node = node;
            this.requestId = requestId;
            this.action = action;
            this.request = request;
        }
    }

    private ConcurrentMap<Long, Tuple<DiscoveryNode, String>> requests = new ConcurrentHashMap<>();

    private BlockingQueue<CapturedRequest> capturedRequests = ConcurrentCollections.newBlockingQueue();

    public CapturedRequest[] capturedRequests() {
        return capturedRequests.toArray(new CapturedRequest[0]);
    }

    public CapturedRequest[] getCapturedRequestsAndClear() {
        List<CapturedRequest> requests = new ArrayList<>(capturedRequests.size());
        capturedRequests.drainTo(requests);
        return requests.toArray(new CapturedRequest[0]);
    }

    private Map<String, List<CapturedRequest>> groupRequestsByTargetNode(Collection<CapturedRequest> requests) {
        Map<String, List<CapturedRequest>> result = new HashMap<>();
        for (CapturedRequest request : requests) {
            result.computeIfAbsent(request.node.getId(), node -> new ArrayList<>()).add(request);
        }
        return result;
    }

    public Map<String, List<CapturedRequest>> capturedRequestsByTargetNode() {
        return groupRequestsByTargetNode(capturedRequests);
    }

    public Map<String, List<CapturedRequest>> getCapturedRequestsByTargetNodeAndClear() {
        List<CapturedRequest> requests = new ArrayList<>(capturedRequests.size());
        capturedRequests.drainTo(requests);
        return groupRequestsByTargetNode(requests);
    }

    public void clear() {
        capturedRequests.clear();
    }

    public void handleResponse(final long requestId, final TransportResponse response) {
        responseHandlers.onResponseReceived(requestId, listener).handleResponse(response);
    }

    public void handleLocalError(final long requestId, final Throwable t) {
        Tuple<DiscoveryNode, String> request = requests.get(requestId);
        assert request != null;
        this.handleError(requestId, new SendRequestTransportException(request.v1(), request.v2(), t));
    }

    public void handleRemoteError(final long requestId, final Throwable t) {
        final RemoteTransportException remoteException;
        if (rarely(Randomness.get())) {
            remoteException = new RemoteTransportException("remote failure, coming from local node", t);
        } else {
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.writeException(t);
                remoteException = new RemoteTransportException("remote failure", output.bytes().streamInput().readException());
            } catch (IOException ioException) {
                throw new ElasticsearchException("failed to serialize/deserialize supplied exception " + t, ioException);
            }
        }
        this.handleError(requestId, remoteException);
    }

    public void handleError(final long requestId, final TransportException e) {
        responseHandlers.onResponseReceived(requestId, listener).handleException(e);
    }

    @Override
    public Connection openConnection(DiscoveryNode node, ConnectionProfile profile) throws IOException {
        return new Connection() {

            @Override
            public DiscoveryNode getNode() {
                return node;
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
                requests.put(requestId, Tuple.tuple(node, action));
                capturedRequests.add(new CapturedRequest(node, requestId, action, request));
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    @Override
    public TransportStats getStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return null;
    }

    @Override
    public Map<String, BoundTransportAddress> profileBoundAddresses() {
        return null;
    }

    @Override
    public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws UnknownHostException {
        return new TransportAddress[0];
    }

    @Override
    public boolean nodeConnected(DiscoveryNode node) {
        return true;
    }

    @Override
    public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile, CheckedBiConsumer<Connection, ConnectionProfile, IOException> connectionValidator) throws ConnectTransportException {
    }

    @Override
    public void disconnectFromNode(DiscoveryNode node) {
    }

    @Override
    public Lifecycle.State lifecycleState() {
        return null;
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {
    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void close() {
    }

    @Override
    public List<String> getLocalAddresses() {
        return Collections.emptyList();
    }

    public Connection getConnection(DiscoveryNode node) {
        try {
            return openConnection(node, null);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public <Request extends TransportRequest> void registerRequestHandler(RequestHandlerRegistry<Request> reg) {
        synchronized (requestHandlerMutex) {
            if (requestHandlers.containsKey(reg.getAction())) {
                throw new IllegalArgumentException("transport handlers for action " + reg.getAction() + " is already registered");
            }
            requestHandlers = MapBuilder.newMapBuilder(requestHandlers).put(reg.getAction(), reg).immutableMap();
        }
    }

    @Override
    public ResponseHandlers getResponseHandlers() {
        return responseHandlers;
    }

    @Override
    public RequestHandlerRegistry getRequestHandler(String action) {
        return requestHandlers.get(action);
    }

    @Override
    public void addConnectionListener(TransportConnectionListener listener) {
        if (this.listener != null) {
            throw new IllegalStateException("listener already set");
        }
        this.listener = listener;
    }

    @Override
    public boolean removeConnectionListener(TransportConnectionListener listener) {
        if (listener == this.listener) {
            this.listener = null;
            return true;
        }
        return false;
    }
}