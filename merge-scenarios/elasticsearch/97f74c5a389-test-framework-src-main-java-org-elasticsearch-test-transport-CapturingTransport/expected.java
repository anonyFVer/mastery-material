package org.elasticsearch.test.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class CapturingTransport extends MockTransport implements Transport {

    public static class CapturedRequest {

        public final DiscoveryNode node;

        public final long requestId;

        public final String action;

        public final TransportRequest request;

        CapturedRequest(DiscoveryNode node, long requestId, String action, TransportRequest request) {
            this.node = node;
            this.requestId = requestId;
            this.action = action;
            this.request = request;
        }
    }

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

    @Override
    protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
        capturedRequests.add(new CapturingTransport.CapturedRequest(node, requestId, action, request));
    }
}