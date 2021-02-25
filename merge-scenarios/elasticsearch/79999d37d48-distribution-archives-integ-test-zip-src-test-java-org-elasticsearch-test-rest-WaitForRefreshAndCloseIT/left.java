package org.elasticsearch.test.rest;

import org.apache.http.util.EntityUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.junit.After;
import org.junit.Before;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

@LuceneTestCase.AwaitsFix(bugUrl = "to be created")
public class WaitForRefreshAndCloseIT extends ESRestTestCase {

    @Before
    public void setupIndex() throws IOException {
        Request request = new Request("PUT", "/test");
        request.setJsonEntity("{\"settings\":{\"refresh_interval\":-1}}");
        client().performRequest(request);
    }

    @After
    public void cleanupIndex() throws IOException {
        client().performRequest(new Request("DELETE", "/test"));
    }

    private String docPath() {
        return "test/_doc/1";
    }

    public void testIndexAndThenClose() throws Exception {
        Request request = new Request("PUT", docPath());
        request.setJsonEntity("{\"test\":\"test\"}");
        closeWhileListenerEngaged(start(request));
    }

    public void testUpdateAndThenClose() throws Exception {
        Request createDoc = new Request("PUT", docPath());
        createDoc.setJsonEntity("{\"test\":\"test\"}");
        client().performRequest(createDoc);
        Request updateDoc = new Request("POST", docPath() + "/_update");
        updateDoc.setJsonEntity("{\"doc\":{\"name\":\"test\"}}");
        closeWhileListenerEngaged(start(updateDoc));
    }

    public void testDeleteAndThenClose() throws Exception {
        Request request = new Request("PUT", docPath());
        request.setJsonEntity("{\"test\":\"test\"}");
        client().performRequest(request);
        closeWhileListenerEngaged(start(new Request("DELETE", docPath())));
    }

    private void closeWhileListenerEngaged(ActionFuture<String> future) throws Exception {
        assertBusy(() -> {
            Map<String, Object> stats;
            try {
                stats = entityAsMap(client().performRequest(new Request("GET", "/test/_stats/refresh")));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Map<?, ?> indices = (Map<?, ?>) stats.get("indices");
            Map<?, ?> theIndex = (Map<?, ?>) indices.get("test");
            Map<?, ?> total = (Map<?, ?>) theIndex.get("total");
            Map<?, ?> refresh = (Map<?, ?>) total.get("refresh");
            int listeners = (Integer) refresh.get("listeners");
            assertEquals(1, listeners);
        });
        client().performRequest(new Request("POST", "/test/_close"));
        try {
            future.get(1, TimeUnit.MINUTES);
        } catch (ExecutionException ee) {
            assertThat(ee.getCause(), instanceOf(ResponseException.class));
            ResponseException re = (ResponseException) ee.getCause();
            assertEquals(403, re.getResponse().getStatusLine().getStatusCode());
            assertThat(EntityUtils.toString(re.getResponse().getEntity()), containsString("FORBIDDEN/4/index closed"));
        }
    }

    private ActionFuture<String> start(Request request) {
        PlainActionFuture<String> future = new PlainActionFuture<>();
        request.addParameter("refresh", "wait_for");
        request.addParameter("error_trace", "");
        client().performRequestAsync(request, new ResponseListener() {

            @Override
            public void onSuccess(Response response) {
                try {
                    future.onResponse(EntityUtils.toString(response.getEntity()));
                } catch (IOException e) {
                    future.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception exception) {
                future.onFailure(exception);
            }
        });
        return future;
    }
}