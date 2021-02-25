package org.elasticsearch.client;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;

public class RestHighLevelClient {

    private final RestClient client;

    private final NamedXContentRegistry registry;

    public RestHighLevelClient(RestClient restClient) {
        this(restClient, Collections.emptyList());
    }

    protected RestHighLevelClient(RestClient restClient, List<NamedXContentRegistry.Entry> namedXContentEntries) {
        this.client = Objects.requireNonNull(restClient);
        this.registry = new NamedXContentRegistry(Stream.of(getNamedXContents().stream(), namedXContentEntries.stream()).flatMap(Function.identity()).collect(toList()));
    }

    public BulkResponse bulk(BulkRequest bulkRequest, Header... headers) throws IOException {
        return performRequestAndParseEntity(bulkRequest, Request::bulk, BulkResponse::fromXContent, emptySet(), headers);
    }

    public void bulkAsync(BulkRequest bulkRequest, ActionListener<BulkResponse> listener, Header... headers) {
        performRequestAsyncAndParseEntity(bulkRequest, Request::bulk, BulkResponse::fromXContent, listener, emptySet(), headers);
    }

    public boolean ping(Header... headers) throws IOException {
        return performRequest(new MainRequest(), (request) -> Request.ping(), RestHighLevelClient::convertExistsResponse, emptySet(), headers);
    }

    public GetResponse get(GetRequest getRequest, Header... headers) throws IOException {
        return performRequestAndParseEntity(getRequest, Request::get, GetResponse::fromXContent, singleton(404), headers);
    }

    public void getAsync(GetRequest getRequest, ActionListener<GetResponse> listener, Header... headers) {
        performRequestAsyncAndParseEntity(getRequest, Request::get, GetResponse::fromXContent, listener, singleton(404), headers);
    }

    public boolean exists(GetRequest getRequest, Header... headers) throws IOException {
        return performRequest(getRequest, Request::exists, RestHighLevelClient::convertExistsResponse, emptySet(), headers);
    }

    public void existsAsync(GetRequest getRequest, ActionListener<Boolean> listener, Header... headers) {
        performRequestAsync(getRequest, Request::exists, RestHighLevelClient::convertExistsResponse, listener, emptySet(), headers);
    }

    public IndexResponse index(IndexRequest indexRequest, Header... headers) throws IOException {
        return performRequestAndParseEntity(indexRequest, Request::index, IndexResponse::fromXContent, emptySet(), headers);
    }

    public void indexAsync(IndexRequest indexRequest, ActionListener<IndexResponse> listener, Header... headers) {
        performRequestAsyncAndParseEntity(indexRequest, Request::index, IndexResponse::fromXContent, listener, emptySet(), headers);
    }

    public UpdateResponse update(UpdateRequest updateRequest, Header... headers) throws IOException {
        return performRequestAndParseEntity(updateRequest, Request::update, UpdateResponse::fromXContent, emptySet(), headers);
    }

    public void updateAsync(UpdateRequest updateRequest, ActionListener<UpdateResponse> listener, Header... headers) {
        performRequestAsyncAndParseEntity(updateRequest, Request::update, UpdateResponse::fromXContent, listener, emptySet(), headers);
    }

    public DeleteResponse delete(DeleteRequest deleteRequest, Header... headers) throws IOException {
        return performRequestAndParseEntity(deleteRequest, Request::delete, DeleteResponse::fromXContent, Collections.singleton(404), headers);
    }

    public void deleteAsync(DeleteRequest deleteRequest, ActionListener<DeleteResponse> listener, Header... headers) {
        performRequestAsyncAndParseEntity(deleteRequest, Request::delete, DeleteResponse::fromXContent, listener, Collections.singleton(404), headers);
    }

    private <Req extends ActionRequest, Resp> Resp performRequestAndParseEntity(Req request, CheckedFunction<Req, Request, IOException> requestConverter, CheckedFunction<XContentParser, Resp, IOException> entityParser, Set<Integer> ignores, Header... headers) throws IOException {
        return performRequest(request, requestConverter, (response) -> parseEntity(response.getEntity(), entityParser), ignores, headers);
    }

    <Req extends ActionRequest, Resp> Resp performRequest(Req request, CheckedFunction<Req, Request, IOException> requestConverter, CheckedFunction<Response, Resp, IOException> responseConverter, Set<Integer> ignores, Header... headers) throws IOException {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            throw validationException;
        }
        Request req = requestConverter.apply(request);
        Response response;
        try {
            response = client.performRequest(req.method, req.endpoint, req.params, req.entity, headers);
        } catch (ResponseException e) {
            if (ignores.contains(e.getResponse().getStatusLine().getStatusCode())) {
                try {
                    return responseConverter.apply(e.getResponse());
                } catch (Exception innerException) {
                    throw parseResponseException(e);
                }
            }
            throw parseResponseException(e);
        }
        try {
            return responseConverter.apply(response);
        } catch (Exception e) {
            throw new IOException("Unable to parse response body for " + response, e);
        }
    }

    private <Req extends ActionRequest, Resp> void performRequestAsyncAndParseEntity(Req request, CheckedFunction<Req, Request, IOException> requestConverter, CheckedFunction<XContentParser, Resp, IOException> entityParser, ActionListener<Resp> listener, Set<Integer> ignores, Header... headers) {
        performRequestAsync(request, requestConverter, (response) -> parseEntity(response.getEntity(), entityParser), listener, ignores, headers);
    }

    <Req extends ActionRequest, Resp> void performRequestAsync(Req request, CheckedFunction<Req, Request, IOException> requestConverter, CheckedFunction<Response, Resp, IOException> responseConverter, ActionListener<Resp> listener, Set<Integer> ignores, Header... headers) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        Request req;
        try {
            req = requestConverter.apply(request);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        ResponseListener responseListener = wrapResponseListener(responseConverter, listener, ignores);
        client.performRequestAsync(req.method, req.endpoint, req.params, req.entity, responseListener, headers);
    }

    <Resp> ResponseListener wrapResponseListener(CheckedFunction<Response, Resp, IOException> responseConverter, ActionListener<Resp> actionListener, Set<Integer> ignores) {
        return new ResponseListener() {

            @Override
            public void onSuccess(Response response) {
                try {
                    actionListener.onResponse(responseConverter.apply(response));
                } catch (Exception e) {
                    IOException ioe = new IOException("Unable to parse response body for " + response, e);
                    onFailure(ioe);
                }
            }

            @Override
            public void onFailure(Exception exception) {
                if (exception instanceof ResponseException) {
                    ResponseException responseException = (ResponseException) exception;
                    Response response = responseException.getResponse();
                    if (ignores.contains(response.getStatusLine().getStatusCode())) {
                        try {
                            actionListener.onResponse(responseConverter.apply(response));
                        } catch (Exception innerException) {
                            actionListener.onFailure(parseResponseException(responseException));
                        }
                    } else {
                        actionListener.onFailure(parseResponseException(responseException));
                    }
                } else {
                    actionListener.onFailure(exception);
                }
            }
        };
    }

    ElasticsearchStatusException parseResponseException(ResponseException responseException) {
        Response response = responseException.getResponse();
        HttpEntity entity = response.getEntity();
        ElasticsearchStatusException elasticsearchException;
        if (entity == null) {
            elasticsearchException = new ElasticsearchStatusException(responseException.getMessage(), RestStatus.fromCode(response.getStatusLine().getStatusCode()), responseException);
        } else {
            try {
                elasticsearchException = parseEntity(entity, BytesRestResponse::errorFromXContent);
                elasticsearchException.addSuppressed(responseException);
            } catch (Exception e) {
                RestStatus restStatus = RestStatus.fromCode(response.getStatusLine().getStatusCode());
                elasticsearchException = new ElasticsearchStatusException("Unable to parse response body", restStatus, responseException);
                elasticsearchException.addSuppressed(e);
            }
        }
        return elasticsearchException;
    }

    <Resp> Resp parseEntity(HttpEntity entity, CheckedFunction<XContentParser, Resp, IOException> entityParser) throws IOException {
        if (entity == null) {
            throw new IllegalStateException("Response body expected but not returned");
        }
        if (entity.getContentType() == null) {
            throw new IllegalStateException("Elasticsearch didn't return the [Content-Type] header, unable to parse response body");
        }
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(entity.getContentType().getValue());
        if (xContentType == null) {
            throw new IllegalStateException("Unsupported Content-Type: " + entity.getContentType().getValue());
        }
        try (XContentParser parser = xContentType.xContent().createParser(registry, entity.getContent())) {
            return entityParser.apply(parser);
        }
    }

    static boolean convertExistsResponse(Response response) {
        return response.getStatusLine().getStatusCode() == 200;
    }

    static List<NamedXContentRegistry.Entry> getNamedXContents() {
        List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>();
        return namedXContents;
    }
}