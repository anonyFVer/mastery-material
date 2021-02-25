package org.apache.dubbo.rpc.protocol.hessian;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.rpc.RpcContext;
import com.caucho.hessian.client.HessianConnection;
import com.caucho.hessian.client.HessianConnectionFactory;
import com.caucho.hessian.client.HessianProxyFactory;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import java.net.URL;

public class HttpClientConnectionFactory implements HessianConnectionFactory {

    private HttpClient httpClient;

    @Override
    public void setHessianProxyFactory(HessianProxyFactory factory) {
        RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout((int) factory.getConnectTimeout()).setSocketTimeout((int) factory.getReadTimeout()).build();
        httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
    }

    @Override
    public HessianConnection open(URL url) {
        HttpClientConnection httpClientConnection = new HttpClientConnection(httpClient, url);
        RpcContext context = RpcContext.getContext();
        for (String key : context.getAttachments().keySet()) {
            httpClientConnection.addHeader(Constants.DEFAULT_EXCHANGER + key, context.getAttachment(key));
        }
        return httpClientConnection;
    }
}