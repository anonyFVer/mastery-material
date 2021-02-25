package org.apache.dubbo.rpc.protocol.hessian;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.rpc.RpcContext;
import com.caucho.hessian.client.HessianConnection;
import com.caucho.hessian.client.HessianURLConnectionFactory;
import java.io.IOException;
import java.net.URL;

public class DubboHessianURLConnectionFactory extends HessianURLConnectionFactory {

    @Override
    public HessianConnection open(URL url) throws IOException {
        HessianConnection connection = super.open(url);
        RpcContext context = RpcContext.getContext();
        for (String key : context.getAttachments().keySet()) {
            connection.addHeader(Constants.DEFAULT_EXCHANGER + key, context.getAttachment(key));
        }
        return connection;
    }
}