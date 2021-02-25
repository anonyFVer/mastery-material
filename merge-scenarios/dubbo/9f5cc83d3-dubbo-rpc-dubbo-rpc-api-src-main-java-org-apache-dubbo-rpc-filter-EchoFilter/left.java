package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import static org.apache.dubbo.rpc.Constants.$ECHO;

@Activate(group = CommonConstants.PROVIDER, order = -110000)
public class EchoFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation inv) throws RpcException {
        if (inv.getMethodName().equals($ECHO) && inv.getArguments() != null && inv.getArguments().length == 1) {
            return AsyncRpcResult.newDefaultAsyncResult(inv.getArguments()[0], inv);
        }
        return invoker.invoke(inv);
    }
}