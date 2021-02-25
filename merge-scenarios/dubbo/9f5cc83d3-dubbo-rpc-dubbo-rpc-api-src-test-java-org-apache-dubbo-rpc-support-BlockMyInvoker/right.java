package org.apache.dubbo.rpc.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;

public class BlockMyInvoker<T> extends MyInvoker<T> {

    private long blockTime = 100;

    public BlockMyInvoker(URL url, long blockTime) {
        super(url);
        this.blockTime = blockTime;
    }

    public BlockMyInvoker(URL url, boolean hasException, long blockTime) {
        super(url, hasException);
        this.blockTime = blockTime;
    }

    @Override
    public Result invoke(Invocation invocation) throws RpcException {
        AppResponse result = new AppResponse();
        if (hasException == false) {
            try {
                Thread.sleep(blockTime);
            } catch (InterruptedException e) {
            }
            result.setValue("Dubbo");
        } else {
            result.setException(new RuntimeException("mocked exception"));
        }
        return AsyncRpcResult.newDefaultAsyncResult(result, invocation);
    }

    public long getBlockTime() {
        return blockTime;
    }

    public void setBlockTime(long blockTime) {
        this.blockTime = blockTime;
    }
}