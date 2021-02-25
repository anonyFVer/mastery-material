package org.apache.dubbo.rpc.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcResult;

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
        RpcResult result = new RpcResult();
        if (hasException == false) {
            try {
                Thread.sleep(blockTime);
            } catch (InterruptedException e) {
            }
            result.setValue("alibaba");
            return result;
        } else {
            result.setException(new RuntimeException("mocked exception"));
            return result;
        }
    }

    public long getBlockTime() {
        return blockTime;
    }

    public void setBlockTime(long blockTime) {
        this.blockTime = blockTime;
    }
}