package net.iems.service.rpc;

import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.SyncUserProcessor;

public class RaftUserProcessor<T> extends SyncUserProcessor<T> {


    @Override
    public Object handleRequest(BizContext bizContext, T t) throws Exception {
        return null;
    }

    @Override
    public String interest() {
        return null;
    }
}