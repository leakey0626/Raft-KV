package net.iems.rpc;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.AbstractUserProcessor;
import com.alipay.remoting.rpc.protocol.SyncUserProcessor;
import net.iems.proto.Request;

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