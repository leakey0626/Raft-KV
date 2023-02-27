package net.iems.service.rpc;

import com.alipay.remoting.exception.RemotingException;
import lombok.extern.slf4j.Slf4j;
import net.iems.service.proto.Request;
import net.iems.service.proto.Response;

import java.util.concurrent.TimeUnit;

/**
 * RPC 客户端
 * Created by 大东 on 2023/2/24.
 */
@Slf4j
public class RpcClient {

    private com.alipay.remoting.rpc.RpcClient CLIENT;

    public RpcClient(){
        CLIENT = new com.alipay.remoting.rpc.RpcClient();
        CLIENT.startup();
    }

    public <R> R send(Request request) throws RemotingException, InterruptedException {
        return send(request, 100);
    }

    public <R> R send(Request request, int timeout) throws RemotingException, InterruptedException {
        Response<R> result;
        result = (Response<R>) CLIENT.invokeSync(request.getUrl(), request, timeout);
        return result.getResult();
    }

    public void destroy() {
        CLIENT.shutdown();
        log.info("destroy success");
    }
}
