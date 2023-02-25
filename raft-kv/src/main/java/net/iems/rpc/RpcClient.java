package net.iems.rpc;

import com.alipay.remoting.exception.RemotingException;
import lombok.extern.slf4j.Slf4j;
import net.iems.proto.Request;
import net.iems.proto.Response;

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

    public <R> R send(Request request) {
        return send(request, (int) TimeUnit.SECONDS.toMillis(5));
    }

    public <R> R send(Request request, int timeout) {
        Response<R> result;
        try {
            result = (Response<R>) CLIENT.invokeSync(request.getUrl(), request, timeout);
            return result.getResult();
        } catch (RemotingException e) {
            //log.error(e.getMessage());
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            // ignore
        }
        return null;
    }


    public void destroy() {
        CLIENT.shutdown();
        log.info("destroy success");
    }
}
