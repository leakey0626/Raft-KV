package net.iems.client;

import net.iems.service.proto.ClientRequest;
import net.iems.service.proto.ClientResponse;
import net.iems.service.proto.Request;
import net.iems.service.rpc.RpcClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 大东 on 2023/2/25.
 */
public class RaftClientRPC {

    private static List<String> list;

    private RpcClient CLIENT;

    private AtomicLong count = new AtomicLong(3);

    public RaftClientRPC()  {
        CLIENT = new RpcClient();
        String[] arr = new String[]{"localhost:8775", "localhost:8776", "localhost:8777", "localhost:8778", "localhost:8779"};
        list = new ArrayList<>();
        Collections.addAll(list, arr);
    }

    /**
     * @param key 查询的key值
     * @return
     */
    public String get(String key, String requestId) {

        // raft客户端协议请求体
        ClientRequest obj = ClientRequest.builder().key(key).type(ClientRequest.GET).requestId(requestId).build();

        int index = (int) (count.incrementAndGet() % list.size());

        String addr = list.get(index);

        // rpc协议请求体
        Request r = Request.builder().obj(obj).url(addr).cmd(Request.CLIENT_REQ).build();

        ClientResponse response = null;
        while (response == null){
            // 不断重试，直到获取服务端响应
            try {
                response = CLIENT.send(r, 500);
            } catch (Exception e) {
                r.setUrl(list.get((int) ((count.incrementAndGet()) % list.size())));
            }
        }
        if (response.getResult() == null || response.getResult().equals("fail")){
            return null;
        }

        return (String) response.getResult();
    }

    public String put(String key, String value, String requestId) {
        int index = (int) (count.incrementAndGet() % list.size());

        String addr = list.get(index);
        ClientRequest obj = ClientRequest.builder().key(key).value(value).type(ClientRequest.PUT).requestId(requestId).build();

        Request r = Request.builder().obj(obj).url(addr).cmd(Request.CLIENT_REQ).build();
        ClientResponse response = null;
        while (response == null || response.getResult().equals("fail")){
            // 不断重试，直到获取服务端响应
            try {
                response = CLIENT.send(r, 500);
            } catch (Exception e) {
                r.setUrl(list.get((int) ((count.incrementAndGet()) % list.size())));
            }
        }

        return (String) response.getResult();
    }

    public String del(String key, String value, String requestId) {
        int index = (int) (count.incrementAndGet() % list.size());

        String addr = list.get(index);
        ClientRequest obj = ClientRequest.builder().key(key).value(value).type(ClientRequest.DEL).requestId(requestId).build();

        Request r = Request.builder().obj(obj).url(addr).cmd(Request.CLIENT_REQ).build();
        ClientResponse response = null;
        while (response == null || response.getResult().equals("fail")){
            // 不断重试，直到获取服务端响应
            try {
                response = CLIENT.send(r, 500);
            } catch (Exception e) {
                r.setUrl(list.get((int) ((count.incrementAndGet()) % list.size())));
            }
        }

        return (String) response.getResult();
    }
}
