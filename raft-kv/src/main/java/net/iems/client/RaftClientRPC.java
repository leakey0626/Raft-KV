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
 * Raft 客户端请求接口
 * Created by 大东 on 2023/2/25.
 */
public class RaftClientRPC {

    private static List<String> list;

    private RpcClient CLIENT;

    private int index;

    private int size;

    private String addr;

    public RaftClientRPC()  {
        CLIENT = new RpcClient();
        String[] arr = new String[]{"localhost:8775", "localhost:8776", "localhost:8777", "localhost:8778", "localhost:8779"};
        list = new ArrayList<>();
        Collections.addAll(list, arr);
        size = list.size();
        index = 0;
        addr = list.get(index);
    }

    /**
     * @param key 查询的key值
     * @return
     */
    public String get(String key, String requestId) throws InterruptedException {

        // raft客户端协议请求体
        ClientRequest obj = ClientRequest.builder().key(key).type(ClientRequest.GET).requestId(requestId).build();

        // rpc协议请求体
        Request r = Request.builder().obj(obj).url(addr).cmd(Request.CLIENT_REQ).build();

        ClientResponse response = null;
        while (response == null || response.getCode() != 0){

            // 不断重试，直到响应成功
            try {
                response = CLIENT.send(r, 500);
            } catch (Exception e) {
                // 请求超时，连接下一个节点
                index = (index + 1) % size;
                addr = list.get(index);
                r.setUrl(addr);
                Thread.sleep(500);
                continue;
            }

            // 解析响应数据
            if (response.getCode() == -1){
                index = (index + 1) % size;
                r.setUrl(list.get(index));
                Thread.sleep(500);
            } else if (response.getCode() == 1){
                // 重定向
                addr = response.getResult().toString();
                r.setUrl(addr);
            }
        }

        return (String) response.getResult();
    }

    public String put(String key, String value, String requestId) throws InterruptedException {

        ClientRequest obj = ClientRequest.builder().key(key).value(value).type(ClientRequest.PUT).requestId(requestId).build();

        Request r = Request.builder().obj(obj).url(addr).cmd(Request.CLIENT_REQ).build();
        ClientResponse response = null;
        while (response == null || response.getCode() != 0){

            // 不断重试，直到响应成功
            try {
                response = CLIENT.send(r, 500);
            } catch (Exception e) {
                // 请求超时，连接下一个节点
                index = (index + 1) % size;
                addr = list.get(index);
                r.setUrl(addr);
                Thread.sleep(500);
                continue;
            }

            // 解析响应数据
            if (response.getCode() == -1){
                index = (index + 1) % size;
                addr = list.get(index);
                r.setUrl(addr);
                Thread.sleep(500);
            } else if (response.getCode() == 1){
                // 重定向
                addr = response.getResult().toString();
                r.setUrl(addr);
            }
        }
        return "ok";
    }

    public String del(String key, String value, String requestId) throws InterruptedException {

        ClientRequest obj = ClientRequest.builder().key(key).value(value).type(ClientRequest.DEL).requestId(requestId).build();

        Request r = Request.builder().obj(obj).url(addr).cmd(Request.CLIENT_REQ).build();
        ClientResponse response = null;
        while (response == null || response.getCode() != 0){

            // 不断重试，直到响应成功
            try {
                response = CLIENT.send(r, 500);
            } catch (Exception e) {
                // 请求超时，连接下一个节点
                index = (index + 1) % size;
                addr = list.get(index);
                r.setUrl(addr);
                Thread.sleep(500);
                continue;
            }

            // 解析响应数据
            if (response.getCode() == -1){
                index = (index + 1) % size;
                addr = list.get(index);
                r.setUrl(addr);
                Thread.sleep(500);
            } else if (response.getCode() == 1){
                // 重定向
                addr = response.getResult().toString();
                r.setUrl(addr);
            }
        }
        return "ok";
    }

}
