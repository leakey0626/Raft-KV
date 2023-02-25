package net.iems.client;

import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.util.UUID;

/**
 * Created by 大东 on 2023/2/25.
 */
@Slf4j
public class RaftClientAuto {

    public static void main(String[] args) throws Throwable {

        RaftClientRPC rpc = new RaftClientRPC();
        InetAddress localHost = InetAddress.getLocalHost();
        String prefix = localHost.getHostAddress() + UUID.randomUUID().toString().substring(0, 5);

        for (int i = 3; i > -1; i++) {
            String key = "[test4:" + i +"]";
            String value = "[test4:" + i + "]";
            // 客户端请求唯一id
            String requestId = prefix + i;
            try {
                String putResult = rpc.put(key, value, requestId);
                log.info("key = {}, value = {}, put response : {}", key, value, putResult);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }

            Thread.sleep(3000);

            try {
                String res = rpc.get(key, requestId);
                log.info("key = {}, value = {}, get response : {}", key, value, res);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }

            Thread.sleep(3000);

        }


    }

}
