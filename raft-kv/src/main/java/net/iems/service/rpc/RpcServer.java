package net.iems.service.rpc;

import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.SyncUserProcessor;
import lombok.extern.slf4j.Slf4j;
import net.iems.service.RaftNode;
import net.iems.service.proto.*;
import net.iems.service.proto.*;

/**
 * Created by 大东 on 2023/2/24.
 */
@Slf4j
public class RpcServer {

    private final RaftNode node;

    private final com.alipay.remoting.rpc.RpcServer SERVER;

    public RpcServer(int port, RaftNode node) {

        // 初始化rpc服务端
        SERVER = new com.alipay.remoting.rpc.RpcServer(port, false, false);

        // 实现用户请求处理器
        SERVER.registerUserProcessor(new SyncUserProcessor<Request>() {

            @Override
            public Object handleRequest(BizContext bizContext, Request request) throws Exception {
                return handlerRequest(request);
            }

            @Override
            public String interest() {
                return Request.class.getName();
            }
        });
        this.node = node;
        SERVER.startup();
    }

    /**
     * 1. 判断请求类型
     * 2. 调用node的处理器进行响应
     * @param request 请求参数.
     * @return
     */
    public Response<?> handlerRequest(Request request) {
        if (request.getCmd() == Request.R_VOTE) {
            // 处理来自其它节点的投票请求，决定是否投票
            return new Response<>(node.requestVote((VoteParam) request.getObj()));
        } else if (request.getCmd() == Request.A_ENTRIES) {
            return new Response<>(node.appendEntries((AppendParam) request.getObj()));
        } else if (request.getCmd() == Request.CLIENT_REQ) {
            return new Response<>(node.propose((ClientRequest) request.getObj()));
        }
        return null;
    }

    public void destroy() {
        log.info("destroy success");
    }
}
