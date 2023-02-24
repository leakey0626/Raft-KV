package net.iems.rpc;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.RpcServer;
import lombok.extern.slf4j.Slf4j;
import net.iems.RaftNode;
import net.iems.request.Request;
import net.iems.request.Response;

/**
 * Created by 大东 on 2023/2/24.
 */
@Slf4j
public class RpcService {

    private final RaftNode node;

    private final RpcServer rpcServer;

    public RpcService(int port, RaftNode node) {

        // 初始化rpc服务端
        rpcServer = new RpcServer(port, false, false);

        // 实现用户请求处理器
        rpcServer.registerUserProcessor(new RaftUserProcessor<Request>() {

            @Override
            public Object handleRequest(BizContext bizCtx, Request request) {
                return handlerRequest(request);
            }
        });

        this.node = node;
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
            return new Response<>(node.handlerRequestVote((RvoteParam) request.getObj()));
        } else if (request.getCmd() == Request.A_ENTRIES) {
            return new Response<>(node.handlerAppendEntries((AentryParam) request.getObj()));
        } else if (request.getCmd() == Request.CLIENT_REQ) {
            return new Response<>(node.handlerClientRequest((ClientKVReq) request.getObj()));
        } else if (request.getCmd() == Request.CHANGE_CONFIG_REMOVE) {
            return new Response<>(((ClusterMembershipChanges) node).removePeer((Peer) request.getObj()));
        } else if (request.getCmd() == Request.CHANGE_CONFIG_ADD) {
            return new Response<>(((ClusterMembershipChanges) node).addPeer((Peer) request.getObj()));
        }
        return null;
    }

    public void destroy() {
        log.info("destroy success");
    }
}
