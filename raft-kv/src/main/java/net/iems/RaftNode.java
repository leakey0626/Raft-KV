package net.iems;

import net.iems.config.RaftConfig;

/**
 * Created by 大东 on 2023/2/24.
 */
public class RaftNode {

    private int heartBeatInterval;

    private int eletionTimeout;

    public RaftNode(){
        setConfig();
        threadPoolInit();
    }

    /**
     * 设置参数
     */
    private void setConfig(){
        heartBeatInterval = RaftConfig.heartBeatInterval;
        eletionTimeout = RaftConfig.electionTimeout;
    }

    /**
     * 初始化线程池
     */
    private void threadPoolInit(){

    }

    /**
     * 处理客户端请求
     */
    public void propose(){

    }

    /**
     * 处理来自其它节点的非选举请求（心跳或追加日志）
     */
    public void appendEntries(){

    }

    /**
     * 处理来自其它节点的投票请求
     */
    public void requestVote(){

    }

    /**
     * 转发给leader处理（重定向）
     */
    public void redirect(){

    }


}
