package net.iems.config;

/**
 * Created by 大东 on 2023/2/24.
 */
public class RaftConfig {

    /**
     * 心跳时间间隔
     */
    static public final int heartBeatInterval = 300;

    /**
     * 选举超时时间
     * 在该时间内没收到心跳信号则发起选举
     */
    static public final int electionTimeout = 3 * 1000;
}
