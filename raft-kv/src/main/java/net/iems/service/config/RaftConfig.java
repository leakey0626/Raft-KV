package net.iems.service.config;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by 大东 on 2023/2/24.
 */
@Getter
@Setter
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

    static private List<String> addrs = new ArrayList<>();

    public static List<String> getList() {
        return addrs;
    }
}
