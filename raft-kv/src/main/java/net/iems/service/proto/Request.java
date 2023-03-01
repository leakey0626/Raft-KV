package net.iems.service.proto;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * RPC调用请求体
 * Created by 大东 on 2023/2/24.
 */
@Builder
@Data
public class Request implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 请求投票 */
    public static final int R_VOTE = 0;
    /** 附加日志 */
    public static final int A_ENTRIES = 1;
    /** 客户端 */
    public static final int CLIENT_REQ = 2;
    /** 配置变更. add */
    public static final int CHANGE_CONFIG_ADD = 3;
    /** 配置变更. remove */
    public static final int CHANGE_CONFIG_REMOVE = 4;

    /** 请求类型 */
    private int cmd = -1;

    /** 附带请求参数 */
    private Object obj;

    /** 目标地址 */
    private String url;

    public Request() {
    }

    public Request(int cmd, Object obj, String url) {
        this.cmd = cmd;
        this.obj = obj;
        this.url = url;
    }
}
