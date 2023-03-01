package net.iems.service.proto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * Created by 大东 on 2023/2/24.
 */
@Getter
@Setter
@ToString
public class ClientResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 响应码
     * ok -- 0
     * fail -- -1
     * redirect -- 1
     */
    int code;

    /** 响应携带数据 */
    Object result;

    private ClientResponse(int code, Object result) {
        this.code = code;
        this.result = result;
    }

    public static ClientResponse ok() {
        return new ClientResponse(0, null);
    }

    public static ClientResponse ok(String value) {
        return new ClientResponse(0, value);
    }

    public static ClientResponse fail() {
        return new ClientResponse(-1, null);
    }

    public static ClientResponse redirect(String addr) {
        return new ClientResponse(1, addr);
    }


}
