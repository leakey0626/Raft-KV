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

    /** 响应携带数据 */
    Object result;

    public ClientResponse(Object result) {
        this.result = result;
    }

    private ClientResponse(Builder builder) {
        setResult(builder.result);
    }

    public static ClientResponse ok() {
        return new ClientResponse("ok");
    }

    public static ClientResponse fail() {
        return new ClientResponse("fail");
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public static final class Builder {

        private Object result;

        private Builder() {
        }

        public Builder result(Object val) {
            result = val;
            return this;
        }

        public ClientResponse build() {
            return new ClientResponse(this);
        }
    }
}
