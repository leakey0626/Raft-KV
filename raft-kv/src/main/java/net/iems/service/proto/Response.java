package net.iems.service.proto;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;


/**
 * RPC调用响应体
 * Created by 大东 on 2023/2/24.
 */
@Getter
@Setter
public class Response<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private T result;

    public Response(T result) {
        this.result = result;
    }

    private Response(Builder builder) {
        setResult((T) builder.result);
    }

    public static Response<String> ok() {
        return new Response<>("ok");
    }

    public static Response<String> fail() {
        return new Response<>("fail");
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    @Override
    public String toString() {
        return "Response{" +
            "result=" + result +
            '}';
    }

    public static final class Builder {

        private Object result;

        private Builder() {
        }

        public Builder result(Object val) {
            result = val;
            return this;
        }

        public Response<?> build() {
            return new Response(this);
        }
    }
}