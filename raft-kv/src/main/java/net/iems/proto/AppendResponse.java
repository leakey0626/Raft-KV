package net.iems.proto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 *
 * Created by 大东 on 2023/2/24.
 */
@Setter
@Getter
@ToString
public class AppendResponse implements Serializable {

    /** 被请求方的任期号，用于领导人去更新自己 */
    long term;

    /** 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真  */
    boolean success;

    public AppendResponse(long term) {
        this.term = term;
    }

    public AppendResponse(boolean success) {
        this.success = success;
    }

    public AppendResponse(long term, boolean success) {
        this.term = term;
        this.success = success;
    }

    private AppendResponse(Builder builder) {
        setTerm(builder.term);
        setSuccess(builder.success);
    }

    public static AppendResponse fail() {
        return new AppendResponse(false);
    }

    public static AppendResponse ok() {
        return new AppendResponse(true);
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public static final class Builder {

        private long term;
        private boolean success;

        private Builder() {
        }

        public Builder term(long val) {
            term = val;
            return this;
        }

        public Builder success(boolean val) {
            success = val;
            return this;
        }

        public AppendResponse build() {
            return new AppendResponse(this);
        }
    }

}
