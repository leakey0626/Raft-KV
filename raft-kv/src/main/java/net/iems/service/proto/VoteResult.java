package net.iems.service.proto;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * 投票请求响应体
 * Created by 大东 on 2023/2/24.
 */
@Getter
@Setter
public class VoteResult implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 当前任期号，以便于候选人去更新自己的任期
     */
    long term;

    /**
     * 候选人赢得了此张选票时为真
     */
    boolean voteGranted;

    public VoteResult(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }

    private VoteResult(Builder builder) {
        setTerm(builder.term);
        setVoteGranted(builder.voteGranted);
    }

    public static VoteResult fail() {
        return new VoteResult(false);
    }

    public static VoteResult ok() {
        return new VoteResult(true);
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public static final class Builder {

        private long term;
        private boolean voteGranted;

        private Builder() {
        }

        public Builder term(long term) {
            this.term = term;
            return this;
        }

        public Builder voteGranted(boolean voteGranted) {
            this.voteGranted = voteGranted;
            return this;
        }

        public VoteResult build() {
            return new VoteResult(this);
        }
    }
}
