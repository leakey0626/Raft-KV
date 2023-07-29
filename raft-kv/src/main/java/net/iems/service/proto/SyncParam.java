package net.iems.service.proto;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * leader-follower日志同步请求参数
 * Created by 大东 on 2023/7/29.
 */
@Getter
@Setter
@Builder
@Data
public class SyncParam implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 请求者 ID(ip:selfPort) */
    private String followerId;

    private long followerIndex;
}
