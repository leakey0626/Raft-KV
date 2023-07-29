package net.iems.service.proto;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * leader-follower日志同步请求响应
 * Created by 大东 on 2023/7/29.
 */
@Getter
@Setter
@Builder
@Data
public class SyncResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 处理结果 */
    private boolean status;

    public static SyncResponse fail(){
        return new SyncResponse(false);
    }

    public static SyncResponse ok(){
        return new SyncResponse(true);
    }

    public boolean isSuccess(){
        return status;
    }

}
