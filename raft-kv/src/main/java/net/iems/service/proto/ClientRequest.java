package net.iems.service.proto;

import lombok.Builder;
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
@Builder
public class ClientRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 操作类型 */
    public static int PUT = 0;
    public static int GET = 1;
    public static int DEL = 2;

    int type;

    /** 键 */
    String key;

    /** 值 */
    String value;

    /** 请求id */
    String requestId;

    public enum Type {
        /** 1111 */
        PUT(0), GET(1);
        int code;

        Type(int code) {
            this.code = code;
        }

        public static Type value(int code ) {
            for (Type type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            return null;
        }
    }
}
