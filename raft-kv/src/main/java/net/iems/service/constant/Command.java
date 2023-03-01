package net.iems.service.constant;

import lombok.*;

import java.io.Serializable;

/**
 * 客户端命令
 * Created by 大东 on 2023/2/24.
 */
@Getter
@Setter
@ToString
@Builder
@AllArgsConstructor
public class Command implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 操作类型
     */
    CommandType type;

    String key;

    String value;

}
