package net.iems.service.log;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import net.iems.service.constant.Command;

import java.io.Serializable;

/**
 * Created by 大东 on 2023/2/24.
 */
@Data
@Builder
@AllArgsConstructor
public class LogEntry implements Serializable {

    private Long index;

    private long term;

    private Command command;

    private String requestId;


}
