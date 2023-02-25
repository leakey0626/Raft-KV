package net.iems;

import lombok.extern.slf4j.Slf4j;
import net.iems.service.RaftNode;
import net.iems.service.config.RaftConfig;

import java.util.Collections;
import java.util.List;

/**
 * Created by 大东 on 2023/2/24.
 */
@Slf4j
public class RaftNodeBootStrap {

    public static void main(String[] args){
        String[] arr = new String[]{"localhost:8775", "localhost:8776", "localhost:8777", "localhost:8778", "localhost:8779"};
        List<String> addrs = RaftConfig.getList();
        Collections.addAll(addrs, arr);
        RaftNode node = new RaftNode();
    }


}
