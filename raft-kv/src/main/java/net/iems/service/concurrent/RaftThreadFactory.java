package net.iems.service.concurrent;

import java.util.concurrent.ThreadFactory;

/**
 * Created by 大东 on 2023/2/24.
 */
public class RaftThreadFactory implements ThreadFactory {

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "Raft thread");
        t.setDaemon(true);
        t.setPriority(5);
        return t;
    }

}
