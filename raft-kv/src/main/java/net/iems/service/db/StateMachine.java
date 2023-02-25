package net.iems.service.db;

import lombok.extern.slf4j.Slf4j;
import net.iems.service.constant.Command;
import net.iems.service.log.LogEntry;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;

import static net.iems.service.constant.CommandType.DELETE;
import static net.iems.service.constant.CommandType.PUT;

/**
 * 通过 RocksDB 实现状态机
 * Created by 大东 on 2023/2/24.
 */
@Slf4j
public class StateMachine {

    private String dbDir;
    private String stateMachineDir;
    /** 获取commit index的key值 */
    public final static byte[] COMMIT = "COMMIT_INDEX".getBytes();
    public RocksDB machineDb;

    private StateMachine() {
        dbDir = "./rocksDB-raft/" + System.getProperty("server.port");
        stateMachineDir = dbDir + "/stateMachine";
        RocksDB.loadLibrary();

        File file = new File(stateMachineDir);
        boolean success = false;

        if (!file.exists()) {
            success = file.mkdirs();
        }
        if (success) {
            log.warn("make a new dir : " + stateMachineDir);
        }
        Options options = new Options();
        options.setCreateIfMissing(true);
        try {
            machineDb = RocksDB.open(options, stateMachineDir);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public static StateMachine getInstance() {
        return StateMachineLazyHolder.INSTANCE;
    }

    public void destroy() throws Throwable {
        machineDb.close();
        log.info("destroy success");
    }

    private static class StateMachineLazyHolder {
        private static final StateMachine INSTANCE = new StateMachine();
    }

    public String getString(String key) {
        try {
            byte[] bytes = machineDb.get(key.getBytes());
            if (bytes != null) {
                return new String(bytes);
            }
        } catch (RocksDBException e) {
            log.error(e.getMessage());
        }
        return null;
    }

    public void setString(String key, String value) {
        try {
            machineDb.put(key.getBytes(), value.getBytes());
        } catch (RocksDBException e) {
            log.error(e.getMessage());
        }
    }

    public void delString(String... key) {
        try {
            for (String s : key) {
                machineDb.delete(s.getBytes());
            }
        } catch (RocksDBException e) {
            log.error(e.getMessage());
        }
    }

    public synchronized void apply(LogEntry logEntry) {

        Command command = logEntry.getCommand();

        if (command == null) {
            return; // no-op
        }
        if (command.getType() == PUT){
            // 写入操作
            String key = command.getKey();
            String value = command.getValue();
            setString(key, value);
        } else if (command.getType() == DELETE){
            // 删除操作
            String key = command.getKey();
            delString(key);
        }

        // 记录请求id，保证幂等性
        setString(logEntry.getRequestId(), "1");

    }

    /**
     * 获取最后一个已提交的日志的index，没有已提交日志时返回-1
     * @return
     */
    public synchronized Long getCommit(){
        byte[] lastCommitIndex = null;
        try {
            lastCommitIndex = machineDb.get(COMMIT);
        } catch (RocksDBException e) {
            log.error(e.getMessage());
        }
        if (lastCommitIndex == null){
            lastCommitIndex = "-1".getBytes();
        }
        return Long.valueOf(new String(lastCommitIndex));
    }

    /**
     * 修改commitIndex的接口（持久化）
     * @param index
     */
    public synchronized void setCommit(Long index){
        try {
            machineDb.put(COMMIT, index.toString().getBytes());
        } catch (RocksDBException e) {
            log.error(e.getMessage());
        }
    }

}
