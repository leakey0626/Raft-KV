package net.iems.service.log;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志模块
 * Created by 大东 on 2023/2/24.
 */
@Setter
@Getter
@Slf4j
public class LogModule{

    /** public just for test */
    public String dbDir;
    public String logsDir;

    /** 存储日志的本地数据库 */
    private RocksDB logDb;

    public final static byte[] LAST_INDEX_KEY = "LAST_INDEX_KEY".getBytes();

    ReentrantLock lock = new ReentrantLock();

    private LogModule() {
        if (dbDir == null) {
            dbDir = "./rocksDB-raft/" + System.getProperty("server.port");
        }
        if (logsDir == null) {
            logsDir = dbDir + "/logModule";
        }
        RocksDB.loadLibrary();
        Options options = new Options();
        options.setCreateIfMissing(true);

        File file = new File(logsDir);
        boolean success = false;
        if (!file.exists()) {
            // 创建日志目录
            success = file.mkdirs();
        }
        if (success) {
            log.warn("make a new dir : " + logsDir);
        }
        try {
            // 使用RocksDB打开日志文件
            logDb = RocksDB.open(options, logsDir);
        } catch (RocksDBException e) {
            log.warn(e.getMessage());
        }
    }

    public static LogModule getInstance() {
        return LogModuleLazyHolder.INSTANCE;
    }

    public void destroy() throws Throwable {
        logDb.close();
        log.info("destroy success");
    }

    private static class LogModuleLazyHolder {

        private static final LogModule INSTANCE = new LogModule();
    }

    /**
     * 把entry添加到日志文件尾部
     * 以logEntry的index为key，将logEntry对象的序列化值存入RocksDB
     * logEntry 的 index 就是 key. 严格保证递增.
     *
     * @param logEntry
     */
    public void write(LogEntry logEntry) {

        boolean success = false;
        try {
            //lock.tryLock(3000, MILLISECONDS);
            lock.lock();
            logEntry.setIndex(getLastIndex() + 1);
            // [k, v] = [index, logEntry]
            logDb.put(logEntry.getIndex().toString().getBytes(), JSON.toJSONBytes(logEntry));
            success = true;
            log.info("DefaultLogModule write rocksDB success, logEntry info : [{}]", logEntry);
        } catch (RocksDBException e) {
            log.warn(e.getMessage());
        } finally {
            if (success) {
                updateLastIndex(logEntry.getIndex());
            }
            lock.unlock();
        }
    }

    /**
     * 根据索引读取日志
     * @param index
     * @return 日志条目 LogEntry 或 null
     */
    public LogEntry read(Long index) {
        try {
            byte[] result = logDb.get(convert(index));
            if (result == null) {
                return null;
            }
            return JSON.parseObject(result, LogEntry.class);
        } catch (RocksDBException e) {
            log.warn(e.getMessage(), e);
        }
        return null;
    }

    /**
     * 删除index在[startIndex, lastIndex]内的日志
     * @param startIndex
     */
    public void removeOnStartIndex(Long startIndex) {
        boolean success = false;
        int count = 0;
        try {
            //lock.tryLock(3000, MILLISECONDS);
            lock.lock();
            for (long i = startIndex; i <= getLastIndex(); i++) {
                logDb.delete(String.valueOf(i).getBytes());
                ++count;
            }
            success = true;
            log.warn("rocksDB removeOnStartIndex success, count={} startIndex={}, lastIndex={}", count, startIndex, getLastIndex());
        } catch (RocksDBException e) {
            log.warn(e.getMessage());
        } finally {
            if (success) {
                updateLastIndex(getLastIndex() - count);
            }
            lock.unlock();
        }
    }

    public LogEntry getLast() {
        try {
            byte[] result = logDb.get(convert(getLastIndex()));
            if (result == null) {
                return null;
            }
            return JSON.parseObject(result, LogEntry.class);
        } catch (RocksDBException e) {
            log.error("RocksDB getLast error", e);
        }
        return null;
    }

    /**
     * 获取最后一个日志的index，没有日志时返回-1
     * @return
     */
    public Long getLastIndex() {
        byte[] lastIndex = "-1".getBytes();
        try {
            lastIndex = logDb.get(LAST_INDEX_KEY);
            if (lastIndex == null) {
                lastIndex = "-1".getBytes();
            }
        } catch (RocksDBException e) {
            log.error("RocksDB getLastIndex error", e);
        }
        return Long.valueOf(new String(lastIndex));
    }

    private byte[] convert(Long key) {
        return key.toString().getBytes();
    }

    // on lock
    private void updateLastIndex(Long index) {
        try {
            // overWrite
            logDb.put(LAST_INDEX_KEY, index.toString().getBytes());
        } catch (RocksDBException e) {
            log.error("RocksDB updateLastIndex error", e);
        }
    }


}
