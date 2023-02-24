package net.iems;

import lombok.extern.slf4j.Slf4j;
import net.iems.config.RaftConfig;
import net.iems.constant.NodeStatus;
import net.iems.db.StateMachine;
import net.iems.log.LogEntry;
import net.iems.log.LogModule;
import net.iems.request.*;
import net.iems.rpc.RpcClient;
import net.iems.rpc.RpcService;
import net.iems.service.RaftService;
import net.iems.service.impl.RaftServiceImpl;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static net.iems.constant.NodeStatus.LEADER;
import static net.iems.constant.NodeStatus.FOLLOWER;
import static net.iems.constant.NodeStatus.CANDIDATE;

/**
 * Created by 大东 on 2023/2/24.
 */
@Slf4j
public class RaftNode {

    private int heartBeatInterval;

    private int electionTimeout;

    private RaftService service;

    private volatile NodeStatus status;

    private volatile long term;

    private volatile String votedFor;

    /** 上次选举时间 */
    private long preElectionTime;

    /** 集群其它节点地址，格式："ip:port" */
    private List<String> peerAddrs;

    /** 对于每一个服务器，需要发送给他的下一个日志条目的索引值 */
    Map<String, Long> nextIndexs;

    /** 当前节点地址 */
    private String myAddr;

    /** 日志模块 */
    LogModule logModule;

    /** 状态机 */
    StateMachine stateMachine;

    /** 线程池 */
    private ScheduledExecutorService ss;
    private ThreadPoolExecutor te;

    /** RPC 客户端 */
    private final RpcClient rpcClient;

    /** RPC 服务端 */
    private RpcService rpcServer;

    public RaftNode(){
        setConfig();
        threadPoolInit();
        service = new RaftServiceImpl();
        rpcClient = new RpcClient();
        logModule = LogModule.getInstance();
        stateMachine = StateMachine.getInstance();
    }

    /**
     * 设置参数
     */
    private void setConfig(){
        heartBeatInterval = RaftConfig.heartBeatInterval;
        electionTimeout = RaftConfig.electionTimeout;
        status = FOLLOWER;
        // TODO 设置 peerAddr
        String port = System.getProperty("server.port");
        myAddr = "localhost" + ":" + Integer.parseInt(port);
        rpcServer = new RpcService(Integer.parseInt(port), this);
    }

    /**
     * 初始化线程池
     */
    private void threadPoolInit(){

        /** 线程池参数 */
        int cup = Runtime.getRuntime().availableProcessors();
        int maxPoolSize = cup * 2;
        final int queueSize = 1024;
        final long keepTime = 1000 * 60;
        TimeUnit keepTimeUnit = TimeUnit.MILLISECONDS;

        ss = new ScheduledThreadPoolExecutor(cup);
        te = new ThreadPoolExecutor(
                cup,
                maxPoolSize,
                keepTime,
                keepTimeUnit,
                new LinkedBlockingQueue<Runnable>(queueSize));
    }

    /**
     * 处理客户端请求
     */
    public void propose(){

    }

    /**
     * 处理来自其它节点的非选举请求（心跳或追加日志）
     */
    public void appendEntries(){

    }

    /**
     * 处理来自其它节点的投票请求
     */
    public void requestVote(){

    }

    /**
     * 转发给leader处理（重定向）
     */
    public void redirect(){

    }

    /**
     * 发起选举
     */
    class ElectionTask implements Runnable{

        public void run() {

            // leader状态下不允许发起选举
            if (status == LEADER){
                return;
            }

            // 判断是否超过选举超时时间
            long current = System.currentTimeMillis();
            if (current - preElectionTime <
                    electionTimeout + ThreadLocalRandom.current().nextInt(10) * 100) {
                return;
            }

            status = CANDIDATE;
            term++;
            votedFor = myAddr;
            log.info("node {} become CANDIDATE and start election, its term : [{}], LastEntry : [{}]",
                    myAddr, term, logModule.getLast());

            ArrayList<Future<VoteResult>> futureArrayList = new ArrayList<>();

            // 发送投票请求
            for (String peer : peerAddrs) {
                // 执行rpc调用并加入list；添加的是submit的返回值
                futureArrayList.add(te.submit(() -> {
                    long lastLogTerm = 0L;
                    long lastLogIndex = 0L;
                    LogEntry lastLog = logModule.getLast();
                    if (lastLog != null) {
                        lastLogTerm = lastLog.getTerm();
                        lastLogIndex = lastLog.getIndex();
                    }

                    // 封装请求体
                    VoteRequest voteRequest = VoteRequest.builder().
                            term(term).
                            candidateAddr(myAddr).
                            lastLogIndex(lastLogIndex).
                            lastLogTerm(lastLogTerm).
                            build();

                    Request request = Request.builder()
                            .cmd(Request.R_VOTE)
                            .obj(voteRequest)
                            .url(peer)
                            .build();

                    try {
                        // 任务执行完成后，会在 future 里放置 VoteResult 对象
                        return rpcClient.send(request);
                    } catch (Exception e) {
                        log.error("ElectionTask RPC Fail , URL : " + request.getUrl());
                        return null;
                    }
                }));
            }
            log.info("futureArrayList.size() : {}", futureArrayList.size());

            // 统计赞同票的数量
            AtomicInteger success2 = new AtomicInteger(0);

            // 计数器
            CountDownLatch latch = new CountDownLatch(futureArrayList.size());

            // 利用多线程获取结果
            for (Future<VoteResult> future : futureArrayList) {
                te.submit(() -> {
                    try {
                        VoteResult result = future.get(1000, MILLISECONDS);
                        if (result == null) {
                            // rpc调用失败或任务超时
                            return -1;
                        }
                        boolean isVoteGranted = result.isVoteGranted();

                        if (isVoteGranted) {
                            success2.incrementAndGet();
                        } else {
                            // 更新自己的任期
                            long resTerm =result.getTerm();
                            if (resTerm >= term) {
                                term = resTerm;
                            }
                        }
                        return 0;
                    } catch (Exception e) {
                        log.error("future.get exception");
                        return -1;
                    } finally {
                        latch.countDown();
                    }
                });
            }

            try {
                // 等待子线程完成选票统计
                latch.await(3500, MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn("InterruptedException By Master election Task");
            }

            // 总票数
            int success = success2.get();

            // 如果投票期间有其他服务器发送 appendEntry , 就可能变成 follower
            if (status == FOLLOWER) {
                log.info("node {} election stops with new appendEntry", myAddr);
                preElectionTime = System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(10) * 100;
                return;
            }

            // 需要获得超过半数节点的投票
            if (success * 2 >= peerAddrs.size()) {
                log.warn("node {} become leader with {} votes ", myAddr, success);
                votedFor = "";
                if (leaderInit())
                    status = LEADER;
            }

            // 未赢得过半的投票，或提交no-op空日志失败
            if (status != LEADER){
                // 重新选举
                log.info("node {} election fail, votes count = {} ", myAddr, success);
                votedFor = "";
                status = FOLLOWER;
            }
            // 更新时间
            preElectionTime = System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(10) * 100;

        }
    }

    /**
     * 1. 初始化所有的 nextIndex 值为自己的最后一条日志的 index + 1
     * 2. 发送并提交no-op空日志，以提交旧领导者未提交的日志
     * 3. apply no-op之前的日志
     */
    private boolean leaderInit() {
        nextIndexs = new ConcurrentHashMap<>();
        for (String peer : peerAddrs) {
            nextIndexs.put(peer, logModule.getLastIndex() + 1);
        }

        // no-op 空日志
        LogEntry logEntry = LogEntry.builder()
                .command(null)
                .term(term)
                .build();

        // 写入本地日志并更新logEntry的index
        logModule.write(logEntry);
        log.info("write no-op log success, log index: {}", logEntry.getIndex());

        final AtomicInteger success = new AtomicInteger(0);

        List<Future<Boolean>> futureList = new ArrayList<>();

        //  复制到其他机器
        for (String peer : peerAddrs) {
            // 并行发起 RPC 复制并获取响应
            futureList.add(replication(peer, logEntry));
        }
        int count = futureList.size();

        CountDownLatch latch = new CountDownLatch(futureList.size());
        List<Boolean> resultList = new CopyOnWriteArrayList<>();

        replicationResult(futureList, latch, resultList);

        try {
            // 等待 replicationResult 中的线程执行完毕
            latch.await(10000, MILLISECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        for (Boolean aBoolean : resultList) {
            if (aBoolean) {
                success.incrementAndGet();
            }
        }

        //  响应客户端(成功一半及以上)
        if (success.get() * 2 >= count) {
            // 提交旧日志并更新 commit index
            long nextCommit = getCommitIndex() + 1;
            while (nextCommit < logEntry.getIndex() && logModule.read(nextCommit) != null){
                stateMachine.apply(logModule.read(nextCommit));
                nextCommit++;
            }
            setCommitIndex(logEntry.getIndex());
            log.info("successfully commit, logEntry info: {}", logEntry);
            // 返回成功
            return true;
        } else {
            // 提交失败，删除日志
            logModule.removeOnStartIndex(logEntry.getIndex());
            log.warn("no-op commit fail, election again");
            return false;
        }
    }

    private void setCommitIndex(long index) {
        stateMachine.setCommit(index);
    }

    private long getCommitIndex() {
        return stateMachine.getCommit();
    }

    /**
     * 等待 future 对应的线程执行完毕，将结果写入 resultList
     * @param futureList
     * @param latch
     * @param resultList
     */
    private void replicationResult(List<Future<Boolean>> futureList, CountDownLatch latch, List<Boolean> resultList) {
        for (Future<Boolean> future : futureList) {
            te.execute(() -> {
                try {
                    resultList.add(future.get(3000, MILLISECONDS));
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    resultList.add(false);
                } finally {
                    latch.countDown();
                }
            });
        }
    }

    /**
     * 复制日志到跟随者节点
     * @param peer 跟随者节点
     * @param entry 日志条目
     * @return 线程执行结果
     */
    public Future<Boolean> replication(String peer, LogEntry entry) {

        return te.submit(() -> {

            long start = System.currentTimeMillis(), end = start;

            // 5秒重试时间
            while (end - start < 5 * 1000L) {

                // 1. 封装append entry请求基本参数
                AppendRequest appendRequest = AppendRequest.builder().build();
                appendRequest.setTerm(term);
                appendRequest.setServerId(peer);
                appendRequest.setLeaderId(myAddr);
                appendRequest.setLeaderCommit(stateMachine.getCommit());

                Long nextIndex = nextIndexs.get(peer);

                // 心跳请求跳过步骤 2~3
                if (entry != null){
                    // 2. 生成日志数组
                    nextIndex = nextIndexs.get(peer);
                    LinkedList<LogEntry> logEntries = new LinkedList<>();
                    if (entry.getIndex() >= nextIndex) {
                        // 把 nextIndex ~ entry.index 之间的日志都加入list
                        for (long i = nextIndex; i <= entry.getIndex(); i++) {
                            LogEntry l = logModule.read(i);
                            if (l != null) {
                                logEntries.add(l);
                            }
                        }
                    } else {
                        logEntries.add(entry);
                    }
                    appendRequest.setEntries(logEntries.toArray(new LogEntry[0]));

                    // 3. 设置preLog相关参数，用于日志匹配
                    LogEntry preLog = getPreLog(logEntries.getFirst());
                    // preLog不存在时，下述参数会被设为-1
                    appendRequest.setPreLogTerm(preLog.getTerm());
                    appendRequest.setPrevLogIndex(preLog.getIndex());
                }

                // 4. 封装RPC请求
                Request request = Request.builder()
                        .cmd(Request.A_ENTRIES)
                        .obj(appendRequest)
                        .url(peer)
                        .build();

                try {
                    // 5. 发送RPC请求；同步调用，阻塞直到获取返回值
                    AppendResult result = rpcClient.send(request);
                    if (result == null) {
                        // timeout
                        return false;
                    }
                    if (result.isSuccess()) {
                        log.info("append follower entry success, follower=[{}], entry=[{}]", peer, appendRequest.getEntries());
                        // update 这两个追踪值
                        nextIndexs.put(peer, entry.getIndex() + 1);
                        return true;
                    } else  {
                        // 失败情况1：对方任期比我大，转变成跟随者
                        if (result.getTerm() > term) {
                            log.warn("follower [{}] term [{}], my term = [{}], so I will become follower",
                                    peer, result.getTerm(), term);
                            term = result.getTerm();
                            status = NodeStatus.FOLLOWER;
                            return false;
                        } else {
                            // 失败情况2：preLog不匹配，nextIndex递减
                            nextIndexs.put(peer, Math.max(nextIndex - 1, 0));
                            log.warn("follower {} nextIndex not match, will reduce nextIndex and retry append, nextIndex : [{}]", peer,
                                    nextIndex);
                            // 重来, 直到成功.
                        }
                    }

                    end = System.currentTimeMillis();

                } catch (Exception e) {
                    log.warn(e.getMessage(), e);
                    return false;
                }
            }

            // 超时了
            return false;
        });

    }

    /**
     * 获取logEntry的前一个日志
     * 没有前一个日志时返回一个index和term为 -1 的空日志
     * @param logEntry
     * @return
     */
    private LogEntry getPreLog(LogEntry logEntry) {
        LogEntry entry = logModule.read(logEntry.getIndex() - 1);

        if (entry == null) {
            log.warn("perLog is null, parameter logEntry : {}", logEntry);
            entry = LogEntry.builder().index(-1L).term(-1).command(null).build();
        }
        return entry;
    }

}
