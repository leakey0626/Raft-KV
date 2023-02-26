package net.iems.service;

import com.alipay.remoting.exception.RemotingException;
import io.netty.util.internal.StringUtil;
import lombok.extern.slf4j.Slf4j;
import net.iems.service.config.RaftConfig;
import net.iems.service.constant.Command;
import net.iems.service.constant.CommandType;
import net.iems.service.constant.NodeStatus;
import net.iems.service.db.StateMachine;
import net.iems.service.log.LogEntry;
import net.iems.service.log.LogModule;
import net.iems.service.proto.*;
import net.iems.service.rpc.RpcClient;
import net.iems.service.rpc.RpcServer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static net.iems.service.constant.NodeStatus.LEADER;
import static net.iems.service.constant.NodeStatus.FOLLOWER;
import static net.iems.service.constant.NodeStatus.CANDIDATE;

/**
 * Created by 大东 on 2023/2/24.
 */
@Slf4j
public class RaftNode {

    private int heartBeatInterval;

    private int electionTimeout;

    private volatile NodeStatus status;

    private volatile long term;

    private volatile String votedFor;

    /** 领导者地址 */
    private volatile String leader;

    /** 上次选举时间 */
    private long preElectionTime;

    /** 上次心跳时间 */
    private long preHeartBeatTime;

    /** 集群其它节点地址，格式："ip:port" */
    private List<String> peerAddrs;

    /** 对于每一个服务器，需要发送给他的下一个日志条目的索引值 */
    Map<String, Long> nextIndexes;

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
    private RpcClient rpcClient;

    /** RPC 服务端 */
    private RpcServer rpcServer;

    /** 一致性信号 */
    public final Integer consistencySignal = 1;

    /** 处理选举请求的锁 */
    public final ReentrantLock voteLock = new ReentrantLock();

    /** 处理日志请求的锁 */
    public final ReentrantLock appendLock = new ReentrantLock();

    public RaftNode(){
        logModule = LogModule.getInstance();
        stateMachine = StateMachine.getInstance();
        setConfig();
        threadPoolInit();
        log.info("Raft node started successfully. The current term is {}", term);
    }

    /**
     * 设置参数
     */
    private void setConfig(){
        heartBeatInterval = RaftConfig.heartBeatInterval;
        electionTimeout = RaftConfig.electionTimeout;
        preElectionTime = System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(10) * 100;
        preHeartBeatTime = System.currentTimeMillis();
        status = FOLLOWER;
        String port = System.getProperty("server.port");
        myAddr = "localhost:" + port;
        rpcServer = new RpcServer(Integer.parseInt(port), this);
        rpcClient = new RpcClient();
        peerAddrs = RaftConfig.getList();
        peerAddrs.remove(myAddr);
        LogEntry last = logModule.getLast();
        if (last != null){
            term = last.getTerm();
        }
    }


    /**
     * 初始化线程池
     */
    private void threadPoolInit(){

        // 线程池参数
        int cup = Runtime.getRuntime().availableProcessors();
        int maxPoolSize = cup * 2;
        final int queueSize = 1024;
        final long keepTime = 1000 * 60;

        ss = new ScheduledThreadPoolExecutor(cup);
        te = new ThreadPoolExecutor(
                cup,
                maxPoolSize,
                keepTime,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(queueSize));

        ss.scheduleWithFixedDelay(new HeartBeatTask(), 0, heartBeatInterval, TimeUnit.MILLISECONDS);
        ss.scheduleAtFixedRate(new ElectionTask(), 3000, 100, TimeUnit.MILLISECONDS);
    }


    /**
     * 处理客户端请求
     */
    public synchronized ClientResponse propose(ClientRequest request){
        log.info("handlerClientRequest handler {} operation, key: [{}], value: [{}]",
                ClientRequest.Type.value(request.getType()), request.getKey(), request.getValue());

        if (status != LEADER) {
            log.warn("I am not leader, invoke redirect method, leader addr : {}", leader);
            return redirect(request);
        }

        // 读操作
        if (request.getType() == ClientRequest.GET) {
            synchronized (consistencySignal){
                try {
                    // 等待一个心跳周期，以保证当前领导者有效
                    consistencySignal.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return ClientResponse.fail();
                }
                String value = stateMachine.getString(request.getKey());
                if (value != null) {
                    return new ClientResponse(value);
                }
                return new ClientResponse(null);
            }
        }

        // 幂等性判断
        if (stateMachine.getString(request.getRequestId()) != null){
            log.warn("request have been ack");
            return ClientResponse.ok();
        }

        // 写操作
        LogEntry logEntry = LogEntry.builder()
                .command(Command.builder()
                        .key(request.getKey())
                        .value(request.getValue())
                        .type(CommandType.PUT)
                        .build())
                .term(term)
                .requestId(request.getRequestId())
                .build();

        // 写入本地日志并更新logEntry的index
        logModule.write(logEntry);
        log.info("write logModule success, logEntry info : {}, log index : {}", logEntry, logEntry.getIndex());

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
            // 等待replicationResult中的线程执行完毕
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
            // 更新
            setCommitIndex(logEntry.getIndex());
            //  应用到状态机
            stateMachine.apply(logEntry);
            log.info("successfully commit, logEntry info: {}", logEntry);
            // 返回成功.
            return ClientResponse.ok();
        } else {
            // 提交失败，删除日志
            logModule.removeOnStartIndex(logEntry.getIndex());
            log.warn("commit fail, logEntry info : {}", logEntry);
            // TODO 不应用到状态机,但已经记录到日志中.由定时任务从重试队列取出,然后重复尝试,当达到条件时,应用到状态机.
            // 这里应该返回错误, 因为没有成功复制过半机器.
            return ClientResponse.fail();
        }
    }


    /**
     * 转发给leader处理（重定向）
     * @param request
     * @return
     */
    public ClientResponse redirect(ClientRequest request){
        Request r = Request.builder()
                .obj(request)
                .cmd(Request.CLIENT_REQ)
                .url(leader)
                .build();
        try {
            return rpcClient.send(r);
        } catch (RemotingException | InterruptedException e) {
            log.error("Redirect to leader fail. Please try again.");
        }
        return ClientResponse.fail();
    }


    /**
     * 处理来自其它节点的非选举请求（心跳或追加日志）
     */
    public AppendResult appendEntries(AppendParam param){
        AppendResult result = AppendResult.fail();
        try {
            appendLock.lock();
            result.setTerm(term);

            // 请求方任期较低，直接拒绝
            if (param.getTerm() < term) {
                return result;
            }

            preHeartBeatTime = System.currentTimeMillis();
            preElectionTime = System.currentTimeMillis();
            leader = param.getLeaderId();

            // 收到了新领导者的 append entry 请求，转为跟随者
            if (status != FOLLOWER) {
                log.info("node {} become FOLLOWER, term : {}, param Term : {}",
                        myAddr, term, param.getTerm());
                status = NodeStatus.FOLLOWER;
            }

            // 更新term
            term = param.getTerm();

            //心跳
            if (param.getEntries() == null || param.getEntries().length == 0) {
                log.info("receive heartbeat from node {}, term : {}",
                        param.getLeaderId(), param.getTerm());
                // 旧日志提交
                long nextCommit = getCommitIndex() + 1;
                while (nextCommit <= param.getLeaderCommit()
                        && logModule.read(nextCommit) != null){
                    stateMachine.apply(logModule.read(nextCommit));
                    nextCommit++;
                }
                setCommitIndex(nextCommit - 1);
                return AppendResult.newBuilder().term(term).success(true).build();
            }

            // 1. preLog匹配判断
            if (logModule.getLastIndex() != param.getPrevLogIndex()){
                // index不匹配
                return result;
            } else if (param.getPrevLogIndex() >= 0) {
                // index匹配且前一个日志存在，比较任期号
                LogEntry preEntry = logModule.read(param.getPrevLogIndex());
                if (preEntry.getTerm() != param.getPreLogTerm()) {
                    // 任期号不匹配
                    return result;
                }
            }

            // 2. 清理多余的旧日志
            long curIdx = param.getPrevLogIndex() + 1;
            if (logModule.read(curIdx) != null){
                logModule.removeOnStartIndex(curIdx);
            }

            // 3. 追加日志到本地文件
            LogEntry[] entries = param.getEntries();
            for (LogEntry logEntry : entries) {
                logModule.write(logEntry);
            }

            // 4. 旧日志提交
            long nextCommit = getCommitIndex() + 1;
            while (nextCommit <= param.getLeaderCommit()){
                stateMachine.apply(logModule.read(nextCommit));
                nextCommit++;
            }
            setCommitIndex(nextCommit - 1);

            // 5. 同意append entry请求
            result.setSuccess(true);
            return result;

        } finally {
            appendLock.unlock();
        }
    }


    /**
     * 处理来自其它节点的投票请求
     */
    public VoteResult requestVote(VoteParam param) {
        //log.info("vote process for {}, its term {} ", param.getCandidateAddr(), param.getTerm());
        try {
            VoteResult.Builder builder = VoteResult.newBuilder();
            voteLock.lock();

            // 对方任期没有自己新
            if (param.getTerm() < term) {
                // 返回投票结果的同时更新对方的term
                log.info("decline to vote for candidate {} because of smaller term", param.getCandidateAddr());
                preElectionTime = System.currentTimeMillis();
                return builder.term(term).voteGranted(false).build();
            }

            if ((StringUtil.isNullOrEmpty(votedFor) || votedFor.equals(param.getCandidateAddr()))) {
                if (logModule.getLast() != null) {
                    // 对方没有自己新
                    if (logModule.getLast().getTerm() > param.getLastLogTerm()) {
                        log.info("decline to vote for candidate {} because of older log term", param.getCandidateAddr());
                        preElectionTime = System.currentTimeMillis();
                        return VoteResult.fail();
                    }
                    // 对方没有自己新
                    if (logModule.getLastIndex() > param.getLastLogIndex()) {
                        log.info("node decline to vote for candidate {} because of older log index", param.getCandidateAddr());
                        preElectionTime = System.currentTimeMillis();
                        return VoteResult.fail();
                    }
                }

                // 切换状态
                status = NodeStatus.FOLLOWER;
                // 更新
                leader = param.getCandidateAddr();
                term = param.getTerm();
                votedFor = param.getCandidateAddr();
                preElectionTime = System.currentTimeMillis();
                log.info("vote for candidate: {}", param.getCandidateAddr());
                // 返回成功
                return builder.term(term).voteGranted(true).build();
            }

            log.info("node decline to vote for candidate {} because there is no vote available", param.getCandidateAddr());
            return builder.term(term).voteGranted(false).build();

        } finally {
            voteLock.unlock();
        }
    }


    /**
     * 发起选举
     */
    class ElectionTask implements Runnable{

        @Override
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
            // leader = "";
            term++;
            votedFor = myAddr;
            log.info("node become CANDIDATE and start election, its term : [{}], LastEntry : [{}]",
                    term, logModule.getLast());

            ArrayList<Future<VoteResult>> futureArrayList = new ArrayList<>();

            // 计数器
            CountDownLatch latch = new CountDownLatch(peerAddrs.size());

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
                    VoteParam voteParam = VoteParam.builder().
                            term(term).
                            candidateAddr(myAddr).
                            lastLogIndex(lastLogIndex).
                            lastLogTerm(lastLogTerm).
                            build();

                    Request request = Request.builder()
                            .cmd(Request.R_VOTE)
                            .obj(voteParam)
                            .url(peer)
                            .build();

                    try {
                        // rpc 调用
                        return rpcClient.send(request);
                    } catch (Exception e) {
                        log.error("ElectionTask RPC Fail , URL : " + request.getUrl());
                        return null;
                    } finally {
                        latch.countDown();
                    }
                }));
            }

            try {
                // 等待子线程完成选票统计
                latch.await(3000, MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn("InterruptedException By Master election Task");
            }

            // 统计赞同票的数量
            int votes = 0;

            // 获取结果
            for (Future<VoteResult> future : futureArrayList) {
                try {
                    VoteResult result = future.get();
                    if (result == null) {
                        // rpc调用失败或任务超时
                        continue;
                    }

                    if (result.isVoteGranted()) {
                        votes++;
                    } else {
                        // 更新自己的任期
                        long resTerm =result.getTerm();
                        if (resTerm > term) {
                            term = resTerm;
                            status = FOLLOWER;
                        }
                    }
                } catch (Exception e) {
                    log.error("future.get() exception");
                }
            }

            // 如果投票期间有其他服务器发送 appendEntry , 就可能变成 follower
            if (status == FOLLOWER) {
                log.info("election stops with newer term {}", term);
                preElectionTime = System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(10) * 100;
                votedFor = "";
                return;
            }

            // 需要获得超过半数节点的投票
            if (votes * 2 >= peerAddrs.size()) {
                votedFor = "";
                if (leaderInit()) {
                    status = LEADER;
                    log.warn("become leader with {} votes ", votes);
                }
            }

            // 未赢得过半的投票，或提交no-op空日志失败
            if (status != LEADER){
                // 重新选举
                log.info("node {} election fail, votes count = {} ", myAddr, votes);
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
        nextIndexes = new ConcurrentHashMap<>();
        for (String peer : peerAddrs) {
            nextIndexes.put(peer, logModule.getLastIndex() + 1);
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
     * 发送心跳信号，通过线程池执行
     * 如果收到了来自任期更大的节点的响应，则转为跟随者
     * RPC请求类型为A_ENTRIES
     */
    class HeartBeatTask implements Runnable {

        @Override
        public void run() {

            if (status != LEADER) {
                return;
            }

            long current = System.currentTimeMillis();
            if (current - preHeartBeatTime < heartBeatInterval) {
                return;
            }

            preHeartBeatTime = System.currentTimeMillis();
            AppendParam param = AppendParam.builder()
                    .entries(null)// 心跳,空日志.
                    .leaderId(myAddr)
                    .term(term)
                    .leaderCommit(getCommitIndex())
                    .build();

            List<Future<Boolean>> futureList = new ArrayList<>();

            for (String peer : peerAddrs) {
                Request request = new Request(
                        Request.A_ENTRIES,
                        param,
                        peer);

                // 并行发起 RPC 复制并获取响应
                futureList.add(te.submit(() -> {
                    try {
                        AppendResult aentryResult = rpcClient.send(request);
                        long resultTermterm = aentryResult.getTerm();

                        if (resultTermterm > term) {
                            log.warn("follow new leader {}", peer);
                            term = resultTermterm;
                            votedFor = "";
                            status = NodeStatus.FOLLOWER;
                        }
                        return aentryResult.isSuccess();
                    } catch (Exception e) {
                        log.error("HeartBeatTask RPC Fail, request URL : {} ", request.getUrl());
                        return false;
                    }
                }));
            }
            int count = futureList.size();
            int success = 0;

            CountDownLatch latch = new CountDownLatch(futureList.size());
            List<Boolean> resultList = new CopyOnWriteArrayList<>();

            replicationResult(futureList, latch, resultList);

            try {
                // 等待 replicationResult 中的线程执行完毕

                // TODO 这里的 await 会影响下一次心跳

                latch.await(3000, MILLISECONDS);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }

            for (Boolean aBoolean : resultList) {
                if (aBoolean) {
                    success++;
                }
            }

            //  心跳响应成功，通知阻塞的线程
            if (success * 2 >= count) {
                synchronized (consistencySignal){
                    consistencySignal.notify();
                }
            }
        }
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
                    log.warn("future get error in replicationResult");
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

            long start = System.currentTimeMillis();
            long end = start;

            // 5秒重试时间
            while (end - start < 5 * 1000L) {

                // 1. 封装append entry请求基本参数
                AppendParam appendParam = AppendParam.builder()
                        .leaderId(myAddr)
                        .term(term)
                        .leaderCommit(getCommitIndex())
                        .serverId(peer)
                        .build();

                // 2. 生成日志数组
                Long nextIndex = nextIndexes.get(peer);
                List<LogEntry> logEntries = new ArrayList<>();
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
                appendParam.setEntries(logEntries.toArray(new LogEntry[0]));

                // 3. 设置preLog相关参数，用于日志匹配
                LogEntry preLog = getPreLog(logEntries.get(0));
                // preLog不存在时，下述参数会被设为-1
                appendParam.setPreLogTerm(preLog.getTerm());
                appendParam.setPrevLogIndex(preLog.getIndex());

                // 4. 封装RPC请求
                Request request = Request.builder()
                        .cmd(Request.A_ENTRIES)
                        .obj(appendParam)
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
                        log.info("append follower entry success, follower=[{}], entry=[{}]", peer, appendParam.getEntries());
                        // update 这两个追踪值
                        nextIndexes.put(peer, entry.getIndex() + 1);
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
                            nextIndexes.put(peer, Math.max(nextIndex - 1, 0));
                            log.warn("follower {} nextIndex not match, will reduce nextIndex and retry append, nextIndex : [{}]", peer,
                                    nextIndex);
                            // 重来, 直到成功.
                        }
                    }

                    end = System.currentTimeMillis();

                } catch (Exception e) {
                    log.error("Append entry RPC fail, request URL : {} ", peer);
                    return false;
                }
            }

            // 超时了
            log.error("Append entry RPC timeout, request URL : {} ", peer);
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
            log.info("perLog is null, parameter logEntry : {}", logEntry);
            entry = LogEntry.builder().index(-1L).term(-1).command(null).build();
        }
        return entry;
    }

}
