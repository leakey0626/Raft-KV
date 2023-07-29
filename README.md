# Raft-KV
基于 Raft 共识算法的 K-V 数据库，具备线性一致性和分区容错性，在少于半数节点发生故障时仍可正常对外提供服务
## 特性
#### 心跳与选举
1. 利用定时线程池触发心跳与选举任务。
2. 领导者节点通过心跳信号（RPC 调用）维护集群的日志提交状态
3. 领导者发生故障时（心跳信号中断）集群自动选出日志最完整的节点当选下任领导者，保持系统整体可用。
#### 日志读写与提交
1. 由领导者节点处理客户端的读写请求。
2. 收到读请求时返回最新的已提交数据。
3. 收到写请求时，将日志复制至跟随者节点，等待超过半数节点复制成功后提交日志、应用命令至状态机并返回响应给客户端。
#### 线性一致性
1. 通过“读等待”机制进一步保证在系统发生网络分区时仍然满足线性一致性。
2. 收到读请求时，等待一个心跳周期以确保集群领导者的有效性——当领导者的心跳响应得到多数派的正常响应时，认为集群领导者有效，否则无效。仅当领导者有效时返回数据给客户端。
#### 读写分离
1. 写操作：由领导者执行写操作；跟随者收到写请求时返回重定向信息，由客户端重试。
2. 读操作：所有节点均可处理读请求。读请求到达跟随者节点时，跟随者主动与领导者对齐已提交日志（领导者需完成上述“读等待”以保证线性一致性），然后返回数据给客户端。
#### 客户端协议
1. 在客户端协议中加入由 ip 和请求序号组成的“请求id”。具有相同 id 的请求只会被 raft 集群处理一次，以此保证幂等性。
2. 跟随者节点收到客户端请求时，回复“重定向”响应。客户端将连接至响应报文提供的地址进行重试。
3. 客户端请求超时后（节点故障），将连接 raft 集群的另一个节点并重试。
## 运行说明
1. 在 VM 参数中配置节点端口号，如 `-Dserver.port=8775`
2. 在 8775、8776、8777、8778 端口下运行 `RaftNodeBootStrap`
3. 启动客户端 `RaftClient` 或 `RaftClientAuto` 进行调试和验证
