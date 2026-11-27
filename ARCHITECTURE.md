# hi-kafka 架构设计

状态：Accepted  
适用范围：hi-kafka 扩展、内嵌 worker、IPC 协议、PHP driver 与框架 Kafka 集成  
目标协议：v2  
最后更新：2026-11-23

## 1. 文档权威性

本文档是 hi-kafka 的唯一架构事实源，定义系统长期成立的边界、所有权、不变量、协议语义与验收标准。

以下内容如与本文档冲突，以本文档为准：

- README 中的阶段说明；
- 历史 RFC 和 code review 记录；
- 源码注释和测试名称；
- 当前实现行为；
- 尚未同步更新的用户文档。

任何改变进程模型、组件职责、IPC、Producer 共享、Consumer 投递、ACK、offset、事务、背压、生命周期或故障语义的修改，都必须先更新本文档或与代码同时更新。实现细节可以在不改变本文不变量的前提下由代码和测试决定。

本文描述目标架构，不以兼容现有错误实现为约束。当前代码与目标不一致时，应修改代码，而不是降低本文要求。

规范词语 MUST、MUST NOT、SHOULD、MAY 分别表示必须、禁止、建议和允许。

## 2. 系统使命

hi-kafka 为 PHP 长驻与短驻运行时提供高性能、可靠、资源有界的 Kafka 能力，同时避免每个 PHP 进程各自持有 librdkafka client。

它必须同时服务：

- PHP CLI 常驻消费者；
- PHP-FPM 请求进程；
- Swoole 多 worker、多协程 HTTP 服务；
- Swow 协程服务；
- 同一运行实例内的多 Kafka 集群；
- 普通 Producer、幂等 Producer；
- 普通 at-least-once Consumer。

当前 protocol v2 不暴露 Kafka transaction。旧版 cluster-global transaction flag、TxnReq/TxnResp 和 PHP begin/commit/abort API 已删除，因为它们没有 owner lease，无法在多 PHP worker 与 RPC pool 下保证归属。第 10、18 节定义的是未来重新引入 transaction 时必须一次性满足的准入架构，不代表当前 wire capability；在全部准入条件落地前，`transactional.id` 配置必须被明确拒绝，不能降级到旧实现。

系统优化目标按优先级排列：

1. 不丢消息与 offset 正确性；
2. 进程和资源所有权清晰；
3. 内存存在可证明的上界；
4. 故障可恢复且结果可解释；
5. 高吞吐和低尾延迟；
6. API 一致、可观测、可维护。

性能优化不得破坏前四项。

## 3. 核心架构决策

hi-kafka 采用扩展内嵌、进程外执行的 worker 模型：

~~~text
PHP CLI / FPM / Swoole / Swow processes
                │
                │ Unix domain sockets
                ▼
one hi-kafka-worker per socket namespace
                │
                ├── Cluster Registry
                ├── shared Producers
                ├── Consumer Subscriptions
                ├── Transaction Coordinator
                └── Metrics / Lifecycle
                         │
                         ▼
                     librdkafka
                         │
                         ▼
                       Kafka
~~~

核心决策如下：

- worker 代码静态链接在扩展中，由扩展自启动，不部署 sidecar。
- librdkafka 只存在于 hi-kafka-worker，不进入 PHP 业务进程。
- 同一 socket 路径代表一个 worker 实例和一个资源共享域。
- 多个 PHP/Swoole worker 通过 UDS 共享同一个 worker。
- 每个有效 cluster 配置最多建立一个普通 Producer 实例。事务 Producer 按 cluster generation 与 transactional ID 分别建立，不能与普通 Producer 混用。
- 每个逻辑 Consumer subscription 建立一个 Kafka Consumer。
- PHP 侧只持有 IPC、协议状态和当前正在处理的有界消息。
- Producer 使用 RPC 模式；Consumer 使用长期、双向、credit 驱动的流模式。
- Consumer 不提供返回完整批次数组的 poll API。
- 应用处理成功通过 ACK 表达；broker commit 与 ACK 是两个不同阶段。

## 4. 明确排除的架构

以下方案不属于 hi-kafka 目标架构：

- Kubernetes sidecar；
- 每个 PHP worker 一个 librdkafka client；
- 把业务 topic、JSON 字段或过滤规则写入扩展；
- 通过减小 fetch 数量掩盖无界内存；
- 用 drop oldest、随机丢弃或跳过 offset 实现背压；
- 在 worker 内完整构造一批消息，再包装成 Iterator 伪装流式；
- 从 librdkafka 后台线程直接调用 PHP；
- 依赖 PHP GC 或定期重启作为正常内存治理；
- 用 commit consumer current position 代替应用 ACK；
- 在多线程 PHP 进程中 fork 后不 exec 就运行 worker（仅作为 exec 不可用时的回退）。

## 5. 组件职责

### 5.1 PHP 扩展

PHP 扩展负责：

- worker 存活探测与安全启动；
- 协议编解码原语；
- 阻塞式 Client；
- 参数边界校验和结构化异常；
- 进程生命周期钩子；
- 必要的 native ConsumerRecord/Stream 对象；
- worker 崩溃后的连接失效与重新建立。

PHP 扩展不负责：

- 持有 librdkafka client；
- 保存整批 Consumer 消息；
- 推断业务消息格式；
- 在 PHP 进程内运行 Kafka 后台线程；
- 替业务决定重试是否安全。

### 5.2 PHP driver

SwooleClient 和 SwowClient 负责：

- 协程感知 UDS IO；
- fork 后连接池重建；
- RPC 连接池；
- Consumer 专用 stream connection；
- frame 缓冲、拆帧与 correlation ID 路由；
- 将 ACK、NACK、commit 和 flow control 映射到协议；
- 将 worker 错误转换成稳定的 PHP 异常。

普通 RPC 连接可以复用；Consumer stream connection 不得返回普通连接池。

协程 Consumer 的协议编排 MUST 只有一个实现：`CoroutineConsumerStream`。为了让核心语义可独立测试，它只负责 IO 顺序、control barrier 与恢复；纯 `ConsumerStreamState` 负责 subscription、epoch、credit、outstanding delivery 与 ACK intent 不变量，`ConsumerFrameReader` 负责 connection-local 增量拆帧，`ConsumerProtocolInterface` 是扩展 codec 的唯一边界。Swoole 和 Swow MUST 只通过 `ConsumerTransportInterface` adapter 提供 connect、完整写、增量读、close 和 coroutine sleep；adapter 的超时单位统一为毫秒，runtime 原生单位只能在 adapter 内转换。任何 runtime-specific class 都不得复制上述状态、协议或恢复逻辑。

只有 adapter 归一化得到的 `ConsumerTransportException` 才能触发 timeout 返回或 stream reconnect。协议错误、correlation/identity 错误与本地状态机错误 MUST 原样暴露，不得被当成网络故障吞掉。只有等待下一帧时的读超时可以映射为 `next() === null`；写超时可能已经写出部分 frame，MUST 丢弃连接并恢复 stream。读超时可能发生在 frame 中间，因此拆帧缓冲属于 stream 状态机，并且 MUST 跨 timeout 保留；断线时才清空。

worker 在 ConsumeOpen/ConsumeResume 成功后 MUST 把该 dedicated connection 绑定到直接的 subscription session handle。后续 ACK、Flow、NACK、commit、pause/resume、close 与 disconnect MUST 通过该 handle 直达 actor command queue，不得为每个 control frame 再查询 worker-wide subscription HashMap。全局 registry 只用于 open、resume token lookup、观测和过期清理；session handle 仍必须逐帧校验 subscription identity 与 epoch，不能因去掉 lookup 而放松协议验证。

### 5.3 hi-kafka-worker

worker 是系统唯一的 Kafka runtime，负责：

- Cluster Registry；
- Producer 实例缓存和代际切换；
- Consumer subscription 与 assignment；
- ACK watermark 和 offset store；
- broker commit；
- transaction ownership；
- 消息级和字节级背压；
- rebalance 协调；
- worker-wide 资源预算；
- shutdown、drain、flush；
- metrics、health 和故障分类。

### 5.4 proto

proto crate 是 wire format 的唯一实现位置，负责：

- frame envelope；
- payload schema；
- 长度与数量上限；
- protocol major；
- 编解码测试；
- 非法输入拒绝。

协议 schema 不携带 PHP 或框架专属概念。

### 5.5 框架集成

框架负责：

- 在安全时机启动 worker；
- 将配置注册到 worker；
- 把业务 Consumer 包装为 next、handle、ACK/NACK、commit 循环；
- 决定何时调用显式 commit；
- 管理业务 scope；
- 将业务异常保留为未 ACK 状态；
- 应用级日志和追踪。

Container scope 不参与 Kafka 消息缓存，也不拥有 worker subscription。

## 6. 进程与所有权模型

### 6.1 worker 唯一性

worker 的唯一键是规范化后的 socket 路径。

同一路径下：

- spawn lock MUST 保证只有一个进程负责启动；
- readiness MUST 通过 HELLO 验证，而不只是 connect 成功；
- PID 文件只用于观测，不能作为唯一存活判断；
- 旧 socket、错误版本和半启动 worker 必须被明确识别；
- bind 时只清理无人监听的残留 socket 文件；路径上已有可连接的 worker 时 MUST 拒绝启动，不得删除后重新 bind（否则新老 worker 同时存活，客户端分裂到两个实例）。

“一个 worker”指一个 socket namespace 内的一个 worker，不泛化为整台 Kubernetes node。不同 Pod 或不共享 socket namespace 的容器自然拥有各自 worker。

### 6.2 worker 启动

worker 代码内嵌在扩展里，启动必须满足：

- 采用 double fork：中间进程只调用 async-signal-safe 的 `setsid`/`fork`/`write`/`_exit`，经 pipe 回报 worker PID 后退出并被 spawner 回收；worker 从启动起即被 init 收养，不成为 PHP-FPM / Swoole manager 的子进程；
- 孙进程 fork 后 MUST 立即 `execve` 一个只加载本扩展的 PHP CLI（`hi-kafka-worker -n -d extension=<本 .so> -r '' -- <socket>`），fork 与 exec 之间只允许 async-signal-safe 调用；新进程的 MINIT 识别 `HI_KAFKA_INTERNAL_WORKER_SOCKET` 后进入 worker 且永不返回 PHP。日志、drain、metrics 等配置由父进程解析后以环境变量传入；
- PHP CLI 的选择：`hi_kafka.php_binary` / `HI_KAFKA_PHP_BINARY` 显式指定优先；否则当前进程本身是 PHP CLI 时用它自己（同一构建，扩展 ABI 一定匹配）；FPM 等其它 SAPI 时取同一安装下的 `php`；
- fork 子进程不得调用 PHP API；
- worker MUST 在任何其它动作之前关闭全部继承的 fd（≥ 3）并清空信号屏蔽字，随后重置信号处理器、stdio 和 runtime；
- worker readiness 只有在监听 socket、runtime 和协议 HELLO 全部可用后成立；
- 在就绪期限内未完成 HELLO 的 worker MUST 被 spawner SIGKILL，下一次 ensure 重新拉起。

多线程进程中的 fork：fork 出的子进程只有调用线程，其它线程持有的锁（malloc 等）永远不会释放。exec 换掉整个地址空间，这些锁随之作废，因此 Swoole / Swow / Workerman / ReactPHP / FPM 在任何时刻懒加载拉起 worker 都是安全的，不需要业务在线程启动前调用 ensureWorker（预热仍可用于消除首次调用的冷启动延迟）。

回退：找不到 PHP CLI 或 exec 失败时，孙进程直接在 fork 出的进程中运行 worker，并记录 warning。此时多线程进程中仍有死锁风险，由「就绪超时即 SIGKILL」把最坏情况限定为一次有界的启动失败；`HI_KAFKA_STRICT_FORK=1` 时在多线程进程中拒绝这种回退。默认不拒绝的原因：worker 崩溃后的惰性恢复通常发生在 Swoole worker 进程中，拒绝会让 Kafka 在进程重启前一直不可用。

### 6.3 PHP fork 安全

所有 PHP client、连接池和 stream 对象 MUST 保存 owner PID（扩展 Rust 连接池与协程 `CoroutineRpcClient` 已实现）。

检测到 PID 改变时必须：

1. 丢弃继承的 UDS fd；
2. 丢弃继承的 Swoole Channel、Swow 连接与 pending request；
3. 重置 per-process 统计和 reader task；
4. 连接已存在的共享 worker；
5. 不得复用父进程创建的连接对象。

可以在 master 中启动 worker，但业务 UDS client 必须在 child worker 中重建。

### 6.4 session ownership

每条连接都有明确角色：

- RPC session：短请求、多次复用；
- Producer pipelined session：可选的多路复用生产连接；
- Consumer stream session：一个逻辑 subscription 的所有者；
- Lifecycle session：退出清理与 Goodbye。

Consumer subscription 必须绑定 owner session 和有界可恢复 lease。owner 连接断开时：

1. 立即暂停新 delivery；
2. 使旧连接的未 ACK delivery 失效；
3. 增加 subscription epoch；
4. 在很短且有上限的 reconnect grace 内保留 Kafka Consumer，避免瞬时 UDS 故障立即触发 rebalance；
5. 原 owner 使用不可猜测的 resume token 可重建 stream，并从每个 partition 最早未 ACK offset 重投；
6. grace 到期仍未恢复时 unsubscribe、离组并释放全部资源。

worker 内部还必须为每次 stream attach 分配单调 attachment lease。disconnect 只接受当前 attachment lease，不能拿业务 epoch 代替：rebalance 提升 epoch 与通知写 socket 之间存在窗口，旧 epoch 会漏掉真实断线；旧 connection 的迟到 disconnect 也不能关闭已经恢复的新 attachment。

resume token 不得记入日志或 metrics。attachment lease 不上 wire。该机制是有界故障恢复，不是让 orphan subscription 无限期存活。

## 7. Cluster Registry

Cluster Registry 是 worker 内 Kafka 配置的权威运行时存储。

每个 cluster 包含：

- cluster name；
- librdkafka configuration；
- configuration generation；
- OAuth token slot；
- producer generation；
- 使用该配置建立的 consumer subscriptions。

配置规则：

- cluster name 在 worker 范围唯一；
- register 是幂等 upsert；
- embedded worker 只接受显式 `RegisterCluster` 与 client replay；不得从 `HI_KAFKA_BROKERS` 等历史单集群环境变量偷偷创建 `default` cluster；
- 每次有效配置变化增加 generation；
- secrets 不得进入普通日志和 metrics label；
- worker 重启后，PHP client 必须能够重放全部 cluster 配置；
- 同一 cluster 的并发注册必须串行并产生确定 generation；
- 配置变更不能让旧/new producer 同时无界存在。

Producer 配置变化时：

1. 新请求进入新 generation；
2. 旧 generation 停止接收新请求；
3. 旧 producer 排干 in-flight 后释放；
4. 事务中的 producer 不得被静默替换。

Consumer subscription 使用创建时的配置 generation。配置变化不静默修改存量 subscription；由显式重建或确定的恢复流程切换。

## 8. IPC 基础设计

### 8.1 传输

IPC 使用 Unix domain SOCK_STREAM。

通用 envelope：

~~~text
[payload_len:u32][frame_type:u8][correlation_id:u64][payload]
~~~

要求：

- 整数使用 network byte order；
- 每种 frame 有独立 hard limit；
- 全局 MAX_PAYLOAD 只是最后防线；
- parser 必须处理拆包、粘包和多个连续 frame；
- payload 未完整到达时不得 dispatch；
- 非法长度、未知 type、错误状态转换关闭该 session；
- Error frame 必须结构化，包含 kind、retryable、native code 和 message；
- 禁止依赖错误字符串做状态判断。

### 8.2 协议版本

目标协议为 v2，并与 v1 不兼容。

- PROTOCOL_MAJOR MUST 为 2；
- HELLO 必须在任意业务 frame 前完成；
- HELLO 验证 major、connection role 和 capability；
- v2 使用版本化 socket，例如 /tmp/hi-kafka-v2.sock；
- 不实现长期 v1/v2 双栈；
- 旧 worker 通过旧 socket 完成存量工作并按 idle policy 退出。

版本化 socket 是必要条件：否则旧 worker connect 成功会阻止新扩展启动 v2 worker，但随后 HELLO 又必然失败。

### 8.3 连接并发

v1 server 在一条连接中串行 read、dispatch、await、write。这适合简单 RPC，但同步 Produce 或 Consumer stream 会产生 head-of-line blocking。

v2 规则：

- Consumer 使用专用 connection，不与普通 RPC 混合；
- 普通 RPC pool 可以通过多连接获得并行度；
- 若实现单连接 multiplex，reader、request execution 和 writer 必须分离；
- response 通过 correlation ID 路由，可以乱序完成；
- writer 由单一任务串行，禁止并发写破坏 frame；
- transaction/control 操作必须按资源 owner 保持顺序。

Producer multiplex 是可选优化，不是 Consumer v2 的前置条件。

### 8.4 缓冲与 IO

- reader 使用固定上限的 reusable buffer；
- writer 支持 buffer reuse 和 vectored write；
- 不得为每个小 response 新建高容量 buffer；
- UnixStream 不需要在每个 frame 后强制 flush；
- 连接曾处理大 frame 后必须按策略 shrink 或归还分级 buffer pool；
- socket send/receive buffer 必须有界；
- 协议 credit 不能依赖内核 socket buffer 作为唯一背压。

## 9. Producer 架构

### 9.1 Producer 共享

worker 按 cluster configuration generation 缓存 Producer。

多个 PHP/Swoole worker 的请求最终进入同一个 Producer，因此：

- 不重复 broker connections；
- 不重复 metadata；
- 不重复 delivery threads；
- librdkafka 可以跨所有 PHP worker 聚合 batch；
- compression、linger、batch size 和 idempotence 在同一 Producer 生效。

### 9.2 Produce 模式

支持两种基本语义：

ProduceFnf：

- PHP 等待 worker 本地 enqueue 结果；
- 成功只表示 librdkafka 接受进入本地 producer queue；
- 不表示 broker 已持久化；
- 同步可知的配置错误、非法参数和 queue full 必须返回；
- 异步 delivery failure 通过 metrics、日志或 delivery observer 暴露。

ProduceSync：

- 等待对应 delivery report；
- 成功返回 partition 和 offset；
- timeout 表示结果未知，不表示消息一定未送达；
- 调用方重试依赖 idempotent producer 和稳定 key/业务幂等；
- worker 超时必须早于 PHP IPC 超时并返回结构化 TIMEOUT。

### 9.3 Producer 背压

Producer 内存由以下共同约束：

- librdkafka queue.buffering.max.messages；
- librdkafka queue.buffering.max.kbytes；
- 每连接 frame hard limit；
- worker in-flight request budget；
- PHP driver pending request budget。

queue full 不得无限等待或无界缓存到 worker 自己的 Vec 中。ProduceFnf/ProduceSync 按各自语义明确失败或在有界 timeout 内等待。

### 9.4 Producer 顺序

Kafka partition ordering 由 librdkafka、partitioner、idempotence 和 max in-flight 配置共同保证。

IPC correlation ID 只负责 response 路由，不承诺跨独立 PHP 进程的业务顺序。需要业务顺序时必须使用相同 key/partition，并采用 Kafka 支持的 Producer 配置。

### 9.5 Producer 性能

Producer 性能依赖：

- 共享 Producer；
- librdkafka batching；
- 持久 UDS connections；
- 有界并发；
- borrowed frame decode；
- 避免不必要的 payload copy；
- 不让 ProduceSync 阻塞其他 connection。

Consumer 重构不得将 Producer 移入 PHP 或降低多 worker 共享能力。

普通 Producer 的缓存键是 cluster configuration generation；事务 Producer 的缓存键是 cluster configuration generation + transactional ID。事务 Producer 可以被不同 PHP session 依次租用，但同一时刻只能有一个有效 owner lease。

## 10. Transaction 架构

本节是未来能力的强制准入规范。当前 v2 不声明 transaction capability，也没有 transaction frame 或公共 API。重新引入必须作为新的原子交付完成本节与第 18 节全部条件，禁止先恢复 begin/commit/abort 的无 owner 版本。

事务是 worker 内的长生命周期资源，必须有 owner，而不是只有 cluster-global active 标志。

BeginTransaction 返回 transaction lease：

~~~text
cluster
producer_generation
transaction_epoch
owner_session
~~~

所有事务 Produce、SendOffsets、Commit 和 Abort 必须携带并验证该 lease。

规则：

- 同一 transactional Producer 同时只有一个 active transaction；
- 非 owner session 的事务操作被拒绝；
- owner disconnect 触发有界 abort；
- producer generation 变化不能跨越 active transaction；
- commit/abort 完成后 lease 失效；
- timeout/cancel/panic 不能让状态永久停在 transitioning；
- transaction response 丢失时结果必须标记为 unknown，而不是虚构成功或失败。

sendOffsetsToTransaction 使用 Consumer subscription 的 group metadata 和连续 ACK watermark，不接受 PHP 从旧批次数组推导的权威 offset。

## 11. Consumer 总体设计

### 11.1 Consumer API 模型

Consumer 是消息流，不是数组拉取。

目标抽象：

~~~php
interface ConsumerStream
{
    public function next(): ?ConsumerRecord;
    public function ack(ConsumerRecord $record): void;
    public function nack(ConsumerRecord $record, ?\Throwable $reason = null): void;
    public function commit(): void;
    public function pause(array $partitions = []): void;
    public function resume(array $partitions = []): void;
    public function close(): void;
}
~~~

框架 callback API 可以存在，但只能包装相同的单消息 stream：

~~~php
$record = $stream->next();

if ($record === null) {
    // 本次等待窗口内没有可交付消息；stream 与 subscription 仍然有效。
    continue;
}

try {
    $businessConsumer->consume($frameworkAdapter->toBusinessMessage($record));
    $stream->ack($record);
} catch (\Throwable $e) {
    $stream->nack($record, $e);
    throw $e;
}
~~~

禁止提供生产可用的 poll(): array 兼容实现。

`ConsumerRecord` 是扩展拥有的交付对象，名称不得占用 framework 已发布的业务
DTO（例如 `Hi\Kafka\Message`）。framework 可以建立一个轻量业务 Message 视图以
保持既有 getter/构造 API，但原生 record 必须由消费循环私有持有并用于 ACK/NACK；
业务 DTO 不携带、也不能伪造 delivery identity。映射可以增加一个常数级 PHP 对象，
不得复制 key/value payload 或建立 batch 数组。

### 11.2 Subscription

ConsumeOpen 一次性携带：

- cluster 和配置 generation；
- group ID；
- topics；
- consumer-specific librdkafka config；
- initial message credit；
- initial byte credit；
- per-subscription hard limits；
- commit 模式（当前 v2 固定 manual）；
- client/session identity。

ConsumeReady 返回：

- subscription ID；
- subscription epoch；
- 只向 owner 返回一次的 resume token；
- negotiated limits；
- 当前 assignment 状态。

worker 重启或恢复重建会产生新 subscription ID 或 epoch。旧 epoch 的 ACK、NACK、commit 和 control frame 必须被拒绝。

### 11.3 Consumer wire frames

v2 Consumer 数据面包含：

| Frame | 方向 | 语义 |
| --- | --- | --- |
| ConsumeOpen | PHP → worker | 建立 subscription 与 stream lease |
| ConsumeReady | worker → PHP | 返回 ID、epoch 和协商参数 |
| ConsumeResume | PHP → worker | 在有界 grace 内使用 resume token 重建 stream |
| ConsumeMessage | worker → PHP | 一条 Kafka record |
| ConsumeAck | PHP → worker | 应用成功处理一条 delivery |
| ConsumeNack | PHP → worker | 应用失败，终止当前投递流程 |
| FlowControl | PHP → worker | 增加 message/byte credit |
| Commit | PHP → worker | 提交连续 ACK watermark |
| Committed | worker → PHP | 返回实际提交 offsets |
| RebalanceAssign | worker → PHP | assignment 生效 |
| RebalanceRevoke | worker → PHP | partition 即将 revoke |
| ConsumePause | PHP → worker | 暂停当前 epoch 内的指定 assignment partitions |
| ConsumePartitionResume | PHP → worker | 恢复当前 epoch 内的指定 assignment partitions |
| ConsumeClose | 双向 | 正常或异常关闭 |
| ShutdownNotice | worker → PHP | worker 进入 drain |

v2 不存在 PollReq 和 PollResp。

### 11.4 ConsumerRecord identity

每条 ConsumeMessage 至少包含：

~~~text
subscription_id
subscription_epoch
delivery_id
topic
partition
offset
timestamp_ms
key
value
headers
wire_size
~~~

delivery ID 在 subscription epoch 内唯一。worker 自己记录真实 wire/resource size，不信任 PHP 回传的字节数。

Kafka key 和 value 在协议与 PHP API 中都是 nullable binary。`null`、空字符串和
非空字节串是三个不同状态；压缩 topic 的 tombstone 必须以 `value() === null`
完整保留，禁止用空串代替。headers 以有序列表传输，重复名称不得在扩展层折叠；
framework 若为旧业务提供关联数组视图，必须同时提供无损重复 header 访问方式。

### 11.5 ConsumerRecord 所有权

- worker 不为 IPC 构造 Vec<ConsumerMessage>；
- worker 只拥有有界 in-flight window；
- 单条 record 可以从 librdkafka detach 为一个 owned delivery；
- 同一 record 在稳态 in-flight 中只保留一份必要的 encoded payload；
- PHP 一次只 materialize credit window 内的消息；
- 不创建 array<array{message}>；
- key、value、headers SHOULD 惰性 materialize；
- 消费热路径禁止 copy_to_bytes → to_vec → PHP string 的连续复制；detach 后的 SourceRecord 与 encoded payload 允许在编码瞬间短暂重叠，但两份 wire image 必须在复制前同时取得全局 byte permits，编码完成立即释放 transient 一份；
- PHP 请求字符串时允许一次不可避免的 PHP ownership copy；
- native `next()` 与 coroutine `hi_kafka_decode_stream_frame()` 创建的 owned Zend object
  必须各自向返回 zval **转移且仅转移一个引用**；PHP 变量被覆盖或 `unset` 后，
  `ConsumerRecord` 的 Rust backing、key/value/header allocations 必须立即可释放，不得依赖
  GC cycle、stream close 或进程退出；
- 当前绑定层不得降级到 `ext-php-rs < 0.15.13`，这些版本的
  `ZBox<ZendClassObject<T>>` 转换存在已知的单引用泄漏。升级或替换 PHP binding 时，
  必须先通过第 26.3 节的 native 与 coroutine 引用所有权回归测试。

### 11.6 Consumer 热路径

每个 subscription 由一个 actor 串行拥有 Kafka Consumer、assignment、epoch、watermark、pause reason 和 in-flight map。推荐数据路径如下：

~~~text
librdkafka bounded fetch queue
  → subscription actor cancellation-safely observes one borrowed record
  → synchronously validate record/header count and try-acquire message/byte/global permits
  → detach exactly that record
  → bounded stream writer
  → PHP bounded frame parser
  → one ConsumerRecord visible to application
  → ACK/NACK
  → release in-flight ownership and optionally replenish credit
~~~

actor 可以使用 select 同时处理 Kafka record、ACK/control frame、rebalance 和 shutdown，但同一 subscription 的状态转换必须在 actor 内串行发生。

`recv()` 之后到 actor 接管 record 之前不得再出现可取消的 await。若此时 try-acquire 失败，必须在 actor 内 seek 回该 record offset，再等待 semaphore 通知；禁止用毫秒 sleep 轮询预算，也禁止丢弃已经从 librdkafka 取出的 record。

禁止在任意一层加入与 credit window 脱钩的预取队列。writer queue 中尚未写完的 record 仍计入 subscription 和 worker 全局预算。Consumer socket 的读、写循环必须独立推进，阻塞的 UDS write 不能阻止 ACK/control frame 被读取。写 frame 时只允许栈上固定 header 加既有 payload 的分段写入，不得再次拼接一份完整 frame。

## 12. Consumer 状态机

~~~text
Closed
  │ ConsumeOpen
  ▼
Joining ── assignment ──► Streaming
                            │
                            ├── no credit ───────► PausedByCredit
                            │                        │ credit
                            │                        └──────► Streaming
                            ├── user pause ──────► PausedByUser
                            ├── revoke ──────────► Revoking
                            │                        │ resolved
                            │                        └──────► Joining
                            ├── NACK/disconnect ─► Recovering
                            │                        │ seek/resubscribe
                            │                        └──────► Joining
                            └── close/shutdown ──► Closing ─► Closed
~~~

状态转换必须由 worker 单一 subscription actor 串行化。不得让多个 connection task 同时直接修改 assignment、watermark 和 pause 状态。

## 13. ACK 与 offset

### 13.1 必须关闭自动 offset store

Consumer 必须设置：

~~~text
enable.auto.commit=false
enable.auto.offset.store=false
~~~

以下事件都不能推进 processed offset：

- librdkafka fetch 到消息；
- worker detach 消息；
- 写入 UDS；
- PHP 收到 frame；
- PHP 构造 ConsumerRecord；
- handler 开始执行。

只有有效 ACK 可以推进 processed watermark。

### 13.2 连续 ACK watermark

worker 为每个 partition 维护：

- 按实际投递顺序排列的 offset；
- delivered but unacked；
- acked above gap；
- highest contiguous ACK；
- stored offset；
- committed offset。

Kafka offset 可能因 compaction、control record 或 isolation level 出现整数空洞。因此“连续”严格指 worker 实际投递序列的无未 ACK 前缀，不是数值必须逐个 `+1`。当前缀全部 ACK 后，committable offset 是该前缀最后一条实际消息的：

~~~text
highest_contiguous_acked_offset + 1
~~~

后面的消息先 ACK 只能进入 bounded gap set，不能跨过失败或未完成 offset。

连续 watermark 前进时，worker 才允许显式 store 对应 partition 的 next offset。commit 必须从 worker 的 watermark table 构造精确 TopicPartitionList；禁止使用可能包含 fetch position 或其他隐式状态的 consumer current position 作为提交来源。

### 13.3 ACK 与 commit 分离

ACK 表示应用处理成功；commit 表示向 Kafka 持久化 watermark。

当前 v2 只暴露 manual commit，避免声明未完整实现的 timer/count policy。ACK 总是逐条发生，应用在需要的边界显式调用 commit。

未来若增加每 N 条、每 T 毫秒、N/T 先到或 sync-each，必须同时实现 actor timer、连续 watermark 计数、失败重试语义和完整测试；不得只增加 wire enum。commit batching 不改变处理成功事实，只改变崩溃后的最大重复窗口。

### 13.4 NACK

NACK 表示当前 delivery 未成功。

默认行为：

1. 停止新投递；
2. 终止当前 stream 与 subscription actor；
3. 丢弃所有未 ACK delivery 的本地投递状态并释放 permits；
4. 立即离开 consumer group，不进入 disconnect resume grace；
5. 后续新 subscription 从 broker committed offset 重投，因此允许重复此前已 ACK 但尚未 commit 的消息。

NACK 绝不能把该消息标记为已处理。

### 13.5 指定起始位置（待实现）

当前只支持 `auto.offset.reset`（earliest / latest），它只在分区没有可用的已提交 offset 时生效。
一次性的回补或重放在运维侧用 `kafka-consumer-groups --reset-offsets` 修改已提交 offset 完成。

禁止“subscribe 之后直接 seek”：seek 时分区可能尚未分配，随后的 rebalance 也会用已提交
offset 覆盖位置。需要在代码中指定起始位置时，按以下方式实现：

- ConsumeOpen 携带起始位置：按分区的 offset，或一个时间戳（worker 用 `offsets_for_times`
  换算）；
- worker 在 rebalance assign 回调中、分区**首次**分配给该 subscription 时，以起始位置 assign；
  之后的 rebalance 照常从已提交 offset 继续，否则每次 rebalance 都会回退到起点；
- 起始位置之前的 offset 不因此被提交，watermark 从起始位置开始；
- framework 的 `ConsumeOffsetType::At` / `AfterMilli` 映射到该能力，并移除
  `AbstractConsumer::assertSupportedOffsetMode()` 的限制。

实现前需先确定：

- 已有提交 offset 时，起始位置是覆盖它，还是只在没有提交 offset 时生效；
- 起始位置对“首次分配”的判定范围：同一 subscription 生命周期内，还是跨进程重启（重启后
  再次覆盖会造成重复消费）；
- 按 offset 指定时，未列出的分区与扩容新增的分区如何处理；
- 时间戳晚于分区最新消息时的行为（等同 latest）。

e2e 至少覆盖：指定 offset 与时间戳、首次分配后发生 rebalance 不回退、cooperative 增量分配、
新增分区。

## 14. Credit 与背压

背压由显式 credit 驱动，不由“缓冲快满后再 drop”驱动。

每个 subscription 同时具有：

- available message credit；
- available byte credit；
- max in-flight messages；
- max in-flight bytes。

worker 还必须有跨 subscription 的 global in-flight byte semaphore。

投递前必须同时获得：

1. subscription message permit；
2. subscription byte permit；
3. worker global byte permit。

缺少任意 permit 时：

- 不 detach 下一条消息；
- pause 对应 Kafka assignment；
- 等待 credit/ACK；
- permit 恢复后 resume；
- 不离开 consumer group；
- 不丢消息。

用户显式 pause 和自动 credit/global-budget pause 是两个独立原因，actor 必须分别持有 pause reason。自动 resume 只恢复未被用户显式暂停的 assignment，不得覆盖用户 pause。断线 grace 期间同样进入自动 pause，避免 librdkafka 队列继续累积。

自动 pause 期间 actor 不取消息，但 MUST 按固定间隔（当前 1 秒）对 librdkafka consumer 队列做一次零超时 keepalive 拉取：librdkafka 要求应用每 `max.poll.interval.ms` 内至少拉取一次，否则主动离开 group；rebalance 回调也只在拉取时执行。顺序式 driver 的窗口为 1，PHP 处理任意一条消息期间 subscription 都处于自动 pause（并发窗口占满时同理），因此 PHP 处理耗时不得受 `max.poll.interval.ms` 约束，该参数只用于发现 worker 自身停止拉取。keepalive 若意外拉到消息（分区未暂停），必须 seek 回其 offset，不得交付或丢弃。

credit 瞬时归零时 actor 必须立即停止调用 `recv()`，但可以对昂贵的 librdkafka assignment pause 做数毫秒有界去抖；ACK 在去抖窗口内恢复 credit 时不得执行每消息一次的 pause/resume。持续背压、全局预算耗尽和断线仍必须进入真实 pause。去抖期间只能增长既有有界 librdkafka queue，不能引入新的应用层队列。

FlowControl 可以与 ACK 合并传输以减少 frame，但内部语义和指标必须独立。官方顺序式 driver MUST 使用该原子能力：`ack()` 只在本地完成 delivery identity settlement；下一次 `next()` 将 pending ACK 与该 delivery 的精确 message/byte credit 合并为一个 ConsumeAck frame。`commit()`、pause、显式 resume 与 close 在发送自身控制帧前 MUST 先以 zero-credit ACK flush pending settlement，因此下一条 ConsumeMessage 仍不可能越过这些 control barrier；zero-credit flush 后，与该 delivery 等额的 replenishment intent 必须留在 driver 本地，若流仍继续，下一次显式 `next()` 再通过 FlowControl 开窗，不能因 control barrier 丢失 credit。NACK 保持立即、终止式发送。

deferred wire ACK 不改变 at-least-once：PHP 在 `ack()` 返回后、flush 前崩溃时允许 redelivery，不允许 offset 前进或消息缺失。pending ACK 只能保存 subscription epoch、delivery ID 与 wire size，MUST NOT 再持有 payload 副本。任何 reconnect/rebalance 都使旧 pending identity 失效，由 worker watermark 决定是否重投。

官方顺序式 PHP driver 不维护消息 inbox，必须协商 `max_in_flight_messages=1`，并以零 initial credit 打开或恢复 subscription。首次 `next()` 使用 FlowControl 开窗；后续 `next()` 使用 pending ACK 携带精确 credit 原子开窗，因此 open、ACK 后的 `commit()` 都不会被未读取的 ConsumeMessage 超越。driver 必须记录唯一 outstanding delivery 或 pending ACK，重复 ACK/NACK、未 settle 就再次 `next()`、以及对 pending ACK 再次 settlement 必须在本地拒绝。wire protocol 仍允许大于 1 的窗口，但使用者必须自行实现同样受窗口约束的 interleaved delivery 存储与 control-response demultiplexing。

协程 driver 通过 `ConcurrentConsumerClientInterface::consumeConcurrent()` 提供大于 1 的窗口：以请求值打开，worker 按自身上限协商（`ConcurrentConsumerStreamInterface::maxInFlightMessages()` 返回协商结果）；stream 同时最多持有窗口条未确认投递，ACK/NACK 可乱序，延迟 ACK 按确认顺序在下一个屏障逐条冲刷，`next()` 冲刷时每条 ACK 原子补充该投递的精确 credit，控制屏障冲刷时额度保留在本地。stream 仍不是并发安全的：所有调用必须由同一个协程发起，业务协程处理完成后把结果交回该协程统一确认。commit 期间到达的消息按到达顺序暂存，不超过窗口。阻塞 `Client` 只提供顺序窗口。

credit 与内存 permit 的规则必须区分：

- ConsumeMessage 发出时扣减 message credit 和实际 wire byte credit；
- delivery 从 detach 开始到 ACK、NACK、revoke 或 epoch 失效为止持续占用 in-flight permits；
- 有效 ACK 释放该 delivery 的 in-flight permits，但只有 ACK 携带的 credit delta 或独立 FlowControl 才补充发送 credit；
- 官方 driver 在本地立即接受 ACK intent；下一次 `next()` 以 ACK + 精确等额 credit 原子 flush，其他 control operation 以 zero-credit ACK flush，使 `commit()` 等操作成为不会被下一条 ConsumeMessage 超越的 delivery barrier；
- rebalance notice 与 FlowControl 可能在 full-duplex stream 上交叉；worker 必须静默丢弃 stale epoch 的纯 FlowControl 且不得修改当前 credit，client 在读到新 epoch notice 后按新窗口重新补充。stale ACK/NACK/commit 仍必须拒绝，因为它们携带业务处理或持久化语义；
- credit 累加不得超过协商窗口，整数溢出或伪造 byte size 必须作为协议错误拒绝；
- NACK 和失效 delivery 释放本地 permits，但不自动授予继续投递 credit；
- 单条 record 大于协商 hard limit 时 seek 回原 offset、返回 `RECORD_TOO_LARGE` 并终止该 subscription；应用必须以足够大的 hard limit 重新打开，offset 不得前进。不可在一个永远无法满足的窗口上自动重试或忙循环。

## 15. Consumer 内存模型

Consumer 内存上界由下列部分构成：

~~~text
bounded librdkafka fetch queue
+ worker global in-flight byte budget
+ per-subscription metadata and ACK gap state
+ bounded UDS socket buffers
+ bounded PHP stream receive buffer
+ PHP active handler objects
~~~

硬性要求：

- 同时按条数和字节限制；
- detach/encode 的瞬时双份 wire image 按 `2 × wire_size` 预留全局 byte permits，编码完成释放 transient 一半；稳态只保留 encoded payload 的一份 permit；
- worker 全局限制优先于 per-subscription 总和；
- ACK gap 数量受 max in-flight 限制；
- Kafka headers 数量在复制或分配前限制为 4096；
- 单条 oversized record 返回显式错误且不提交；
- buffer 满时等待或 pause，绝不 pop_front/drop；
- connection reusable buffer 按容量等级回收；
- RSS 允许存在 allocator high-water，但经过 warm-up 后不能随处理总量线性增长。
- 即使 payload 很小、worker RSS 有界，也必须单独验证 PHP Zend object/refcount；
  每条消息固定泄漏一个小对象同样属于无界内存，不能被 byte credit 模型掩盖。

## 16. Rebalance

Rebalance 由 worker 主动推送，不由 PHP 轮询事件队列。

Revoke 流程：

1. librdkafka callback 只向 subscription actor 投递 notice，不直接修改 actor state；
2. callback 在 `recv()` 内触发时，同次 `recv()` 返回的 record 不得越过 notice：若该 partition 仍属于 librdkafka 的最终 assignment，可在取得全局 permit 后最多 detach 这一条 record，由 actor 先顺序应用全部 queued notice 再投递；若 ownership 已丢失则直接丢弃该本地视图。禁止无条件 seek/refetch，因为压缩消息队列会产生第二份解压高水位；仅在全局 permit 暂不可用时允许 notice 处理后 seek 回 offset；
3. cooperative rebalance 中仍保留 ownership 的 partition seek 到各自最早未 ACK offset；
4. 提升 subscription epoch，使全部旧 delivery 失效并释放 permits；
5. 对真正 revoked 的 partition 完全删除本地 watermark，包括 ACKed-but-uncommitted 进度；ownership 转移后 broker committed offset 是唯一权威恢复点；
6. 更新 assignment，并向 PHP 推送 RebalanceRevoke。旧 epoch 的业务 frame 不得推进状态。

Assign 流程：

1. 采用 librdkafka 已完成的 assignment；新获 ownership 的 partition 从 Kafka committed offset 初始化，不继承历史本地 watermark；
2. 更新 actor assignment，并重新应用 user-pause 与 backpressure-pause 原因；
3. 向 PHP 发送 RebalanceAssign；
4. 当前 epoch credit 可用时开始投递。

不得因 rebalance commit 超过未 ACK gap，也不得将 revoked partition 的未提交本地进度带给下一任 owner。

## 17. Consumer 故障恢复

| 故障点 | 结果 |
| --- | --- |
| PHP 收到消息前死亡 | 未 ACK，重新投递 |
| handler 执行中死亡 | 未 ACK，允许重复 |
| handler 成功、ACK 前死亡 | 允许重复，不允许丢失 |
| ACK 到 worker、commit 前 worker 死亡 | 允许重复，不允许丢失 |
| broker commit 成功、响应丢失 | 恢复时以 Kafka committed offset 为准 |
| stream 断开且有多条 in-flight | 从每 partition 最早未 ACK 位置恢复 |
| stale epoch ACK 到达 | 拒绝，不影响 offset |
| credit 耗尽 | pause/wait |
| 单条消息超限 | 显式错误，offset 不前进 |
| worker buffer/预算耗尽 | pause/wait，禁止丢弃 |
| fatal auth/config 错误 | 终止 subscription，结构化报告 |
| transient broker 错误 | 有界退避并保持资源上限 |

系统默认提供 at-least-once，不承诺 PHP 外部副作用 exactly-once。

## 18. EOS 与 Consumer 事务协作

本节仍是未来 transaction capability 的原子准入设计，不是当前 protocol v2 已实现功能。只有第 10 节的 owner lease、下述 Consumer epoch/watermark 绑定和对应故障注入同时完成，才能新增 capability 与 wire frame。

事务流为：

~~~text
BeginTransaction
→ ConsumeMessage
→ application processing
→ transactional Produce
→ ConsumeAck
→ SendAckedOffsetsToTransaction
→ CommitTransaction
~~~

SendAckedOffsetsToTransaction：

- 引用 subscription ID 和 epoch；
- 读取 worker 内连续 ACK watermark；
- 使用该 subscription 的 group metadata；
- 不接受跨 gap offset；
- 不信任 PHP 自行计算的 offset 数组；
- transaction lease 与 subscription epoch 都必须有效。

事务 abort 后，参与事务但未提交的 Consumer offset 不前进，消息可重新投递。

## 19. PHP 运行时模型

### 19.1 CLI

阻塞 Client 使用相同 v2 协议和状态语义。同步 IO 不能改变 ACK、commit 或恢复规则。

### 19.2 Swoole

- worker 必须在 reactor 和线程启动前 ensure；
- 每个 Swoole process 创建自己的 UDS pool；
- 多协程 Produce 共享该 process 的协程连接池；
- Consumer stream 使用独占协程 socket；
- socket IO 必须 yield reactor；
- 不在 librdkafka/native thread 调 PHP；
- shutdown 在协程可运行阶段显式 close stream/pool；
- OS process exit 是最后兜底。

### 19.3 Swow

Swow 与 Swoole 共用唯一的 `CoroutineConsumerStream` 状态机，只替换协程 socket adapter。公开 API、超时单位、异常分类、拆帧、credit 和恢复行为必须一致；Swow 原生微秒超时只允许在 adapter 边界出现。

### 19.4 PHP-FPM

每个 FPM child 可有自己的 UDS connections，但共享 worker Producer。请求结束不能关闭其他 PHP process 使用的 worker。

## 20. 生命周期与 shutdown

### 20.1 PHP client 退出

- 关闭 idle RPC connections；
- Consumer owner 主动发送 ConsumeClose；
- 未 ACK delivery 保持未提交；
- transaction owner 尝试 abort；
- 发送 Goodbye；worker 声明 `CAP_GOODBYE_FLUSH` 时带上等待预算（drain timeout，默认
  10s）并等待应答。worker 按对端 PID（`SO_PEERCRED` / `LOCAL_PEERPID`）判断：没有
  其它进程连着它时，先 flush Producer 再应答，因为 CLI 定时任务或容器 1 号进程退出后
  worker 会随容器一起被 SIGKILL，不等待就会丢掉队列里的 fire-and-forget 消息；还有
  其它进程（FPM、Swoole 多进程）时立即应答，不拖慢退出；
- 不在 MSHUTDOWN 触发新 worker；
- 清理失败由连接关闭和 worker lease timeout 兜底。

### 20.2 worker drain

收到 SIGTERM/SIGINT 或自退条件后：

1. 进入 Draining；
2. 拒绝新 session 和 Produce；
3. 通知 Consumer streams；
4. 停止新 delivery；
5. 处理已收到 ACK；
6. 按策略提交连续 watermark；
7. 等待已接受的 Produce response；
8. flush Producer；
9. abort 未完成 transaction；
10. 关闭 Consumer 并离组；
11. 删除 socket 和 PID 文件；
12. 使用 _exit 退出。

drain timeout 到达后允许中止剩余任务，但不得伪造成功响应。

### 20.3 worker 自退

worker 只有在以下条件同时满足时才可 idle self-terminate：

- 无 RPC/stream owner；
- 无 active transaction；
- 无 active subscription lease；
- 无待发送 Producer queue 或 in-flight delivery；
- idle deadline 到达，或最后一个明确 Goodbye 完成。

## 21. 安全边界

- socket 应位于权限受控目录，而不是可被不可信用户替换的公共路径；
- socket 和 lock 文件权限遵循最小权限；
- 默认 socket 在公共的 `/tmp`，因此两端都校验对端 uid（`worker/src/peer.rs`），不依赖
  socket 文件权限位（worker 继承拉起者的 umask）：
  - worker 只接受与自己同 uid 或 root 的客户端（`SO_PEERCRED` / `getpeereid`），拒绝的
    连接记 warning、不刷新 idle 计时；
  - 客户端只连接同 uid 或 root 的 worker，root 客户端信任任何 worker（兼容在容器里以
    root 执行命令、连接 FPM 用户拉起的 worker）。Rust 侧在 connect 后校验对端 uid；协程
    driver 取不到对端凭据，连接前检查 socket 文件属主（`hi_kafka_check_socket_owner`）；
  - 路径被其它用户占用时 ensure 立即报错，不等就绪超时。`.spawn-lock` / `.pid` 被抢先创建
    仍会导致拉起失败（拒绝服务），多用户共用机器时应把 socket 放到私有目录；
- 不记录 broker password、OAuth token、SASL secret 或消息正文；
- 所有数量、长度、timeout、partition、offset 输入必须 checked conversion；
- per-frame limit 在 allocation 前验证；
- decode 不得依据攻击者声明进行无上限 Vec::with_capacity；
- panic 只能终止当前 request/session，不应直接杀整个 worker；
- worker fork child 不调用继承的 PHP runtime；
- metrics endpoint 需要明确绑定地址和访问边界。

## 22. 可观测性

### 22.1 Worker

必须暴露：

- PID、build/version、protocol major、uptime；
- active RPC connections 和 stream sessions；
- cluster configuration generations；
- 每 cluster Producer instance 数；
- Producer queue messages/bytes；
- Produce accepted、delivered、failed、unknown、timeout；
- active subscriptions、epoch 和 assignment；
- per-subscription/global in-flight messages/bytes；
- available credits；
- delivered、ACKed、stored、committed watermark；
- ACK gap size；
- ACK latency、commit latency、delivery latency；
- pause/resume 次数、时长和原因；
- NACK、stale ACK、redelivery、reconnect；
- rebalance assign/revoke/error；
- rejected oversized frame/record；
- drain 状态和耗时。

禁止以 topic、group、partition 的无限组合创建不可控 metrics cardinality。详细 per-partition 状态通过 debug endpoint 或采样日志提供。

### 22.2 PHP

PHP driver 暴露：

- pool size、idle、created、closed、poisoned；
- stream reconnect；
- pending RPC；
- receive buffer bytes；
- active PHP messages；
- protocol errors；
- worker generation/epoch。

### 22.3 不变量指标

内部 message-drop 指标在正确架构下必须恒为 0。任何非零都视为 correctness incident，而不是正常 backpressure 指标。

## 23. 性能设计

### 23.1 Producer

- 依赖共享 librdkafka Producer 聚合来自多个 PHP worker 的流量；
- 保留多个 RPC connections，避免 ProduceSync head-of-line；
- 可选 multiplex 必须支持并发 dispatch 和单 writer；
- borrowed decode 后直接 enqueue；
- 避免 frame payload 的重复 Vec；
- 不在 worker 另建无界 Producer queue。

### 23.2 Consumer

- Kafka fetch 仍可批量；
- worker 逐条、有界地向 PHP 暴露；
- 多个 ConsumeMessage frame 可聚合到一次 write；
- PHP 一次 recv 可解析多个 frame；
- ACK/FlowControl 可合并写；
- commit 可按 N/T 合并；
- 可跨 partition 并发；
- 单 partition 保持顺序和 ACK gap 约束。

“逐条业务处理”不等于“一条消息两次 syscall”。性能依赖缓冲 IO 和 credit window，而不是 PHP 可见的批次数组。

### 23.3 性能门槛

在相同 broker、payload、partition、compression、ACK、commit 和并发配置下：

- Producer throughput/p99 不得比重构前退化超过 5%；
- Consumer throughput/p99 不得比重构前退化超过 5%；
- UDS syscall 必须通过 buffered read/write 摊销；
- 增加 Swoole HTTP worker 数不得增加 worker 内同 cluster Producer 数；
- 提高已处理消息总量不得导致稳定期 RSS 线性增长。

如无法满足，优化实现，不回退到完整批次 materialization。

性能回归的标准入口是 `benchmarks/run.sh`，基线比较入口是 `benchmarks/compare.php`。benchmark MUST 将 worker/cluster 初始化、Consumer 预装数据和 warm-up 排除在计时区间外；MUST 记录 runtime、场景、payload、并发、cluster 配置、版本、吞吐、payload 带宽、P50/P95/P99、PHP/worker RSS。Producer FNF、Producer Sync 和 Consumer `next+ack` 必须分别报告，commit latency 不得混入 Consumer 吞吐。Native blocking 结果不得伪装为单进程协程并发。

并发 Consumer 的统一 start gate MUST 位于 warm-up 之前。每条 stream 完成自身 warm-up 后直接进入 measured phase，不得在零 credit 状态等待 post-warm-up barrier；该等待会触发正常的 backpressure pause，随后把 idle resume/fetch latency 错算为 steady-state 吞吐退化。idle-resume latency 如需测量，必须作为独立场景报告。

基线与候选只有在 broker、硬件、payload、partition、compression、acks、commit 和 concurrency 相同时才可比较。一次短跑只用于 smoke；release 性能结论 SHOULD 使用至少 100000 条 measured messages、10000 条 warm-up，并重复运行以观察方差。

## 24. 错误模型

所有错误必须有稳定 kind：

- INVALID_ARGUMENT；
- PROTOCOL；
- FRAME_TOO_LARGE；
- WORKER_DRAINING；
- CLUSTER_NOT_REGISTERED；
- SUBSCRIPTION_NOT_FOUND；
- STALE_EPOCH；
- STALE_DELIVERY；
- CREDIT_EXHAUSTED；
- RECORD_TOO_LARGE；
- AUTHENTICATION；
- AUTHORIZATION；
- REBALANCE；
- COMMIT；
- PRODUCER_QUEUE_FULL；
- TIMEOUT；
- TRANSACTION_STATE；
- TRANSACTION_OWNER；
- INTERNAL。

错误同时标记：

- retryable；
- operation outcome：not_applied、applied、unknown；
- native Kafka code；
- correlation/subscription/delivery identity。

仅有 retryable 不足以表达 Produce timeout 后的 unknown delivery outcome。

## 25. 配置原则

配置分三层：

1. extension/worker lifecycle；
2. cluster librdkafka config；
3. subscription/stream resource policy。

资源相关配置必须有安全默认值和 hard maximum：

- frame limits；
- worker global in-flight bytes；
- subscription messages/bytes；
- socket buffers；
- Producer queue；
- drain timeout；
- commit interval；
- reconnect backoff；
- consumer resume grace；
- rebalance drain timeout。

环境变量和 php.ini 只是配置入口，不是架构。配置值在 worker 启动或 session negotiation 时固化并可观测。

## 26. 测试战略

### 26.1 协议

- frame 拆包、粘包和连续多 frame；
- 每种长度边界；
- malformed count/length；
- stale epoch/delivery；
- out-of-order response；
- v1/v2 HELLO 拒绝；
- fuzz decode 不 panic、不无界 allocation。

### 26.2 Consumer correctness

在以下每个点注入 PHP kill 和 worker kill：

- delivery 前；
- PHP handler 前；
- handler 中；
- handler 成功、ACK 前；
- ACK 后、offset store 前；
- offset store 后、commit 前；
- commit 后、response 前。

验证：

- 每条生产记录最终至少被观察一次；
- 没有 committed offset 跨过未 ACK gap；
- 允许的结果只有成功一次或重复；
- 不允许静默缺失。

### 26.3 背压与内存

- 持续 backlog；
- 可变消息大小；
- 慢 handler；
- zero credit；
- 多 subscription；
- 超大单条消息；
- reconnect loop；
- rebalance storm。

分别测量 PHP 与 worker RSS。warm-up 后 RSS 必须位于配置预算加文档化 allocator tolerance 内，且对累计处理条数无正线性斜率。

仓库中的 `scripts/run-consumer-memory-e2e.sh` 是大 payload/worker 预算门禁的标准入口：
默认流过 2048 × 64 KiB，检查 PHP peak、worker peak RSS，以及完成前 25% warm-up
后的 RSS 增长。

`consumer-record-memory-v2.php` 与 `consumer-record-memory-swoole-v2.php` 是 Zend
引用所有权门禁：默认各自在单个 PHP 进程内顺序消费 50,000 条小消息，不保存消息数组，
预热后 PHP 内存增长不得超过 4 MiB。前者必须覆盖 native `ConsumerStream::next()`，
后者必须在加载 Swoole 时覆盖 `SwooleConsumerTransport` +
`hi_kafka_decode_stream_frame()`；Swow transport 共享相同 decoder，但发布环境具备 Swow
工具链时也必须运行同等门禁。

### 26.4 吞吐与延迟

- `benchmarks/run.sh` 覆盖 ProduceFnf、ProduceSync 与 Consumer `next+ack`；
- Swoole/Swow concurrency 必须建立真实 coroutine load，不得把串行循环标成并发；
- Consumer concurrency 使用独立单分区 topic/stream，或使用预先验证 partition 数的同 group topic；
- latency sampling 必须有上限，避免测量数组本身改变长跑内存曲线；
- runner 在 PHP 进程外采样 PHP/worker RSS 与 CPU，采样不得进入 measured operation；
- `benchmarks/compare.php` 执行第 23.3 节的 5% throughput/P99 回归门槛。

### 26.4 Producer

- 多 PHP process 并发 autospawn；
- 多 Swoole worker 并发 ProduceFnf/Sync；
- 验证每 cluster 单 Producer；
- queue full；
- delivery timeout unknown outcome；
- idempotence；
- cluster config generation 切换；
- transaction owner 冲突和 owner crash。

### 26.5 生命周期

- pre-reactor ensure；
- multi-thread late fork 拒绝；
- fork child pool reset；
- PHP owner crash；
- Consumer stream 在 grace 内恢复、grace 到期释放；
- worker crash/restart；
- graceful drain；
- v1/v2 versioned socket 共存和旧 worker idle 退出。

## 27. 重构交付顺序

这是一次 protocol-major 重构，不建立永久双栈。

1. 建立 v2 frame、错误模型与 state-machine 测试。
2. 实现 subscription actor、epoch、delivery identity。
3. 关闭 automatic offset store，实现连续 ACK watermark。
4. 实现全局/per-subscription count+byte permits。
5. 实现 dedicated full-duplex Consumer stream。
6. 删除 VecDeque overflow eviction 和 batch PollResp。
7. 实现 blocking、Swoole、Swow ConsumerStream。
8. 修改框架为 next、handle、ACK/NACK、commit。
9. 将 rebalance 改为 stream push。
10. 完成 PID-aware pool 与 pre-reactor startup。
11. 切换 protocol v2 和 versioned socket。
12. 删除 v1 poll API、codec、tests 和文档。
13. 通过 correctness、memory、performance、lifecycle release gates。

以下中间状态禁止发布：

- 关闭 auto offset store 但 ACK path 尚未完成；
- wire 逐条发送但 worker 仍构造完整 Vec；
- worker 有 hard limit但 overflow 仍 drop；
- 新 API 内部仍调用旧 poll(): array；
- v2 与旧 worker 共用同一 socket；
- Consumer stream 与普通 RPC 共享并发生 head-of-line 的连接。

## 28. 完成定义

架构重构只有在以下条件全部满足时完成：

- 生产 Consumer API 不再返回消息批次数组；
- Consumer IPC 不存在 PollReq/PollResp；
- 热路径不构造 Vec<ConsumerMessage>；
- enable.auto.offset.store 明确关闭；
- commit 全部来自连续应用 ACK watermark；
- 内部压力路径不丢消息；
- PHP 与 worker 内存通过有界测试；
- worker 仍是唯一 librdkafka owner；
- 多 HTTP worker 仍共享每 cluster Producer；
- 当前 capability 中不存在无 owner 的 transaction；未来重新引入时必须满足第 10、18 节；
- fork、shutdown、rebalance、恢复语义通过故障注入；
- v1 consumer 实现和冲突文档已删除；
- 代码、测试和用户文档均符合本文档。
