# 可靠性验证（账本 + 故障注入 + soak）

用于回答「组件在故障与长时间运行下是否达到预期」。单元测试与 `tests/php` 的 e2e 证明缺陷被修复、功能不回退；这里证明**持续负载 + 周期故障**下的不变量。

## 组成

| 文件 | 作用 |
| --- | --- |
| `producer.php` | 按固定速率逐条 `produceSync`；只有 broker 确认后才把 seq 写入 `acked-*.log` |
| `consumer.php` | 消费循环与 framework `AbstractConsumer` 一致（逐条 ACK，每 N 条或 1s commit）；crc 校验后写 `consumed-*.log` |
| `ledger-check.php` | 位图比对两本账：丢失、重复、损坏 |
| `soak.php` | 编排：按 PID 管理并自动重启进程、注入故障、采样资源与 metrics、排空后出报告 |
| `report.php` | 把原始数据变成逐条判据的 `report.md` / `report.json`，全部通过时退出码 0 |

消息格式 `run|producer|seq|crc32|payload`，账本 append-only 且逐行 flush，进程被杀也不丢已写入的行。

## 运行

```bash
# 单节点 broker（4 分区 topic 由 soak 自动创建并在结束后删除）
docker compose -f docker-compose.kafka.yml up -d
cargo build --release -p hi-kafka --features kafka

# 10 分钟冒烟（6 种故障各约 1–2 次）
php -n tests/reliability/soak.php --out=/tmp/soak --ext=target/release/libhi_kafka.so \
    --duration=600 --warmup=60 --fault-interval=60

# 协程运行时（swoole 与 swow 不能同时加载）
php -n tests/reliability/soak.php --out=/tmp/soak-swoole --ext=... \
    --runtime=swoole --php-args="-d extension=swoole"

# 长稳：24h，随机故障顺序，判定内存 / fd 斜率
php -n tests/reliability/soak.php --out=/tmp/soak-24h --ext=... \
    --duration=86400 --warmup=600 --fault-interval=300 --fault-order=random
```

报告可单独重跑并调整阈值：`php -n tests/reliability/report.php --dir=/tmp/soak --max-recovery=20`。

## 故障

| 名称 | 动作 | 验证点 |
| --- | --- | --- |
| `worker-kill` | SIGKILL worker | 客户端重新拉起 worker、重放注册、恢复订阅；FNF 队列中未送达的消息会丢失，因此生产者只用 produceSync 记账 |
| `worker-term` | SIGTERM worker | drain：空闲连接立即关闭、消费者自动迁移到新 worker |
| `worker-stop` | SIGSTOP 10s 后 SIGCONT | worker 假死：超时为结构化可重试错误，恢复后不失配 |
| `consumer-kill` | SIGKILL 一个消费者 | 订阅随 resume grace 过期离组，其分区由其它成员接管 |
| `broker-restart` | `docker restart` | commit / produce 在 coordinator 不可用期间返回可重试错误 |
| `broker-pause` | `docker pause` 15s | broker 无响应：有界超时，不卡死 |

## 判据（默认阈值，按业务 SLA 调整）

| 判据 | 默认 |
| --- | --- |
| 已确认消息全部被消费 | missing = 0 |
| 无损坏 | corrupted = 0 |
| 重复有界 | ≤ (commit_every + rate) × 扰动次数 |
| 所有故障后恢复 | 0 个未恢复 |
| 恢复时间最大值 | ≤ 30s |
| 故障窗口外的消费停顿 | ≤ 30s |
| worker RSS 峰值 | ≤ 512 MiB |
| 同一 worker 进程内 RSS / fd 增长斜率 | ≤ 5 MiB/h、≤ 10/h，需 ≥ 30min 窗口，否则 N/A |

## 局限

- 单节点 broker 无法覆盖 leader 切换、ISR 收缩、多 broker 网络分区；需要 3 节点集群。
- 网络级故障（延迟、丢包、带宽限制）需要 toxiproxy 或 `tc netem`，尚未引入。
- 消费者 crash 后的恢复时间受 `session.timeout.ms`（测试中 6s）主导，属于 Kafka group 协议语义。
