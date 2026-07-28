# hi-kafka benchmark

这套 benchmark 测量真实 PHP → UDS → hi-kafka-worker → Kafka 链路，不替代 correctness E2E，也不把集群初始化、预装消息和 warm-up 算入吞吐。

## 场景

- `produce-fnf`：每次完成 worker 本地 enqueue ack，不等待 broker delivery；计时结束后追加 ProduceSync 只验证 broker path 仍可用，它不是跨 partition 的 durability fence，因此该结果表示 IPC/local-enqueue 吞吐。
- `produce-sync`：每条等待 broker ack。
- `consume`：计时区间是 `next() + payload 长度校验 + ack()`；预装消息、建组和 warm-up 在计时前完成，commit latency 单独报告。

Swoole/Swow 的 concurrency 表示同一 PHP process 内真实并发 coroutine。Consumer concurrency 使用独立 topic 和 stream，避免单分区 topic 让并发数字失真。Native 是 blocking 基线，只接受 concurrency=1；多 PHP process 压力应由外层 workload orchestrator 产生，不能伪装成单进程并发。

延迟最多均匀采样 10000 次，吞吐统计全部消息。Coroutine 在 warm-up 前由统一 gate 同时释放，每条 stream 完成自己的 warm-up 后直接进入 measured phase；禁止在 warm-up 后设置 barrier，因为零 credit 等待会按架构触发 Kafka pause/resume，把 idle 恢复延迟错误计入稳态吞吐。Runner 每 100ms 采样 PHP/worker RSS 与 CPU，采样频率可配置。

## 快速运行

```bash
BENCH_MESSAGES=5000 BENCH_WARMUP=500 \
BENCH_RUNTIMES=native,swoole \
BENCH_SCENARIOS=produce-fnf,produce-sync,consume \
BENCH_CONCURRENCIES=1,16 \
EXT_PATH="$PWD/target/release/libhi_kafka.so" \
./benchmarks/run.sh
```

macOS 使用 `.dylib`。`SWOOLE_EXT_PATH`、`SWOW_EXT_PATH` 可显式指定；不存在的可选 runtime 会被跳过。

完整矩阵示例：

```bash
BENCH_MESSAGES=100000 \
BENCH_WARMUP=10000 \
BENCH_MESSAGE_SIZES=1024,16384,65536 \
BENCH_CONCURRENCIES=1,16,64,256 \
BENCH_RUNTIMES=native,swoole,swow \
BENCH_RESULT_FILE=benchmarks/results/candidate.jsonl \
./benchmarks/run.sh
```

可通过 `HI_KAFKA_BENCH_CLUSTER_CONFIG` 传入 JSON 覆盖 cluster 配置。基线和候选必须使用相同 broker、payload、partition、compression、acks、concurrency 与硬件。

## 回归门禁

```bash
php benchmarks/compare.php \
  --baseline=benchmarks/results/baseline.jsonl \
  --candidate=benchmarks/results/candidate.jsonl \
  --max-regression=5
```

任一匹配场景 throughput 下降超过 5%，或 operation P99 上升超过 5%，比较器返回非零。结果键包含 runtime、scenario、message bytes、concurrency、measured/warm-up 数量与 cluster config hash，避免不同 workload 被误配为可比较结果。

JSONL 同时记录版本、OS、cluster config、吞吐、payload 带宽、延迟、PHP 分配峰值，以及 PHP/worker 的 RSS/CPU 采样。结果文件默认写入 `benchmarks/results/`，该目录不提交。
