# iroh-netbench

[English](README.md) | 中文

[![crates.io](https://img.shields.io/crates/v/iroh-netbench.svg)](https://crates.io/crates/iroh-netbench)
[![docs.rs](https://img.shields.io/docsrs/iroh-netbench)](https://docs.rs/iroh-netbench)

一个用于测试和监控 [iroh](https://github.com/n0-computer/iroh) QUIC 连接的轻量 Rust
组件。

`iroh-netbench` 只在宿主已经建立好的 iroh `Connection` 内运行测速业务流。它不持有
`Endpoint`、不解析对端地址、不拨号、不选择建联策略，也不关闭整条 Connection。

> 当前维护分支使用线协议 v1、报告 schema v1。可复用库已经通过 Direct、Relay 和受损
> 链路验收矩阵；在 crate 达到 `1.0` 前，Rust API 仍可能调整。

## 发布范围

可构建和发布范围只包含 `iroh-netbench` 库 crate，不提供 demo 应用产物。

当前源码仍保留独立 `NETBENCH_ALPN` responder 入口；Velinka 的目标集成会把 responder 也改成
宿主 PeerSession 内的 business-flow dispatcher，并直接删除该入口，不提供兼容层。完成前以
Velinka 的实施计划为准，不能把独立 ALPN 当成最终 1.0 API。

库本身不创建或持有 Relay、Discovery、PKARR 或证书基础设施。宿主应用负责提供已配置的
`Endpoint`，并决定哪些对端地址和路径允许进行完整吞吐测速。

## 测试内容

| 指标 | 测量方式 |
|---|---|
| 延迟与抖动 | QUIC Datagram 请求/响应 RTT |
| 探测超时 | Datagram 超时、重复与乱序 |
| 下载速率 | 客户端实际收到的 payload 字节 |
| 上传速率 | 服务端实际收到并确认的 payload 字节 |
| 负载延迟 | 上传和下载期间的 Datagram RTT |
| QUIC 指标 | 路径、丢包、拥塞、MTU 和 UDP 计数 |
| 实时监控 | 单 Connection 和会话聚合的速率、RTT 与丢包 |

## 添加依赖

从 [crates.io](https://crates.io/crates/iroh-netbench) 添加正式发布的 crate；API 文档见
[docs.rs](https://docs.rs/iroh-netbench)：

```toml
[dependencies]
iroh = "1.0.3"
iroh-netbench = "0.1.0"
serde_json = "1.0"
```

Endpoint 的创建、地址发现和 Relay 策略仍由宿主应用负责。

## 挂载测速协议

`NetBenchProtocol` 用于接受对端发起的测速。被测速端必须将它与宿主自己的协议挂载
到同一个 Router：

```rust,ignore
use std::time::Duration;

use iroh::protocol::Router;
use iroh_netbench::{NETBENCH_ALPN, NetBenchProtocol};

let protocol = NetBenchProtocol::builder()
    .max_concurrent_tests(2)
    .max_test_duration(Duration::from_secs(30))
    .max_parallel_streams(8)
    .build();

let router = Router::builder(endpoint.clone())
    .accept(MY_APPLICATION_ALPN, my_protocol)
    .accept(NETBENCH_ALPN, protocol)
    .spawn();
```

例如 A 发起测速 B：

```text
A：iroh 建联 + NetBenchInitiator
B：NetBenchProtocol
```

A 不需要为了本次测速挂载协议；B 必须挂载。如果 B 也需要主动测速 A，则 A、B 两端
都应挂载 `NetBenchProtocol`，并在发起测速时先由宿主建立 Connection，再创建
`NetBenchInitiator`。

## 运行测速

```rust,ignore
use iroh_netbench::{NETBENCH_ALPN, NetBenchConfig, NetBenchInitiator};

let connection = endpoint.connect(peer_addr, NETBENCH_ALPN).await?;
let report = NetBenchInitiator::new()
    .run(connection.clone(), NetBenchConfig::standard())
    .await?;

println!("{}", serde_json::to_string_pretty(&report)?);
```

需要实时进度时：

```rust,ignore
let client = NetBenchInitiator::new();
let mut test = client.start(connection.clone(), NetBenchConfig::quick()).await?;

while let Some(event) = test.next().await {
    update_ui(event);
}

let report = test.result().await?;
```

同一个 Connection 内包含 netbench 控制 Stream、并行上传/下载 Stream 和 Datagram Probe。
`report.path` 只描述业务流测量窗口内的初始路径、最终路径和迁移；QUIC 建联耗时及候选信息应由
创建 Connection 的宿主记录。

### 有界时长与取消

吞吐配置时长是固定的计量窗口。到达截止时间后，发送端会 reset 数据 Stream，接收端会 stop
尚未读取的数据；QUIC 队列中的积压不会再通过 graceful drain 混入报告时长。
`ThroughputReport.measurement_duration` 因而保持为配置窗口，速率只统计窗口内实际收到的
payload。

两端都会让双向控制 Stream 使用高于吞吐 Stream 的 QUIC 调度优先级，避免 `TestReady`、进度、
错误和 `TestFinished` 与占满带宽的 payload 处于相同优先级。每个阶段的 cleanup 仍有五秒硬
上限；这段时间只用于控制消息送达，不会计入测速窗口。

`NetBenchConfig::quick()` 的整轮硬上限为 45 秒，`standard()` 为 75 秒。
`overall_timeout` 包含协议协商、路径稳定、各测量阶段和有界收尾；超时会返回带当前阶段名的
`Error::Timeout { stage }`，但不会关闭调用方持有的 Connection。接收端也会把请求时长用于约束实际
吞吐阶段，`max_test_duration` 不再只是请求参数校验。

完整测速任务可以显式取消并等待退出：

```rust,ignore
let test = client.start(connection.clone(), NetBenchConfig::quick()).await?;

// 用户取消或外层操作结束时调用。
test.cancel();
test.abort_and_wait().await?;
```

直接丢弃 `NetBenchTest` 或仍在等待的 `run()` future 会终止 netbench 内部任务和 Stream，整条
Connection 的后续处置仍由宿主决定。如果调用方必须在继续前证明任务已经退出，应使用
`abort_and_wait()`。

### 低带宽探测

延迟、抖动和 Datagram 超时测量可以独立运行，不会打开吞吐 Stream：

```rust,ignore
use iroh_netbench::NetBenchProbeConfig;

let report = NetBenchInitiator::new()
    .run_probes(connection.clone(), NetBenchProbeConfig::quick())
    .await?;
```

结果类型为 `NetBenchProbeReport`，包含连接、延迟、丢包和传输统计，不包含上传或下载结果。
Probe 有独立的 `overall_timeout`（Quick 15 秒、Standard 25 秒），其成功或失败与完整吞吐
测速分别记录。

### 接收端吞吐策略

接收端可以允许或拒绝占用大量带宽的上传和下载测量：

```rust,ignore
use iroh_netbench::{NetBenchProtocol, ThroughputPolicy};

let protocol = NetBenchProtocol::builder()
    .throughput_policy(ThroughputPolicy::Deny)
    .build();

// 保留一个 clone 后，可以在不重启 Endpoint 的情况下更新策略。
protocol.set_throughput_policy(ThroughputPolicy::Allow);
```

独立协议的明确默认值是 `Allow`。设置为 `Deny` 后，连通性检查和 `run_probes` 仍可运行；Quick、
Standard 以及其他完整吞吐测试会在任何测量开始前返回
`Error::ThroughputDeniedByPeer`，客户端不会静默降级测试内容。

该能力复用协议 v1 现有的 `ServerLimits` 字段，不更新线协议版本。固定吞吐截止和取消也保持
`PROTOCOL_VERSION = 1`，没有增加控制消息类型。服务端在收到吞吐阶段请求时也会再次强制检查
策略。

以 A 发起测速 B 为例，控制角色在整轮测试中保持不变：

| 项目 | 数据方向 | 结果统计 |
|---|---|---|
| 延迟 | A 发 Probe，B 回显 | A 计算 RTT |
| 下载 | B → A | A 按收到的 payload 统计 |
| 上传 | A → B | B 按收到的 payload 统计并确认 |

### Connection 路径选择

路径候选、Direct/Relay 约束和建联结果完全由宿主负责。netbench 不接受 `EndpointAddr` 或
`DialPolicy`。测速完成后可读取 `report.path.initial_path`、`final_path` 和 `path_changed`，了解
业务流窗口内是否发生路径迁移；即使最终路径回到初始路径，中间发生过切换时
`path_changed` 仍为 `true`。只有业务流从非 Direct 路径进入、随后在稳定或测量阶段首次转为
Direct 时，`became_direct` 才为 `true`，`time_to_direct` 记录该时刻。它们不能代替宿主记录的
建联候选和握手结果。

对于尚未确认或对带宽敏感的路径，建议默认使用低带宽 Probe。完整吞吐测速应由用户明确
触发，并服从被测端的 `ThroughputPolicy`。

`0.1.0` 验收矩阵已经通过私有 Relay-only Quick、Direct/Relay Quick、对端拒绝、拒绝时的
Probe-only、实时事件和主动取消。Linux netns 在 `12 Mbit/s + 50±10 ms` 下通过 `0%`、
`0.5%` 和 `2%` 丢包测试；下载与上传的五秒计量窗口保持在 ±250 ms 内，Quick 整轮小于
45 秒，结束或取消后不再继续占用大量带宽，业务 PeerSession 保持可用。

## 监控已有连接

`SessionMonitor` 被动读取 iroh 的传输统计。建议为每个逻辑对端创建一个 monitor，并
注册属于该会话的 Connection：

```rust,ignore
use iroh_netbench::{SessionMonitor, SessionMonitorConfig};

let monitor = SessionMonitor::spawn(SessionMonitorConfig::default());
let connection_id = monitor.track("application", connection.clone());
let mut updates = monitor.subscribe();

tokio::spawn(async move {
    while updates.changed().await.is_ok() {
        let session = updates.borrow_and_update();
        println!(
            "下载={:.1} Mbps 上传={:.1} Mbps 丢包={:.2}%",
            session.download_bps / 1_000_000.0,
            session.upload_bps / 1_000_000.0,
            session.recent_loss_ratio * 100.0,
        );

        if let Some(connection) = session.connection(connection_id) {
            println!(
                "路径={:?} RTT={:?} 丢包={:.2}%",
                connection.path,
                connection.rtt,
                connection.recent_loss_ratio * 100.0,
            );
        }
    }
});
```

将同一个 monitor 交给客户端和协议，可把测速流量计入会话总量：

```rust,ignore
let protocol = NetBenchProtocol::builder()
    .with_monitor(monitor.clone())
    .build();

let client = NetBenchInitiator::new()
    .with_monitor(monitor.clone());
```

monitor 不持有连接生命周期，不发送心跳，也不会主动产生测速流量。

## 指标口径

被动监控与主动测速使用不同的数据来源：

| 数据 | 含义 |
|---|---|
| `SessionMetrics` / `ConnectionMetrics` 速率 | UDP wire-level 字节差值，包含 QUIC 开销 |
| `ConnectionMetrics.rtt` | 当前路径的 QUIC 平滑传输 RTT；不是最新应用交互延迟 |
| `ConnectionMetrics.recent_loss_ratio` | QUIC 近期判定的丢包 |
| `NetBenchReport.download` | 客户端收到的应用 payload |
| `NetBenchReport.upload` | 服务端收到的应用 payload |
| `NetBenchProbeReport` | 低带宽连接、延迟、丢包和传输结果 |
| `LatencyReport` | 应用层 Datagram 往返样本 |
| `LossReport.timeout_ratio` | 探测包超时率，不代表绝对物理丢包率 |

会话速率是所有已注册 Connection 实时速率的总和。

## 测速预设

| 阶段 | Quick | Standard |
|---|---:|---:|
| 空闲延迟 | 2 秒 | 3 秒 |
| 超时探测 | 2 秒 | 5 秒 |
| 下载测试 | 5 秒 | 10 秒 |
| 上传测试 | 5 秒 | 10 秒 |
| 并行 Stream | 4 | 4 |

交互式检查使用 `NetBenchConfig::quick()`；需要更稳定的结果时使用
`NetBenchConfig::standard()`。

`NetBenchProbeConfig::quick()` 和 `NetBenchProbeConfig::standard()` 使用对应的延迟、丢包
参数，但不会运行预热、上传、下载或负载延迟阶段。

## 示例

启动一组仅使用直连的测试端：

```bash
cargo run --example netbench-server
cargo run --example netbench-client -- '<服务端输出的 EndpointAddr JSON>'
```

## 兼容性

- Rust 1.91+
- ALPN：`/iroh/netbench/1`
- 线协议：`1`
- 报告 schema：`1`

线协议版本只在网络报文或语义发生不兼容变更时递增；本地 Rust API、UI 和被动监控
调整不会推动该版本增长。

## 开发检查

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo doc -p iroh-netbench --no-deps
cargo package -p iroh-netbench --list
```

版本内容见 [CHANGELOG.md](CHANGELOG.md)。

## 许可证

可任选 `MIT` 或 `Apache-2.0`。
