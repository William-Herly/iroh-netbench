# iroh-netbench

[English](README.md) | 中文

一个用于测试和监控 [iroh](https://github.com/n0-computer/iroh) QUIC 连接的轻量 Rust
组件。

`iroh-netbench` 复用宿主应用已有的 `Endpoint`。宿主在 Router 中挂载测速协议，只有用户发起测速时才会建立一条独立的 NetBench Connection。

> 当前状态：可用 MVP，线协议 v1。API 仍在调整，暂未发布到 crates.io。

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

项目发布前可使用本地路径或 Git 依赖：

```toml
[dependencies]
iroh = "1.0.3"
iroh-netbench = { path = "../iroh-netbench/crates/iroh-netbench" }
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
A：NetBenchClient
B：NetBenchProtocol
```

A 不需要为了本次测速挂载协议；B 必须挂载。如果 B 也需要主动测速 A，则 A、B 两端
都应挂载 `NetBenchProtocol`，并在发起测速时各自创建 `NetBenchClient`。

## 运行测速

```rust,ignore
use iroh_netbench::{NetBenchClient, NetBenchConfig};

let report = NetBenchClient::new(endpoint.clone())
    .run(peer_addr, NetBenchConfig::standard())
    .await?;

println!("{}", serde_json::to_string_pretty(&report)?);
```

需要实时进度时：

```rust,ignore
let client = NetBenchClient::new(endpoint.clone());
let mut test = client.start(peer_addr, NetBenchConfig::quick()).await?;

while let Some(event) = test.next().await {
    update_ui(event);
}

let report = test.result().await?;
```

`NetBenchClient` 复用宿主 Endpoint，但会创建一条独立的 `/iroh/netbench/1`
Connection。该连接包含控制 Stream、并行上传/下载 Stream 和 Datagram Probe。

### 有界时长与取消

吞吐配置时长是固定的计量窗口。到达截止时间后，发送端会 reset 数据 Stream，接收端会 stop
尚未读取的数据；QUIC 队列中的积压不会再通过 graceful drain 混入报告时长。
`ThroughputReport.measurement_duration` 因而保持为配置窗口，速率只统计窗口内实际收到的
payload。

`NetBenchConfig::quick()` 的整轮硬上限为 45 秒，`standard()` 为 75 秒。
`overall_timeout` 包含建联、路径稳定、各测量阶段和有界收尾；超时会返回带当前阶段名的
`Error::Timeout { stage }`，并关闭专用测速 Connection。接收端也会把请求时长用于约束实际
吞吐阶段，`max_test_duration` 不再只是请求参数校验。

完整测速任务可以显式取消并等待退出：

```rust,ignore
let test = client.start(peer_addr, NetBenchConfig::quick()).await?;

// 用户取消或外层操作结束时调用。
test.cancel();
test.abort_and_wait().await?;
```

直接丢弃 `NetBenchTest` 或仍在等待的 `run()` future，也会关闭测速 Connection 并终止内部
任务。如果调用方必须在继续前证明任务已经退出，应使用 `abort_and_wait()`。

### 低带宽探测

延迟、抖动和 Datagram 超时测量可以独立运行，不会打开吞吐 Stream：

```rust,ignore
use iroh_netbench::NetBenchProbeConfig;

let report = NetBenchClient::new(endpoint.clone())
    .run_probes(peer_addr, NetBenchProbeConfig::quick())
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

为保持兼容，默认值是 `Allow`。设置为 `Deny` 后，连通性检查和 `run_probes` 仍可运行；Quick、
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

let client = NetBenchClient::new(endpoint.clone())
    .with_monitor(monitor.clone());
```

monitor 不持有连接生命周期，不发送心跳，也不会主动产生测速流量。

## 指标口径

被动监控与主动测速使用不同的数据来源：

| 数据 | 含义 |
|---|---|
| `SessionMetrics` / `ConnectionMetrics` 速率 | UDP wire-level 字节差值，包含 QUIC 开销 |
| `ConnectionMetrics.rtt` | 当前路径的 QUIC 传输层 RTT |
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
```

## 许可证

可任选 `MIT` 或 `Apache-2.0`。
