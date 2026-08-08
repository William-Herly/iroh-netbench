# iroh-netbench

[English](README.md) | 中文

`iroh-netbench` 用于评估调用方已持有、已认证 PeerSession 上的应用可见网络质量。宿主负责选择
和认证物理连接、准入测速业务流，并分发其可靠 Stream 与不可靠 Datagram。这个库不会拨号、
注册 ALPN，也不会关闭宿主 Session。

它面向 iroh 及其他具备多路复用 Stream/Datagram 的 QUIC 类 Session，但不直接依赖 iroh，
因此不会强制应用跟随某个 iroh 版本。

## 安装

```toml
[dependencies]
iroh-netbench = "0.2"
```

最低 Rust 版本为 `1.91`。当前仍处于 1.0 前，线协议与 JSON 报告 schema 都保持初版 `1`。

## 使用已经准入的 Flow

```rust,ignore
use iroh_netbench::{
    NetBenchConfig, NetBenchFlow, NetBenchInitiator, NetBenchReport, NetBenchResponder, Result,
};

pub async fn run(flow: NetBenchFlow) -> Result<NetBenchReport> {
    NetBenchInitiator::new()
        .run(flow, NetBenchConfig::quick())
        .await
}

pub async fn serve(flow: NetBenchFlow, responder: &NetBenchResponder) -> Result<()> {
    responder.serve(flow).await
}
```

宿主只能在完成对端认证和业务流准入后构造 `NetBenchFlow`。完整边界可参考
[内存宿主示例](examples/in_memory_host.rs)和[接入说明](docs/integration.md)。

## 测量内容

| 模式 | 内容 |
| --- | --- |
| Probe | 应用层空闲 RTT、抖动、Datagram 超时/乱序、路径和传输增量 |
| Full | Probe 全部内容，加上下行应用载荷吞吐与负载延迟 |

Datagram 超时是应用层观测，不等同于底层网络丢包率；吞吐量是固定计时窗口内收到的应用载荷。
准确口径和限制见[测量语义](docs/measurements.md)。

## 宿主契约

宿主需要提供：

- 只属于一个已准入测速 Flow 的 `NetBenchSession`；
- 支持 flow-local finish/cancel 的可靠双向 Stream；
- 已按 Flow 分流的 Datagram 发送和接收；
- 不含秘密的对端标识与传输 telemetry 快照。

`accept_bi` 和 `read_datagram` 不能返回其他测速 Flow、Mesh、控制面或其他业务协议的流量。取消
和正常完成只释放本次测速资源，PeerSession 生命周期仍由宿主负责。

Responder 会限制并发数、持续时间、Stream 数量、块大小和运行时吞吐策略；它只回显当前活动
测试中合法且不重复的 Probe，并按声明的探测频率限制回复总量。应用必须在构造 Flow 前完成
认证与授权。

## 文档

- [宿主与 iroh 接入](docs/integration.md)
- [测量语义](docs/measurements.md)
- [版本记录](CHANGELOG.md)
- [API 文档](https://docs.rs/iroh-netbench)

许可证可任选 [MIT](LICENSE-MIT) 或 [Apache-2.0](LICENSE-APACHE)。
