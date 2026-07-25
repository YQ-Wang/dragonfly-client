# Dragonfly RDMA P2P 当前实现

## 概述

当前实现使用：

- **TCP 控制面**：能力发现、连接协商、元数据、窗口同步和错误通知；
- **libfabric 数据面**：通过 `FI_EP_RDM` 的双边 Tagged Messaging 传输 piece 内容；
- **流式接收窗口**：限制注册内存和在途操作，并将接收、校验和写盘重叠执行；
- **可选 mmap 上传路径**：从 content store 的文件映射填充注册发送窗口；
- **TCP fallback**：RDMA 不可用或传输失败时重新通过 TCP 下载。

RDMA 默认关闭，是 TCP piece transport 之上的可选优化层。

## 架构

```text
下载节点                                           Parent 节点
   │                                                   │
   │── TCP Discover ──────────────────────────────────>│
   │<─ provider / fabricTag / RDMA port ──────────────│
   │                                                   │
   │── TCP Request(task, piece, endpoint, tag) ───────>│
   │<─ Ready(offset, length, digest, endpoint) ───────│
   │                                                   │
   │  post 一批 fi_trecv                              │
   │── RecvPosted(window) ────────────────────────────>│
   │<══════ EFA fi_tsend / fi_trecv，按 tag 匹配 ═════│
   │  将完成的窗口交给存储层写盘                       │
   │  同时准备后续接收窗口                             │
   │                                                   │
   │<─ Done ──────────────────────────────────────────│
   │  完成 digest 校验                                 │
```

## 传输模型

### 双边 Tagged Messaging

当前实现不是 one-sided RDMA Read/Write：

- Parent 调用 `fi_tsend`；
- 下载节点提前调用 `fi_trecv`；
- chunk `i` 使用 `base_tag + i`；
- 两端都参与数据传输；
- 不向远端暴露内存地址或 rkey。

EFA 和 RoCE/InfiniBand 都通过 libfabric 抽象接入：

- AWS EFA：`efa` provider；
- RoCE/InfiniBand：`verbs;ofi_rxm`；
- 软件 provider 仅用于开发和测试。

### Piece、chunk 和 window

Dragonfly 的传输单位是 piece，每个 piece 再拆成多个 chunk。

默认配置：

- `chunkSize`: 4 MiB；
- `maxInflightChunks`: 16；
- 单个窗口大小：`4 MiB × 16 = 64 MiB`；
- 每个 piece 最多 4096 个 chunk。

每次只允许一个连续窗口的 send/receive operation 在途。客户端下载节点必须先 post
receive，再通过 TCP 发送 `RecvPosted`，Parent 才能发送对应窗口。这避免依赖 EFA
容量有限的 unexpected-message queue。

## 控制面

TCP 控制面负责以下工作：

1. 查询 Parent 当前是否已启用 RDMA；
2. 获取 RDMA rendezvous port；
3. 比较双方的 libfabric provider；
4. 比较双方的 `fabricTag`；
5. 交换 provider opaque endpoint；
6. 协商 chunk 大小和窗口大小；
7. 发送 `Ready`、`RecvPosted`、`Done` 和 `Error`。

只有双方 provider 相同，并且非空 `fabricTag` 完全相同时，才会尝试 RDMA。

## Parent 发送路径

Parent 使用注册内存 staging ring：

1. 从 storage 获取 piece metadata；
2. 计算协商后的 chunk 和窗口大小；
3. 申请一个或两个注册发送窗口；
4. 填充第一个窗口；
5. 等待客户端发送 `RecvPosted`；
6. 对窗口中的每个 chunk 调用 `fi_tsend`；
7. 双窗口模式下，在发送当前窗口时填充下一窗口；
8. 所有 send completion 完成后发送 `Done`。

当注册内存预算足够时使用双窗口；预算只允许一个窗口时，安全退化为顺序复用。

## mmap 上传路径

配置：

```yaml
storage:
  server:
    rdma:
      mmapContent: true
```

启用后，Parent 会优先：

1. 将 content store 中已完成的 piece 映射到进程地址空间；
2. 将 mmap 中的窗口复制到注册发送 ring；
3. 从注册 ring 调用 `fi_tsend`。

如果 mmap 失败，或者 piece 位于内存 cache 中，则回退到原来的 `AsyncRead` 路径。

当前 mmap 路径仍然不是完全零拷贝：

```text
content file pages → mmap → memcpy → registered staging ring → EFA
```

它移除了 `File → BufReader → staging ring` 的中间读取路径，但尚未将 mmap 页面直接注册给
libfabric。直接注册需要进一步解决：

- content 文件和 mapping 生命周期；
- GC/eviction 与在途 send 的同步；
- registration cache；
- memlock 和注册内存预算；
- provider 对 file-backed mapping 的支持。

`mmapContent` 默认值为 `false`。

## 下载端流式接收

旧实现为整个 piece 申请注册内存，完整接收后才开始写盘。对于约 10 GiB 的模型 shard，
单个传输可能需要约 10 GiB 注册内存。

当前实现改为窗口化流式接收：

1. 只申请当前接收窗口；
2. 为窗口内所有 chunk post `fi_trecv`；
3. 通知 Parent `RecvPosted`；
4. 等待窗口的 receive completion；
5. 将完成的注册 buffer 交给 `RDMAStreamReader`；
6. storage 从 reader 读取、计算 CRC 并写盘；
7. 消费完的注册 buffer 返回 buffer pool；
8. 后台继续接收后续窗口。

由此可以重叠：

```text
Parent storage read / mmap copy
    ∥ Parent EFA send
    ∥ Client EFA receive
    ∥ Client CRC
    ∥ Client storage write
```

在 4 MiB chunk、16 chunk window 配置下，一个窗口为 64 MiB。根据消费者速度和 channel
深度，一个传输通常持有约 64–192 MiB 注册内存，而不再与 piece 大小线性增长。

## 完整性校验

Parent 在 `Ready` 中返回：

- piece offset；
- piece length；
- piece digest；
- 协商后的 chunk/window 参数。

客户端下载节点将 RDMA reader 交给现有 storage write path。storage 在写入时增量计算
CRC，并在完整 piece 写完后比较 Parent 提供的 digest。只有长度和 digest 都正确时，
piece metadata 才会标记为完成。

测试 harness 还会对模型文件执行 SHA-256 验证。

## TCP fallback

RDMA 是可选优化，TCP 始终保留。

### RDMA 建立前失败

以下错误会立即改用 TCP：

- RDMA fabric 初始化失败；
- Parent 没有发布 RDMA capability；
- provider 或 `fabricTag` 不兼容；
- rendezvous 连接失败；
- endpoint 无法解析；
- 参数、大小或注册内存检查失败。

### 流式传输期间失败

如果已经开始流式写入，但发生以下错误：

- fabric receive/send 失败；
- operation timeout；
- Parent 返回控制面错误；
- 短读或长度不匹配；
- target storage 写入失败；
- digest mismatch；

客户端会：

1. 删除失败的 piece metadata；
2. 从相同 piece offset 重新开始；
3. 通过 TCP 下载完整 piece；
4. 重新执行长度和 digest 校验。

regular、persistent 和 persistent-cache 三种 piece namespace 使用相同策略。

## 资源边界和安全性

当前实现包含以下边界：

- `maxRegisteredBytes`：限制活跃和缓存的注册内存；
- `maxInflightChunks`：限制单个 piece 的在途 operation；
- `maxConcurrentTransfers`：限制 RDMA rendezvous 并发数；
- 每个传输分配互不重叠的 4096-tag block；
- operation、rendezvous 和完整 piece 都有 timeout；
- malformed frame、乱序窗口和超限参数会 fail closed；
- endpoint 失败后会被 retire，后续请求重新创建 fabric；
- buffer 只有在所有 completion 被回收后才能复用。

## 主要配置

```yaml
download:
  protocol: rdma

storage:
  server:
    rdma:
      enable: true
      port: 4007
      provider: efa
      device: rdmap16s27-rdm
      fabricTag: vpc-and-availability-zone
      maxRegisteredBytes: 512MiB
      chunkSize: 4MiB
      maxInflightChunks: 16
      maxConcurrentTransfers: 64
      transferTimeout: 10s
      mmapContent: false
```

其中：

- `enable` 只控制本节点是否作为 RDMA Parent 提供上传；
- `download.protocol: rdma` 控制下载时是否优先尝试 RDMA；
- TCP piece server 仍用于 discovery 和 fallback；
- `allowSoftwareProvider` 只应在测试环境启用。

## 当前性能结果

最新的硬件测量在 RoCE 上完成，记录于 `RDMA-ONPREM-VALIDATION.md`，harness 与驱动脚本位于
`scripts/rdma-bench/`。引用任何 RDMA 吞吐数字之前请先读该文档。

在两个 RoCE 节点之间传输 26,034,233,427 字节的 Llama-2-13B 文件布局，内容盘为内存文件系统：

| 路径 | 有效并发 | 完整任务 goodput |
|---|---:|---:|
| 纯传输（不校验、不落盘） | 3 | 147–158 Gbps |
| CRC32 + memfs（等价于 dfdaemon 的工作量） | 3 | 51–54 Gbps |
| SHA-256 全量校验 + memfs | 3 | 37.7–38.1 Gbps |

所有文件 SHA-256 匹配，且 `fabric_failed=false`。fabric 只占约 4 秒传输中的 18–38 ms，瓶颈完全在
接收端 CPU：内存复制与校验，而不是线速。

下面这组早期 EFA 数字仅作历史记录，**不应再被引用**：

| 路径 | 并发 | 完整任务 goodput |
|---|---:|---:|
| 原始 whole-piece RDMA | 3 | 2.25 Gbps |
| 流式 receiver RDMA | 3 | 2.18 Gbps |
| mmap sender + streaming digest | 3 | 2.72 Gbps |
| TCP tar baseline | 1 | 约 2.4 Gbps |

它们是磁盘落盘的测量，且取自接收路径重写之前，所以"RDMA 只比 TCP 快一点"这个结论并不成立：同一
工作负载在内存文件系统上已达到 51–54 Gbps。另外 `TCP tar baseline` 也不是 dfdaemon TCP piece
路径的对照，因此这张表无法回答"RDMA 比 TCP 快多少"。要重新引用 EFA 结果，需要用当前 harness 重测。

## 尚未实现

- batch post 和 reusable context slab；
- completion 的窗口级批量等待；
- multi-rail 和 piece-level rail 分配；
- NUMA-local buffer allocation 和 progress thread 绑定；
- mmap 页面直接注册（已评估并暂缓：在同时跑训练的节点上会 pin page cache 并占用与 NCCL 共享的
  NIC memory region，NVMe 上还会读入即将被整块覆盖的页；见 `RDMA-ONPREM-VALIDATION.md`）；
- rendezvous 连接复用；
- RDMA-aware parent scheduling 和跨 Parent retry；
- one-sided RMA；
- `FI_HMEM` / GPU Direct。

后续优化应优先测量完整任务的 CPU/GiB、存储带宽、内存带宽和 fallback rate，而不能只依据
`fi_pingpong` 的理论带宽判断收益。
