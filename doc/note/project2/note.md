- 可以看看 etcd 的raft 部分，里面注释比较多
- 群里建议2a面向测试编程
- 2b就不好调试了
- 没有达成共识的log不需要持久化，但是要确保达成共识的log再commit前持久化（这其实是一种参考做法而已，我也可以已收到就持久化）
- 使用 badger 来实现 raft 日志数据 的持久化
# 2a
- 在 `raft/` 目录下完成
- raft 内部并不使用 timer 这些定时器，所有的选举和心跳都是通过 逻辑时钟tick 来驱动。应用层通过调用 RawNode.Tick() 来驱动该逻辑时钟
- 发送和接收都是异步的，比如，不会阻塞在任何请求的响应
- 应该粗劣看下 `proto/proto/eraftpb.proto.`，一些相应的发送和接收的结构定义在这
- 整个实现分为三步
    - Leader election
    - Log replication
    - Raw node interface：向应用层提供的接口
## 实现
- `raft/raft.go` 中的 `raft.Raft` 是核心，包括消息处理和逻辑时钟驱动
- 参考 `raft/doc.go` ，从而了解设计概要以及 `MessageTypes`的类型



### leader 选举
- `raft.Raft.tick()` 驱动选举超时或者心跳超时
- 现在不需要关心，消息的发送和接收。发送只需push到`raft.Raft.msgs`，接收只需从`raft.Raft.Step()`获取。
- 测试代码将从 `raft.Raft.msgs` 获取消息，然后将响应传给 `raft.Raft.Step()`
- `raft.Raft.Step()`是消息处理的入口，应该处理`MsgRequestVote, MsgHeartbeat` 的响应

### 日志复制
- 先处理 MsgAppend 和 MsgAppendResponse。发送方和接收方的处理
- 完成 raft.RaftLog in raft/log.go ，用来做一些持久化，比如 log entries 和 snapshot


### Raw node interface

- `raft.RawNode` in raft/rawnode.go 为向上层(应用层)提供的接口
- `Ready` 用来与应用层交互，通过 `RawNode.Ready()` 返回给上层应用，处理完后通过`RawNode.Advance()` 跟新 raft.Raft 的内部状态
  - 发送消息到对端
  - 持久化 log entries
  - 保存硬状态（index term）
  - allpy 已提交的log
### hints
- 添加任何需要用到的 `eraftpb.proto` 上的状态，到 raft.Raft, raft.RaftLog, raft.RawNode
- term init 0
- dump entry new leader
- commit ==> vroadcast the commit index
- 本地消息，没有设置term。MessageType_MsgHup, MessageType_MsgBeat and MessageType_MsgPropose
- append log 的时候 leader 和 follower 的处理机制非常不同
- 一些再rawnode.go的包装函数可以被通过raft.Step实现
- 启动一个新的raft时，从Storage获取最后一个stabled的状态来初始化raft.Raft和raft.RaftLog