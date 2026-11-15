# 职责说明

本轮只拆职责与依赖边界，协议子集、异常策略与可观察轨迹不变。

## 模块划分

- `errors.py`：`ProtocolError`、`TransferError` 定义；`collector` 原样再导出，导入身份不变。
- `control.py`：`ControlSession`，独占控制线。
- `delivery.py`：`transfer(control, path, sink, trace)`，只依赖会话端口。
- `collector.py`：`Session` 仅装配与兼容转发（`trace`/`closed`/`busy`/`fetch`/`close`），
  `collect` 保留原顺序与关闭行为；不留旧执行链，无备用执行器。

依赖方向：`collector → control/delivery/errors`，`control → errors`，`delivery` 无依赖。
控制模块不依赖交付或入口，交付模块不依赖控制实现或入口。

## 正文所有权

数据连接在 `open()` 交出前归控制组件：EPSV/连接/RETR 任一失败（含 550、协议错误）
由它关闭并记 `["data.close"]`。`open()` 取得 150 后交出仅含 `recv(size)`、`close()`
的正文通道，此后只由交付组件释放；每次 `recv` 请求长度恒为 4。控制组件从不
`recv` 正文、不累计字节；交付组件先释放数据通道再进入收尾，抛错路径也只释放一次。

## 回复归属

所有控制回复只由 `ControlSession` 消费与归属：

- `complete()` 结清正常结尾（226 / 426+226 / 451），先结清再返回或抛传输错误，
  已结清的失败不会留作下一次 abort。
- `abort()` 对未结清传输发 ABOR+NOOP，核销 225/226/426/451 交错序列直到 200；
  不见 200 不算结清，不能见第一个 226 就交接下一文件。已结清或已关闭时无副作用。
- 协议损坏或控制侧 I/O 失败由控制组件自行销毁（`["broken"]`），销毁裁决不交给
  交付组件；交付组件不复制 pending、回复序列或可复用状态。

## busy 与准入

`busy` 存在 `ControlSession`：`acquire(path)` 按 closed→busy→path 顺序检查并持有，
记住本次路径，无 I/O，失败不持有；`release()` 在 `sink.close` 尝试结束后解除，
同样无 I/O。回调内重入在 `acquire` 处得 `busy`，消费 226 后也不提前放行。

## 首错

首错（primary）存在 `delivery.transfer`：主异常对象原样保留并重新抛出；数据关闭、
`abort`、`sink.close` 的后续错误仅在无首错时补位，收尾错误不覆盖主异常。
无首错时按实际发生顺序报告第一个错误。
