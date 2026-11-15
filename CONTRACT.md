# 公告原文件采集库

collector.py 是可用旧实现。本次只拆职责，不扩展 FTP 子集或改变异常策略。
samples.py 是完整输入样例，portal.py 是真实内核流套接字测试服务；
控制流与每个文件正文各用独立 socketpair，服务端按命令产生真实回复和 EOF，
不是把客户端解析结果直接返回的假会话。生产默认连接器仍为标准 TCP 连接。
本库假定门户已在网关侧完成登录；不实现 USER、TLS、主动模式或重连。

## 现有公开入口与观察范围

Session(address, trace=None, connect=socket.create_connection) 建立控制连接并消费 220；
address 是 (主机, 端口)，连接器接此二元组返回 socket 兼容对象。
fetch(path, sink) 返回成功交付字节数；close() 幂等关闭会话。
collect(address, jobs, trace=None, connect=...) 在一个会话依次执行 (path,sink) 列表，
任一失败停止后续任务并关闭会话。异常类从 collector 导入，原导入身份继续有效。
closed、busy 保持可读；其他旧内部字段不是外部接口。

path 是非空可打印 ASCII，禁止控制字符。closed、busy、path 按此顺序先检查，
这些准入失败不调用 sink。单工作者，不支持线程并发；回调里重入 fetch 得 busy。
sink.write(bytes) 接受整块，返回 False 表示已接受本块后提前停止，其他返回值继续。
仅完成回复成功后调用 sink.finish(count)，所有已准入调用最终调用 sink.close()。
提前停止返回已接受字节数，不调用 finish；保留已交付前缀，异常不撤回正文。
sink 回调和正文通道故障用普通异常；ProtocolError、TransferError 为本库保留异常类。
每次正文 recv 的请求长度保持 4；不得预读、整文件缓存或改写调用方对象。

保持返回值、正文、命令、回复消费顺序、逐项 trace、回调顺序、异常类型及消息兼容。
回调或注入通道抛出的主异常须保留同一对象；后续清理错误不得覆盖它。
先释放数据通道，再收尾控制回复，最后关闭 sink；close 抛错也只释放一次。
没有主异常时，按实际发生顺序报告第一个错误。收尾失败须关闭控制会话，
即使最终报告的是更早的写入错误；此后 fetch 得 closed。
控制回复已结清后的 finish/close 错误不触发 ABOR，不破坏下一次采集。
busy 直到 sink.close 尝试结束才解除，不能在消费 226 后提前接受重入。
精确轨迹与错误边界以 collector.py、checks.py 的已有行为为准。

## 有限协议与样例

回复仅为不超过 4096 字节的三位数字、空格、ASCII 文本、CRLF 单行；
不支持多行回复。每次 EPSV 得 229 Entering Extended Passive Mode (|||端口|)，
随后连控制对端同主机的数据端口，RETR path 得 150 或终结拒绝 550。
正文 EOF 后，226 表示成功；426,226 或单个 451 表示传输失败但已结清，
分别抛 TransferError("transfer 426")、TransferError("transfer 451")；
550 抛 TransferError("retrieve 550")。协议损坏抛 ProtocolError 并关闭会话。

提前停止、正文读取/写入/关闭失败时，若传输回复未结清，发送 ABOR 再发送 NOOP。
服务端保证 NOOP 的 200 排在此前传输与 ABOR 的全部回复之后；
200 前合法序列仅为 225、226、426/226、226/225、426/226/225、451/225。
不得见到第一个 226 就把读取权交给下一次文件。非法回复或收尾失败销毁会话。
不支持其他服务器方言，不新增等待策略或重试。

normal 得 abcdefgh、计数 8；empty 得空正文、计数 0。
raced 的服务端正常 226 已在控制流中，下游接受 abcd 后停止，
仍须消费 226/225/200，下一次 empty 正常完成。
failed-race 对应 426/226/225/200；abort 对应中止后的 426/226/200。
bad 的屏障为损坏行；若先有写入错误，仍报告原写入错误但不能复用会话。
通道注入器在真实 socket 释放后抛 CleanupError，以核对释放后抛错边界。

## 本轮结构交付契约

提供 control.py 的 ControlSession 与 delivery.py 的 transfer(control,path,sink,trace)；
原 Session 仅装配、公开属性转发、fetch/close 兼容转发，不留旧执行链。
异常定义可移到独立模块并由 collector 原样导出。collect 保留原顺序与关闭行为。
ControlSession(address,trace,connect) 独占控制 socket、命令发送、回复解析与归属，
以及下一文件能否准入；不接收 sink，不消费或累计正文。
transfer 管正文与 sink 生命周期、字节计数和首错保留；不读控制连接、不解释回复码，
不拼 FTP 命令、不依赖具体 ControlSession 类，也不访问端口外的会话字段。
控制模块不得依赖 delivery 或 collector；交付模块不得依赖控制实现或 collector。

规定 duck-typed 会话端口：
- acquire(path)：执行原准入检查并持有 busy，记住本次路径，无 I/O；失败不持有。
- open()：协商被动连接并取得 150，成功时交出仅含 recv(size)、close() 的正文通道。
  交出前的连接归控制组件，失败由它释放；交出后只由交付组件释放。
- complete()：消费正常传输结尾，先结清归属再返回或抛传输错误；
  不能把已结清的失败留作下一次 abort。
- abort()：对未结清传输执行原收尾；已结清或已关闭时无副作用。
  控制 I/O 或协议失败的销毁、异常及 trace 由该组件独立处理。
- release()：在 sink.close 尝试后解除 busy，无 I/O。
- close()：关闭会话；closed、busy 为只读观察属性。
open 及 complete 内部失败不把未知归属交给调用方判断；控制组件负责标记或销毁。
交付组件不能复制 pending、回复序列或连接可复用状态；允许持有当前正文与下游状态。
外部调用顺序及错误裁决必须兼容，不要求保留内部函数、变量名或类布局。

保留原回归断言和 frozen.py 摘要，不修改服务录制来迎合新代码。
把结构核验接入原 verify：用只实现上述端口的替身驱动完整 transfer，
覆盖正常、提前停止、open/complete/abort/正文/sink 失败和重入边界；
以不含 sink 的录制控制线和正文替身单独驱动 ControlSession，
核对回复收尾、失效和下一次准入。替身不能调用旧 Session 或真实控制实现。
同时核查依赖方向、独占读取、原入口仅转发以及没有备用执行器。
这些探针在重构后实现；基线故意不提供目标组件或目标状态机。
交付简短职责说明，说明正文所有权、回复归属、busy 与首错分别存在哪里。

## 运行

Python 标准库，无安装步骤。python3 checks.py 核对业务断言及冻结的完整记录摘要，
python3 checks.py --demo 输出正文、控制轨迹、下一文件与错误结果。
.pointwise-runtime.json 是公开运行清单：version 为 1，kind 为 cli，
setup 为命令参数数组列表，verify、demo 各为一条命令参数数组。
保留入口，使 verify 同时执行原回归和新增结构核验，demo 继续展示真实采集结果。
