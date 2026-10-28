# 消息类型合并与渠道分发调整

日期：2026-10-26。状态：已撤回。用户决定恢复原 eventType 版本，不执行本文方案。

## 目标与已确认方向

用户提出：EventType 和 MessageType 合并；在统一后的 EventType 中增加渠道码；删除 routingKey，按渠道码选择发布器。

本方案保留 EventType 名称。类型表示“是什么消息”，渠道表示“交给哪个渠道发布”。业务方只声明类型，由类型取得渠道，不再额外传 routingKey。

以下编号、报文布局、缺省行为和迁移方式是 AI 的建议，尚未获得用户确认。

## 当前代码与问题

- EventType 同时含 AI、IM 业务分类及 PING、ACK、READ 等具体消息；还决定报文在哪一段结束、是否发布。
- TypedEvent 同时提供 EventType 和 short messageType。订阅表分为事件处理与业务消息处理两套表。
- Protocol 的事件码占 1 字节；正文控制段中 messageType 占 2 字节。PING/PONG 当前只有 1 字节。
- 普通及 typed 发布策略共用 RoutingKeyPublisherMapping；当前实现还拼接事件名称，并在配置缺失时回退。
- WebSocketGateway 先按 publishable 执行业务发布，再执行事件处理以回复 ACK 等。类型合并不能导致一次业务消息被消费两次，也不能丢失回执。
- 当前工作区已有 9 个文件的未提交修改，包括上述发布策略、配置和 DefaultEventType。实施须基于这些修改继续，不回滚或覆盖。

## 1. 统一类型与渠道

保留 EventType，删除 MessageType 接口和 TypedEvent.getMessageType()。统一类型提供：

| 方法 | 含义 |
| --- | --- |
| getIdentity() | 唯一的消息类型码，0–65535 |
| getChannel() | 渠道码，0–255 |
| publishable() | 网关收到后是否调用发布策略 |
| getEndOfSegmentIndex() | 报文结束于类型头、内容、时间或用户段 |

类型码全局唯一。同一类型固定归属一个渠道；渠道不是类型的第二级编号空间。重复类型码即使渠道不同也拒绝登记，避免解码和订阅查找产生歧义。

建议渠道分配：SYSTEM=0、AI=1、IM=8。原 AI/IM 从消息类型迁为渠道；原 IM 下的业务类型（例如 101）直接成为统一类型。控制消息 PING/PONG/ACK/READ 等属于 SYSTEM，保留正数类型编号；ERROR 从旧的 -99 显式改为 99。

渠道仅表示业务分发目标，不包含 Topic、文件路径或发布器 Bean 名称。示例自定义渠道另分配编号，用于演示配置。

普通 Event 删除 getRoutingKey()，增加缺省返回 SYSTEM 的 getChannel()。TypedEvent 的 getChannel() 从 getEventType() 推导；Protocol 不维护一个可独立修改的渠道字段，避免类型和渠道不一致。

## 2. 二进制报文

建议固定头如下，整数采用大端：

```text
类型头：channel(1，无符号) + eventType(2，无符号)
内容段：device(1) + chatType(1) + payloadLength(4) + payload
时间段及用户会话段：沿用当前顺序和格式
```

结束段规则继续由统一 EventType 决定；不另外增加长度、版本或 routingKey 字段。

- 只含类型头的 PING/PONG 等：从 1 字节变为 3 字节。
- 包含内容段的消息：原 eventType(1) + messageType(2) 合计 3 字节，改后仍为 3 字节；仅位置和含义改变。
- 类型码用 Java int 表达 0–65535，编码使用两个字节；避免 short 的符号问题。渠道也用 int 表达无符号范围。
- 解码先检查固定头，查找类型，再校验报文中的渠道与类型声明一致。不允许通过修改报文字节把同一类型发往另一渠道。
- 拒绝未知类型、渠道不一致、超范围编号、截断和不符合结束段的多余字节。

增加可显式传入 Protocol 的类型注册表；默认构造器使用内置控制类型。业务启动时从明确声明的业务类型集合建立不可变注册表，并注入网关解码入口。编码、解码共用同一注册表；不得仅在订阅者里登记类型，因为只发布、不本地消费的类型也必须可解码。注册表校验重复编号、渠道范围、合法结束段，启动失败时明确提示。

## 3. 发布器选择

用 ChannelPublisherMapping 替换 RoutingKeyPublisherMapping。普通和 typed 发布策略都只按 event.getChannel() 选择目标。

建议配置改为两张渠道映射表，分别供普通和 typed 发布使用，避免同一渠道同时使用两种发布接口时互相覆盖：

```properties
sparrow.massaging.channel-publisher.mapping.plain={0:'defaultPublisher',16:'customPublisher',17:'filePublisher'}
sparrow.massaging.channel-publisher.mapping.typed={8:'defaultTypedPublisher',16:'customTypedPublisher',17:'fileTypedPublisher'}
```

此次保留现有 massaging 拼写，避免夹带无关配置迁移。

- 未配置某个渠道时，保持现有普通/typed 各自默认发布器的回退行为。
- 明确配置了目标而对应发布器不存在时，启动报错，不悄悄回退。
- 不再把渠道码当作 Bean 名称，不拼接事件名称，不保留旧 routingKey 配置旁路。
- 发布策略不得把自己作为分发目标，防止递归调用。

## 4. 订阅与网关回复

业务订阅者只声明统一 EventType，按类型码查找。删除旧的“eventType + messageType”嵌套索引；重复业务订阅登记报错，不后者覆盖前者。

将旧的两个处理用途明确分开：

1. 业务订阅：DefaultTypedPublisher 按统一类型调用一次业务订阅者。
2. 网关回复：独立的网关处理登记。PING、USER_STATUS 等控制处理按类型登记；IM 发布成功后的 ACK 按渠道登记。它们不混入业务订阅表。

普通 IM 业务消息：先发布成功，再回复 ACK；发布失败则不回复成功 ACK。PING：仅执行控制处理，不进入业务发布。不要求每一种业务类型都另行注册一个 ACK 订阅者。

ACK 使用 SYSTEM 渠道、ACK 类型；保留现有时间和消息标识关联规则。PING/PONG 和其他控制回复使用各自类型声明的渠道；客户端同样按新格式编码解码。

AI 等可发布渠道若没有回复处理器，发布成功后正常完成，不再因缺少旧式事件订阅者关闭连接。未知类型或无处理规则的不可发布控制消息仍明确拒绝。

## 5. 兼容性与迁移

本次建议直接切换格式，不实现自动猜测旧报文。旧格式首字节可能与新渠道相同，不能可靠自动区分；需要客户端、服务端和文件读写器同步迁移。

文件示例将旧的 eventType、messageType、routingKey 合并为 eventType 与 channel，读取时校验类型及渠道。不自动改写既有文件；示例新文件使用独立路径，旧文件保留供人工迁移。

源代码接口、Spring 配置和二进制格式均不向后兼容。若存在必须继续连接的旧客户端或需回放的旧文件，必须先另行确认兼容范围，再调整实施方案；不能直接混用两种格式。

不涉及数据库结构、真实业务数据迁移、外部发布或部署。

## 6. 实施顺序与文件范围

1. messaging-protocol：统一类型接口、渠道定义和注册表；修改 Protocol 编解码、ACK/PONG 构造和复制；调整协议测试。
2. messaging-domain：渠道发布器映射、普通/typed 发布策略、统一业务订阅表；补齐分发和重复登记测试。
3. messaging-infrastructure：Spring 渠道配置、类型注册、网关解码及回复处理分离；验证 ACK/PONG 和失败路径。
4. messaging-starter：更新自动装配、代理订阅和启动校验；保留当前工作区已有修改的有效行为。
5. messaging-example 及 README：迁移普通/typed、自定义/文件/WebSocket 示例、属性配置和报文说明；检查其他模块的引用。

每步可编译检查，但不发布中间版本。全部迁移完成后再交付可评估的编码结果，提交和发布属于后续环节。

## 7. 验证与环境范围

- 协议：固定字节样例、各结束段往返、0/255 渠道边界、0/65535 类型边界、未知/重复类型、渠道不一致、截断与多余字节。
- 分发：同渠道多个类型选择同一发布器；不同渠道选不同发布器；普通/typed 分开配置；缺省回退、配置错误和自递归目标。
- 订阅：只按类型查找、重复登记拒绝、Spring 代理订阅正常注册、只发布类型无需业务订阅者。
- 网关：PING→PONG；IM 业务发布一次且 ACK 一次；发布失败无成功 ACK；AI 无回复处理器不误关连接；连接失效行为保持。
- 文件：新格式往返、旧格式明确拒绝、渠道不匹配拒绝；README 中不再残留旧分发规则。
- 运行相关模块 Maven 测试，最后在依赖具备时运行仓库完整测试；具体命令沿用项目 Maven 配置，不跳过失败来宣称通过。

所需环境仅限读取项目/本地 Maven 缓存、编译与运行本地测试，以及写入仓库 target 和临时测试文件。测试生成的文件可随测试清理；不删除已有示例消息文件。不启动 Docker、不访问真实 MQ/数据库、不改全局配置、不引入付费服务。依赖缺失需要联网时按环境权限另行申请。

## 8. 待确认的主要取舍

批准本方案将包含以下范围：固定 3 字节新头、类型码全局唯一、每类型固定渠道、控制类型 SYSTEM 渠道、普通消息一并移除 routingKey、发布后网关回复分离、新旧格式直接切换。

当前只完成静态代码核实；尚未修改产品代码或运行实施测试。下一环节是按已确认方案编码与相应测试，不包含提交、推送或部署。
