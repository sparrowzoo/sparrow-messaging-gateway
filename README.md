# Sparrow Messaging Gateway

**消息网关：负责连接管理，以及消息的接收和发送。**

`sparrow-messaging-gateway` 为客户端与后端业务服务提供消息出入口。网关维护连接、接收客户端消息并转交业务服务，也接收业务服务的发送请求，将消息发送到目标连接。

## 当前状态

项目已整理为可编译的 Maven 多模块骨架，保留连接接口和原有 Netty WebSocket 接入代码，尚未形成可独立运行的完整网关。

已明确的职责为连接管理、消息接收和消息发送。当前保留 WebSocket 接入作为后续开发基础；业务服务之间的消息交接、消息可靠性与部署方式尚未实现或验证。

## 核心职责

| 职责 | 范围 |
| --- | --- |
| 连接管理 | 管理连接的建立、存活与关闭，维护连接状态，定位消息对应的目标连接。 |
| 消息接收 | 接收客户端消息，完成接入所需的基础检查，将消息及必要的来源信息转交后端业务服务。 |
| 消息发送 | 接收后端业务服务的发送请求，将消息发送到目标连接，反馈能够确认的发送结果。 |

网关处理消息传输，消息内容的业务含义与处理结果由后端业务服务决定。

## 职责边界

- **客户端**：建立连接、发送消息、接收消息。
- **消息网关**：管理连接，完成客户端与业务服务之间的消息收发。
- **业务服务**：处理业务规则、业务权限、业务数据，以及决定回复内容和接收对象。

意图识别、模型调用、智能体编排、客服流程、订单处理与素材生成不属于消息网关职责。

连接身份校验与业务权限判断需要区分：接入侧需要确认连接身份，具体业务操作是否允许由业务服务判断。身份校验方式及两者之间的信息传递方式待后续明确。

## 消息传递方向

客户端上行：客户端 → 消息网关 → 业务服务。

业务下行：业务服务 → 消息网关 → 目标客户端。

上述关系仅表达职责与消息方向，不限定内部通信方式。

## 待明确事项

- **接入对象与协议**：自有客户端连接、外部平台消息渠道的覆盖范围，以及首期支持的协议。
- **连接规则**：身份绑定、心跳与超时、多设备登录、断线重连后的处理方式。
- **消息交接**：网关与业务服务如何交接消息，以及失败时由谁处理。
- **可靠性范围**：是否承担确认回执、重发、去重、顺序保证及离线消息保存。
- **部署与容量**：单实例或多实例运行、连接定位、连接数与消息速率目标。

“网关已接受发送请求”“消息已写入连接”“客户端已确认收到”和“用户已读”分别表示不同结果，后续接口需明确实际承诺的范围。

## 本地开发

### 项目结构

```text
pom.xml                      聚合全部模块
bom/                         依赖版本管理，继承 Sparrow infra-bom
messaging-protocol/           连接查询条件、传输指令、消息与会话 DTO
messaging-po/                 消息领号种子 SessionMessageSeed
messaging-dao-api/            种子的持久化访问接口
messaging-dao-sparrow/        种子与 Sparrow ORM 的类型绑定
messaging-domain/             连接接口、消息和会话仓储接口
messaging-infrastructure/     Netty WebSocket 接入、握手认证、连接容器和心跳处理
messaging-adapter/            HTTP 认证、当前用户与在线状态查询
messaging-starter/            @EnableMessage 与 Spring 消息组件配置
```

源码模块直接继承 `messaging-bom`，父 POM 统一配置 `<relativePath/>`，按坐标从 Maven 仓库解析；根项目负责聚合构建，BOM 负责公共构建配置和依赖版本。

主要模块依赖：

- `adapter → infrastructure → domain → protocol`
- `infrastructure → dao-sparrow → dao-api → po`

Java 包名统一使用 `com.sparrow.messaging`，Maven groupId 为 `com.sparrowzoo.messaging`。

所有原有模块均保留。`SessionMessageSeed` 的字段、主键和表映射沿用原始定义；DAO 主键类型为 `String`，对应 `seedKey`。这里只整理接口和 ORM 类型绑定，没有实现消息领号、原子递增、校准或恢复流程。

清理的是依赖缺失的旧聊天实体及服务的类，例如旧 `Message`、`Session`、`SessionMeta` DAO 和持久化实现、联系人和群同步、聊天登录及历史消息接口；保留消息和会话的普通 DTO、仓储接口、Netty 接入及调试页。

`messaging-po/bin` 中原有生成脚本和 SQL 已恢复保留，仍是旧项目的参考文件，不是当前种子表的迁移脚本，构建不会执行这些文件。

### 编译

环境要求：JDK 25+、Maven 3.8.6+。需预先提供 Sparrow 内部 `1.0.0-SNAPSHOT` 依赖，包括 `sparrow-parent`、`infra-bom`、`sparrow-protocol`、`authenticator-core` 和 `sparrow-starter` 及其传递依赖；通过本地 Maven 仓库或已有内部仓库解析。

沿用 Sparrow 父项目的代码检查规则，`SPARROW_STYLE_DIR` 需指向包含 `sparrow_checkstyle.xml` 和 `check_style_suppressions.xml` 的目录。

首次构建前，将本项目 BOM 安装到本地 Maven 仓库（已由私服提供时可跳过）：

```bash
mvn -B -N -f bom/pom.xml install
```

然后在根目录构建：

```bash
mvn -B clean verify
```

依赖已缓存时也可离线构建：

```bash
mvn -o -B clean verify
```

构建会执行代码规范检查、源码编译和 JAR 打包，不需要启动数据库、Redis 或其他服务。本次没有新增业务实现和自动化测试。

### 当前限制

- `ClientConnection` 和 `ConnectionRegistry` 只有接口，尚无实现；原有 `UserContainer` 仍是 Netty 侧的旧连接容器，尚未接入这些接口。
- 保留的帧处理器仅处理原有文本 `PING/PONG`，消息接收、业务转交和下行发送留待后续实现，其他应用帧当前不会被业务处理。
- 握手认证仍依赖 Spring 容器中的 `Authenticator`；尚未提供应用启动模块和运行配置，不提供独立服务启动命令。
- 种子 DAO 目前只复用现有 ORM 的基础接口；自然主键读写、并发领号和 DDL 生成尚未验证，没有执行任何数据库操作。
- 编译通过仅表示项目骨架和依赖正确，不代表网关功能已联调或可部署。

## 仓库

[sparrowzoo/sparrow-messaging-gateway](https://github.com/sparrowzoo/sparrow-messaging-gateway)


### Spring 接入

业务项目引入 `com.sparrowzoo.messaging:messaging-starter:1.0.0-SNAPSHOT`，在 Spring 配置类上添加：

```java
import com.sparrow.messaging.starter.EnableMessage;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableMessage
public class MessagingConfig {
}
```

`@EnableMessage` 注册多设备连接管理、普通和分类消息处理器映射表、分发器，以及 MESSAGE、PING、USER_STATUS 事件处理器。默认消息分发使用本地 `RoutablePublisher`。

业务处理器继承 `AbstractSpringTypedHandler` 并注册为 Spring Bean，初始化时自动加入分类消息处理器映射表。启用注解不启动 WebSocket、不绑定端口，也不初始化数据库或消息队列；WebSocket 服务启动和认证接入仍由业务项目负责。此模块使用 Spring Context，可以用于普通 Spring 或 Spring Boot 应用；无需额外扫描消息模块。

验证 starter 配置与业务处理器接入：

```bash
mvn -o -B -pl messaging-starter -am test -DskipTests=false
```
