# Messaging 依赖管理

`messaging-bom` 继承公司 `com.sparrowzoo:infra-bom:1.0.0-SNAPSHOT`，管理本项目全部七个源码模块的版本，并沿用公司的公共构建配置。

根项目负责聚合构建；每个源码模块直接继承 `messaging-bom`，配置 `<relativePath/>`，按坐标从 Maven 仓库解析父 POM。源码模块不继承根聚合项目。

BOM 不聚合业务模块，也不引入旧聊天、联系人、文件或 Passport 业务依赖。
