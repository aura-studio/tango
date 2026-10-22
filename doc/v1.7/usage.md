# tango 命令行使用说明（v1.7）

tango 是单一二进制，**运行角色由配置键 `role.mode` 选择**（不再用子命令）。所有上报角色共享同一引擎，区别在数据来源/入口：

```bash
tango --config daemon.yaml      # role.mode=daemon：常驻采集上报，追尾日志文件 → 写 MongoDB
tango --config gateway.yaml     # role.mode=gateway：常驻 HTTP gateway，暴露 /upload（httpbody 源）
tango --config cli.yaml         # role.mode=cli：一次性，从 stdin 读日志数组上报（process.mode 选策略）
tango --role.mode gateway       # 角色也可用 flag / 环境变量覆盖（等价 TANGO_ROLE_MODE=gateway）
# api 角色无运行入口：作为 Go 库被业务代码 import（见“作为库使用”）
```

## 通用规则

- **角色由配置键 `role.mode` 指定**（`daemon` / `gateway` / `cli`，默认 `daemon`），和其它配置键一样可经文件 / `TANGO_ROLE_MODE` / `--role.mode` 设置；不再有角色子命令。
- **三个途径完全一致**：每个配置键都可经 ① 配置文件、② `TANGO_*` 环境变量、③ `--<键>` 命令行参数 三种方式设置，键名一致。优先级（低→高）：默认值 < 文件 < 环境变量 < 命令行参数。
  - 文件键：`dao.mongo.uri`
  - 环境变量：嵌套键 `.` 转 `_` 并大写加 `TANGO_` 前缀 → `TANGO_DAO_MONGO_URI`
  - 命令行：flag 名即键路径 → `--dao.mongo.uri`
  - **配置键路径 = 包路径**（`internal/` 下）：`logging.*`、`dao.mongo.*`、`dao.store.*`、`parser.filter.*`、`source.tailer.*`、`source.file.*`、`backfill.*`、`process.*`、`role.gateway.*`。
- **唯一的例外**：`--config <path>`（配置文件路径，`.yaml`/`.yml`/`.json`）只有命令行这一种途径；它不是配置键。留空时在二进制同级目录查找 `tango.{yaml,yml,json}`，缺失则静默回退到默认值 + 环境变量 + flag。
- 不要混淆两个 `mode`：`role.mode` 选**运行角色**（daemon/gateway/cli）；`process.mode` 选**上传策略**（`single`/`batch`/`pipeline`，默认 `batch`，CLI/gateway/api 共用）。

| role.mode | 主要配置段 |
|---|---|
| `daemon`（默认） | `logging` · `dao` · `parser` · `source` · `process` |
| `gateway` | `logging` · `dao` · `parser` · `process` · `role.gateway` |
| `cli` | `logging` · `dao` · `parser` · `process` · `role.cli`（`function=file` 时另加 `source.file`；`function=backfill` 时另加 `backfill`；`function=ejson`/`sql` 时仅 `logging` · `dao` · `role.cli`） |

## Daemon Service

```bash
tango --config daemon.yaml                                       # role.mode=daemon（写在配置里）
tango --role.mode daemon --dao.mongo.uri mongodb://localhost:27017/tango
```

职责：追尾 `source.tailer.logPattern` 匹配的日志文件 → 解析 TA JSON → 上报 filter → identity resolve → 流水线批量写 MongoDB。

常用参数（任意配置键都有同名 flag，下面只列最常用的）：

| 参数 | 说明 |
|---|---|
| `--dao.mongo.uri` | MongoDB 连接串（配置键 `dao.mongo.uri`） |
| `--logging.level` | 日志级别（配置键 `logging.level`） |
| `--source.tailer.logPattern` | 追尾文件模式（配置键 `source.tailer.logPattern`） |

## HTTP Gateway Service

```bash
tango --config gateway.yaml                          # role.mode=gateway（写在配置里）
tango --role.mode gateway --role.gateway.addr :8080  # 角色与监听地址都是普通配置键
```

gateway 是常驻 HTTP 服务，读取共享段 `logging` + `dao` + `parser` + `process`，外加 gateway 专属的
`role.gateway`（`addr`）。上传模式、批量/流水线参数与过滤器即顶层 `process.*` / `parser.filter.*`。
监听地址用配置键 `role.gateway.addr`（文件 / `TANGO_ROLE_GATEWAY_ADDR` / `--role.gateway.addr` 三选一）。

| 方法 | 路径 | body | 功能 |
|---|---|---|---|
| GET | `/healthz` | - | 健康检查 |
| POST | `/upload` | `{"line":...,"lines":[...]}` | 日志数组上报，策略由 `process.mode` 决定 |
| POST | `/ejson` | EJSON `{action, collection, ...}` | Mongo Data API：通用 CRUD/aggregate（见下） |
| POST | `/sql` | JSON `{"sql":"..."}` | SQL Data API：SQL → MongoDB（见下） |

请求体的 `line` / `lines` 会被包成一个 httpbody 源，按 `process.mode` 选 single / batch / pipeline
三种上传策略之一写入 MongoDB，返回本次统计（行数 / user / event / 死信等）。

## Mongo Data API（`/ejson` · cli `ejson` · `api.EJSON`）

通用的 MongoDB 读写接口，与 `/upload` 完全独立。功能核心在 `internal/dao/ejson`（由 `dao` 根包经 `dao.go` 中转），三端共享、只是入口不同：
gateway 的 `POST /ejson`、cli 的 `role.cli.function=ejson`（stdin→stdout）、库的 `engine.EJSON(ctx, req)`。

请求/响应体均为 **Extended JSON v2**（`bson.UnmarshalExtJSON` / `MarshalExtJSON`）；请求 `Content-Type`
建议 `application/ejson`，也接受 `application/json`（JSON 是 EJSON 子集）；响应为 relaxed EJSON。

**这是完全放开的接口**：可访问任意 database / collection，任意 filter / operator / aggregate pipeline，
不设白名单、不设返回条数 / body / 超时上限——访问控制由调用方负责。

action 列表：`findOne`、`find`、`insertOne`、`updateOne`、`deleteOne`、`aggregate`。
请求外壳字段：`action`（必填）、`collection`（必填）、`database`（缺省取连接 URI 里的库）、
`filter`、`projection`、`sort`、`limit`、`skip`、`document`（insertOne）、`update`（updateOne）、
`pipeline`（aggregate）、`upsert`。

```bash
# find（限制 5 条，按 #time 倒序）
curl -X POST localhost:8080/ejson -H 'Content-Type: application/ejson' -d '{
  "action":"find","collection":"event",
  "filter":{"#event_name":"login"},"sort":{"#time":-1},"limit":5}'

# insertOne
curl -X POST localhost:8080/ejson -H 'Content-Type: application/ejson' -d '{
  "action":"insertOne","collection":"event",
  "document":{"#event_name":"login","#time":{"$date":"2026-01-01T00:00:00Z"}}}'

# updateOne（upsert）
curl -X POST localhost:8080/ejson -H 'Content-Type: application/ejson' -d '{
  "action":"updateOne","collection":"user","filter":{"#user_id":{"$numberLong":"1"}},
  "update":{"$set":{"vip":true}},"upsert":true}'

# aggregate
curl -X POST localhost:8080/ejson -H 'Content-Type: application/ejson' -d '{
  "action":"aggregate","collection":"event",
  "pipeline":[{"$group":{"_id":"$#event_name","n":{"$sum":1}}}]}'
```

cli 端等价（一次性，从 stdin 读一个请求、stdout 输出一个响应）：

```bash
echo '{"action":"find","collection":"event","filter":{},"limit":5}' \
  | tango --role.mode cli --role.cli.function ejson --dao.mongo.uri mongodb://localhost:27017/tango
```

## SQL Data API（`/sql` · cli `sql` · `api.SQL`）

用 SQL 读写 MongoDB，与 `/upload`、`/ejson` 完全独立。功能核心在 `internal/dao/sql`（由 `dao` 根包经 `dao.go`
中转；代码自 `github.com/aura-studio/mongosql` 拷贝并适配），三端共享：gateway 的 `POST /sql`、cli 的
`role.cli.function=sql`（stdin→stdout）、库的 `engine.SQL(ctx, query)`。

SQL 由 vitess 解析（MySQL 方言），**表名即集合名**，操作的库取自连接 URI。请求体是 JSON `{"sql":"..."}`；
响应是 relaxed Extended JSON（SELECT 行含 BSON 类型）。支持 `SELECT`（含 WHERE/ORDER BY/LIMIT/GROUP BY/聚合）、
`INSERT`、`UPDATE`、`DELETE`、`INSERT ... SELECT`。

```bash
# SELECT
curl -X POST localhost:8080/sql -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT * FROM event WHERE `#event_name` = '"'"'login'"'"' LIMIT 5"}'

# INSERT
curl -X POST localhost:8080/sql -H 'Content-Type: application/json' \
  -d '{"sql":"INSERT INTO event (`#event_name`) VALUES ('"'"'login'"'"')"}'
```

cli 端等价（从 stdin 读一条 SQL）：

```bash
echo "SELECT * FROM event LIMIT 5" \
  | tango --role.mode cli --role.cli.function sql --dao.mongo.uri mongodb://localhost:27017/tango
```

⚠️ DocumentDB 不支持含表达式的 `UPDATE`（翻译为 aggregation-pipeline 形式），如 `SET n = n + 1`；
常量赋值 `SET n = 10` 走普通 `$set`，正常。DDL（CREATE/ALTER TABLE）未引入（在 mongosql 的 MySQL 协议层），
故 AUTO_INCREMENT/DEFAULT/ON UPDATE 需要的 schema 不存在时自动跳过。

## CLI Upload

```bash
# 从 stdin 读 newline 分隔的 TA JSON 日志数组，按 process.mode 上报，打印统计 JSON
cat events.ndjson | tango --role.mode cli --process.mode batch --dao.mongo.uri mongodb://localhost:27017/tango
```

`process.mode` 取 `single` / `batch` / `pipeline`（默认 `batch`），可来自配置文件、`TANGO_PROCESS_MODE` 或 `--process.mode`。
`role.mode=cli` 是 gateway `POST /upload` 的控制台等价入口（从 stdin 读取）。

cli 角色由 `role.cli.function` 选功能：`upload`（默认，上面这种日志上报）、`file`（存量文件一次性导入，
**不读 stdin**，见下节）、`backfill`（TA OpenAPI 历史回填，**不读 stdin**，见下节）、`ejson`（Mongo Data API，
读一个 EJSON 请求、输出 EJSON 响应，等价 `POST /ejson`）或 `sql`（SQL Data API，读一条 SQL、输出 EJSON 结果，
等价 `POST /sql`）或 `config`（cfgsync 配置发布，读一个配置文档、输出 `{version}`，等价 `POST /config`；
见 §cfgsync 一节）。

## CLI File（存量文件一次性导入）

```bash
# 把 source.file.paths 匹配到的存量日志文件一次性灌入上报链路，打印统计 JSON
# 注意：不读 stdin，没有管道——输入来自 logPattern 匹配的文件
tango --config cli.file.max.yaml
```

`role.cli.function=file`（v1.6 新增）是 `upload` 的**存量文件版**：输入不来自 stdin，而是
`source.file.paths`（**显式文件路径列表**——无 glob、无目录、不依赖 tailer）列出的已落盘文件——
按列表顺序逐文件**从头读到 EOF**（跳过空行；目录路径会被跳过、不展开），读完即退出，统计 JSON
（与 `upload` 同形：行数 / user / event / 死信等）写 stdout。与其它键一样可经
`TANGO_SOURCE_FILE_PATHS`（**逗号分隔**多个路径）/ `--source.file.paths` 覆盖。
完整可运行样例见 [`examples/config/cli/`](../../examples/config/cli) 的
`cli.file.{min,max}.{yaml,json}`（min = `role.mode` + `role.cli.function` + `dao.mongo.uri` +
`source.file.paths` 四个 required 键；max 含 `logging`/`parser`/`process`/`maxLineBytes` 逐字段说明）。

- **缺 `source.file.paths` 即 fail-fast**：连 Mongo 之前就报
  `cli: function=file requires source.file.paths`。
- **无 checkpoint / 断点续传**：重跑全量重导，幂等由写模型**按操作类型**保证——event 按 `#uuid` upsert
  （`$setOnInsert`）重导零新增；`user_set`/`user_setOnce`/`user_uniq_append` 收敛（重导只推进 `$max`
  保护的 `_ts` 元字段）；但 **`user_add`（`$inc`）/`user_append`（`$push`）不幂等**——`_ts` 是摄取时刻、
  `$lte` 守卫只防乱序不防重放，重跑会**重复累加/追加**，含此类操作的存量文件不宜盲目重跑；
  `dead_letter` 按内容派生的确定性 `_id` 去重，**重跑不会再涨**（每次出现仍计入返回的 `DeadLetters` 计数）。
- **坏行/坏文件不拖垮整次导入**：单行超 `source.file.maxLineBytes`（默认 10485760 = 10MB，对齐 tailer）
  时记日志（`bufio.ErrTooLong`）并跳过**该文件剩余部分**（超限行不会入库），打不开的文件同样记日志跳过，
  其余匹配文件继续导入。
- **边界**：`source.tailer`（daemon）= 常驻追**新增**；`cli upload` = stdin 喂行；`file` = **存量**文件、
  有限运行（读完即止）。gateway / daemon **不设** file 入口（v1.6 需求 §7），也没有新增集合。

## CLI Backfill（TA OpenAPI 历史回填）

```bash
# 从 ThinkingData OpenAPI 拉取历史数据回填进上报链路，打印统计 JSON
# 注意：不读 stdin，没有管道——数据来自 TA OpenAPI（按 backfill.* 配置拉取）
tango --config cli.backfill.max.yaml
```

`role.cli.function=backfill`（v1.7 起，源自 v1.6.1）拉取 ThinkingData（TA）OpenAPI 的历史数据回填进库，**不读 stdin**，
配置全部来自 `backfill.*`。核心机制：拉到的每一行都被编码成一条 TA JSON 日志行，推进一个**内存中转源**
（`internal/source/mem`），由引擎**正常的上传流水线**消费——所以回填的行走的是和实时摄入**完全相同**的
parse → filter → identity → DocumentDB-safe 写那条链路（事件按 `#uuid` upsert，用户按 `#user_id` 快照），
**无自定义写模型、无选择 filter、无 checkpoint**。两种表：

- **event 表**（`v_event_<projectID>`）：按 `partDateRange` 逐天（可选叠加 `eventTimeRange`）拉取。每天一段
  TA SQL（`submit-sql` → 轮询 `sql-task-info` → 分页拉 `sql-result-page` NDJSON）。无 `#type` 的行注入
  `#type=track`。`backfill.events`（事件名白名单）下推为 SQL 的 `"#event_name" IN (...)`；**更细的选择性过滤
  不在 backfill 里，而由引擎的上报 filter（`parser.filter.*`）在这条同一链路上照常生效**。
- **user 表**（`v_user_<projectID>`，`table=user`）：整表同步（无分区 / event-time）。行同样走中转源 → 流水线，
  identity 从 `#account_id`/`#distinct_id` 解析，因此用户文档按 tango **解析后的 `#user_id`** 归档（与事件一致，
  不是源表里的 `#user_id`）——这要求 `v_user` 视图带 identity 列。无 `#type` 的行按 `forceSkipExisting` 注入
  `#type=user_setOnce`（默认）或 `user_set`。另外,`v_user` 快照没有 `#uuid`、时间列也不叫 `#time`,而 talog
  二者都要;故 user 行在编码时**按身份确定性合成 `#uuid`**（仅为过校验,去重仍按解析后 `#user_id`）、并把
  `userTimeColumn`（默认 `#update_time`）**映射成 `#time`**（该列缺则回退合成时间戳）。所以你**无需**让 `v_user` 自带
  `#uuid`/`#time`,只要有 identity 列即可。

```bash
# 也可纯 flag / 环境变量驱动（键名即 backfill.*）
tango --role.mode cli --role.cli.function backfill \
  --dao.mongo.uri mongodb://localhost:27017/tango \
  --backfill.apiBaseURL https://ta.example.com --backfill.token "$TA_TOKEN" \
  --backfill.projectID 3 \
  --backfill.partDateRange.start 2026-01-01 --backfill.partDateRange.end 2026-03-31
```

完整可运行样例见 [`examples/config/cli/`](../../examples/config/cli) 的 `cli.backfill.{min,max}.{yaml,json}`
（min = `role.mode=cli` + `role.cli.function=backfill` + `dao.mongo.uri` + `backfill.apiBaseURL`/`token`/`projectID`
+ `backfill.partDateRange.{start,end}` 这几个 required 键；max 含 `table`/`events`/`schemaPrefix`/`eventTimeRange`/
`pageSize`/`limit`/`paginate`/`pageRetries`/`pollInterval`/`pollTimeout`/`forceSkipExisting`/`proxy`
逐字段说明）。配置在**连 Mongo 之前**就做 `FromTree` 校验（缺 `apiBaseURL`/`token`/`projectID`，或 event 表缺
`partDateRange` 即 fail-fast）；跑完把统计 JSON（`api.Result`：行数 / user / event / 死信等，同 `upload` 形）写 stdout。

- **流水线驱动、无 checkpoint**：`RunBackfill` **强制 `process.mode=pipeline`**——一个后台流水线 uploader 消费
  中转源，同时 Fetcher 边拉边 `Push`（背压由 `mem` 的有缓冲 channel 提供；若流水线失败，派生 ctx 会取消，
  解除生产者阻塞的 `Push`）。**不落进度、不存 RunID、没有 `_backfill_progress` 集合、不分页续跑**。
- **重跑即重拉，幂等靠写模型**：中断或重跑会**从头重新拉取**，正确性由写模型保证去重——event 按 `#uuid`
  `$setOnInsert`、user 按解析后的 `#user_id` `user_setOnce`。因此重跑是**幂等收敛**的，无需 checkpoint。
- **`forceSkipExisting`（默认 true）只影响 user 表 `#type`，永不覆盖线上数据**：`true` → `user_setOnce`
  （`$setOnInsert`，只补空绝不覆写已有字段），`false` → `user_set`（`$set`）。**它不影响 event 表**——事件恒为
  `track`、恒按 `#uuid` `$setOnInsert`。
- **代理**：`backfill.proxy` 支持 `http`/`https`/`socks5`（经 `golang.org/x/net/proxy`）；token 作为 query
  参数随请求带上。
- **边界**：backfill 是 **cli-only** 的有限运行入口；gateway / daemon **不设** backfill 入口，也**没有同步
  `POST /backfill`**（v1.6 需求 §7）。它借用引擎已开的 Mongo + dao 连接（**不自开自关**）。

## cfgsync 配置发布与运行时热替换（`/config` · cli `config` · `api.PublishConfig`）

cfgsync 把上报 filter 等**显式 allowlist 的配置**在运行中对齐中心文档 `_tango_config`：daemon / gateway
（`cfgsync.enabled=true`）内嵌 Watcher 持续读取并**原子热替换** live filter；发布侧三面同核
（`cfgsync.Publish`），先按 allowlist 校验 + 编译 filter 再写，坏配置在源头就挡掉。文档 schema：
`{_id, version(单调), filter:{include,exclude}}`，`_id`/`version` 由 cfgsync 拥有（发布自动 `$inc`）。

发布支持两种模式：**set**（默认，整树替换——发布 `{"filter":{...}}` 会覆盖存储的整个 filter，
连省略的 `exclude` 都会被清掉）与 **append**（拉取→合并→乐观锁写回：include/exclude 做有序并集
（存量在前、新增在后、完全相同的串去重），delta 省略的一侧保留存量；带版本守卫重试，**并发 append 互不丢失**）。

```bash
# gateway：POST /config（默认 set，整树替换），返回 {"version": N}
curl -X POST localhost:8080/config -H 'Content-Type: application/json' \
  -d '{"filter":{"include":["#type == \"track\""],"exclude":[]}}'

# gateway：POST /config?mode=append —— 只追加一条规则，存量 include/exclude 保留
curl -X POST 'localhost:8080/config?mode=append' -H 'Content-Type: application/json' \
  -d '{"filter":{"include":["#event_name == \"NewEvent\""]}}'

# gateway：GET /config —— 查询当前远端过滤（含 version）；未发布过返回 404
curl localhost:8080/config

# cli：从 stdin 读配置文档发布（role.cli.configMode 选 set/append，默认 set）
echo '{"filter":{"include":["#type == \"track\""]}}' \
  | tango --role.mode cli --role.cli.function config --dao.mongo.uri mongodb://localhost:27017/tango
echo '{"filter":{"include":["#event_name == \"NewEvent\""]}}' \
  | tango --role.mode cli --role.cli.function config --role.cli.configMode append \
      --dao.mongo.uri mongodb://localhost:27017/tango

# cli：查询当前远端过滤（等价 GET /config）
tango --role.mode cli --role.cli.function configget --dao.mongo.uri mongodb://localhost:27017/tango
```

读侧由 daemon / gateway 开启（默认关闭）：

```bash
tango --role.mode gateway --dao.mongo.uri mongodb://localhost:27017/tango \
  --cfgsync.enabled true --cfgsync.backend poll --cfgsync.pollInterval 5s
```

`backend=poll`（默认）任意拓扑可用，最坏陈旧 = 一个 `pollInterval`；`backend=changestream` 亚秒级，
需副本集（普通 MongoDB）或 DocumentDB 开启 `modifyChangeStreams`，standalone mongod 会清晰报错提示改用 `poll`。
单调版本守卫保证不回退（旧/重放版本丢弃）；新 filter 编译失败保留 last-good（坏配置打不挂）。

**daemon 拉取门禁（pull-before-ingest）**：daemon 在 `cfgsync.enabled=true` 时**先拉到并应用中央配置
才开始摄入**——发布前 tailer 一行都不读（fail-closed，启动时不会用空/基线 filter 把存量日志全量灌库），
等待期间每 30s 打 WARN 提示去发布或关掉 cfgsync；SIGTERM 在等待中也能干净退出。gateway 不设此门禁
（请求由客户端驱动，基线 filter 先行、中央文档随后热替换）。

## 作为库使用（api 角色）

`internal/role/api` 是可复用引擎（仅本仓库内部 import）。gateway / cli 都内嵌它，三者上传能力完全一致：

```go
import (
    "github.com/aura-studio/tango/internal/dao"
    daomongo "github.com/aura-studio/tango/internal/dao/mongo"
    "github.com/aura-studio/tango/internal/process"
    "github.com/aura-studio/tango/internal/role/api"
)

eng, _ := api.New(ctx, &dao.Config{Mongo: &daomongo.Config{URI: "mongodb://localhost:27017/tango"}}, &process.Config{Mode: string(process.ModeBatch)}, nil, nil)
defer eng.Close()
eng.EnsureIndexes(ctx)

res, _ := eng.Upload(ctx, lines)
```

v1.6 起同一引擎多一个存量文件导入面：嵌入方直接调 `eng.File(ctx, &api.FileConfig{Paths: []string{...}})`
（`api.FileConfig` 是 `source/file` 配置的类型别名，套路同 `DaoConfig`；空 `Paths` 在任何
source / 数据库动作之前即被拒绝：`api: file paths is required`）。仓库外的使用方走**公开 `client` 包**
（它只依赖引擎、从不 import `internal/source`），文件路径在**调用时**传：

```go
import "github.com/aura-studio/tango/client"

c, _ := client.New(
    client.WithDaoMongoURI("mongodb://localhost:27017/tango"),
    client.WithSourceFileMaxLineBytes(10485760), // 可选：单行上限（== source.file.maxLineBytes，默认 10MB）
)
defer c.Close()

res, _ := c.File(ctx, "/var/log/app/ta.2024-01-01.log", "/var/log/app/ta.2024-01-02.log") // 显式路径（无 glob/目录）；无 checkpoint：重跑全量重导（event/user_set 类收敛；含 user_add/user_append 的文件勿盲目重跑）
```

v1.7 起（源自 v1.6.1）同一引擎再多一个 TA OpenAPI 历史回填面：嵌入方直接调 `eng.RunBackfill(ctx, &api.BackfillConfig{...}, memCfg)`
（`api.BackfillConfig` 是 `internal/backfill` 配置的类型别名；先 `Validate` 再跑，**借用引擎已开的 Mongo + dao
连接、不自开自关**；Fetcher 把每行编码成 TA JSON 推进内存中转源（`source/mem`），`RunBackfill` **强制
`process.mode=pipeline`** 让后台流水线边拉边消费——**无 checkpoint、无 RunID、重跑即重拉，幂等靠写模型**。
第二个参数 `memCfg *api.MemConfig`（= `source.mem.*`）给中转源定缓冲；传 `nil` 取默认 2000）：

```go
cfg := &api.BackfillConfig{
    APIBaseURL: "https://ta.example.com",
    Token:      "...",
    ProjectID:  3,
    Events:     []string{"login"}, // 下推为 SQL 的 "#event_name" IN (...)
}
cfg.PartDateRange.Start, cfg.PartDateRange.End = "2026-01-01", "2026-03-31"
res, _ := eng.RunBackfill(ctx, cfg, nil) // nil = 默认中转缓冲；或传 &api.MemConfig{BufferSize: 5000}
// res 同 Upload 形（行数 / user / event / 死信）
```

仓库外的使用方走**公开 `client` 包**（同样**从不 import `internal/backfill`**——经 `api.BackfillConfig` 中转，
有 importboundary 测试守着），配置经 `WithBackfill*` options 传，调 `c.RunBackfill(ctx)`：

```go
c, _ := client.New(
    client.WithDaoMongoURI("mongodb://localhost:27017/tango"),
    client.WithBackfillAPIBaseURL("https://ta.example.com"),
    client.WithBackfillToken("..."),
    client.WithBackfillProjectID(3),
    client.WithBackfillPartDateRange("2026-01-01", "2026-03-31"),
    client.WithBackfillEvents("login"), // 下推为 "#event_name" IN (...)；还有 WithBackfillTable/
                                        // EventTimeRange/SchemaPrefix/Proxy/PageSize/Limit/ForceSkipExisting
    // client.WithSourceMemBufferSize(5000), // 可选：中转源缓冲（== source.mem.bufferSize，默认 2000）
)
defer c.Close()

res, _ := c.RunBackfill(ctx) // forceSkipExisting 默认 true：user 表用 user_setOnce 补空不覆写；重跑重拉、幂等收敛
```

同一引擎也暴露 Mongo Data API（与上报共用连接，不需要 process/parser 配置；类型经 `dao` 根包中转）：

```go
import "github.com/aura-studio/tango/internal/dao"

resp, _ := eng.EJSON(ctx, &dao.EJSONRequest{
    Action: dao.EJSONActionFind, Collection: "event",
    Filter: bson.M{"#event_name": "login"}, Limit: 5,
})
// resp.Documents ...
```

以及 SQL Data API（SQL → MongoDB，同样复用引擎连接）：

```go
res, _ := eng.SQL(ctx, "SELECT * FROM event LIMIT 5")
// res.Kind == "select"; *res.Rows ...（指针字段：kind=select 时必有，可能为空切片）
```

以及 cfgsync 配置发布（同核 `cfgsync.Publish`；校验+编译 filter 后原子写中心文档，返回新单调 version）：

```go
import "go.mongodb.org/mongo-driver/v2/bson"

version, _ := eng.PublishConfig(ctx, bson.M{
    "filter": bson.M{"include": []string{`#type == "track"`}},
})
// version 单调递增；daemon/gateway 的 Watcher 会在 ≤pollInterval（或亚秒，changestream）内热替换生效
```

## 部署与运维（daemon）

daemon 是常驻进程，部署在容器（生产为 EKS/Fargate）里长时间追尾日志、批量写 MongoDB。本节讲清楚二进制
**怎么找配置**、**怎么用纯环境变量驱动多环境**，以及 **fd 看门狗**这一兜底机制的运维语义。

### 二进制怎么找配置（`resolveConfigPath`）

配置文件路径的解析逻辑全部在 `main.go` 的 `resolveConfigPath(flagVal, "tango.yaml", "tango.yml", "tango.json")`，
优先级三步：

1. **`--config <path>` 显式指定**：非空时原样返回（`.yaml`/`.yml`/`.json` 由扩展名决定 YAML/JSON 解析器，见
   `config/loader.go` 的 `readConfigFile`）。
2. **二进制同级目录自动探测**：`--config` 留空时，用 `os.Executable()` 取二进制自身所在目录（**不是当前工作目录**），
   依次探测 `tango.yaml` → `tango.yml` → `tango.json`，返回第一个存在且非目录的文件。这样无论从哪个 cwd 启动，
   只要把 `tango.yaml` 和二进制放一起即可被自动加载。
3. **纯环境变量 / flag**：以上都没有时返回 `""`，`config.Load` 静默跳过文件（`readConfigFile` 对空路径和
   `os.ErrNotExist` 都返回 nil），配置完全来自 **默认值 + `TANGO_*` 环境变量 + `--<键>` flag**。

```bash
# (1) 显式指定配置文件
tango --config /etc/tango/daemon.yaml

# (2) 把 tango.yaml 放在二进制同级目录，直接裸跑即自动加载（cwd 无关）
/opt/tango/tango          # 自动读 /opt/tango/tango.yaml（若存在）

# (3) 完全无配置文件，纯环境变量驱动（容器里最常用，见下）
tango                     # role.mode 默认 daemon
```

### 纯 `TANGO_*` 环境变量驱动多环境（一份镜像多集群）

配置分层是 **默认值 < 文件 < `TANGO_*` 环境变量 < flag**（`config/loader.go` 的 viper：`SetEnvPrefix("TANGO")` +
`SetEnvKeyReplacer(".".→."_")` + `AutomaticEnv`；`registerAll` 预注册所有键的默认值，使 `AllSettings()` 能
**物化出仅由环境变量提供的叶子值**）。因此**同一个二进制 / 同一份基础配置**，靠每集群不同的环境变量即可服务多个
集群，**无需改代码、无需改配置文件**。最常改的两个键：

| 配置键 | 环境变量 | 说明 |
|---|---|---|
| `dao.mongo.uri` | `TANGO_DAO_MONGO_URI` | 每集群指向各自的 MongoDB / DocumentDB |
| `source.tailer.logPattern` | `TANGO_SOURCE_TAILER_LOGPATTERN` | 追尾文件 glob；**逗号分隔**可配多个 glob（`cfgtree.Into` 装了 `StringToSliceHookFunc(",")`，把 `a,b` 解成 `[]string{"a","b"}`） |
| `source.tailer.maxOpenFDs` | `TANGO_SOURCE_TAILER_MAXOPENFDS` | fd 看门狗阈值（见下，生产务必设非 0） |
| `logging.level` | `TANGO_LOGGING_LEVEL` | 日志级别 |

```bash
# 同一镜像，A 集群
export TANGO_DAO_MONGO_URI='mongodb://.../tango_a'
export TANGO_SOURCE_TAILER_LOGPATTERN='/var/log/app/*.log,/var/log/app/**/*.ta.log'
export TANGO_SOURCE_TAILER_MAXOPENFDS=4096
tango                                 # role.mode 默认 daemon

# 同一镜像，B 集群——只换环境变量
export TANGO_DAO_MONGO_URI='mongodb://.../tango_b'
export TANGO_SOURCE_TAILER_LOGPATTERN='/data/logs/*.log'
tango
```

> `time.Duration` 类型的键（如 `source.tailer.rescanInterval`、`process.pipeline.flushInterval`）支持
> `"5s"`/`"200ms"` 字符串（`cfgtree.Into` 的 `StringToTimeDurationHookFunc`），所以环境变量里直接写
> `TANGO_SOURCE_TAILER_RESCANINTERVAL=10s` 即可。

### fd 看门狗（生产必开）

源码：`internal/role/daemon/report.go` 的 `reportStats` / `fdWatchdogTriggered`，阈值键
`source.tailer.maxOpenFDs`（`internal/source/tailer/config.go`，**默认 0 = 关闭**）。

- **判定**：`fdWatchdogTriggered(openFDs, threshold) = (threshold > 0 && openFDs > threshold)`——**严格大于**，
  且非正阈值永不触发。fd 计数来自 `openFDCount()`（`internal/role/daemon/procstats.go`），**仅 Linux** 读
  `/proc/self/fd`，其它平台返回 `-1`（`-1` 永远不 `>` 任何阈值，故看门狗在非 Linux 上**天然惰性、不会误触**）。
- **触发动作**：`reportStats` 每 `statsReportInterval`（60s）跑一次检查，越线时打 ERROR 日志
  `report: open fd count exceeded threshold — triggering graceful restart ...`，随后调用 `triggerRestart()`
  取消 `runCtx`——这会让 pipeline **优雅排空 + flush 在途批次到 Mongo**，`Run` 干净返回、进程退出。
  这是**主动自杀式重启**：进程带着干净的退出码退出，交给编排器用新容器（**全新的 fd 表**）把它拉起来。
- **生产配置**：务必把 `maxOpenFDs` 设成**非 0**，否则这道安全网是关的。取值经验：设在容器 `ulimit -n`
  之下、正常用量之上（正常用量 ≈ 追尾文件数 + 少量 Mongo / 连接 fd）。fd 泄漏的**根因已由 tailer reaping 修复**
  （见 `doc/test.md` D/E/G 组），看门狗只是 defense-in-depth 兜底。
- **编排器侧**：把容器的 `restartPolicy` 设为 `Always`（K8s Deployment 默认即是），看门狗退出后由 kubelet
  自动重建，配合 graceful drain 实现「零丢数据的自愈」。

```yaml
# K8s 片段：看门狗 + 自动重启 + fd 上限
spec:
  template:
    spec:
      containers:
        - name: tango
          env:
            - name: TANGO_SOURCE_TAILER_MAXOPENFDS
              value: "4096"          # 务必非 0，否则安全网关闭
          # restartPolicy: Always 是 Deployment Pod 的默认值，看门狗退出后自动重建
```

## 可观测性

daemon 每 60s（`statsReportInterval`，`internal/role/daemon/report.go`，是 `var` 故测试可缩短）打三类统计日志，
退出时打一份 shutdown summary。运维关注的是 **runtime stats** 这一行，它是 fd 泄漏的早期信号。

### 每 60s 的 `report: runtime stats`

`reportStats` 每个周期输出（除 `report: periodic stats (last 60s)` 增量 + `report: cumulative stats` 累计外）：

```text
report: runtime stats   goroutines=42  open_fds=137  tailed_files=12
```

三个字段含义与读法：

| 字段 | 来源 | 含义 / 读法 |
|---|---|---|
| `goroutines` | `runtime.NumGoroutine()` | 进程内 goroutine 数；稳态应平稳，**持续单调上升**= goroutine 泄漏 |
| `open_fds` | `openFDCount()` 读 `/proc/self/fd` | 进程打开的 fd 总数（Linux）；**非 Linux 恒为 `-1`（"unknown"）**。**持续上升**= fd 泄漏 |
| `tailed_files` | `Tailer.TailedCount()`（`internal/source/tailer/tailer.go`，即 `len(t.tailed)` 活跃 per-file tail goroutine 数） | **当前正在追尾的文件数，是打开的日志 fd 最直接的代理**。reaping 正常时它随真实文件数涨落；**只增不减、远超磁盘上实际文件数**= deleted-but-open 泄漏复现 |

判断 fd 泄漏的最快办法：盯 `open_fds` 和 `tailed_files`，**两者随时间稳步攀升而日志文件实际数量没变**，
就是泄漏。修复后这两个值应跟随 `reapMissing` / event 模式 ticker（`hybridPollInterval` ~500ms）在
≤1 个 `rescanInterval`（默认 30s）内回落到真实文件数。

### shutdown summary（`logFinalStats`）

优雅退出（SIGTERM 或看门狗触发）时 `Run` 调 `logFinalStats` 打印一份总账：

```text
report: ========== shutdown summary ==========
report: final stats   total_lines=... total_event_writes=... total_dead_letters=... total_retries=... uptime=...
report: average throughput   lines_per_second=...
report: ========== SHUTDOWN COMPLETE ==========      # 若有 parse/identity/write 错误则为 SHUTDOWN WITH ERRORS
```

其中 `total_retries` 取自 `dao.Store.Stats().TotalRetries()`（含 identity id_counter 的 dup-key 重试次数，
见 §吞吐压测里的修复），`total_dead_letters > 0` 说明有事件被丢进 `dead_letter` 而非 `event`，需排查。

### Linux 上的 ground-truth 检查：`/proc/<pid>/fd`

日志里的 `open_fds` 来自同一来源，但要**实锤** deleted-but-open（已删除但仍被持有的 fd），直接看 `/proc`：

```bash
# 该进程打开的 fd 总数（对应日志的 open_fds）
ls /proc/<pid>/fd | wc -l

# 实锤 deleted-but-open：仍被持有的已删除文件（泄漏时这里会有一长串日志文件）
ls -l /proc/<pid>/fd | grep '(deleted)'

# 按打开文件聚合（需要 lsof）
lsof -p <pid> | grep deleted
```

修复后 `grep '(deleted)'` 应为空或仅短暂出现（下一个 ticker / rescan 即释放）。

## 吞吐压测

压测器：[`test/perf/main.go`](../../test/perf/main.go)。它预填一个含 `-n` 条 TA track 事件的日志文件，
跑**真实的 daemon `Service`**（强制 pipeline 策略）打到 `TANGO_TEST_MONGO_URI`，统计落库速率；用一份扔后即弃的
`tango_perf_<unixsec>` 库、结束即 drop。完整报告见 [`doc/v1.5/perf-daemon-throughput.md`](../v1.5/perf-daemon-throughput.md)。

一行复现（在 in-VPC 的 EC2 上、直连 DocumentDB）：

```bash
export TANGO_TEST_MONGO_URI='mongodb://USER:PASS@<docdb>:27017/?tls=true&tlsCAFile=./global-bundle.pem&replicaSet=rs0&readPreference=primary&retryWrites=false'
go run ./test/perf -n 20000 -workers 4 -batch 1000     # 或交叉编译 CGO_ENABLED=0 GOOS=linux GOARCH=amd64 后 scp 上去跑
```

压测器 flag（`flag.Int`）：`-n`（事件数，默认 50000）、`-workers`（pipeline batch workers，默认 4）、
`-batch`（batch size，默认 1000）、`-users`（不同 `#account_id` 数，默认 500，给 identity 缓存施压）。

实测数字（测试环境单个 DocumentDB 实例，in-VPC）：

| 配置 | 落库 | 耗时 | 吞吐 |
|---|---|---|---|
| n=20000，workers=4，batch=1000 | 20000 / 20000 | 17.0 s | ≈1175 events/s |
| n=20000，workers=8，batch=1000 | 20000 / 20000 | 13.7 s | ≈1456 events/s |

- 全部 20000 条**完整落库、零 dead-letter**（这次压测顺带抓出并修复了 identity `id_counter` 的 `E11000`
  dup-key 竞争——`internal/dao/store/identity_atomic.go` 的 `nextUserID` 现对 dup-key 重试 ≤8 次；该竞争在
  原生 MongoDB 潜伏、在 DocumentDB 的 upsert 并发语义下高概率触发，详见 perf 报告）。
- **吞吐杠杆**：`process.pipeline.batchWorkers`（4→8 吞吐 +24%，已验证）与 `process.pipeline.batchSize`
  （增大摊薄往返）。workers 提升非线性收敛说明**瓶颈在 DocumentDB 写延迟**，不是 daemon 单线程。
  identity 缓存（`internal/dao/store/identity_cache.go`）让 500 个用户里只有首现的 500 次走库、其余命中缓存，
  所以瓶颈是**写**不是 identity 查。

## fd 泄漏排查

历史背景：v1.5.0 在 rocket-nano 把 EKS overlay 以 ~5GB/h 撑满，根因是 hpcloud/tail 的 `ReOpen:true` 持有
已删除/已轮转的日志 fd 不释放（inotify 在**我们仍持有文件**时不投递 `IN_DELETE_SELF`），形成 deleted-but-open
累积。v1.5 已修复（tailer reaping + 每文件 ticker 自检），本节是出现疑似症状时的快速排查路径。

**症状识别**：

- 容器 **overlay / 磁盘占用持续增长**，但 `du -sh <日志目录>` 算出来的实际占用**基本不变**——这个「磁盘在涨、
  du 却平」的背离，就是 deleted-but-open（fd 还按着已删除文件，空间不被回收）的典型特征。
- 日志里 `report: runtime stats` 的 `open_fds` / `tailed_files` **只增不减**，且 `tailed_files` 远超磁盘上
  实际日志文件数。

**排查步骤**（Linux）：

```bash
# 1) 拿到 daemon 进程 pid
pidof tango

# 2) 实锤 deleted-but-open：仍被持有的已删除文件
ls -l /proc/<pid>/fd | grep '(deleted)'        # 泄漏时是一长串日志文件；修复后应为空/瞬时

# 3) 对照日志的 open_fds（应与 fd 目录条数一致）
ls /proc/<pid>/fd | wc -l
```

**兜底**：即便出现泄漏，只要生产设了 `source.tailer.maxOpenFDs`（非 0），fd 看门狗会在 `open_fds` 越线时打
ERROR `triggering graceful restart` 并优雅 drain + flush + 退出，由编排器（`restartPolicy: Always`）用全新
fd 表的新容器自愈——是泄漏的最后一道 backstop（见 §部署与运维 的 fd 看门狗）。
