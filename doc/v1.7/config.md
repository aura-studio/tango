# tango 配置参考（v1.7）

本文是**字段参考**（每个键的含义、required/optional、默认值），对应 **v1.7**。命令行用法见
[usage.md](usage.md)，设计与数据流见 [arch.md](arch.md)。完整可运行样例见
[examples/config](../../examples/config)。

所有角色共用**单一 schema**，且**配置键路径 = 消费它的包路径**（`internal/` 下）。
最外层 `config` 包不定义任何字段，只做加载/覆盖；每个角色只取自己需要的段。

**角色由配置键 `role.mode` 指定**（`daemon`/`gateway`/`cli`，默认 `daemon`），不再用子命令。`role.mode=cli` 是 gateway `POST /upload` 的控制台等价入口（默认 `function=upload` 从 stdin 读取；`function=file` 不读 stdin、改按 `source.file.paths` 导入存量文件）。注意区分两个 mode：`role.mode` 选运行角色，`process.mode` 选上传策略（`single`/`batch`/`pipeline`）。**三个途径完全一致**：
每个配置键都可经 配置文件 / `TANGO_*` 环境变量 / `--<键>` 命令行参数 三种方式设置，键名相同、可互换；
唯一例外是 `--config <path>`（只有命令行、不是配置键）。

## 角色（role.mode）与配置段

| role.mode | 主要配置段 |
|------|--------|
| `daemon`（默认） | `logging` · `dao` · `parser` · `source` · `process` |
| `gateway` | `logging` · `dao` · `parser` · `process` · `role.gateway` |
| `cli` | `logging` · `dao` · `parser` · `process` · `role.cli`（`function=file` 时另读 `source.file`；`function=backfill` 时另读 `backfill.*` + `source.mem`；`function=ejson`/`sql` 时仅 `logging` · `dao` · `role.cli`） |

`--config` 留空时在**二进制同级目录**按 `tango.yaml → tango.yml → tango.json` 取首个存在者。
文件缺失或解析为空时静默跳过（回退到默认值 + 环境变量 + flag）。

## 来源与优先级（低 → 高）

1. 内置默认值
2. 配置文件（YAML/JSON，按扩展名识别）
3. `TANGO_*` 环境变量
4. CLI flag（**每个配置键都有同名 flag**，如 `--dao.mongo.uri`；只有用户显式传入的 flag 才覆盖文件/环境变量。`--config` 是文件路径、非配置键）

加载实现见 `config/load.go` 的 `Load(path, flags)`、`config/loader.go` 的 `newViper`/`readConfigFile`/`bindFlagsTo`、`config/config.go` 的 `registerAll`，以及载体 `internal/cfgtree/cfgtree.go`。其精确机制（**为什么三个途径完全等价**）如下：

1. **`registerAll` 先把所有键以默认值播种进 viper**（`config/config.go`：对 `logging`/`dao`/`parser`/`source`/`process`/`cfgsync`/`role` 各调 `RegisterDefaults(v.SetDefault, prefix)`）。这一步是 env 绑定能工作的前提——viper 的 `AutomaticEnv` 只对**已知键**生效，而"已知"正是靠默认值播种建立的。所以即使某个叶子既不在文件里、也没默认业务值，只要 `RegisterDefaults` 注册过它（哪怕注册的是 `""`/`"0s"`/`0`/`[]string{}` 占位），对应的 `TANGO_*` 环境变量就能被读到。
2. **文件读入**（`readConfigFile`）：`path==""` 或文件不存在静默跳过（`os.Stat` + `errors.Is(err, os.ErrNotExist)`）；扩展名 `.yaml/.yml/.json` 选择 viper 的解析器（`SetConfigFile` + `ReadInConfig`）。
3. **env 绑定**（`newViper`）：`SetEnvPrefix("TANGO")` + `SetEnvKeyReplacer(strings.NewReplacer(".", "_"))` + `AutomaticEnv()`。键 `dao.mongo.uri` 经 replacer 变 `dao_mongo_uri`、加前缀转大写 → `TANGO_DAO_MONGO_URI`。
4. **flag 绑定**（`bindFlagsTo`）：用 `flags.Visit`——**只遍历用户在命令行实际设过的 flag**，对每个调 `v.BindPFlag(f.Name, f)`，键名与 flag 名同名（无别名表）。`--config` 被显式跳过（它是文件路径不是配置键）。没设的 flag 不参与，于是不会用 flag 的空默认值去覆盖文件/env。这就是"只有用户显式传入的 flag 才覆盖"的实现。
5. **物化**（`Load` 末尾）：`cfgtree.New(v.AllSettings())`。`AllSettings()` 把每个已注册键经 `默认 < 文件 < env < flag` 的优先级解析成一个纯嵌套 `map[string]any`，交给 `cfgtree.Tree`。**这里必须用 `AllSettings()` 一次性物化**：直接对 live viper 做 `Sub` 切片会丢掉只来自 env/flag、文件里不存在的叶子（`Load` 的注释明确点出这一点）。第 1 步的默认播种又一次发挥作用——它保证 env-only 的叶子在 `AllSettings()` 里能被物化出来。
6. **解码**（各模块 `FromTree` → `cfgtree.Tree.Into`）：模块按键路径 `Sub(...)` 切到自己的分支，再用 mapstructure 解码进自己的 `*Config`。解码器开 `WeaklyTypedInput: true`（让 env/flag 的字符串值强转成 `time.Duration`/`int` 等目标类型），并挂两个 DecodeHook：`StringToTimeDurationHookFunc()`（`"30s"` → `time.Duration`）与 `StringToSliceHookFunc(",")`（**逗号分隔字符串 → `[]string`**）。然后各模块自行 `ApplyDefaults()` + `Validate()`。

> 注意默认值的两套：`RegisterDefaults` 注册的是 viper 的 env-绑定占位默认（多为 `""`/`"0s"`/`0`/`[]`），真正的业务默认值在各模块的 `ApplyDefaults()` 里填（如 `rescanInterval` 占位 `"0s"`，`ApplyDefaults` 把 `<=0` 修正成 `30s`）。所以"默认值"列以 `ApplyDefaults` 为准。

### 环境变量映射

`TANGO_` 前缀 + 嵌套键 `.` → `_`、转大写（`SetEnvKeyReplacer(strings.NewReplacer(".", "_"))` + `SetEnvPrefix("TANGO")`）。

| 配置键 | 环境变量 |
|--------|----------|
| `dao.mongo.uri` | `TANGO_DAO_MONGO_URI` |
| `logging.level` | `TANGO_LOGGING_LEVEL` |
| `process.mode` | `TANGO_PROCESS_MODE` |
| `role.mode` | `TANGO_ROLE_MODE` |
| `source.tailer.tailMode` | `TANGO_SOURCE_TAILER_TAILMODE` |
| `source.tailer.logPattern` | `TANGO_SOURCE_TAILER_LOGPATTERN` |
| `source.tailer.maxOpenFDs` | `TANGO_SOURCE_TAILER_MAXOPENFDS` |
| `source.file.paths` | `TANGO_SOURCE_FILE_PATHS`（逗号分隔，见下） |
| `source.file.maxLineBytes` | `TANGO_SOURCE_FILE_MAXLINEBYTES` |
| `source.mem.bufferSize` | `TANGO_SOURCE_MEM_BUFFERSIZE` |
| `backfill.apiBaseURL` | `TANGO_BACKFILL_APIBASEURL` |
| `backfill.token` | `TANGO_BACKFILL_TOKEN` |
| `backfill.projectID` | `TANGO_BACKFILL_PROJECTID` |
| `backfill.events` | `TANGO_BACKFILL_EVENTS`（逗号分隔，见下） |
| `role.gateway.addr` | `TANGO_ROLE_GATEWAY_ADDR` |
| `role.cli.function` | `TANGO_ROLE_CLI_FUNCTION` |

#### 切片（`[]string`）字段的 env 写法：逗号分隔

`logPattern`（`source.tailer` 与 `source.file` 各有一个，以及 `parser.filter.include`/`exclude`，还有 `backfill.events`）是 `[]string`。配置文件里用 YAML 列表，env 里则用**逗号分隔的单个字符串**，由 `StringToSliceHookFunc(",")` 在解码阶段切回 `[]string`。

实例——一个 env 变量给出多元素 `[]string`（同时追尾两类日志、跨多个挂载点）：

```bash
export TANGO_SOURCE_TAILER_LOGPATTERN='/var/log/ta/.*\.log,/data/*/events-*.ndjson'
```

解码后等价于配置文件：

```yaml
source:
  tailer:
    logPattern:
      - '/var/log/ta/.*\.log'
      - '/data/*/events-*.ndjson'
```

即 `tcfg.Paths == []string{"/var/log/ta/.*\\.log", "/data/*/events-*.ndjson"}`（`len==2`，`daemon` 的 `logPattern` required 校验通过）。注意：模式里**不能含逗号**（会被当分隔符切开）；glob/正则一般用不到逗号，如确需可改走配置文件列表形式。

---

## 多环境一份配置（env 覆盖）

上面 `默认 < 文件 < TANGO_* env < flag` 的机制让**一份基线配置文件 + 每集群少量 `TANGO_*` 环境变量**就能服务多个集群，**无需任何代码或文件分支**。把"各集群都一样"的部分（filter、process、tailMode、logging、role.mode）放进打进镜像的基线 `tango.yaml`，把"逐集群不同"的两项——MongoDB/DocumentDB 端点与日志路径——交给环境变量。

逐集群差异通常就两个键：

- `TANGO_DAO_MONGO_URI` —— 每个集群指向自己的 DocumentDB 集群端点（含库名与上面的 DocumentDB query 参数）。
- `TANGO_SOURCE_TAILER_LOGPATTERN` —— 每个集群的日志挂载/glob（逗号分隔多 glob，见上一节）。

基线 `tango.yaml`（打进镜像，所有集群相同）：

```yaml
role:
  mode: daemon
logging:
  level: info
  format: json
parser:
  filter:
    include:
      - '#type == "track"'
process:
  pipeline:
    batchWorkers: 4
source:
  tailer:
    tailMode: hybrid
    maxOpenFDs: 4096        # fd 看门狗兜底，设在容器 ulimit -n 之下
# dao.mongo.uri 与 source.tailer.logPattern 故意留空，由 env 注入
```

US 集群（Deployment env）：

```bash
TANGO_DAO_MONGO_URI='mongodb://u:p@docdb-us.cluster-xxx.us-east-1.docdb.amazonaws.com:27017/tango?tls=true&tlsCAFile=/etc/ssl/global-bundle.pem&replicaSet=rs0&readPreference=primary&retryWrites=false'
TANGO_SOURCE_TAILER_LOGPATTERN='/var/log/ta/.*\.log'
```

Brazil 集群（同一镜像、同一 `tango.yaml`，只换这两个 env）：

```bash
TANGO_DAO_MONGO_URI='mongodb://u:p@docdb-br.cluster-yyy.sa-east-1.docdb.amazonaws.com:27017/tango?tls=true&tlsCAFile=/etc/ssl/global-bundle.pem&replicaSet=rs0&readPreference=primary&retryWrites=false'
TANGO_SOURCE_TAILER_LOGPATTERN='/var/log/ta/.*\.log,/data/extra/*.ndjson'
```

两个集群跑**完全相同的二进制 + 完全相同的 `tango.yaml`**，差异全在 env，由 viper 的 `AutomaticEnv` 在 `AllSettings()` 物化时覆盖进对应键。这套已**经验验证**。

**Secret vs ConfigMap 边界**（Kubernetes 落法）：`TANGO_DAO_MONGO_URI` 含库凭据，应放 **Secret**（`envFrom`/`secretKeyRef` 注入）；`TANGO_SOURCE_TAILER_LOGPATTERN` 等无敏感信息的逐集群差异放 **ConfigMap**；与集群无关的基线 `tango.yaml` 直接打进镜像或挂 ConfigMap。这样凭据不进镜像、不进基线配置，轮转只动 Secret。

---

## Schema（键路径 = 包路径）

### logging（所有角色） → `internal/logging`

| 键 | required/optional | 默认 | 说明 |
|----|----|----|----|
| `logging.level` | optional | `info` | `debug`/`info`/`warn`/`error` |
| `logging.format` | optional | `text` | `text`/`json` |

### dao（所有角色） → `internal/dao`

| 键 | required/optional | 默认 | 说明 |
|----|----|----|----|
| `dao.mongo.uri` | **required**（daemon/gateway 可空降级） | `""`（占位） | MongoDB/DocumentDB 连接串；库名取自 URI 路径。`mongo.Config.Validate`（`internal/dao/mongo/config.go`）强制非空——但 **daemon/gateway 两个长驻角色对"刻意留空"做了降级**（见下），库调用（api/client/cli）仍然报错 |
| `dao.mongo.connectTimeout` | optional | `10s` | 初次连接握手超时。占位 `"0s"`，`ApplyDefaults` 修正 |
| `dao.mongo.serverSelectionTimeout` | optional | `30s` | 选择可用节点超时。占位 `"0s"`，`ApplyDefaults` 修正 |
| `dao.store.maxElapsedTime` | optional | `10s` | 单次 bulk-write 退避重试总时长上限（属于 store，不属于 mongo 连接；`internal/dao/store` 的 backoff `MaxElapsedTime`） |

#### uri 留空时的降级行为（daemon / gateway）

`dao.mongo.uri` 为空时进程**不再启动失败**，而是进入降级模式（`dao.URIConfigured` 在 Role 层判定，
typed `New`/`NewFromTree` 契约不变、库调用方照旧拿到 `uri is required`）：

- **daemon**（`internal/role/daemon/role.go`）：正常启动后**空转**——不起 tailer/pipeline/Mongo，
  打一条 WARN（`running idle, no reporting will happen`），收到 SIGTERM 干净退出。
- **gateway**（`internal/role/gateway/role.go`）：正常监听——`/healthz` 保持 200（编排器不杀 pod），
  但 `/upload` `/ejson` `/sql` `/config` 一律返回 **503** + `{"error":"uri is empty, service unavailable"}`，
  不触碰任何数据库。
- 补上 `dao.mongo.uri`（或 `TANGO_DAO_MONGO_URI`）并重启即恢复正常模式。

#### 库名取自 URI path（`MongoDBFromURI`）

不单独配库名：`mongo.MongoDBFromURI(uri)`（`internal/dao/mongo/config.go`）从 URI path 段取库名，`mongodb://host:27017/tango` → `tango`；path 为空（如 `mongodb://host:27017/` 或 `mongodb://host:27017`）则回退默认库名 `tango`。所以 DocumentDB 这种 query-string 很长、path 只放库名的 URI，要把库名写在第一个 `/` 后、`?` 之前（`mongodb://user:pass@host:27017/tango?tls=true&...` → 库 `tango`）。

#### DocumentDB 连接串

Amazon DocumentDB 是 Mongo 兼容引擎但**不是副本集自发现**，连它要在 URI query 里显式带上若干参数（取自 `scripts/changestreams.sh` 与 `test/perf/main.go` 的实测样例）：

```
mongodb://user:pass@<cluster-endpoint>:27017/tango?tls=true&tlsCAFile=/path/global-bundle.pem&replicaSet=rs0&readPreference=primary&retryWrites=false
```

| query 参数 | 取值 | 为什么 |
|------------|------|--------|
| `tls` | `true` | DocumentDB 强制 TLS |
| `tlsCAFile` | AWS `global-bundle.pem` 的**绝对路径** | RDS/DocumentDB 的 CA 包（`https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem`，本仓库镜像在 `assets/global-bundle.pem`）。`changestreams.sh` 会运行时下载并把 URI 里的 `tlsCAFile=` 改写成下载后的绝对路径 |
| `replicaSet` | `rs0` | DocumentDB 集群对外呈现为名为 `rs0` 的副本集，显式指定后驱动才能做拓扑发现（否则 in-VPC 连接才工作） |
| `readPreference` | `primary` | 读主，避免读到副本的陈旧数据 |
| `retryWrites` | `false` | **DocumentDB 不支持可重试写**，必须显式关掉，否则驱动报错；tango 的写重试由 store 层 `maxElapsedTime` 退避自己兜（不依赖驱动 retryWrites） |

库名 `tango` 写在 host 后的 path 段、`?` 之前。整串可经文件 `dao.mongo.uri`、`TANGO_DAO_MONGO_URI` 或 `--dao.mongo.uri` 任一途径给出。日志里 URI 的凭据段会被 `maskURI`（`internal/role/daemon/role.go`）打码成 `scheme://***:***@host...`。

> DocumentDB 上 `cfgsync.backend=changestream` 需先用 `scripts/changestreams.sh -enable` 跑一次 `modifyChangeStreams`（普通 MongoDB 只需副本集即可）；否则用 `backend=poll`（任意拓扑可用）。

### parser（daemon / cli） → `internal/parser/filter`

| 键 | required/optional | 默认 | 说明 |
|----|----|----|----|
| `parser.filter.include` | optional | `[]`(全放行) | expr 表达式，OR 语义命中其一即保留 |
| `parser.filter.exclude` | optional | `[]` | 命中其一即丢弃（在 include 之后） |

### source（daemon / cli `function=file` / cli `function=backfill`） → `internal/source/{tailer,file,mem}`

`source` 段现含三个**有配置**的子包：`source.tailer.*` 供 daemon 常驻追尾，`source.file.*` 供 cli `function=file` 的有限存量导入（v1.6 新增），`source.mem.*` 供 cli `function=backfill`（及 `Engine.RunBackfill`）的内存中转源调容量（v1.7 新增）。`httpbody`/`stdin` 在调用期拿输入，无配置。

#### source.tailer.*（daemon） → `internal/source/tailer`

字段、默认值与语义见 `internal/source/tailer/config.go`（`Config` 结构、`RegisterDefaults`、`ApplyDefaults`、`Validate`）。

| 键 | required/optional | 默认 | 说明 |
|----|----|----|----|
| `source.tailer.logPattern` | **required**（daemon） | `[]`（占位） | `[]string`，至少一条 glob/正则，匹配要追尾的日志文件路径。env 用逗号分隔（见上）。`Config.Validate` 不强制非空（空 = 匹配 0 文件，仍是合法 tailer 配置）；**是 daemon 角色单独强制**——`daemon.NewFromTree`（`internal/role/daemon/role.go`）在 `dao.New` 连 Mongo 之前先校验 `len(Paths)==0` 直接 fail-fast，`Run` 里也再查一次 |
| `source.tailer.tailMode` | optional | `hybrid` | `hybrid`/`poll`/`event`，见下。`Validate` 拒绝其它值（空串视为用默认） |
| `source.tailer.rescanInterval` | optional | `30s` | 重新扫描新文件 + 反向 reap 已消失路径的间隔。占位 `"0s"`，`ApplyDefaults` 把 `<=0` 修正成 `30s` |
| `source.tailer.pollInterval` | optional | `200ms` | poll/hybrid 模式轮询节奏。占位 `"0s"`，`ApplyDefaults` 把 `<=0` 修正成 `200ms` |
| `source.tailer.maxLineBytes` | optional | `10485760`(10MB) | 单行最大字节（`defaultMaxLineSize = 10*1024*1024`，`tailer.go`）。占位 `0`，`ApplyDefaults` 把 `<=0` 修正成默认 |
| `source.tailer.maxOpenFDs` | optional | `0`（关闭） | fd 看门狗阈值，见下。占位 `0`；`ApplyDefaults` 把负值归一为 `0`（无意义 → 视为关闭） |

#### tailMode 三种语义（`internal/source/tailer/config.go` 常量）

| 模式 | 常量 | 机制 | 取舍 |
|------|------|------|------|
| `hybrid`（默认） | `TailModeHybrid` | 以 `hpcloud/tail` 的事件驱动 watcher 为主，叠加周期 poll 兜底，检测漏掉的通知（事件 + poll 回退） | 低延迟 + 可靠，是默认 |
| `poll` | `TailModePoll` | 纯轮询循环（自己 `os.Open` + scanner，read → sleep → retry），对"通知丢失"竞态免疫 | 最稳，适用所有负载 |
| `event` | `TailModeEvent` | 纯 `hpcloud/tail` 的 kqueue/inotify 事件驱动 watcher | 延迟最低，但持续并发写下可能因上游库已知的 `sendOnlyIfEmpty` 竞态卡住 |

> tailMode 直接影响 fd 生命周期。这一轮的 fd 泄漏修复（`reapMissing` 反向回收路径已消失的 tail、`startFile` 的 per-file `context.CancelFunc`、event/hybrid 的 `os.Stat`+`os.SameFile` ticker 释放门、`stopTail` 在停 tail 时排空 `tt.Lines`）就活在这三种模式的实现里；带 "RELEASE-GATE INVARIANT — do not remove" 注释的代码不可删。

#### maxOpenFDs fd 看门狗（`source.tailer.maxOpenFDs`）

进程级安全阀，**纵深防御兜底**（fd 泄漏的根因已由上面的 tailer reaping 修复，这是第二道闸）。语义：

- **默认 `0` = 关闭**。判定函数 `fdWatchdogTriggered(openFDs, threshold)`（`internal/role/daemon/report.go`）= `threshold > 0 && openFDs > threshold`——**严格大于**，且非正阈值（默认/关闭）永不触发。
- 触发时（`reportStats` 每 `statsReportInterval`=60s 检查一次）：记 ERROR 日志（含 `open_fds`/`threshold`/`goroutines`/`tailed_files`）`triggering graceful restart`，调 `triggerRestart()`（即 `Run` 里派生的 `cancelRun`）取消 run 上下文——**优雅自重启**：drain + flush 在途 batch 到 Mongo、`Run` 干净返回、进程退出，交编排器的 restart policy 用全新 fd 表重建容器。
- **仅 Linux 生效**。`openFDCount()`（`internal/role/daemon/procstats.go`）在 Linux 读 `/proc/self/fd`（并减 1 排除 `ReadDir` 自己持有的目录 fd），其它平台返回 `-1`——`-1` 永不 `> ` 任何 `>=1` 的阈值，看门狗在 Windows/macOS 上天然 inert。
- 取值建议：设在容器 `ulimit -n` **之下**、正常用量**之上**（≈ 被追尾文件数 + 少量 Mongo/连接 fd）。
- 同一 60s tick 还会打 `report: runtime stats` 日志：`goroutines`（`runtime.NumGoroutine`）、`open_fds`（`openFDCount`）、`tailed_files`（tailer 的 `TailedCount()`，活跃 tail goroutine 数，是 fd 泄漏最直接的信号）。优雅退出时 `logFinalStats` 再打一份关停汇总。

#### source.file.*（cli `function=file`） → `internal/source/file`

有限一次性的存量文件导入 Source（与 tailer 的常驻追新增相对：把列出的文件从头到尾发完一遍即收尾）。**只接收 `paths` 里的显式文件路径——无 glob、无目录展开、不依赖 tailer**（目录路径 `os.Stat` 检出后记日志跳过，不递归）。扫描语义对齐 tailer（`bufio.Scanner`，64KiB 起始缓冲、上限 `maxLineBytes`，但为 `source/file` 自有实现）。**无 checkpoint/断点**：重跑会全量重导，幂等由写模型按操作类型保证（event 按 `#uuid` `$setOnInsert` upsert 零新增；`user_set`/`user_setOnce`/`user_uniq_append` 收敛；**`user_add`/`user_append` 会重复累加/追加**——`_ts` 守卫只防乱序不防重放，含此类操作的文件不宜盲目重跑；dead_letter 按内容派生的确定性 `_id` 去重，重跑收敛不增长，但每次出现仍计入返回的 `DeadLetters` 计数）。字段见 `internal/source/file/config.go`（`RegisterDefaults`/`ApplyDefaults`；无 `Validate`——没有可枚举取值）。

| 键 | required/optional | 默认 | 说明 |
|----|----|----|----|
| `source.file.paths` | **required**（cli `function=file`） | `[]`（占位） | `[]string`，至少一条**显式文件路径**（无 glob、无目录）。env 用逗号分隔（见上）。`Config` 自身不强制非空；**由消费面强制**——cli 分发（`internal/role/cli/role.go`）在连 Mongo 之前 fail-fast（`cli: function=file requires source.file.paths`），api 面 `Engine.File` 在任何 source/数据库工作之前也拒绝 nil/空（`api: file paths is required`） |
| `source.file.maxLineBytes` | optional | `10485760`(10MB) | 单行最大字节，对齐 tailer 的默认值（各自包内同名同值常量 `defaultMaxLineSize`）。占位 `0`，`ApplyDefaults` 把 `<=0` 修正成默认。超限行记 `bufio.ErrTooLong` 日志并**跳过该文件剩余部分**（超限行不发出），其余文件继续导入；打不开的文件同样记日志跳过 |

#### source.mem.*（cli `function=backfill` / `Engine.RunBackfill`） → `internal/source/mem`

内存中转源（v1.7 新增）：单生产者把已成形的 TA JSON 日志行 `Push` 进一个带缓冲的 channel，process pipeline 并发抽干，`Close` 收尾。它是 `source/file` 的内存对偶（file 从磁盘读、mem 由同进程生产者喂），唯一可调的就是缓冲容量。`Engine.RunBackfill` 据 `source.mem.*` 给中转源定容量，回灌的 fetch 生产者据此对 pipeline 施加背压。字段见 `internal/source/mem/config.go`（`RegisterDefaults`/`ApplyDefaults`；与 file 一样**无 `Validate`**——没有可枚举取值）。

| 键 | required/optional | 默认 | 说明 |
|----|----|----|----|
| `source.mem.bufferSize` | optional | `2000` | 中转 channel 容量（行）：单生产者（如回灌 fetcher）最多可超前抽干中的 pipeline 这么多行，超出则 `Push` 阻塞（背压）。占位 `0`，`ApplyDefaults` 把 `<=0` 修正成默认 2000（`defaultBuffer`，`mem.go`） |

### backfill（cli `function=backfill`） → `internal/backfill`

历史数据回灌（v1.7 起，源自 v1.6.1）。从 ThinkingData（TA）OpenAPI 按**日期区间**（事件表）或**整表**（用户表）拉取历史：`submit-sql`（提交查询）→ 轮询 `sql-task-info`（等任务就绪）→ 翻页拉 `sql-result-page`（NDJSON）逐页处理。每行经 `rowdecode.EncodeRowAsJSONLine` 编成一条 TA JSON 日志行（缺 `#type` 时按表注入：事件表 → `track`，用户表 → `user_setOnce`/`user_set`），推入**内存中转源** `internal/source/mem`，由 Engine **强制 pipeline** 的上传管线并发抽干——回灌行因此走**与线上摄入完全相同的** parse → filter → identity → write 路径，无自定义写模型、无回灌内置 filter、无 checkpoint。`internal/backfill` 只 import `internal/logging` + `internal/cfgtree`（近叶子，不依赖 dao / parser / process / source）。token 走 query 参数；代理支持 http/https/socks5。仅 cli `function=backfill` 一面消费，**不在 gateway/daemon 上暴露**（v1.6 需求 §7：无同步 `POST /backfill`）。字段见 `internal/backfill/config.go`（约定 `FromTree`/`RegisterDefaults`/`ApplyDefaults`/`Validate`）。

**TA 数仓 schema 与日志格式的差异调和（v1.7.2，真实项目实测后加固)**:① **列名映射 `#time`**——数仓事件视图用 `#event_time`、用户视图用 `#update_time`,而 talog 要求 `#time` 非空,故按表把 `eventTimeColumn`/`userTimeColumn` 映射成 `#time`(缺则回退合成时间戳);② **headers 兜底**——TA 对**超宽 `SELECT *`(如 ~985 列的事件视图)的 `sql-task-info` 不返回列名**,此时改用同步 `/querySql ... LIMIT 1` 探一次列名并缓存,**取不到则硬报错**(杜绝"空 headers→整页静默丢弃"的零写入);③ **丢弃 `$` 伪列**——`SELECT *` 会带回 `$part_date`/`$part_event` 等分区伪列,MongoDB/DocumentDB 拒收 `$` 前缀字段名,故编码时丢弃(非记录字段)。**这些都因 mock 测试用手写 schema 而长期被掩盖,经真实 TA 项目端到端测试暴露并修复。**

| 键 | required/optional | 默认 | 说明 |
|----|----|----|----|
| `backfill.apiBaseURL` | **required** | `""`（占位） | TA OpenAPI 基址，必须 `http(s)://` 前缀 |
| `backfill.token` | **required** | `""`（占位） | TA OpenAPI token，作为 query 参数附在请求上 |
| `backfill.proxy` | optional | `""` | 出站代理，支持 `http`/`https`/`socks5`（socks5 经 `golang.org/x/net/proxy`） |
| `backfill.projectID` | **required** | `0`（占位） | TA 项目 ID，须 `>0`；拼出表名 `v_event_<projectID>` / `v_user_<projectID>` |
| `backfill.table` | optional | `event` | 目标表：`event`（事件表，按日期分区）/ `user`（用户表，整表快照） |
| `backfill.events` | optional | `[]` | `[]string`，限定要回灌的事件名（事件表），编进 SQL 的 `"#event_name" IN (...)`。env 逗号分隔。超出事件名的选取交给 Engine 上报 filter（`parser.filter.*`） |
| `backfill.schemaPrefix` | optional | `""` | TA SQL 表名 schema 前缀（`[schema.]v_event_<pid>`），留空则不加 |
| `backfill.userTimeColumn` | optional | `#update_time` | **仅用户表**：把 `v_user_<pid>` 的哪一列映射成 `#time`。TA 用户表没有名为 `#time` 的列（那是事件概念）、其按用户的时间列是 `#update_time`,但 talog 要求 user_* 记录的 `#time` 非空。该列缺失时回退为一次性合成时间戳。事件表忽略此项 |
| `backfill.eventTimeColumn` | optional | `#event_time` | **仅事件表**：把 `v_event_<pid>` 的哪一列映射成 `#time`。TA 数仓事件视图暴露的是 `#event_time`（非日志上传的 `#time`),talog 要求事件 `#time` 非空 → 须映射。该列缺失时回退为一次性合成时间戳。用户表忽略此项 |
| `backfill.userOrderBy` | optional | `""` | **仅用户表**：可选 `ORDER BY` 子句(列+ASC/DESC,不含 `ORDER BY` 前缀),让 `limit` 取确定性切片而非任意取样——如 `last_login_time DESC` 配 `limit=10000` 导入"最近登录的 1 万个用户"。空=不排序。事件表忽略 |
| `backfill.userWhere` | optional | `""` | **仅用户表**：可选 `WHERE` 谓词(不含 `WHERE` 前缀),AND 进用户表查询。这是分布式编排给用户表**取单个分片**的原语(用户表无 `$part_date` 分区,故均匀/完整/不相交的分片用谓词表达),如 `mod(cast("#user_id" AS bigint) / 4194304, 8) = 3`——丢掉雪花 id 低 22 位倾斜的序列号/机器号,对高位毫秒时间戳取模。空=无谓词。事件表忽略(它按 `$part_date` 按天切) |
| `backfill.eventWhere` | optional | `""` | **仅事件表**：可选 `WHERE` 谓词(不含前缀),AND 进事件表查询(与 `$part_date`/事件时间/事件名并列)。用于把**一天再切成定行数小片**(事件表只按天分区,更细的切分用谓词),对随机 `#uuid` 做均匀哈希,如 `mod(from_base(substr("#uuid", 1, 8), 16), 8) = 3`。空=无谓词。用户表忽略(用 `userWhere`) |
| `backfill.partDateRange.start` | **required（event 表）** | `""` | 分区日期起（含），`YYYY-MM-DD`；事件表必填、用户表忽略 |
| `backfill.partDateRange.end` | **required（event 表）** | `""` | 分区日期止（含），`YYYY-MM-DD` |
| `backfill.eventTimeRange.start` | optional | `""` | 事件时间窗起（含），`YYYY-MM-DD HH:MM:SS`，在分区内再收窄；仅事件表 |
| `backfill.eventTimeRange.end` | optional | `""` | 事件时间窗止（含），`YYYY-MM-DD HH:MM:SS` |
| `backfill.limit` | optional | `0`（不限） | SQL 的 `LIMIT n`（event / user **两表均施加**），`0` 表示不加 |
| `backfill.pageSize` | optional | `10000` | 单页拉取条数，最小 `1000`（低于则按下限处理） |
| `backfill.paginate` | optional（tri-state `*bool`） | `true` | 是否服务端分页；注册默认 `true`，置 `false` 时提交不带 pageSize、整个结果集作为单页一次取回（全量仍取，只是不切成多页） |
| `backfill.pageRetries` | optional | `3` | 单页拉取失败重试次数（backoff/v4） |
| `backfill.pollInterval` | optional | `3s` | 轮询 `sql-task-info` 的节奏 |
| `backfill.pollTimeout` | optional | `30m` | 单个任务从提交到就绪的轮询总超时 |
| `backfill.forceSkipExisting` | optional（tri-state `*bool`） | `true` | **仅决定用户表行注入的 `#type`**：`true`（默认）→ `user_setOnce`（历史数据**永不覆盖**线上已有字段）；`false` → `user_set`（覆盖）。注册默认 `true`。**对事件表无影响**——事件行恒注入 `track`、按 `#uuid` `$setOnInsert` 去重 |

#### submit → poll → paginate 流程

每个待回灌单元（事件表的每天，或用户表的整表单块 `UserChunkKey`）走同一三步：`submit-sql` 提交一条 TA SQL（`BuildSQL` 生成，见下）拿到 taskId → 按 `pollInterval` 轮询 `sql-task-info` 直到就绪或 `pollTimeout` 超时 → 翻页拉 `sql-result-page`（NDJSON），每页失败按 `pageRetries` 退避重试；`paginate=false` 时提交不带 pageSize、整个结果集作为单页一次取回。每行就地编成 TA JSON 日志行后 `Push` 进内存中转源（`source/mem`），由强制 pipeline 的上传管线抽干——producer（fetcher）与 consumer（管线写）并发；管线失败时派生 ctx 被取消，解开 producer 阻塞的 `Push`。**无 checkpoint、无续跑**：重跑直接重新拉取，幂等由写模型保证（事件按 `#uuid` `$setOnInsert`、用户按 `#user_id` `user_setOnce`）。

#### SQL 生成（无 filter 下推）

事件表 `BuildSQL`：`SELECT * FROM [schema.]v_event_<pid> WHERE "$part_date"='<day>' [AND "#event_time">='...'][AND "#event_time"<='...'][AND "#event_name" IN ('a','b')] [LIMIT n]`；用户表 = `SELECT * FROM [schema.]v_user_<pid> [LIMIT n]`（无分区、无 event-time、无 event-name）。**没有任何 include/exclude filter 下推**——SQL 端的选取仅限事件名（`backfill.events` → `"#event_name" IN`）；超出事件名的过滤是 Engine 上报 filter（`parser.filter.*`），在管线里统一作用于事件与用户两路。因此 `internal/backfill` 无需依赖 `parser/filter`。

#### 事件表 vs 用户表（同一条管线）

事件表与用户表的行**走同一条普通上传管线**，无自定义写模型、无两路分叉：

- **事件表**（`v_event_<pid>`）：每行编成 TA JSON 日志行后（`#`/`_`/`$` 前缀列 → 顶层，其余 → `properties`，nil 丢弃），缺 `#type` 注入 `track`，经 parse → filter → identity → DocumentDB-safe 写（按 `#uuid` `$setOnInsert` 去重）。
- **用户表**（`v_user_<pid>`）：行同样进管线（**不再绕过 parser**），缺 `#type` 注入 `user_setOnce`（默认，永不覆盖）或 `user_set`（`forceSkipExisting=false` 时）；身份从 `#account_id`/`#distinct_id` 解析，用户文档按 tango **解析后的 `#user_id`** 落库（与事件一致，**而非源表的 `#user_id`**）。这要求 `v_user` 携带身份列。

#### forceSkipExisting 与幂等

`forceSkipExisting` **只决定用户表行注入的 `#type`**：`true`（默认）→ `user_setOnce`（`$setOnInsert` 语义，历史数据**永不覆盖**线上已有字段）；`false` → `user_set`（`$set` 覆盖）。**事件表与本项无关**——事件行恒为 `track`、按 `#uuid` `$setOnInsert` 去重、重导零新增。整条路径就是线上摄入的写模型，重跑收敛（事件按 `#uuid`、用户按解析后的 `#user_id`），无需 checkpoint。

#### tri-state `*bool` 默认值

`paginate` 与 `forceSkipExisting` 是三态指针布尔（`*bool`），`RegisterDefaults` 注册的默认即 `true`：不在配置里出现时取 `true`，显式写 `false` 才关闭。配套 helper：`ForceSkip`/`ShouldPaginate`/`EffectivePageSize`。

### process（daemon / cli） → `internal/process{,/pipeline}`

| 键 | required/optional | 默认 | 说明 |
|----|----|----|----|
| `process.mode` | optional | `batch` | 上传策略：`single`/`batch`/`pipeline`。gateway / cli / api 统一读取该配置；daemon 常驻追尾固定使用 pipeline 语义 |
| `process.batchSize` | optional | `1000` | single/batch 策略 bulk-write 批大小 |
| `process.pipeline.batchSize` | optional | `1000` | pipeline 单次 bulk-write 目标条数 |
| `process.pipeline.batchSizeMin` | optional | `0`(自动 = batchSize/4) | 自适应下限 |
| `process.pipeline.batchSizeMax` | optional | `0`(自动 = batchSize*2) | 自适应上限 |
| `process.pipeline.batchWorkers` | optional | `2` | 并行写 worker 数 |
| `process.pipeline.flushInterval` | optional | `1s` | 未满批次刷新间隔 |
| `process.pipeline.channelBuffer` | optional | `0`(自动 = batchSize*2) | 每 worker 通道缓冲 |
| `process.pipeline.deadLetterCap` | optional | `128` | 每 worker 死信批容量 |
| `process.pipeline.maxStuckDuration` | optional | `3m` | 一批写入持续失败时允许施加背压的最长时长。超时后该批交给 `Store.GiveUp`、计入 `total_abandoned`，然后清空批次让管道继续读取。`GiveUp` 分两种：目标是 `user`/`event` 时尽力隔离进 `dead_letter`（可回捞）；目标**就是** `dead_letter` 时**不落库**、只逐条打 Error 日志带 2 KiB 预览（死信模型是不幂等的 `InsertOneModel`，再往自己集合里写会重复），这条路上日志是唯一留存所以不受 5 条的详情上限约束。`0`/未设置 = 取默认（`flushBatch` 内部也会兜底，防止绕过 `ApplyDefaults` 时保险丝失效）；负值 = 无限重试（旧行为，仅作逃生阀）。**必须有界**：源日志挂在 `emptyDir` 上、约 15 分钟就被生产方轮转删除，超过该窗口的背压保不住旧数据，只会让新数据全部读不进来。3m 远长于 DocumentDB 故障切换、远短于留存窗口 |

### cfgsync（daemon / gateway） → `internal/cfgsync`

运行时动态配置同步（盯中心文档 `_tango_config`，运行中热替换上报 filter）。默认**关闭**——
打开远程运行时重配是显式 opt-in。仅 daemon / gateway 读侧内嵌；发布面（写）由 gateway / cli / api 提供。

| 键 | required/optional | 默认 | 说明 |
|----|----|----|----|
| `cfgsync.enabled` | optional | `false` | 是否启动 Watcher（读侧热替换） |
| `cfgsync.backend` | optional | `poll` | 同步 backend：`poll`（任意拓扑可用，最稳）/ `changestream`（亚秒级，需副本集 / DocumentDB 开 `modifyChangeStreams`） |
| `cfgsync.documentID` | optional | `filter` | `_tango_config` 中被跟踪文档的 `_id` |
| `cfgsync.pollInterval` | optional | `5s` | poll backend 轮询周期 = 最坏陈旧窗口 |
| `cfgsync.reconcileInterval` | optional | `60s` | changestream backend 的兜底全量读周期（补漏自愈） |
| `cfgsync.collection` | optional | `_tango_config` | 中心配置集合名，一般不改（与发布侧同一集合） |

**`_tango_config` 文档 schema**（`_id` 与单调 `version` 由 cfgsync 拥有，发布时自动 `$inc`）：

```json
{ "_id": "filter", "version": 7, "filter": { "include": ["#type == \"track\""], "exclude": [] } }
```

**配置发布三面**（同核 `cfgsync.Publish`，先按 allowlist 校验 + 编译 filter 再写）：gateway `POST /config`、
cli `role.cli.function=config`、api `(*Engine).PublishConfig`。默认动态 allowlist 只放 `parser.filter`
（`dao.mongo.*` / `role.mode` / `role.gateway.addr` / `cfgsync.*` 自身绝不可远程覆盖）。用法见 [usage.md](usage.md)。

**发布双模式**：set（默认，整树替换，省略的 `exclude` 会被清掉）/ **append**（拉取→include/exclude
有序并集→乐观版本锁写回，并发 append 互不丢失；`cfgsync.PublishAppend`）。选择方式：gateway
`POST /config?mode=append`、cli `role.cli.configMode=append`、api `(*Engine).AppendConfig`。

**远端过滤查询面**（`cfgsync.Fetch`）：gateway `GET /config`（404=未发布）、cli
`role.cli.function=configget`、api `(*Engine).FetchConfig`（未发布返回 `nil,nil`）。

**daemon 拉取门禁**：`cfgsync.enabled=true` 时 daemon **先拉到并应用中央配置才开始摄入**
（`Watcher.Ready()` 信号；未发布= fail-closed 等待并每 30s WARN，不会用基线 filter 全量灌库；
等待中可被 SIGTERM 干净打断）。gateway 无此门禁。

### role.mode → `internal/role`

| 键 | required/optional | 默认 | 说明 |
|----|----|----|----|
| `role.mode` | optional | `daemon` | 运行角色：`daemon` / `gateway` / `cli`。替代旧的角色子命令，单一 `tango` 二进制据此分发。 |

### role.daemon（daemon） → `internal/role/daemon`

暂无字段：daemon 完全由顶层 `logging`/`dao`/`parser`/`source`/`process` 驱动。该段仅为 schema 对称保留。

### role.gateway（gateway） → `internal/role/gateway`

只含 gateway 专属字段；上传的处理参数与过滤器复用顶层共享模块 `process.*` 与 `parser.filter.*`
（与 daemon 同一套），不在 `role.gateway` 下重复定义。

| 键 | required/optional | 默认 | 说明 |
|----|----|----|----|
| `role.gateway.addr` | optional | `:8080` | HTTP 监听地址 |

gateway 同时暴露三个独立路径（与 `/upload` 互不影响，无额外配置项）：`POST /ejson`（Mongo Data API,
完全放开）、`POST /sql`（SQL Data API，SQL→MongoDB）、`POST /config`（cfgsync 配置发布，body = 配置文档，
返回 `{version}`）。用法见 [usage.md](usage.md)。

### role.cli（cli） → `internal/role/cli`

| 键 | required/optional | 默认 | 说明 |
|----|----|----|----|
| `role.cli.function` | optional | `upload` | cli 角色功能：`upload`（stdin 日志数组上报）/ `file`（按 `source.file.*` 导入存量日志文件，**不读 stdin**）/ `backfill`（按 `backfill.*` 从 TA OpenAPI 回灌历史，**不读 stdin**，输出 `api.Result` JSON）/ `ejson`（stdin 一个 EJSON Mongo Data API 请求，等价 `POST /ejson`）/ `sql`（stdin 一条 SQL，等价 `POST /sql`）/ `config`（stdin 一个 cfgsync 配置文档，等价 `POST /config`，输出 `{version}`）/ `configget`（查询当前中央配置文档，等价 `GET /config`），输出均为 EJSON/JSON |
| `role.cli.configMode` | optional | `set` | `function=config` 的发布模式：`set`（整树替换）/ `append`（include/exclude 并集合并，等价 `POST /config?mode=append`） |

`function=file` 与 `function=backfill` 是仅有的两个读取 `source` 段的 cli 功能：`function=file` 分发处（`internal/role/cli/role.go`）解码 `source.file.*`，校验 `source.file.paths` 非空后才连 Mongo（空则 fail-fast `cli: function=file requires source.file.paths`），跑完后向 stdout 打印与 `function=upload` 相同的 stats JSON；`function=backfill` 则解码 `source.mem.*`（中转源缓冲容量）连同 `backfill.*` 一并传入 `RunBackfill`。gateway / daemon **没有** file/backfill 入口（v1.6 需求 §7）。

`function=backfill` 同理是唯一读取 `backfill.*` 段的 cli 功能：分发处先 `FromTree` 解码 + `Validate`（在连 Mongo **之前**做完校验），跑完向 stdout 打印 `api.Result` JSON，**不读 stdin**。gateway / daemon 同样**没有** backfill 入口（v1.6 需求 §7：无同步 `POST /backfill`）。

完整样例：[daemon](../../examples/config/daemon/daemon.max.yaml)、
[gateway](../../examples/config/gateway/gateway.max.yaml)、
[cli upload](../../examples/config/cli/cli.upload.max.yaml)、
[cli file](../../examples/config/cli/cli.file.max.yaml)、
[cli backfill](../../examples/config/cli/cli.backfill.max.yaml)、
[cli ejson](../../examples/config/cli/cli.ejson.max.yaml)、
[cli sql](../../examples/config/cli/cli.sql.max.yaml)。

---

## 默认值矩阵（逐键速查，CFG-16）

各模块 `ApplyDefaults()` 实际填入的业务默认值汇总（与上面分段 schema 一致；`required` 项无默认，须显式给出）。回归断言见 `doc/ultra_test.md` CFG-16，源在各模块 `config.go`。

| 配置键 | 默认值 | 源（`ApplyDefaults`/常量） |
|--------|--------|------|
| `logging.level` | `info` | `internal/logging` |
| `logging.format` | `text` | `internal/logging` |
| `dao.mongo.uri` | **required**（无默认） | `mongo.Config.Validate` |
| `dao.mongo.connectTimeout` | `10s` | `internal/dao/mongo` |
| `dao.mongo.serverSelectionTimeout` | `30s` | `internal/dao/mongo` |
| `dao.store.maxElapsedTime` | `10s` | `internal/dao/store` |
| `parser.filter.include` | `[]`（全放行） | `internal/parser/filter` |
| `parser.filter.exclude` | `[]` | `internal/parser/filter` |
| `source.tailer.logPattern` | **required:daemon**（无默认） | `daemon.NewFromTree` 强制 |
| `source.tailer.tailMode` | `hybrid` | `internal/source/tailer` |
| `source.tailer.rescanInterval` | `30s` | `internal/source/tailer` |
| `source.tailer.pollInterval` | `200ms` | `internal/source/tailer` |
| `source.tailer.maxLineBytes` | `10485760`（10MiB） | `defaultMaxLineSize`（`tailer.go`） |
| `source.tailer.maxOpenFDs` | `0`（关闭） | `internal/source/tailer`（负值归一为 `0`） |
| `source.file.paths` | **required:cli `function=file`**（无默认） | `cli` 分发（`role.go`）/ `Engine.File` 强制 |
| `source.file.maxLineBytes` | `10485760`（10MiB） | `internal/source/file`（与 tailer 共用 `defaultMaxLineSize`） |
| `source.mem.bufferSize` | `2000` | `internal/source/mem`（`<=0` 归一为 `defaultBuffer`；cli `function=backfill` / `Engine.RunBackfill` 用） |
| `backfill.apiBaseURL` | **required**（无默认） | `internal/backfill`（`Validate`） |
| `backfill.token` | **required**（无默认） | `internal/backfill`（`Validate`） |
| `backfill.proxy` | `""` | `internal/backfill` |
| `backfill.projectID` | **required `>0`**（无默认） | `internal/backfill`（`Validate`） |
| `backfill.table` | `event` | `internal/backfill` |
| `backfill.events` | `[]` | `internal/backfill` |
| `backfill.schemaPrefix` | `""` | `internal/backfill` |
| `backfill.userTimeColumn` | `#update_time` | `internal/backfill`（`ApplyDefaults`；仅用户表→`#time` 映射） |
| `backfill.eventTimeColumn` | `#event_time` | `internal/backfill`（`ApplyDefaults`；仅事件表→`#time` 映射） |
| `backfill.userOrderBy` | `""` | `internal/backfill`（仅用户表 SQL 加 `ORDER BY`；配 limit 取 top-N） |
| `backfill.userWhere` | `""` | `internal/backfill`（仅用户表 SQL 加 `WHERE` 分片谓词；分布式编排取单分片用） |
| `backfill.eventWhere` | `""` | `internal/backfill`（仅事件表 SQL 加 `WHERE` 分片谓词；把一天再切成定行数小片用） |
| `backfill.partDateRange.start` | **required:event 表**（无默认） | `internal/backfill`（`Validate`） |
| `backfill.partDateRange.end` | **required:event 表**（无默认） | `internal/backfill`（`Validate`） |
| `backfill.eventTimeRange.start` | `""`（可选） | `internal/backfill` |
| `backfill.eventTimeRange.end` | `""`（可选） | `internal/backfill` |
| `backfill.limit` | `0`（不限） | `internal/backfill` |
| `backfill.pageSize` | `10000`（最小 `1000`） | `internal/backfill` |
| `backfill.paginate` | `true`（tri-state `*bool`） | `internal/backfill`（`RegisterDefaults`） |
| `backfill.pageRetries` | `3` | `internal/backfill` |
| `backfill.pollInterval` | `3s` | `internal/backfill` |
| `backfill.pollTimeout` | `30m` | `internal/backfill` |
| `backfill.forceSkipExisting` | `true`（tri-state `*bool`） | `internal/backfill`（`RegisterDefaults`） |
| `process.mode` | `batch`（daemon 忽略，固定 pipeline） | `internal/process` |
| `process.batchSize` | `1000` | `internal/process` |
| `process.pipeline.batchSize` | `1000` | `internal/process/pipeline` |
| `process.pipeline.batchSizeMin` | `0`（自动 = batchSize/4） | `internal/process/pipeline` |
| `process.pipeline.batchSizeMax` | `0`（自动 = batchSize*2） | `internal/process/pipeline` |
| `process.pipeline.batchWorkers` | `2` | `internal/process/pipeline` |
| `process.pipeline.flushInterval` | `1s` | `internal/process/pipeline` |
| `process.pipeline.channelBuffer` | `0`(自动 = batchSize*2) | `internal/process/pipeline` |
| `process.pipeline.deadLetterCap` | `128` | `internal/process/pipeline` |
| `process.pipeline.maxStuckDuration` | `3m`（`0` = 取默认，负值 = 无限背压） | `internal/process/pipeline` |
| `cfgsync.enabled` | `false` | `internal/cfgsync` |
| `cfgsync.backend` | `poll` | `internal/cfgsync` |
| `cfgsync.documentID` | `filter` | `internal/cfgsync` |
| `cfgsync.pollInterval` | `5s` | `internal/cfgsync` |
| `cfgsync.reconcileInterval` | `60s` | `internal/cfgsync` |
| `cfgsync.collection` | `_tango_config` | `internal/cfgsync` |
| `role.mode` | `daemon` | `internal/role` |
| `role.gateway.addr` | `:8080` | `internal/role/gateway` |
| `role.cli.function` | `upload` | `internal/role/cli` |

---

## 上报 filter

上报 filter（顶层 `parser.filter`，daemon 与 gateway 上传共享）维度为
`#type` / `#event_name` / `properties.*`，用 `include` / `exclude`（expr-lang）
表达式。示例（作用于扁平化记录，`#` 前缀字段可直接引用）：

```yaml
parser:
  filter:
    include:
      - '#type == "track" && #event_name in ["login", "pay"]'
      - '#type startsWith "user_"'
    exclude:
      - 'properties.is_loadtest == true'
```

被过滤掉的记录**不写 dead_letter**，是有意丢弃。
