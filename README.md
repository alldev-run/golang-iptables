# golang-iptables

WebSocket 网关代理服务，支持高并发、单后端防护转发、限流和 IP 封禁。

## 功能特性

- **HTTP/WebSocket 代理**：支持 HTTP 和 WebSocket 两种协议的代理转发
- **单后端代理**：HTTP/WebSocket 均转发到单一后端，专注防护能力
- **连接数限制**：基于 Redis 的分布式连接数控制（可配置）
- **滑动窗口限流**：基于 Redis 的滑动窗口限流（可配置）
- **IP 黑名单**：自动封禁超限 IP，并同步到 ipset
- **Redis 故障降级**：Redis 不可用时网关继续服务（请求放行 + 本地兜底封禁）
- **集群实时封禁同步（可选）**：基于 memberlist Gossip 的 MQ 广播，节点自动发现并实时下发 ipset
- **安全加固**：支持 Upgrade 前限流/鉴权、X-Forwarded-For 防伪造、Header/Slowloris 防护
- **管理接口访问控制**：支持基于网段/IP 的 `/admin/ban` 访问限制
- **配置热重载**：支持运行时动态修改配置文件
- **高并发安全**：使用 sync.RWMutex 和 context 确保线程安全

## 依赖

- Go 1.25+
- Redis 服务器
- ipset（Linux 系统）

## 安装

```bash
go mod download
```

## 配置

创建 `config.json` 配置文件：

```json
{
  "backend": "127.0.0.1:9502",
  "redis": {
    "addr": "127.0.0.1:6379",
    "db": 14,
    "password": "",
    "adminBanQueue": "",
    "adminUnbanQueue": "",
    "opTimeoutMs": 500,
    "fastFailWindowMs": 2000,
    "dialTimeoutMs": 500,
    "readTimeoutMs": 500,
    "writeTimeoutMs": 500,
    "poolTimeoutMs": 500,
    "poolSize": 128,
    "minIdleConns": 16,
    "maxRetries": -1
  },
  "cluster": {
    "enable": false,
    "bindAddr": "0.0.0.0",
    "bindPort": 7946,
    "join": [],
    "sourceAllowCidrs": [],
    "secret": "",
    "retransmitMult": 3,
    "nodeName": "",
    "publishQueueSize": 1024,
    "publishBatchSize": 32,
    "publishFlushMs": 100,
    "publishMaxPerSec": 500
  },
  "rateLimit": {
    "maxRequests": 100,
    "globalMaxRequests": 5000,
    "windowSec": 10,
    "maxConn": 5
  },
  "auth": {
    "token": "123456",
    "adminToken": ""
  },
  "trustedProxies": [
    "127.0.0.1/8",
    "::1/128"
  ],
  "adminAllowedNetworks": [
    "127.0.0.1/8",
    "::1/128"
  ],
  "enableWebSocket": true,
  "limits": {
    "maxConnections": 2000,
    "maxBodySizeMB": 2,
    "webSocketBufferSize": 262144,
    "webSocketMaxLifetimeSec": 1800,
    "ipsetConcurrency": 20,
    "ipsetTimeoutSec": 3,
    "ipsetHashSize": 262144,
    "ipsetMaxElem": 1048576,
    "machineHealthCheckSec": 2,
    "machineMaxCPUPercent": 92,
    "machineMaxLoadPerCpu": 1.5,
    "machineMaxMemoryPercent": 95,
    "machineUnhealthyThreshold": 3,
    "machineRecoveryThreshold": 3,
    "ipsetSyncIntervalSec": 30,
    "ipsetCacheMaxEntries": 200000,
    "ipsetSyncMaxPerRound": 50000,
    "blacklistIpKeyMaxLen": 64,
    "localBanPersistEnabled": true,
    "localBanPersistFile": "./data/local_bans.json",
    "localBanPersistWindowSec": 1800,
    "localBanPersistFlushMs": 3000,
    "localBanPersistMaxEntries": 50000,
    "authTimeoutSec": 3,
    "shutdownTimeoutSec": 15
  }
}
```

### 配置说明

- `backend`：单一后端服务地址（支持 HTTP 和 WebSocket）
- `redis.addr`：Redis 服务器地址
- `redis.db`：Redis 数据库编号
- `redis.password`：Redis 密码（无密码留空）
- `redis.adminBanQueue`：Redis 订阅队列名（封禁队列，留空表示禁用）
- `redis.adminUnbanQueue`：Redis 订阅队列名（解封队列，留空表示禁用）
- `redis.opTimeoutMs`：Redis 操作超时（毫秒，默认 `500`）
- `redis.fastFailWindowMs`：Redis 快速失败窗口（毫秒，默认 `2000`）
- `redis.dialTimeoutMs`：Redis 建连超时（毫秒，默认 `500`）
- `redis.readTimeoutMs`：Redis 读超时（毫秒，默认 `500`）
- `redis.writeTimeoutMs`：Redis 写超时（毫秒，默认 `500`）
- `redis.poolTimeoutMs`：Redis 连接池等待超时（毫秒，默认 `500`）
- `redis.poolSize`：Redis 连接池大小（默认 `128`）
- `redis.minIdleConns`：Redis 最小空闲连接数（默认 `16`）
- `redis.maxRetries`：Redis 命令重试次数（默认 `-1`，表示禁用重试）
- `cluster.enable`：是否启用 Gossip 集群同步（默认 false）
- `cluster.bindAddr`：memberlist 监听地址（默认 `0.0.0.0`）
- `cluster.bindPort`：memberlist 监听端口（默认 `7946`）
- `cluster.join`：启动时尝试加入的种子节点列表（如 `"10.0.0.2:7946"`）
- `cluster.sourceAllowCidrs`：允许接收 Gossip 事件的来源 CIDR 列表（为空表示不过滤）
- `cluster.secret`：事件签名密钥（建议生产环境配置，空则不验签）
- `cluster.retransmitMult`：Gossip 重传倍数（默认 `3`）
- `cluster.nodeName`：节点名（默认 `hostname:bindPort`）
- `cluster.publishQueueSize`：本地广播队列长度（默认 `1024`）
- `cluster.publishBatchSize`：单次批量发送上限（默认 `32`）
- `cluster.publishFlushMs`：批处理刷盘周期毫秒（默认 `100`）
- `cluster.publishMaxPerSec`：每秒广播上限（默认 `500`）
- `rateLimit.maxRequests`：限流窗口内最大请求数（默认 100）
- `rateLimit.globalMaxRequests`：全局限流窗口内最大请求数（默认 5000）
- `rateLimit.windowSec`：限流窗口时间（秒，默认 10）
- `rateLimit.maxConn`：单个 IP 最大并发连接数（默认 5）
- `auth.token`：WebSocket Upgrade 前 Header 鉴权 token（已废弃，使用 enableWebSocket 控制）
- `auth.adminToken`：内部管理接口认证 token（用于手动封禁 IP）
- `enableWebSocket`：是否启用 WebSocket 转发（默认 true，设为 false 则禁用 WebSocket）
- `trustedProxies`：可信代理来源列表（支持 IP/CIDR），仅来自这些地址时才信任 `X-Forwarded-For`
- `adminAllowedNetworks`：允许访问内部管理接口的来源列表（支持 IP/CIDR，默认仅本机网段）
- `limits.maxConnections`：全局最大连接数（默认 5000）
- `limits.maxBodySizeMB`：HTTP 请求体最大大小（MB，默认 10）
- `limits.webSocketBufferSize`：WebSocket 缓冲区大小（bytes，默认 1048576）
- `limits.webSocketMaxLifetimeSec`：WebSocket 最大连接生命周期（秒，默认 7200）
- `limits.ipsetConcurrency`：ipset 并发数（默认 10），同时用于计算 admin ban 处理并发和队列容量（见下方“队列大小控制”）
- `limits.ipsetTimeoutSec`：ipset 命令超时（秒，默认 5）
- `limits.ipsetHashSize`：ipset `blacklist` 集合 `hashsize`（默认 262144）
- `limits.ipsetMaxElem`：ipset `blacklist` 集合 `maxelem`（默认 1048576）
- `limits.ipsetSyncIntervalSec`：ipset 从 Redis 同步间隔（秒，默认 30，0 表示不同步）
- `limits.ipsetCacheMaxEntries`：ipset 本地缓存最大条目数（默认 200000，用于控制内存上限）
- `limits.ipsetSyncMaxPerRound`：每轮 Redis→ipset 最大同步条目数（默认 50000，防止单轮同步过载）
- `limits.blacklistIpKeyMaxLen`：blacklist key 中 IP 最大长度（默认 64，过滤异常 key）
- `limits.localBanPersistEnabled`：是否启用本地短期封禁持久化（默认 true）
- `limits.localBanPersistFile`：本地短期封禁快照文件路径（默认 `./data/local_bans.json`）
- `limits.localBanPersistWindowSec`：本地短期封禁持久化窗口（秒，默认 1800，范围 600~3600）
- `limits.localBanPersistFlushMs`：本地短期封禁快照刷盘间隔（毫秒，默认 3000）
- `limits.localBanPersistMaxEntries`：本地短期封禁快照最大条目（默认 50000）
- `limits.machineHealthCheckSec`：机器健康检查间隔（秒，默认 2）
- `limits.machineMaxCPUPercent`：CPU 使用率熔断阈值（%，默认 92）
- `limits.machineMaxLoadPerCpu`：每核 Load 熔断阈值（默认 1.5）
- `limits.machineMaxMemoryPercent`：内存使用率熔断阈值（%，默认 95）
- `limits.machineUnhealthyThreshold`：连续异常次数触发熔断（默认 3）
- `limits.machineRecoveryThreshold`：连续恢复次数关闭熔断（默认 3）
- `limits.authTimeoutSec`：认证超时（保留字段，当前版本未使用）
- `limits.shutdownTimeoutSec`：优雅关闭超时（秒，默认 30）

### 队列大小控制（admin ban 相关）

`/admin/ban` 采用异步队列模型，队列容量由 `limits.ipsetConcurrency` 间接控制：

- admin worker 数：`max(1, floor(ipsetConcurrency / 2))`
- 主任务队列 `adminBanQueue`：`workerCount * 1024`
- 溢出队列 `adminBanOverflowQueue`：`workerCount * 2048`
- fallback ipset 队列 `adminFallbackIPSetQueue`：`workerCount * 2048`

调优建议：

- 如果压测中看到大量溢出日志（`admin_queue_overflow_*`），优先增大 `limits.ipsetConcurrency`
- `ipsetConcurrency` 越大，worker 与队列越大，但会增加 CPU/内存占用
- 如果出现 `Hash is full, cannot add more elements`，请增大 `limits.ipsetMaxElem`（必要时同步增大 `limits.ipsetHashSize`）

## 启动

```bash
# 使用默认配置文件 config.json
go run main.go 8080

# 指定配置文件路径
go run main.go 8080 /path/to/config.json
```

## 测试

运行测试：

```bash
go test -v
```

## 使用示例

### 集群模式（自动发现 + 实时下发）

1. 所有节点使用相同的 `cluster.secret`
2. 至少指定一个种子节点到 `cluster.join`
3. 在所有节点启用 `cluster.enable=true`
4. 任一节点触发封禁后，本机先立即执行 `ipset`，再进入 Gossip 广播队列（限速 + 批量）
5. 其它节点收到事件后会做去重、源节点判断、来源 CIDR 白名单校验，再执行 `ipset`

> 说明：Redis 在本项目中保持弱依赖。即使 Redis 不可用，本地限流与本地兜底封禁仍可工作；集群内封禁传播由 memberlist Gossip 负责。

### 本地短期持久化（推荐）

- 模型：`本机内存 + 本地文件`，不依赖 Redis
- 目的：节点重启后自动恢复近期封禁，避免瞬时空窗
- 建议窗口：`10 分钟 ~ 1 小时`（程序会自动约束在该范围）
- 恢复逻辑：启动时回放未过期封禁并立即补写本机 `ipset`

**本地三节点联调（快速验证）：**

```bash
bash scripts/run-cluster-demo.sh
```

- 脚本会启动 3 个节点（自动 join）并打印日志目录
- 可通过节点1调用 `/admin/ban`，观察节点2/3实时接收并下发 ipset
- `Ctrl+C` 后会自动清理临时配置和进程

### WebSocket 接入

**接入流程：**

1. 客户端连接到网关 WebSocket 端口
2. 网关检查 `enableWebSocket` 配置，若为 false 则拒绝连接
3. 网关在 `Upgrade` 前先执行黑名单检查、限流、连接数限制
4. 通过后才升级 WebSocket，并与后端建立连接双向转发
5. 后端连接失败时会进行最多 3 次指数退避重试
6. 网关每 30 秒发送 Ping，连接读超时约 65 秒，达到最大生命周期后会主动断开

**客户端代码示例：**

```javascript
// Node.js 示例（ws 库）
import WebSocket from 'ws';

const ws = new WebSocket('ws://localhost:8080');

// 接收消息
ws.onmessage = (event) => {
  console.log('收到消息:', event.data);
};

// 发送业务消息
ws.onopen = () => {
  ws.send('hello');
};
```

**配置说明：**
- 通过配置文件中的 `enableWebSocket` 字段控制是否启用 WebSocket 转发（默认 true）
- 设为 `false` 时，所有 WebSocket 连接请求将被拒绝
- 不再需要 Header 鉴权，可直接连接

**心跳说明：**
- 网关每 30 秒发送一次 Ping
- 客户端需在约 65 秒内响应 Pong（WebSocket 标准自动处理）
- 心跳超时会导致连接断开
- 客户端无需额外实现心跳，WebSocket 标准库会自动响应

**消息大小限制：**
- WebSocket 单条消息大小受 `limits.maxBodySizeMB` 限制（默认 10MB）
- 超过上限会被连接读限制拒绝

### HTTP 接入

**接入流程：**

1. 客户端发送 HTTP 请求到网关
2. 网关将请求转发到单一后端
3. 请求转发到后端，响应返回给客户端

**保护行为：**
- 请求体大小受 `limits.maxBodySizeMB` 限制（超限直接拒绝）
- 后端响应体在已知 `Content-Length` 且超限时会被拒绝
- 反向代理传输层限制 `MaxConnsPerHost`，避免单后端被过量并发打爆

**客户端代码示例：**

```javascript
// 普通请求
fetch('http://localhost:8080/api/test')
  .then(response => response.json())
  .then(data => console.log(data));
```

**后端路由说明：**
- 当前为单后端模式，HTTP/WebSocket 均转发到 `backend`

### 机器健康熔断

- 网关会后台采样机器健康指标（Linux）：CPU、每核 Load、内存占用
- 当连续 `limits.machineUnhealthyThreshold` 次超过阈值时，自动熔断并返回 `503`
- 当连续 `limits.machineRecoveryThreshold` 次恢复健康后，自动退出熔断
- 建议结合 `config.emergency.json` 在高压时进一步收紧限流

**HTTP 安全与连接保护：**
- 已启用 `ReadHeaderTimeout`、`IdleTimeout` 和 `MaxHeaderBytes`，用于缓解 Slowloris 和超大 Header 攻击

### 后端服务要求

**WebSocket 后端：**
- 需监听 WebSocket 连接（无需额外鉴权，网关已鉴权）
- 需支持 WebSocket 长连接
- 建议实现心跳机制（可选）

**HTTP 后端：**
- 需监听 HTTP 请求
- 无需特殊配置，标准 HTTP 服务即可

### 限流规则

- 每个 IP 最多 `rateLimit.maxConn` 个并发连接（默认 5）
- 每 `rateLimit.windowSec` 秒最多 `rateLimit.maxRequests` 个请求（默认 10 秒 100 个请求）
- 每 `rateLimit.windowSec` 秒最多 `rateLimit.globalMaxRequests` 个全局请求（默认 10 秒 5000 个请求）
- 限流触发后采用渐进式封禁：前 2 次不封禁，随后按 1 分钟、5 分钟、10 分钟递进，并在重复违规时进行封禁时长累加
- 单个 IP 的累计封禁时长上限为 30 天（超过后封顶）
- Redis 不可用时，分布式计数会降级放行，但仍会通过本地违规计数触发本地封禁兜底

**累加示例（同一 IP 持续违规）：**
- 第 3 次：1 分钟
- 第 4 次：2 分钟（1m + 1m）
- 第 5 次：3 分钟（2m + 1m）
- 第 6 次：8 分钟（3m + 5m）
- 第 10 次：累计时长继续增加，但最大不超过 30 天

### 热重载

修改 `config.json` 配置文件后，服务会自动重新加载配置，无需重启。

### 内部黑名单管理接口

网关提供内部管理接口，允许内部程序手动添加 IP 到黑名单。

**接口地址：** `POST /admin/ban`

**认证方式：**
- Header 中携带 admin token：
  - `Authorization: Bearer <adminToken>`
  - 或 `X-Admin-Token: <adminToken>`

**来源限制：**
- 请求来源 IP 必须命中 `adminAllowedNetworks`（支持 CIDR 或单 IP）
- 即使 token 正确，若来源不在允许列表也会返回 `401 Unauthorized`

**Redis 故障兜底：**
- 当 Redis 不可用或写入失败时，`/admin/ban` 仍会执行本地封禁（本地缓存 + ipset）
- Redis 恢复后，后续封禁会继续写入 Redis

**队列与回退路径（高压场景）：**
- 请求优先进入 `adminBanQueue` 异步处理
- 主队列满时，先做本地兜底，再进入 `adminBanOverflowQueue` 批量回灌
- 溢出持续拥堵时，任务会走受限并发的直接执行回退
- fallback 中的 ipset 更新通过 `adminFallbackIPSetQueue` 异步执行，避免请求线程被 ipset 慢路径阻塞

**Redis 订阅队列（可选）：**
- 可通过 `redis.adminBanQueue` / `redis.adminUnbanQueue` 配置两个独立订阅队列
- 服务启动后会订阅这两个队列，并将消息转为 `adminBanTask` 走同一套异步处理链路
- 两个队列名不能相同（配置校验会拒绝）

封禁队列消息格式（`redis.adminBanQueue`）：

```json
{
  "ip": "1.2.3.4",
  "duration": 600,
  "reason": "from redis queue"
}
```

- 也支持纯字符串消息（仅 IP），例如：`1.2.3.4`
- `duration` 单位为秒；缺省时默认 `600`

解封队列消息格式（`redis.adminUnbanQueue`）：

```json
{
  "ip": "1.2.3.4"
}
```

- 也支持纯字符串消息（仅 IP），例如：`1.2.3.4`

**请求参数（JSON）：**
```json
{
  "ip": "1.2.3.4",
  "duration": 600,
  "reason": "manual ban"
}
```
- `ip`：必填，要封禁的 IP 地址
- `duration`：可选，封禁时长（秒），默认 600 秒（10 分钟）
- `reason`：可选，封禁原因

**响应示例（成功）：**
```json
{
  "status": "success",
  "ip": "1.2.3.4",
  "duration": "10m0s"
}
```

**使用示例：**
```bash
curl -X POST http://localhost:8080/admin/ban \
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"ip": "1.2.3.4", "duration": 3600, "reason": "abuse"}'
```

**安全提示：**
- admin token 仅用于内部管理接口，请妥善保管
- 建议通过内网或防火墙限制访问 `/admin/ban` 接口
- 生产环境请修改默认的 admin token

## ipset 配置

服务启动时会自动检测并初始化 ipset 和 iptables：

- 自动检测操作系统（仅支持 Linux）
- 自动检测并安装 ipset（支持 apt/yum）
- 自动检测并安装 iptables（支持 apt/yum）
- 自动创建 ipset 集合（blacklist4 用于 IPv4，blacklist6 用于 IPv6）
- 自动添加 iptables/ip6tables 规则（匹配对应集合并 DROP）

如果自动初始化失败，请手动执行：

```bash
# 创建 IPv4 blacklist4 集合
sudo ipset create blacklist4 hash:ip family inet timeout 600

# 创建 IPv6 blacklist6 集合
sudo ipset create blacklist6 hash:ip family inet6 timeout 600

# 查看 IPv4 集合
sudo ipset list blacklist4

# 查看 IPv6 集合
sudo ipset list blacklist6

# 添加 iptables 规则（IPv4）
sudo iptables -I INPUT -m set --match-set blacklist4 src -j DROP

# 添加 ip6tables 规则（IPv6）
sudo ip6tables -I INPUT -m set --match-set blacklist6 src -j DROP

# 删除 IPv4 IP
sudo ipset del blacklist4 1.2.3.4

# 删除 IPv6 IP
sudo ipset del blacklist6 2001:db8::1
```

## 注意事项

1. Redis 建议保持可用；若 Redis 不可用，服务会降级运行（请求放行 + 本地兜底封禁，Redis 分布式能力暂不生效）
2. ipset 需要管理员权限，确保运行用户有权限执行 ipset 命令
3. 配置文件热重载支持 backends、rateLimit 和 auth 配置，Redis 配置修改需要重启服务
4. 鉴权 token 通过配置文件 `auth.token` 设置，生产环境请使用安全的 token
5. `X-Forwarded-For` 仅在可信代理来源（`trustedProxies`）下被信任，外部直连请求不能依赖伪造 XFF
6. Redis 不可用时服务会降级继续运行：黑名单查询/限流/分布式连接计数会放行，但封禁路径会走本地兜底并同步 ipset；恢复后自动继续使用 Redis 能力
7. 为避免长 TTL 和高基数 IP 导致内存增长，黑名单本地缓存和 ipset 本地缓存都包含容量上限与淘汰保护

## 许可证

Apache License 2.0（`Apache-2.0`），详见 `LICENSE` 文件。

## 作者

- John James
- 邮箱：nbjohn999@gmail.com
