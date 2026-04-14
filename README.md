# golang-iptables

WebSocket 网关代理服务，支持高并发、单后端防护转发、限流和 IP 封禁。

## 功能特性

- **HTTP/WebSocket 代理**：支持 HTTP 和 WebSocket 两种协议的代理转发
- **单后端代理**：HTTP/WebSocket 均转发到单一后端，专注防护能力
- **连接数限制**：基于 Redis 的分布式连接数控制（可配置）
- **滑动窗口限流**：基于 Redis 的滑动窗口限流（可配置）
- **IP 黑名单**：自动封禁超限 IP，并同步到 ipset
- **安全加固**：支持 Upgrade 前限流/鉴权、X-Forwarded-For 防伪造、Header/Slowloris 防护
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
    "password": ""
  },
  "rateLimit": {
    "maxRequests": 100,
    "globalMaxRequests": 5000,
    "windowSec": 10,
    "maxConn": 5
  },
  "auth": {
    "token": "123456"
  },
  "trustedProxies": [
    "127.0.0.1/8",
    "::1/128"
  ],
  "limits": {
    "maxConnections": 2000,
    "maxBodySizeMB": 2,
    "webSocketBufferSize": 262144,
    "webSocketMaxLifetimeSec": 1800,
    "ipsetConcurrency": 20,
    "ipsetTimeoutSec": 3,
    "machineHealthCheckSec": 2,
    "machineMaxCPUPercent": 92,
    "machineMaxLoadPerCpu": 1.5,
    "machineMaxMemoryPercent": 95,
    "machineUnhealthyThreshold": 3,
    "machineRecoveryThreshold": 3,
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
- `rateLimit.maxRequests`：限流窗口内最大请求数（默认 100）
- `rateLimit.globalMaxRequests`：全局限流窗口内最大请求数（默认 5000）
- `rateLimit.windowSec`：限流窗口时间（秒，默认 10）
- `rateLimit.maxConn`：单个 IP 最大并发连接数（默认 5）
- `auth.token`：WebSocket Upgrade 前 Header 鉴权 token
- `trustedProxies`：可信代理来源列表（支持 IP/CIDR），仅来自这些地址时才信任 `X-Forwarded-For`
- `limits.maxConnections`：全局最大连接数（默认 5000）
- `limits.maxBodySizeMB`：HTTP 请求体最大大小（MB，默认 10）
- `limits.webSocketBufferSize`：WebSocket 缓冲区大小（bytes，默认 1048576）
- `limits.webSocketMaxLifetimeSec`：WebSocket 最大连接生命周期（秒，默认 7200）
- `limits.ipsetConcurrency`：ipset 并发数（默认 10）
- `limits.ipsetTimeoutSec`：ipset 命令超时（秒，默认 5）
- `limits.machineHealthCheckSec`：机器健康检查间隔（秒，默认 2）
- `limits.machineMaxCPUPercent`：CPU 使用率熔断阈值（%，默认 92）
- `limits.machineMaxLoadPerCpu`：每核 Load 熔断阈值（默认 1.5）
- `limits.machineMaxMemoryPercent`：内存使用率熔断阈值（%，默认 95）
- `limits.machineUnhealthyThreshold`：连续异常次数触发熔断（默认 3）
- `limits.machineRecoveryThreshold`：连续恢复次数关闭熔断（默认 3）
- `limits.authTimeoutSec`：认证超时（保留字段，当前版本未使用）
- `limits.shutdownTimeoutSec`：优雅关闭超时（秒，默认 30）

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

### WebSocket 接入

**接入流程：**

1. 客户端连接到网关 WebSocket 端口
2. 网关在 `Upgrade` 前先执行黑名单检查、限流、连接数限制
3. 网关在 `Upgrade` 前校验鉴权 Header（`Authorization` 或 `X-Auth-Token`）
4. 通过后才升级 WebSocket，并与后端建立连接双向转发
5. 网关每 30 秒发送 Ping，连接读超时约 65 秒，达到最大生命周期后会主动断开

**客户端代码示例：**

```javascript
// Node.js 示例（ws 库），可设置 Header
import WebSocket from 'ws';

const ws = new WebSocket('ws://localhost:8080', {
  headers: {
    Authorization: 'Bearer 123456',
    // 或者: 'X-Auth-Token': '123456'
  },
});

// 接收消息
ws.onmessage = (event) => {
  console.log('收到消息:', event.data);
};

// 发送业务消息
ws.onopen = () => {
  ws.send('hello');
};
```

**鉴权说明：**
- Header 鉴权，支持：
  - `Authorization: Bearer <token>`
  - `X-Auth-Token: <token>`
- 鉴权在 `Upgrade` 前执行，失败返回 `401 Unauthorized`
- 浏览器原生 WebSocket 无法自定义 Header，浏览器接入需通过反向代理注入 Header 或改造鉴权方式

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

**客户端代码示例：**

```javascript
// 普通请求
fetch('http://localhost:8080/api/test')
  .then(response => response.json())
  .then(data => console.log(data));

// 带认证头的请求
fetch('http://localhost:8080/api/test', {
  headers: {
    'Authorization': 'Bearer your-token'
  }
});
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
- 限流触发后采用渐进式封禁：前 2 次不封禁，随后按 1 分钟、5 分钟、10 分钟递进

### 热重载

修改 `config.json` 配置文件后，服务会自动重新加载配置，无需重启。

## ipset 配置

服务启动时会自动检测并初始化 ipset 和 iptables：

- 自动检测操作系统（仅支持 Linux）
- 自动检测并安装 ipset（支持 apt/yum）
- 自动检测并安装 iptables（支持 apt/yum）
- 自动创建 ipset blacklist 集合（hash:ip timeout 600）
- 自动添加 iptables 规则（匹配 blacklist 集合并 DROP）

如果自动初始化失败，请手动执行：

```bash
# 创建 blacklist 集合
sudo ipset create blacklist hash:ip timeout 600

# 查看 blacklist 集合
sudo ipset list blacklist

# 添加 iptables 规则
sudo iptables -I INPUT -m set --match-set blacklist src -j DROP

# 删除 IP
sudo ipset del blacklist 1.2.3.4
```

## 注意事项

1. Redis 必须正常运行，否则服务无法启动
2. ipset 需要管理员权限，确保运行用户有权限执行 ipset 命令
3. 配置文件热重载支持 backends、rateLimit 和 auth 配置，Redis 配置修改需要重启服务
4. 鉴权 token 通过配置文件 `auth.token` 设置，生产环境请使用安全的 token
5. `X-Forwarded-For` 仅在可信代理来源（`trustedProxies`）下被信任，外部直连请求不能依赖伪造 XFF

## 许可证

Apache License 2.0（`Apache-2.0`），详见 `LICENSE` 文件。

## 作者

- John James
- 邮箱：nbjohn999@gmail.com
