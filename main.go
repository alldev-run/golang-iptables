package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	// Redis
	rdb *redis.Client

	// 限流序列号
	rateLimitSeq uint64

	// ipset 去重锁
	ipsetMutex sync.Mutex
	ipsetCache = make(map[string]time.Time)

	blacklistSetMaxTTLScript = redis.NewScript(`
local ttl = redis.call("PTTL", KEYS[1])
local newTTL = tonumber(ARGV[1])
local newLevel = tonumber(ARGV[2])
if ttl < 0 then
	redis.call("SET", KEYS[1], ARGV[3], "PX", newTTL)
	redis.call("SET", KEYS[2], newLevel, "PX", newTTL)
	return 1
end
local currentLevel = tonumber(redis.call("GET", KEYS[2]) or "0")
if newLevel > currentLevel then
	redis.call("PEXPIRE", KEYS[1], newTTL)
	redis.call("SET", KEYS[2], newLevel, "PX", newTTL)
	return 1
end
return 0
`)

	rateLimitGlobalAndIPScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local globalLimit = tonumber(ARGV[3])
local ipLimit = tonumber(ARGV[4])

redis.call("ZADD", KEYS[1], now, ARGV[5])
redis.call("ZREMRANGEBYSCORE", KEYS[1], "0", tostring(now - window))
local globalCount = redis.call("ZCARD", KEYS[1])
redis.call("PEXPIRE", KEYS[1], window)
if globalCount > globalLimit then
	return 0
end

redis.call("ZADD", KEYS[2], now, ARGV[6])
redis.call("ZREMRANGEBYSCORE", KEYS[2], "0", tostring(now - window))
local ipCount = redis.call("ZCARD", KEYS[2])
redis.call("PEXPIRE", KEYS[2], window)
if ipCount > ipLimit then
	return 1
end

return 2
`)

	connWindowLimitScript = redis.NewScript(`
local val = redis.call("INCR", KEYS[1])
redis.call("EXPIRE", KEYS[1], tonumber(ARGV[1]))
if val <= tonumber(ARGV[2]) then
	return 1
end
return 0
`)

	proxyTransport = &http.Transport{
		Proxy:                  http.ProxyFromEnvironment,
		DialContext:            (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:           100,
		IdleConnTimeout:        90 * time.Second,
		TLSHandshakeTimeout:    5 * time.Second,
		ExpectContinueTimeout:  1 * time.Second,
		ResponseHeaderTimeout:  5 * time.Second,
		MaxResponseHeaderBytes: 1 << 20,
	}

	// ipset 并发限制（防止 ipset 慢或卡住时拖垮服务）
	ipsetSem chan struct{}

	// 全局连接数限制（防止攻击时创建过多 goroutine）
	connSem chan struct{}

	// 配置文件路径
	configFile = "config.json"

	// 全局配置
	cfg Config

	httpReverseProxy atomic.Value

	trustedProxyMu   sync.RWMutex
	trustedProxyNets []*net.IPNet

	machineHealthMu        sync.Mutex
	machinePrevCPUSnapshot cpuSnapshot
	machineHasPrevCPU      bool
	machineUnhealthyStreak int
	machineHealthyStreak   int
	machineCircuitOpen     atomic.Bool
	machineCircuitReason   atomic.Value
)

type Config struct {
	Backend        string          `json:"backend"`
	Redis          RedisConfig     `json:"redis"`
	RateLimit      RateLimitConfig `json:"rateLimit"`
	Auth           AuthConfig      `json:"auth"`
	TrustedProxies []string        `json:"trustedProxies"`
	Limits         LimitConfig     `json:"limits"`
	EnableWebSocket bool           `json:"enableWebSocket"`
}

type RedisConfig struct {
	Addr     string `json:"addr"`
	DB       int    `json:"db"`
	Password string `json:"password"`
}

type RateLimitConfig struct {
	MaxRequests       int `json:"maxRequests"`
	GlobalMaxRequests int `json:"globalMaxRequests"`
	WindowSec         int `json:"windowSec"`
	MaxConn           int `json:"maxConn"`
}

type AuthConfig struct {
	Token      string `json:"token"`
	AdminToken string `json:"adminToken"`
}

type LimitConfig struct {
	MaxConnections      int `json:"maxConnections"`      // 全局最大连接数
	MaxBodySizeMB       int `json:"maxBodySizeMB"`       // HTTP 请求体最大大小 (MB)
	WebSocketBufferSize int `json:"webSocketBufferSize"` // WebSocket 缓冲区大小 (bytes)
	WebSocketMaxLifetimeSec int `json:"webSocketMaxLifetimeSec"` // WebSocket 最大连接生命周期 (秒)
	IpsetConcurrency    int `json:"ipsetConcurrency"`    // ipset 并发数
	IpsetTimeoutSec     int `json:"ipsetTimeoutSec"`     // ipset 命令超时 (秒)
	MachineHealthCheckSec   int     `json:"machineHealthCheckSec"`   // 机器健康检查间隔 (秒)
	MachineMaxCPUPercent    float64 `json:"machineMaxCPUPercent"`    // CPU 使用率阈值 (%)
	MachineMaxLoadPerCPU    float64 `json:"machineMaxLoadPerCpu"`    // 每核 Load 阈值
	MachineMaxMemoryPercent float64 `json:"machineMaxMemoryPercent"` // 内存使用率阈值 (%)
	MachineUnhealthyThreshold int   `json:"machineUnhealthyThreshold"` // 连续异常次数触发熔断
	MachineRecoveryThreshold  int   `json:"machineRecoveryThreshold"`  // 连续恢复次数关闭熔断
	IpsetSyncIntervalSec int `json:"ipsetSyncIntervalSec"` // ipset 从 Redis 同步间隔 (秒，0 表示不同步)
	AuthTimeoutSec      int `json:"authTimeoutSec"`      // 认证超时 (秒)
	ShutdownTimeoutSec  int `json:"shutdownTimeoutSec"`  // 优雅关闭超时 (秒)
}

type cpuSnapshot struct {
	total uint64
	idle  uint64
}

func defaultConfig() Config {
	return Config{
		Backend: "127.0.0.1:9502",
		Redis: RedisConfig{
			Addr:     "127.0.0.1:6379",
			DB:       14,
			Password: "",
		},
		RateLimit: RateLimitConfig{
			MaxRequests:       100,
			GlobalMaxRequests: 5000,
			WindowSec:         10,
			MaxConn:           5,
		},
		Auth: AuthConfig{
			Token:      "123456",
			AdminToken: "",
		},
		TrustedProxies: []string{"127.0.0.1/8", "::1/128"},
		Limits: LimitConfig{
			MaxConnections:      5000,
			MaxBodySizeMB:       10,
			WebSocketBufferSize: 1024 * 1024,
			WebSocketMaxLifetimeSec: 7200,
			IpsetConcurrency:    10,
			IpsetTimeoutSec:     5,
			MachineHealthCheckSec:   2,
			MachineMaxCPUPercent:    92,
			MachineMaxLoadPerCPU:    1.5,
			MachineMaxMemoryPercent: 95,
			MachineUnhealthyThreshold: 3,
			MachineRecoveryThreshold:  3,
			IpsetSyncIntervalSec: 30,
			AuthTimeoutSec:      3,
			ShutdownTimeoutSec:  30,
		},
		EnableWebSocket: true,
	}
}

func normalizeConfig(c *Config) {
	c.Backend = strings.TrimSpace(c.Backend)
	if c.Backend == "" {
		c.Backend = "127.0.0.1:9502"
	}

	filteredTrustedProxies := make([]string, 0, len(c.TrustedProxies))
	seenTrustedProxies := make(map[string]struct{}, len(c.TrustedProxies))
	for _, proxy := range c.TrustedProxies {
		proxy = strings.TrimSpace(proxy)
		if proxy == "" {
			continue
		}
		if _, exists := seenTrustedProxies[proxy]; exists {
			continue
		}
		seenTrustedProxies[proxy] = struct{}{}
		filteredTrustedProxies = append(filteredTrustedProxies, proxy)
	}
	if len(filteredTrustedProxies) == 0 {
		filteredTrustedProxies = []string{"127.0.0.1/8", "::1/128"}
	}
	c.TrustedProxies = filteredTrustedProxies

	if c.RateLimit.MaxRequests <= 0 {
		c.RateLimit.MaxRequests = 100
	}
	if c.RateLimit.GlobalMaxRequests <= 0 {
		c.RateLimit.GlobalMaxRequests = c.RateLimit.MaxRequests * 50
	}
	if c.RateLimit.WindowSec <= 0 {
		c.RateLimit.WindowSec = 10
	}
	if c.RateLimit.MaxConn <= 0 {
		c.RateLimit.MaxConn = 5
	}

	if c.Limits.MaxConnections <= 0 {
		c.Limits.MaxConnections = 5000
	}
	if c.Limits.MaxBodySizeMB <= 0 {
		c.Limits.MaxBodySizeMB = 10
	}
	if c.Limits.WebSocketBufferSize <= 0 {
		c.Limits.WebSocketBufferSize = 1024 * 1024
	}
	if c.Limits.WebSocketMaxLifetimeSec <= 0 {
		c.Limits.WebSocketMaxLifetimeSec = 7200
	}
	if c.Limits.IpsetConcurrency <= 0 {
		c.Limits.IpsetConcurrency = 10
	}
	if c.Limits.IpsetTimeoutSec <= 0 {
		c.Limits.IpsetTimeoutSec = 5
	}
	if c.Limits.MachineHealthCheckSec <= 0 {
		c.Limits.MachineHealthCheckSec = 2
	}
	if c.Limits.MachineMaxCPUPercent <= 0 {
		c.Limits.MachineMaxCPUPercent = 92
	}
	if c.Limits.MachineMaxLoadPerCPU <= 0 {
		c.Limits.MachineMaxLoadPerCPU = 1.5
	}
	if c.Limits.MachineMaxMemoryPercent <= 0 {
		c.Limits.MachineMaxMemoryPercent = 95
	}
	if c.Limits.MachineUnhealthyThreshold <= 0 {
		c.Limits.MachineUnhealthyThreshold = 3
	}
	if c.Limits.MachineRecoveryThreshold <= 0 {
		c.Limits.MachineRecoveryThreshold = 3
	}
	if c.Limits.AuthTimeoutSec <= 0 {
		c.Limits.AuthTimeoutSec = 3
	}
	if c.Limits.ShutdownTimeoutSec <= 0 {
		c.Limits.ShutdownTimeoutSec = 30
	}
}

func parseTrustedProxy(entry string) (*net.IPNet, error) {
	entry = strings.TrimSpace(entry)
	if entry == "" {
		return nil, fmt.Errorf("empty trusted proxy")
	}

	if strings.Contains(entry, "/") {
		_, ipNet, err := net.ParseCIDR(entry)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted proxy CIDR %q: %w", entry, err)
		}
		return ipNet, nil
	}

	ip := net.ParseIP(entry)
	if ip == nil {
		return nil, fmt.Errorf("invalid trusted proxy IP %q", entry)
	}

	bits := 32
	if ip.To4() == nil {
		bits = 128
	}

	return &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)}, nil
}

func buildTrustedProxyNets(entries []string) ([]*net.IPNet, error) {
	nets := make([]*net.IPNet, 0, len(entries))
	for _, entry := range entries {
		ipNet, err := parseTrustedProxy(entry)
		if err != nil {
			return nil, err
		}
		nets = append(nets, ipNet)
	}
	return nets, nil
}

func isTrustedProxyIP(ip net.IP) bool {
	if ip == nil {
		return false
	}

	trustedProxyMu.RLock()
	defer trustedProxyMu.RUnlock()

	for _, ipNet := range trustedProxyNets {
		if ipNet.Contains(ip) {
			return true
		}
	}

	return false
}

func applyConfig(newCfg Config) error {
	normalizeConfig(&newCfg)
	if newCfg.Backend == "" {
		return fmt.Errorf("配置文件中 backend 不能为空")
	}

	parsedTrustedProxyNets, err := buildTrustedProxyNets(newCfg.TrustedProxies)
	if err != nil {
		return fmt.Errorf("trustedProxies 配置无效: %w", err)
	}

	cfg = newCfg
	trustedProxyMu.Lock()
	trustedProxyNets = parsedTrustedProxyNets
	trustedProxyMu.Unlock()

	targetURL, err := url.Parse("http://" + cfg.Backend)
	if err != nil {
		return fmt.Errorf("backend 配置无效: %w", err)
	}
	reverseProxy := httputil.NewSingleHostReverseProxy(targetURL)
	reverseProxy.Transport = proxyTransport
	reverseProxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Println("代理错误:", err)
		http.Error(w, "Bad Gateway", 502)
	}
	httpReverseProxy.Store(reverseProxy)

	upgrader.ReadBufferSize = cfg.Limits.WebSocketBufferSize
	upgrader.WriteBufferSize = cfg.Limits.WebSocketBufferSize

	ipsetSem = make(chan struct{}, cfg.Limits.IpsetConcurrency)
	connSem = make(chan struct{}, cfg.Limits.MaxConnections)

	return nil
}

// ---------------- Redis初始化 ----------------
func initRedis(addr string, db int, password string) {
	rdb = redis.NewClient(&redis.Options{
		Addr:     addr,
		DB:       db,
		Password: password,
	})
}

// ---------------- 配置文件加载 ----------------
func loadConfig() error {
	data, err := os.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("读取配置文件失败: %w", err)
	}

	var loadedCfg Config
	if err := json.Unmarshal(data, &loadedCfg); err != nil {
		return fmt.Errorf("解析配置文件失败: %w", err)
	}

	if err := applyConfig(loadedCfg); err != nil {
		return err
	}

	log.Println("配置已加载, 后端:", cfg.Backend)
	return nil
}

// ---------------- 配置文件监听 ----------------
func watchConfig() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Println("创建文件监听器失败:", err)
		return
	}
	defer watcher.Close()

	absPath, err := filepath.Abs(configFile)
	if err != nil {
		log.Println("获取配置文件绝对路径失败:", err)
		return
	}

	configDir := filepath.Dir(absPath)
	if err := watcher.Add(configDir); err != nil {
		log.Println("添加监听目录失败:", err)
		return
	}

	log.Println("开始监听配置文件变化:", absPath)

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			// 监听写入和重命名事件
			if (event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Rename == fsnotify.Rename || event.Op&fsnotify.Create == fsnotify.Create) && filepath.Base(event.Name) == filepath.Base(configFile) {
				// 等待文件写入完成
				time.Sleep(100 * time.Millisecond)
				if err := loadConfig(); err != nil {
					log.Println("热重载配置失败:", err)
				} else {
					log.Println("配置热重载成功")
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Println("文件监听错误:", err)
		}
	}
}

// ---------------- 获取IP ----------------
func getClientIP(r *http.Request) string {
	remoteHost, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		remoteHost = strings.TrimSpace(r.RemoteAddr)
	}

	remoteIP := net.ParseIP(remoteHost)
	if remoteIP == nil {
		return remoteHost
	}

	// 仅在可信代理来源下信任 X-Forwarded-For，避免被外部直连请求伪造
	if remoteIP.IsLoopback() || isTrustedProxyIP(remoteIP) {
		xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For"))
		if xff != "" {
			first := strings.TrimSpace(strings.Split(xff, ",")[0])
			if forwardedIP := net.ParseIP(first); forwardedIP != nil {
				return forwardedIP.String()
			}
		}
	}

	return remoteIP.String()
}

// ---------------- 检测是否为WebSocket请求 ----------------
func isWebSocket(r *http.Request) bool {
	if !cfg.EnableWebSocket {
		return false
	}
	return strings.ToLower(r.Header.Get("Upgrade")) == "websocket" &&
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

// ---------------- 黑名单 ----------------
func isBlacklisted(ctx context.Context, ip string) bool {
	val, err := rdb.Exists(ctx, "blacklist:"+ip).Result()
	if err != nil {
		log.Println("Redis error in isBlacklisted:", err)
		return false
	}
	return val == 1
}

func banDurationByStrikes(strikes int64) time.Duration {
	switch {
	case strikes < 3:
		return 0
	case strikes < 6:
		return 1 * time.Minute
	case strikes < 10:
		return 5 * time.Minute
	default:
		return 10 * time.Minute
	}
}

func banLevelByStrikes(strikes int64) int {
	switch {
	case strikes < 3:
		return 0
	case strikes < 6:
		return 1
	case strikes < 10:
		return 2
	default:
		return 3
	}
}

func setBlacklistWithMaxTTL(ctx context.Context, key string, levelKey string, duration time.Duration, level int) (bool, error) {
	ms := duration.Milliseconds()
	if ms <= 0 {
		ms = 1
	}
	updated, err := blacklistSetMaxTTLScript.Run(ctx, rdb, []string{key, levelKey}, ms, level, "1").Int64()
	if err != nil {
		return false, err
	}
	return updated == 1, nil
}

func syncIPSetBan(ip string, banDuration time.Duration) {
	expireAt := time.Now().Add(banDuration)
	ipLocal := ip

	ipsetMutex.Lock()
	currentExpireAt, ok := ipsetCache[ipLocal]
	if ok && !expireAt.After(currentExpireAt) {
		ipsetMutex.Unlock()
		return
	}
	ipsetCache[ipLocal] = expireAt
	ipsetMutex.Unlock()

	go func(expectedExpireAt time.Time) {
		sleepFor := time.Until(expectedExpireAt)
		if sleepFor > 0 {
			time.Sleep(sleepFor)
		}
		ipsetMutex.Lock()
		current, exists := ipsetCache[ipLocal]
		if exists && !current.After(expectedExpireAt) {
			delete(ipsetCache, ipLocal)
		}
		ipsetMutex.Unlock()
	}(expireAt)

	go func(expectedExpireAt time.Time) {
		sem := ipsetSem
		if sem == nil {
			log.Println("ipset semaphore 未初始化，跳过 ipset 同步")
			return
		}

		ipsetMutex.Lock()
		current, exists := ipsetCache[ipLocal]
		if !exists || current.After(expectedExpireAt) {
			ipsetMutex.Unlock()
			return
		}
		ipsetMutex.Unlock()

		sem <- struct{}{}
		defer func() { <-sem }()

		ipsetMutex.Lock()
		current, exists = ipsetCache[ipLocal]
		if !exists || current.After(expectedExpireAt) {
			ipsetMutex.Unlock()
			return
		}
		ipsetMutex.Unlock()

		cmdCtx, cmdCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Limits.IpsetTimeoutSec)*time.Second)
		defer cmdCancel()

		banSeconds := int(time.Until(expectedExpireAt).Seconds())
		if banSeconds <= 0 {
			return
		}

		cmd := exec.CommandContext(cmdCtx, "ipset", "-exist", "add", "blacklist", ipLocal, "timeout", fmt.Sprintf("%d", banSeconds))
		if output, err := cmd.CombinedOutput(); err != nil {
			log.Printf("ipset error: %v, output=%s", err, strings.TrimSpace(string(output)))
		}
	}(expireAt)
}

func syncIPSetFromRedis() {
	if rdb == nil {
		return
	}

	ctx := context.Background()
	var cursor uint64
	pattern := "blacklist:*"
	count := int64(100)

	for {
		keys, nextCursor, err := rdb.Scan(ctx, cursor, pattern, count).Result()
		if err != nil {
			log.Printf("Redis scan error in syncIPSetFromRedis: %v", err)
			return
		}

		for _, key := range keys {
			ip := strings.TrimPrefix(key, "blacklist:")
			ttl, err := rdb.TTL(ctx, key).Result()
			if err != nil {
				log.Printf("Redis TTL error for %s: %v", key, err)
				continue
			}

			if ttl > 0 {
				ipsetMutex.Lock()
				currentExpireAt, ok := ipsetCache[ip]
				newExpireAt := time.Now().Add(ttl)
				if !ok || newExpireAt.After(currentExpireAt) {
					ipsetCache[ip] = newExpireAt
				}
				ipsetMutex.Unlock()

				go func(ip string, banDuration time.Duration) {
					sem := ipsetSem
					if sem == nil {
						return
					}
					sem <- struct{}{}
					defer func() { <-sem }()

					banSeconds := int(banDuration.Seconds())
					if banSeconds <= 0 {
						return
					}

					cmdCtx, cmdCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Limits.IpsetTimeoutSec)*time.Second)
					defer cmdCancel()

					cmd := exec.CommandContext(cmdCtx, "ipset", "-exist", "add", "blacklist", ip, "timeout", fmt.Sprintf("%d", banSeconds))
					if output, err := cmd.CombinedOutput(); err != nil {
						log.Printf("ipset sync error for %s: %v, output=%s", ip, err, strings.TrimSpace(string(output)))
					}
				}(ip, ttl)
			}
		}

		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}
}

func startIPSetSync() {
	if cfg.Limits.IpsetSyncIntervalSec <= 0 {
		log.Println("ipset 定期同步已禁用")
		return
	}

	go func() {
		ticker := time.NewTicker(time.Duration(cfg.Limits.IpsetSyncIntervalSec) * time.Second)
		defer ticker.Stop()

		log.Printf("启动 ipset 定期同步，间隔 %d 秒", cfg.Limits.IpsetSyncIntervalSec)
		for {
			select {
			case <-ticker.C:
				log.Println("开始从 Redis 同步黑名单到 ipset")
				syncIPSetFromRedis()
			}
		}
	}()
}

func banIP(ctx context.Context, ip string) {
	strikeKey := "abuse:" + ip
	strikes, err := rdb.Incr(ctx, strikeKey).Result()
	if err != nil {
		log.Println("Redis error in banIP strike increment:", err)
		return
	}
	if err := rdb.Expire(ctx, strikeKey, 30*time.Minute).Err(); err != nil {
		log.Println("Redis error in banIP strike expire:", err)
	}

	banDuration := banDurationByStrikes(strikes)
	banLevel := banLevelByStrikes(strikes)
	if banDuration == 0 {
		log.Printf("IP %s 触发限流，累计违规次数=%d，未封禁（渐进式策略）", ip, strikes)
		return
	}

	updated, err := setBlacklistWithMaxTTL(ctx, "blacklist:"+ip, "blacklist_level:"+ip, banDuration, banLevel)
	if err != nil {
		log.Println("Redis error in banIP:", err)
		return
	}
	if !updated {
		return
	}

	log.Printf("封禁IP: %s, 违规次数=%d, 封禁时长=%s", ip, strikes, banDuration)

	syncIPSetBan(ip, banDuration)
}

// ---------------- 分布式连接数 ----------------
func incConn(ctx context.Context, ip string) (bool, error) {
	// 使用时间窗口前缀防止单 key 热点
	timeWindow := time.Now().Unix() / 60 // 每分钟一个窗口
	key := "conn:" + ip + ":" + strconv.FormatInt(timeWindow, 10)
	allowed, err := connWindowLimitScript.Run(ctx, rdb, []string{key}, 60, cfg.RateLimit.MaxConn).Int64()
	if err != nil {
		return false, err
	}
	return allowed == 1, nil
}

func decConn(ctx context.Context, ip string) {
	timeWindow := time.Now().Unix() / 60
	key := "conn:" + ip + ":" + strconv.FormatInt(timeWindow, 10)
	if err := rdb.Decr(ctx, key).Err(); err != nil {
		log.Println("Redis error in decConn:", err)
	}
}

// ---------------- 滑动窗口限流 ----------------
func allowRequestWithReason(ctx context.Context, ip string) (bool, string) {
	now := time.Now().UnixMilli()
	member := strconv.FormatInt(now, 10) + "-" + strconv.FormatUint(atomic.AddUint64(&rateLimitSeq, 1), 10)
	windowSec := cfg.RateLimit.WindowSec
	windowMillis := int64(windowSec) * 1000
	if windowMillis <= 0 {
		windowMillis = 10000
	}
	if cfg.RateLimit.GlobalMaxRequests <= 0 {
		return false, "global"
	}
	if cfg.RateLimit.MaxRequests <= 0 {
		return false, "ip"
	}

	result, err := rateLimitGlobalAndIPScript.Run(
		ctx,
		rdb,
		[]string{"rate:global", "rate:ip:" + ip},
		now,
		windowMillis,
		cfg.RateLimit.GlobalMaxRequests,
		cfg.RateLimit.MaxRequests,
		member+"-g",
		member+"-i",
	).Int64()
	if err != nil {
		log.Println("Redis error in allowRequest:", err)
		return false, "redis"
	}

	switch result {
	case 2:
		return true, ""
	case 1:
		return false, "ip"
	case 0:
		return false, "global"
	default:
		log.Printf("Unexpected rate limit result: %d", result)
		return false, "redis"
	}
}

func allowRequest(ctx context.Context, ip string) bool {
	allowed, _ := allowRequestWithReason(ctx, ip)
	return allowed
}

// ---------------- 鉴权 ----------------
func auth(r *http.Request) bool {
	token := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[7:])
	}
	if token == "" {
		token = strings.TrimSpace(r.Header.Get("X-Auth-Token"))
	}
	return token != "" && token == cfg.Auth.Token
}

func adminAuth(r *http.Request) bool {
	token := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[7:])
	}
	if token == "" {
		token = strings.TrimSpace(r.Header.Get("X-Admin-Token"))
	}
	return token != "" && token == cfg.Auth.AdminToken
}

// ---------------- 内部管理接口 ----------------
func adminBanHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !adminAuth(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var req struct {
		IP       string `json:"ip"`
		Duration int    `json:"duration"` // seconds
		Reason   string `json:"reason"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.IP == "" {
		http.Error(w, "IP is required", http.StatusBadRequest)
		return
	}

	parsedIP := net.ParseIP(req.IP)
	if parsedIP == nil {
		http.Error(w, "Invalid IP address", http.StatusBadRequest)
		return
	}

	ipStr := parsedIP.String()
	duration := 10 * time.Minute
	if req.Duration > 0 {
		duration = time.Duration(req.Duration) * time.Second
	}

	if rdb == nil {
		log.Printf("Redis client not initialized in admin ban")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	ctx := context.Background()
	blacklistKey := "blacklist:" + ipStr
	levelKey := "blacklist_level:" + ipStr
	if _, err := setBlacklistWithMaxTTL(ctx, blacklistKey, levelKey, duration, 3); err != nil {
		log.Printf("Redis error in admin ban: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	syncIPSetBan(ipStr, duration)

	reason := req.Reason
	if reason == "" {
		reason = "manual ban"
	}
	log.Printf("Admin manual ban: ip=%s, duration=%v, reason=%s", ipStr, duration, reason)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"ip":      ipStr,
		"duration": duration.String(),
	})
}

// ---------------- 心跳 ----------------
func heartbeat(ws *wsWriteConn, ctx context.Context, cancel context.CancelFunc) {
	const wsReadTimeout = 65 * time.Second
	ws.conn.SetPongHandler(func(string) error {
		ws.conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
		return nil
	})
	_ = ws.conn.SetReadDeadline(time.Now().Add(wsReadTimeout))

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				err := ws.WriteMessage(websocket.PingMessage, nil)
				if err != nil {
					cancel()
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

type wsWriteConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (w *wsWriteConn) WriteMessage(messageType int, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.conn.WriteMessage(messageType, data)
}

func (w *wsWriteConn) WriteControl(messageType int, data []byte, deadline time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.conn.WriteControl(messageType, data, deadline)
}

func isDialFailure(err error) bool {
	if err == nil {
		return false
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) && strings.EqualFold(opErr.Op, "dial") {
		return true
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return isDialFailure(urlErr.Err)
	}

	errText := strings.ToLower(err.Error())
	return strings.Contains(errText, "dial tcp") || strings.Contains(errText, "connection refused")
}

func readCPUSnapshot() (cpuSnapshot, error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return cpuSnapshot{}, err
	}
	line := strings.SplitN(string(data), "\n", 2)[0]
	fields := strings.Fields(line)
	if len(fields) < 5 || fields[0] != "cpu" {
		return cpuSnapshot{}, fmt.Errorf("invalid /proc/stat format")
	}

	var total uint64
	values := make([]uint64, 0, len(fields)-1)
	for _, field := range fields[1:] {
		v, parseErr := strconv.ParseUint(field, 10, 64)
		if parseErr != nil {
			return cpuSnapshot{}, parseErr
		}
		total += v
		values = append(values, v)
	}

	idle := values[3]
	if len(values) > 4 {
		idle += values[4]
	}

	return cpuSnapshot{total: total, idle: idle}, nil
}

func cpuUsagePercent(prev cpuSnapshot, curr cpuSnapshot) (float64, bool) {
	if curr.total <= prev.total || curr.idle < prev.idle {
		return 0, false
	}
	totalDelta := curr.total - prev.total
	if totalDelta == 0 {
		return 0, false
	}
	idleDelta := curr.idle - prev.idle
	used := float64(totalDelta-idleDelta) / float64(totalDelta) * 100
	if used < 0 {
		used = 0
	}
	if used > 100 {
		used = 100
	}
	return used, true
}

func readLoadPerCPU() (float64, error) {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(string(data))
	if len(fields) == 0 {
		return 0, fmt.Errorf("invalid /proc/loadavg format")
	}
	load1, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, err
	}
	cpuCount := runtime.NumCPU()
	if cpuCount <= 0 {
		cpuCount = 1
	}
	return load1 / float64(cpuCount), nil
}

func readMemoryUsagePercent() (float64, error) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	var total float64
	var available float64
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			total, err = strconv.ParseFloat(fields[1], 64)
			if err != nil {
				return 0, err
			}
		case "MemAvailable:":
			available, err = strconv.ParseFloat(fields[1], 64)
			if err != nil {
				return 0, err
			}
		}
	}
	if total <= 0 {
		return 0, fmt.Errorf("MemTotal not found in /proc/meminfo")
	}
	if available < 0 {
		available = 0
	}
	used := (total - available) / total * 100
	if used < 0 {
		used = 0
	}
	if used > 100 {
		used = 100
	}
	return used, nil
}

func evaluateMachineHealth() (bool, string) {
	if runtime.GOOS != "linux" {
		return true, ""
	}

	reasons := make([]string, 0, 3)

	if curr, err := readCPUSnapshot(); err == nil {
		machineHealthMu.Lock()
		if machineHasPrevCPU {
			if usage, ok := cpuUsagePercent(machinePrevCPUSnapshot, curr); ok && usage > cfg.Limits.MachineMaxCPUPercent {
				reasons = append(reasons, fmt.Sprintf("cpu=%.1f%%", usage))
			}
		}
		machinePrevCPUSnapshot = curr
		machineHasPrevCPU = true
		machineHealthMu.Unlock()
	}

	if loadPerCPU, err := readLoadPerCPU(); err == nil {
		if loadPerCPU > cfg.Limits.MachineMaxLoadPerCPU {
			reasons = append(reasons, fmt.Sprintf("loadPerCPU=%.2f", loadPerCPU))
		}
	}

	if memPercent, err := readMemoryUsagePercent(); err == nil {
		if memPercent > cfg.Limits.MachineMaxMemoryPercent {
			reasons = append(reasons, fmt.Sprintf("mem=%.1f%%", memPercent))
		}
	}

	if len(reasons) == 0 {
		return true, ""
	}
	return false, strings.Join(reasons, ",")
}

func updateMachineCircuitState(healthy bool, reason string) {
	machineHealthMu.Lock()
	defer machineHealthMu.Unlock()

	if healthy {
		machineUnhealthyStreak = 0
		machineHealthyStreak++
		if machineCircuitOpen.Load() && machineHealthyStreak >= cfg.Limits.MachineRecoveryThreshold {
			machineCircuitOpen.Store(false)
			machineCircuitReason.Store("")
			log.Println("机器健康熔断已恢复")
		}
		return
	}

	machineHealthyStreak = 0
	machineUnhealthyStreak++
	machineCircuitReason.Store(reason)
	if !machineCircuitOpen.Load() && machineUnhealthyStreak >= cfg.Limits.MachineUnhealthyThreshold {
		machineCircuitOpen.Store(true)
		log.Printf("机器健康熔断已触发: %s", reason)
	}
}

func machineCircuitStatus() (bool, string) {
	if !machineCircuitOpen.Load() {
		return false, ""
	}
	reason, _ := machineCircuitReason.Load().(string)
	return true, reason
}

func startMachineHealthCircuitBreaker() {
	if runtime.GOOS != "linux" {
		log.Println("机器健康熔断仅支持 Linux，当前系统跳过")
		return
	}

	check := func() {
		healthy, reason := evaluateMachineHealth()
		updateMachineCircuitState(healthy, reason)
	}

	check()
	ticker := time.NewTicker(time.Duration(cfg.Limits.MachineHealthCheckSec) * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		check()
	}
}

func validateRequestHeaders(headers http.Header) bool {
	const maxHeaderCount = 100
	const maxSingleHeaderValueBytes = 8 * 1024
	const maxTotalHeaderBytes = 64 * 1024

	if len(headers) > maxHeaderCount {
		return false
	}

	total := 0
	for name, values := range headers {
		total += len(name)
		if len(name) > 256 {
			return false
		}
		for _, value := range values {
			if len(value) > maxSingleHeaderValueBytes {
				return false
			}
			total += len(value)
			if total > maxTotalHeaderBytes {
				return false
			}
		}
	}

	return true
}

// ---------------- HTTP代理处理器 ----------------
func httpProxyHandler(w http.ResponseWriter, r *http.Request) {
	if !validateRequestHeaders(r.Header) {
		http.Error(w, "Request Header Fields Too Large", http.StatusRequestHeaderFieldsTooLarge)
		return
	}
	if open, _ := machineCircuitStatus(); open {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	// 全局连接数限制（防止攻击时创建过多 goroutine）
	select {
	case connSem <- struct{}{}:
	default:
		http.Error(w, "Too many connections", 429)
		return
	}
	defer func() { <-connSem }()

	ctx := context.Background()
	ip := getClientIP(r)

	// 限制请求体大小（防止超大 body 打爆内存）
	r.Body = http.MaxBytesReader(w, r.Body, int64(cfg.Limits.MaxBodySizeMB)*1024*1024)

	// 黑名单
	if isBlacklisted(ctx, ip) {
		http.Error(w, "Forbidden", 403)
		return
	}

	// 限流
	if allowed, reason := allowRequestWithReason(ctx, ip); !allowed {
		if reason == "ip" {
			banIP(ctx, ip)
		}
		http.Error(w, "Too many requests", 429)
		return
	}

	// 使用单一后端
	loadedProxy := httpReverseProxy.Load()
	proxy, ok := loadedProxy.(*httputil.ReverseProxy)
	if !ok || proxy == nil {
		http.Error(w, "Bad Gateway", 502)
		return
	}

	// 添加X-Forwarded-For头
	r.Header.Set("X-Forwarded-For", ip)

	proxy.ServeHTTP(w, r)
}

// ---------------- WebSocket代理处理器 ----------------
func websocketProxyHandler(w http.ResponseWriter, r *http.Request) {
	if open, _ := machineCircuitStatus(); open {
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	// 全局连接数限制（防止攻击时创建过多 goroutine）
	select {
	case connSem <- struct{}{}:
	default:
		http.Error(w, "Too many connections", 429)
		return
	}
	defer func() { <-connSem }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Limits.WebSocketMaxLifetimeSec)*time.Second)
	defer cancel()

	ip := getClientIP(r)

	// 黑名单
	if isBlacklisted(ctx, ip) {
		http.Error(w, "Forbidden", 403)
		return
	}

	// Upgrade 前限流，提前拒绝恶意请求
	if allowed, reason := allowRequestWithReason(ctx, ip); !allowed {
		if reason == "ip" {
			banIP(ctx, ip)
		}
		http.Error(w, "Too many requests", 429)
		return
	}

	// Upgrade 前连接数限制
	allowed, err := incConn(ctx, ip)
	if err != nil {
		log.Println("Redis error in incConn:", err)
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}
	if !allowed {
		http.Error(w, "Too many connections", 429)
		return
	}
	defer decConn(ctx, ip)

	// 升级
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	clientWS := &wsWriteConn{conn: conn}

	// 心跳
	heartbeat(clientWS, ctx, cancel)

	// 使用单一后端
	target := cfg.Backend
	targetConn, _, err := websocket.DefaultDialer.Dial("ws://"+target, nil)
	if err != nil {
		log.Println("后端连接失败:", target, err)
		_ = clientWS.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "backend connection error"))
		return
	}
	defer targetConn.Close()
	backendWS := &wsWriteConn{conn: targetConn}
	maxWSMessageSize := int64(cfg.Limits.MaxBodySizeMB) * 1024 * 1024
	if maxWSMessageSize <= 0 {
		maxWSMessageSize = 10 * 1024 * 1024
	}
	conn.SetReadLimit(maxWSMessageSize)
	targetConn.SetReadLimit(maxWSMessageSize)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		const wsReadTimeout = 65 * time.Second
		for {
			_ = conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				if gctx.Err() != nil {
					return gctx.Err()
				}
				return err
			}

			if allowed, reason := allowRequestWithReason(ctx, ip); !allowed {
				if reason == "ip" {
					banIP(ctx, ip)
				}
				return fmt.Errorf("websocket message rejected by rate limit: %s", reason)
			}

			if err := backendWS.WriteMessage(mt, msg); err != nil {
				return err
			}
		}
	})

	g.Go(func() error {
		const backendReadTimeout = 65 * time.Second
		for {
			_ = targetConn.SetReadDeadline(time.Now().Add(backendReadTimeout))
			mt, msg, err := targetConn.ReadMessage()
			if err != nil {
				if gctx.Err() != nil {
					return gctx.Err()
				}
				return err
			}

			if err := clientWS.WriteMessage(mt, msg); err != nil {
				return err
			}
		}
	})

	g.Go(func() error {
		<-gctx.Done()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			_ = clientWS.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "connection lifetime exceeded"), time.Now().Add(time.Second))
		}
		_ = conn.Close()
		_ = targetConn.Close()
		return nil
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
			log.Println("WebSocket proxy error:", err)
		}
	}
}

// ---------------- 核心代理 ----------------
func proxyHandler(w http.ResponseWriter, r *http.Request) {
	if isWebSocket(r) {
		websocketProxyHandler(w, r)
	} else {
		httpProxyHandler(w, r)
	}
}

// ---------------- 系统初始化 ----------------
func initSystem() error {
	// 检测操作系统
	if runtime.GOOS != "linux" {
		log.Println("警告: 当前系统不是 Linux，跳过 ipset/iptables 初始化")
		return nil
	}

	log.Println("开始系统初始化...")

	// 检查并安装 ipset
	if err := ensureCommand("ipset", []string{"apt-get install -y ipset", "yum install -y ipset"}); err != nil {
		return fmt.Errorf("安装 ipset 失败: %w", err)
	}

	// 检查并安装 iptables
	if err := ensureCommand("iptables", []string{"apt-get install -y iptables", "yum install -y iptables"}); err != nil {
		return fmt.Errorf("安装 iptables 失败: %w", err)
	}

	// 创建 ipset blacklist 集合
	if err := createIpsetBlacklist(); err != nil {
		return fmt.Errorf("创建 ipset blacklist 失败: %w", err)
	}

	// 添加 iptables 规则
	if err := addIptablesRule(); err != nil {
		return fmt.Errorf("添加 iptables 规则失败: %w", err)
	}

	log.Println("系统初始化完成")
	return nil
}

// 检查命令是否存在，不存在则尝试安装
func ensureCommand(cmd string, installCmds []string) error {
	if _, err := exec.LookPath(cmd); err == nil {
		log.Printf("%s 已安装", cmd)
		return nil
	}

	log.Printf("%s 未安装，尝试安装...", cmd)

	for _, installCmd := range installCmds {
		// 检测包管理器
		parts := strings.Fields(installCmd)
		if len(parts) == 0 {
			continue
		}
		pkgManager := parts[0]

		if _, err := exec.LookPath(pkgManager); err != nil {
			continue
		}

		log.Printf("使用 %s 安装 %s", pkgManager, cmd)

		// 需要 sudo 权限
		cmdArgs := strings.Fields(installCmd)
		installCmdFull := append([]string{"sudo"}, cmdArgs...)

		if output, err := exec.Command(installCmdFull[0], installCmdFull[1:]...).CombinedOutput(); err != nil {
			log.Printf("安装失败: %s", string(output))
			continue
		}

		log.Printf("%s 安装成功", cmd)
		return nil
	}

	return fmt.Errorf("无法安装 %s，请手动安装", cmd)
}

// 创建 ipset blacklist 集合
func createIpsetBlacklist() error {
	// 检查集合是否已存在
	_, err := exec.Command("ipset", "list", "blacklist").CombinedOutput()
	if err == nil {
		log.Println("ipset blacklist 集合已存在")
		return nil
	}

	log.Println("创建 ipset blacklist 集合...")

	// 创建集合
	cmd := exec.Command("sudo", "ipset", "create", "blacklist", "hash:ip", "timeout", "600")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("创建 ipset 失败: %s", string(output))
	}

	log.Println("ipset blacklist 集合创建成功")
	return nil
}

// 添加 iptables 规则
func addIptablesRule() error {
	// 检查规则是否已存在
	output, err := exec.Command("sudo", "iptables", "-L", "INPUT", "-n", "--line-numbers").CombinedOutput()
	if err != nil {
		log.Printf("获取 iptables 规则失败: %s", string(output))
		return err
	}

	// 检查是否已存在 blacklist 规则
	if strings.Contains(string(output), "match-set blacklist") {
		log.Println("iptables 规则已存在")
		return nil
	}

	log.Println("添加 iptables 规则...")

	// 添加规则
	cmd := exec.Command("sudo", "iptables", "-I", "INPUT", "-m", "set", "--match-set", "blacklist", "src", "-j", "DROP")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("添加 iptables 规则失败: %s", string(output))
	}

	log.Println("iptables 规则添加成功")
	return nil
}

// ---------------- 主函数 ----------------
func main() {
	if len(os.Args) < 2 {
		fmt.Println("用法: go run main.go 端口 [配置文件路径]")
		fmt.Println("示例: go run main.go 8080 config.json")
		return
	}

	port := os.Args[1]
	if len(os.Args) >= 3 {
		configFile = os.Args[2]
	}

	// 加载配置文件
	if err := loadConfig(); err != nil {
		log.Println("加载配置文件失败:", err)
		log.Println("使用默认配置")
		if err := applyConfig(defaultConfig()); err != nil {
			log.Fatal("应用默认配置失败:", err)
		}
	}

	// 初始化 Redis
	initRedis(cfg.Redis.Addr, cfg.Redis.DB, cfg.Redis.Password)

	// 系统初始化（ipset/iptables）
	if err := initSystem(); err != nil {
		log.Println("系统初始化失败:", err)
		log.Println("请手动配置 ipset 和 iptables")
	}

	// 启动配置文件监听
	go watchConfig()
	go startMachineHealthCircuitBreaker()
	go startIPSetSync()

	http.HandleFunc("/", proxyHandler)
	http.HandleFunc("/admin/ban", adminBanHandler)

	srv := &http.Server{
		Addr:         ":" + port,
		ReadTimeout:  10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	// 启动服务器
	go func() {
		log.Println("网关启动:", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("服务器错误:", err)
		}
	}()

	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("正在关闭服务器...")

	// 优雅关闭，等待现有请求完成
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Limits.ShutdownTimeoutSec)*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Println("服务器关闭失败:", err)
	}

	log.Println("服务器已关闭")
}
