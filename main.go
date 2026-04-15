package main

import (
	"bytes"
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

	// ipset 去重锁
	ipsetMutex sync.Mutex
	ipsetCache = make(map[string]time.Time)
	ipsetQueue chan ipsetBanTask
	ipsetStop  chan struct{}
	ipsetWG    sync.WaitGroup

	blacklistCacheMu    sync.RWMutex
	blacklistHitCache   = make(map[string]time.Time)
	blacklistMissCache  = make(map[string]time.Time)
	localStrikeMu       sync.Mutex
	localStrikeCache    = make(map[string]localStrikeState)
	rateLimiter         localRateLimiter
	rateLimiterConfig   rateLimiterConfigSnapshot
	rateLimiterConfigMu sync.Mutex
	logThrottleMu       sync.Mutex
	logThrottleLastAt   = make(map[string]time.Time)

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

	proxyTransport = &http.Transport{
		Proxy:                  http.ProxyFromEnvironment,
		DialContext:            (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:           100,
		IdleConnTimeout:        90 * time.Second,
		TLSHandshakeTimeout:    5 * time.Second,
		ExpectContinueTimeout:  1 * time.Second,
		ResponseHeaderTimeout:  5 * time.Second,
		MaxResponseHeaderBytes: 1 << 20,
		MaxConnsPerHost:        100,
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

	ipsetCleanupTicker *time.Ticker
	ipsetCleanupDone   chan struct{}
)

type Config struct {
	Backend        string          `json:"backend"`
	Redis          RedisConfig     `json:"redis"`
	RateLimit      RateLimitConfig `json:"rateLimit"`
	Auth           AuthConfig      `json:"auth"`
	TrustedProxies []string        `json:"trustedProxies"`
	AdminAllowedNetworks []string  `json:"adminAllowedNetworks"`
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

type ipsetBanTask struct {
	ip         string
	banSeconds int
}

type tokenBucket struct {
	tokens     float64
	lastRefill time.Time
	lastSeen   time.Time
}

type localRateLimiter struct {
	mu         sync.Mutex
	global     tokenBucket
	ipBuckets  map[string]*tokenBucket
	lastGCAt   time.Time
}

type rateLimiterConfigSnapshot struct {
	maxRequests       int
	globalMaxRequests int
	windowSec         int
}

type localStrikeState struct {
	count    int64
	expireAt time.Time
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
		AdminAllowedNetworks: []string{"127.0.0.1/8", "::1/128"},
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

	filteredAdminAllowedNetworks := make([]string, 0, len(c.AdminAllowedNetworks))
	seenAdminAllowedNetworks := make(map[string]struct{}, len(c.AdminAllowedNetworks))
	for _, network := range c.AdminAllowedNetworks {
		network = strings.TrimSpace(network)
		if network == "" {
			continue
		}
		if _, exists := seenAdminAllowedNetworks[network]; exists {
			continue
		}
		seenAdminAllowedNetworks[network] = struct{}{}
		filteredAdminAllowedNetworks = append(filteredAdminAllowedNetworks, network)
	}
	if len(filteredAdminAllowedNetworks) == 0 {
		filteredAdminAllowedNetworks = []string{"127.0.0.1/8", "::1/128"}
	}
	c.AdminAllowedNetworks = filteredAdminAllowedNetworks

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
	reverseProxy.BufferPool = nil
	reverseProxy.ModifyResponse = func(resp *http.Response) error {
		if resp.ContentLength > 0 && resp.ContentLength > int64(cfg.Limits.MaxBodySizeMB)*1024*1024 {
			_ = resp.Body.Close()
			return fmt.Errorf("response body too large: %d bytes", resp.ContentLength)
		}
		return nil
	}
	httpReverseProxy.Store(reverseProxy)

	upgrader.ReadBufferSize = cfg.Limits.WebSocketBufferSize
	upgrader.WriteBufferSize = cfg.Limits.WebSocketBufferSize

	ipsetSem = make(chan struct{}, cfg.Limits.IpsetConcurrency)
	connSem = make(chan struct{}, cfg.Limits.MaxConnections)
	resetLocalRateLimiter()

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
		if forwarded := firstForwardedIP(r.Header.Get("X-Forwarded-For")); forwarded != "" {
			if forwardedIP := net.ParseIP(forwarded); forwardedIP != nil {
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
	now := time.Now()

	blacklistCacheMu.RLock()
	hitExpire, hitFound := blacklistHitCache[ip]
	missExpire, missFound := blacklistMissCache[ip]
	blacklistCacheMu.RUnlock()

	if hitFound {
		if now.Before(hitExpire) {
			return true
		}
		blacklistCacheMu.Lock()
		if exp, ok := blacklistHitCache[ip]; ok && !now.Before(exp) {
			delete(blacklistHitCache, ip)
		}
		blacklistCacheMu.Unlock()
	}

	if missFound && now.Before(missExpire) {
		return false
	}

	if rdb == nil {
		blacklistCacheMu.Lock()
		blacklistMissCache[ip] = now.Add(time.Second)
		blacklistCacheMu.Unlock()
		return false
	}

	ttl, err := rdb.TTL(ctx, "blacklist:"+ip).Result()
	if err != nil {
		logThrottled("redis_is_blacklisted", 3*time.Second, "Redis error in isBlacklisted: %v", err)
		blacklistCacheMu.Lock()
		blacklistMissCache[ip] = now.Add(time.Second)
		blacklistCacheMu.Unlock()
		return false
	}

	if ttl > 0 {
		blacklistCacheMu.Lock()
		blacklistHitCache[ip] = now.Add(ttl)
		delete(blacklistMissCache, ip)
		blacklistCacheMu.Unlock()
		return true
	}

	blacklistCacheMu.Lock()
	blacklistMissCache[ip] = now.Add(time.Second)
	delete(blacklistHitCache, ip)
	blacklistCacheMu.Unlock()
	return false
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

	cacheBlacklistLocal(ipLocal, banDuration)
	enqueueIPSetBan(ipLocal, banDuration)
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
				cacheBlacklistLocal(ip, ttl)
				enqueueIPSetBan(ip, ttl)
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

func startIPSetCleanup() {
	ipsetCleanupTicker = time.NewTicker(60 * time.Second)
	ipsetCleanupDone = make(chan struct{})
	go func() {
		defer ipsetCleanupTicker.Stop()
		for {
			select {
			case <-ipsetCleanupTicker.C:
				ipsetMutex.Lock()
				now := time.Now()
				for ip, expireAt := range ipsetCache {
					if now.After(expireAt) {
						delete(ipsetCache, ip)
					}
				}
				ipsetMutex.Unlock()

				blacklistCacheMu.Lock()
				for ip, expireAt := range blacklistHitCache {
					if now.After(expireAt) {
						delete(blacklistHitCache, ip)
					}
				}
				for ip, expireAt := range blacklistMissCache {
					if now.After(expireAt) {
						delete(blacklistMissCache, ip)
					}
				}
				blacklistCacheMu.Unlock()

				localStrikeMu.Lock()
				for ip, state := range localStrikeCache {
					if now.After(state.expireAt) {
						delete(localStrikeCache, ip)
					}
				}
				localStrikeMu.Unlock()
			case <-ipsetCleanupDone:
				return
			}
		}
	}()
}

func stopIPSetCleanup() {
	if ipsetCleanupDone != nil {
		close(ipsetCleanupDone)
	}
}

func startIPSetWorkers() {
	if ipsetQueue != nil {
		return
	}

	workerCount := cfg.Limits.IpsetConcurrency
	if workerCount <= 0 {
		workerCount = 1
	}

	ipsetQueue = make(chan ipsetBanTask, workerCount*256)
	ipsetStop = make(chan struct{})

	for i := 0; i < workerCount; i++ {
		ipsetWG.Add(1)
		go func() {
			defer ipsetWG.Done()
			for {
				select {
				case <-ipsetStop:
					return
				case task := <-ipsetQueue:
					runIPSetAdd(task.ip, task.banSeconds)
				}
			}
		}()
	}
}

func stopIPSetWorkers() {
	if ipsetStop == nil {
		return
	}
	close(ipsetStop)
	ipsetWG.Wait()
	ipsetStop = nil
	ipsetQueue = nil
}

func enqueueIPSetBan(ip string, banDuration time.Duration) {
	banSeconds := int(banDuration.Seconds())
	if banSeconds <= 0 {
		return
	}

	if ipsetQueue == nil {
		runIPSetAdd(ip, banSeconds)
		return
	}

	task := ipsetBanTask{ip: ip, banSeconds: banSeconds}
	select {
	case ipsetQueue <- task:
	default:
		logThrottled("ipset_queue_drop", 2*time.Second, "ipset 队列拥堵，丢弃任务: ip=%s timeout=%d", ip, banSeconds)
	}
}

func runIPSetAdd(ip string, banSeconds int) {
	cmdCtx, cmdCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Limits.IpsetTimeoutSec)*time.Second)
	defer cmdCancel()

	cmd := exec.CommandContext(cmdCtx, "ipset", "-exist", "add", "blacklist", ip, "timeout", fmt.Sprintf("%d", banSeconds))
	if output, err := cmd.CombinedOutput(); err != nil {
		logThrottled("ipset_add_error", 2*time.Second, "ipset error: %v, output=%s", err, strings.TrimSpace(string(output)))
	}
}

func banIP(ctx context.Context, ip string) {
	if rdb == nil {
		applyLocalFallbackBan(ip, "redis_not_initialized")
		return
	}

	strikeKey := "abuse:" + ip
	strikes, err := rdb.Incr(ctx, strikeKey).Result()
	if err != nil {
		logThrottled("redis_ban_strike_incr", 3*time.Second, "Redis error in banIP strike increment: %v", err)
		applyLocalFallbackBan(ip, "strike_increment_error")
		return
	}
	if err := rdb.Expire(ctx, strikeKey, 30*time.Minute).Err(); err != nil {
		logThrottled("redis_ban_strike_expire", 3*time.Second, "Redis error in banIP strike expire: %v", err)
	}

	banDuration := banDurationByStrikes(strikes)
	banLevel := banLevelByStrikes(strikes)
	if banDuration == 0 {
		log.Printf("IP %s 触发限流，累计违规次数=%d，未封禁（渐进式策略）", ip, strikes)
		return
	}

	updated, err := setBlacklistWithMaxTTL(ctx, "blacklist:"+ip, "blacklist_level:"+ip, banDuration, banLevel)
	if err != nil {
		logThrottled("redis_ban_set_blacklist", 3*time.Second, "Redis error in banIP: %v", err)
		cacheBlacklistLocal(ip, banDuration)
		syncIPSetBan(ip, banDuration)
		log.Printf("Redis 不可用，已执行本地封禁兜底: ip=%s, 封禁时长=%s", ip, banDuration)
		return
	}
	if !updated {
		return
	}

	log.Printf("封禁IP: %s, 违规次数=%d, 封禁时长=%s", ip, strikes, banDuration)

	cacheBlacklistLocal(ip, banDuration)
	syncIPSetBan(ip, banDuration)
}

// ---------------- 分布式连接数 ----------------
func incConn(ctx context.Context, ip string) (bool, error) {
	if rdb == nil {
		return true, nil
	}

	// 使用时间窗口前缀防止单 key 热点
	timeWindow := time.Now().Unix() / 60 // 每分钟一个窗口
	key := "conn:" + ip + ":" + strconv.FormatInt(timeWindow, 10)
	allowed, err := rdb.Incr(ctx, key).Result()
	if err != nil {
		logThrottled("redis_inc_conn", 3*time.Second, "Redis error in incConn: %v", err)
		return true, nil
	}
	if err := rdb.Expire(ctx, key, time.Minute).Err(); err != nil {
		logThrottled("redis_inc_conn_expire", 3*time.Second, "Redis error in incConn expire: %v", err)
	}
	return allowed <= int64(cfg.RateLimit.MaxConn), nil
}

func decConn(ctx context.Context, ip string) {
	if rdb == nil {
		return
	}

	timeWindow := time.Now().Unix() / 60
	key := "conn:" + ip + ":" + strconv.FormatInt(timeWindow, 10)
	if err := rdb.Decr(ctx, key).Err(); err != nil {
		log.Println("Redis error in decConn:", err)
	}
}

// ---------------- 滑动窗口限流 ----------------
func allowRequestWithReason(ctx context.Context, ip string) (bool, string) {
	_ = ctx

	windowSec := cfg.RateLimit.WindowSec
	if cfg.RateLimit.GlobalMaxRequests <= 0 {
		return false, "global"
	}
	if cfg.RateLimit.MaxRequests <= 0 {
		return false, "ip"
	}
	if windowSec <= 0 {
		windowSec = 10
	}

	ensureRateLimiterConfig()

	now := time.Now()
	rateLimiter.mu.Lock()
	defer rateLimiter.mu.Unlock()

	gcLocalIPBuckets(now, windowSec)

	globalCapacity := float64(cfg.RateLimit.GlobalMaxRequests)
	globalRefill := globalCapacity / float64(windowSec)
	if !consumeToken(&rateLimiter.global, globalCapacity, globalRefill, now) {
		return false, "global"
	}

	bucket, ok := rateLimiter.ipBuckets[ip]
	if !ok {
		bucket = &tokenBucket{}
		rateLimiter.ipBuckets[ip] = bucket
	}

	ipCapacity := float64(cfg.RateLimit.MaxRequests)
	ipRefill := ipCapacity / float64(windowSec)
	if !consumeToken(bucket, ipCapacity, ipRefill, now) {
		refundToken(&rateLimiter.global, globalCapacity)
		return false, "ip"
	}

	return true, ""
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

func isAdminAllowedIP(r *http.Request) bool {
	ip := net.ParseIP(getClientIP(r))
	if ip == nil {
		return false
	}

	for _, network := range cfg.AdminAllowedNetworks {
		_, ipNet, err := net.ParseCIDR(network)
		if err == nil && ipNet.Contains(ip) {
			return true
		}
		if network == ip.String() {
			return true
		}
	}

	return false
}

func adminAuth(r *http.Request) bool {
	token := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[7:])
	}
	if token == "" {
		token = strings.TrimSpace(r.Header.Get("X-Admin-Token"))
	}
	return token != "" && token == cfg.Auth.AdminToken && isAdminAllowedIP(r)
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

	redisPersisted := false
	if rdb == nil {
		logThrottled("admin_ban_redis_nil", 3*time.Second, "Redis client not initialized in admin ban")
	} else {
		ctx := context.Background()
		blacklistKey := "blacklist:" + ipStr
		levelKey := "blacklist_level:" + ipStr
		if _, err := setBlacklistWithMaxTTL(ctx, blacklistKey, levelKey, duration, 3); err != nil {
			logThrottled("admin_ban_redis_error", 3*time.Second, "Redis error in admin ban: %v", err)
		} else {
			redisPersisted = true
		}
	}

	cacheBlacklistLocal(ipStr, duration)
	syncIPSetBan(ipStr, duration)
	if !redisPersisted {
		log.Printf("Admin ban 已走本地兜底: ip=%s, duration=%v", ipStr, duration)
	}

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
	lineEnd := bytes.IndexByte(data, '\n')
	if lineEnd < 0 {
		lineEnd = len(data)
	}
	line := data[:lineEnd]
	fields := bytes.Fields(line)
	if len(fields) < 5 || !bytes.Equal(fields[0], []byte("cpu")) {
		return cpuSnapshot{}, fmt.Errorf("invalid /proc/stat format")
	}

	var total uint64
	values := make([]uint64, 0, 10)
	for _, field := range fields[1:] {
		v, parseErr := parseUintASCII(field)
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
	firstTokenEnd := bytes.IndexByte(data, ' ')
	if firstTokenEnd < 0 {
		firstTokenEnd = bytes.IndexByte(data, '\n')
	}
	if firstTokenEnd <= 0 {
		return 0, fmt.Errorf("invalid /proc/loadavg format")
	}
	load1, err := strconv.ParseFloat(string(data[:firstTokenEnd]), 64)
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
	for len(data) > 0 {
		lineEnd := bytes.IndexByte(data, '\n')
		line := data
		if lineEnd >= 0 {
			line = data[:lineEnd]
			data = data[lineEnd+1:]
		} else {
			data = nil
		}

		if bytes.HasPrefix(line, []byte("MemTotal:")) {
			v, parseErr := parseFirstUintAfterColon(line)
			if parseErr != nil {
				return 0, parseErr
			}
			total = float64(v)
			continue
		}

		if bytes.HasPrefix(line, []byte("MemAvailable:")) {
			v, parseErr := parseFirstUintAfterColon(line)
			if parseErr != nil {
				return 0, parseErr
			}
			available = float64(v)
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
	var targetConn *websocket.Conn
	var dialErr error

	const maxRetries = 3
	const baseRetryDelay = 100 * time.Millisecond

	for retry := 0; retry < maxRetries; retry++ {
		if retry > 0 {
			delay := baseRetryDelay * time.Duration(1<<uint(retry-1))
			if delay > 2*time.Second {
				delay = 2 * time.Second
			}
			log.Printf("WebSocket 后端连接重试 %d/%d，等待 %v", retry, maxRetries, delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				_ = clientWS.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "connection canceled"))
				return
			}
		}

		targetConn, _, dialErr = websocket.DefaultDialer.Dial("ws://"+target, nil)
		if dialErr == nil {
			break
		}
		log.Printf("WebSocket 后端连接失败 (尝试 %d/%d): %v", retry+1, maxRetries, dialErr)
	}

	if dialErr != nil {
		log.Println("WebSocket 后端连接最终失败:", target, dialErr)
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
	go startIPSetCleanup()
	startIPSetWorkers()

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

	stopIPSetCleanup()
	stopIPSetWorkers()

	// 优雅关闭，等待现有请求完成
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Limits.ShutdownTimeoutSec)*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Println("服务器关闭失败:", err)
	}

	log.Println("服务器已关闭")
}

func cacheBlacklistLocal(ip string, banDuration time.Duration) {
	if banDuration <= 0 {
		return
	}

	blacklistCacheMu.Lock()
	blacklistHitCache[ip] = time.Now().Add(banDuration)
	delete(blacklistMissCache, ip)
	blacklistCacheMu.Unlock()
}

func incrementLocalStrike(ip string, now time.Time) int64 {
	localStrikeMu.Lock()
	defer localStrikeMu.Unlock()

	state := localStrikeCache[ip]
	if now.After(state.expireAt) {
		state.count = 0
	}
	state.count++
	state.expireAt = now.Add(30 * time.Minute)
	localStrikeCache[ip] = state
	return state.count
}

func applyLocalFallbackBan(ip string, reason string) {
	strikes := incrementLocalStrike(ip, time.Now())
	banDuration := banDurationByStrikes(strikes)
	if banDuration <= 0 {
		logThrottled("local_fallback_no_ban", 3*time.Second, "本地兜底记录违规但未封禁: ip=%s, strikes=%d, reason=%s", ip, strikes, reason)
		return
	}

	cacheBlacklistLocal(ip, banDuration)
	syncIPSetBan(ip, banDuration)
	log.Printf("本地兜底封禁生效: ip=%s, strikes=%d, duration=%s, reason=%s", ip, strikes, banDuration, reason)
}

func ensureRateLimiterConfig() {
	rateLimiterConfigMu.Lock()
	defer rateLimiterConfigMu.Unlock()

	current := rateLimiterConfigSnapshot{
		maxRequests:       cfg.RateLimit.MaxRequests,
		globalMaxRequests: cfg.RateLimit.GlobalMaxRequests,
		windowSec:         cfg.RateLimit.WindowSec,
	}
	if current == rateLimiterConfig {
		return
	}
	rateLimiterConfig = current
	resetLocalRateLimiter()
}

func resetLocalRateLimiter() {
	rateLimiter.mu.Lock()
	defer rateLimiter.mu.Unlock()

	rateLimiter.global = tokenBucket{}
	rateLimiter.ipBuckets = make(map[string]*tokenBucket)
	rateLimiter.lastGCAt = time.Now()
}

func gcLocalIPBuckets(now time.Time, windowSec int) {
	if rateLimiter.ipBuckets == nil {
		rateLimiter.ipBuckets = make(map[string]*tokenBucket)
		rateLimiter.lastGCAt = now
		return
	}

	if now.Sub(rateLimiter.lastGCAt) < 30*time.Second {
		return
	}

	idleTTL := time.Duration(windowSec*6) * time.Second
	if idleTTL < 2*time.Minute {
		idleTTL = 2 * time.Minute
	}
	deadline := now.Add(-idleTTL)

	for ip, bucket := range rateLimiter.ipBuckets {
		if bucket.lastSeen.Before(deadline) {
			delete(rateLimiter.ipBuckets, ip)
		}
	}
	rateLimiter.lastGCAt = now
}

func consumeToken(bucket *tokenBucket, capacity float64, refillPerSec float64, now time.Time) bool {
	if capacity <= 0 || refillPerSec <= 0 {
		return false
	}

	if bucket.lastRefill.IsZero() {
		bucket.tokens = capacity
		bucket.lastRefill = now
	} else {
		elapsed := now.Sub(bucket.lastRefill).Seconds()
		if elapsed > 0 {
			bucket.tokens += elapsed * refillPerSec
			if bucket.tokens > capacity {
				bucket.tokens = capacity
			}
			bucket.lastRefill = now
		}
	}

	bucket.lastSeen = now
	if bucket.tokens >= 1 {
		bucket.tokens -= 1
		return true
	}
	return false
}

func refundToken(bucket *tokenBucket, capacity float64) {
	bucket.tokens += 1
	if bucket.tokens > capacity {
		bucket.tokens = capacity
	}
}

func parseUintASCII(b []byte) (uint64, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("empty numeric field")
	}

	var v uint64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid digit %q", c)
		}
		v = v*10 + uint64(c-'0')
	}
	return v, nil
}

func parseFirstUintAfterColon(line []byte) (uint64, error) {
	colon := bytes.IndexByte(line, ':')
	if colon < 0 || colon+1 >= len(line) {
		return 0, fmt.Errorf("invalid meminfo line")
	}

	i := colon + 1
	for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
		i++
	}
	start := i
	for i < len(line) && line[i] >= '0' && line[i] <= '9' {
		i++
	}
	if start == i {
		return 0, fmt.Errorf("meminfo value not found")
	}

	return parseUintASCII(line[start:i])
}

func firstForwardedIP(xff string) string {
	xff = strings.TrimSpace(xff)
	if xff == "" {
		return ""
	}

	if comma := strings.IndexByte(xff, ','); comma >= 0 {
		xff = xff[:comma]
	}

	return strings.TrimSpace(xff)
}

func logThrottled(key string, interval time.Duration, format string, args ...any) {
	now := time.Now()

	logThrottleMu.Lock()
	last, ok := logThrottleLastAt[key]
	if ok && now.Sub(last) < interval {
		logThrottleMu.Unlock()
		return
	}
	logThrottleLastAt[key] = now
	logThrottleMu.Unlock()

	log.Printf(format, args...)
}
