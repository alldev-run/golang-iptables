package main

import (
	"context"
	"encoding/json"
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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	// Redis
	rdb *redis.Client

	// 后端列表（线程安全）
	backends   []string
	backendsMu sync.RWMutex

	// 后端健康状态
	backendHealth map[string]bool
	healthMu      sync.RWMutex

	// 轮询计数器
	backendIndex uint64
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

	// ipset 并发限制（防止 ipset 慢或卡住时拖垮服务）
	ipsetSem chan struct{}

	// 全局连接数限制（防止攻击时创建过多 goroutine）
	connSem chan struct{}

	// 配置文件路径
	configFile = "config.json"

	// 全局配置
	cfg Config

	// Prometheus metrics
	totalConnections = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_total_connections",
			Help: "Total number of connections",
		},
		[]string{"type"}, // http or websocket
	)

	activeConnections = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gateway_active_connections",
			Help: "Number of active connections",
		},
		[]string{"type"},
	)

	totalRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_total_requests",
			Help: "Total number of requests",
		},
		[]string{"method", "path"},
	)

	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gateway_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type"},
	)

	backendHealthMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gateway_backend_health",
			Help: "Backend health status (1=healthy, 0=unhealthy)",
		},
		[]string{"backend"},
	)

	rateLimitRejections = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_rate_limit_rejections",
			Help: "Number of requests rejected due to rate limiting",
		},
		[]string{"reason"},
	)

	blacklistHits = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gateway_blacklist_hits",
			Help: "Number of requests from blacklisted IPs",
		},
	)
)

type Config struct {
	Backends  []string        `json:"backends"`
	Redis     RedisConfig     `json:"redis"`
	RateLimit RateLimitConfig `json:"rateLimit"`
	Auth      AuthConfig      `json:"auth"`
	Limits    LimitConfig     `json:"limits"`
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
	Token string `json:"token"`
}

type LimitConfig struct {
	MaxConnections      int `json:"maxConnections"`      // 全局最大连接数
	MaxBodySizeMB       int `json:"maxBodySizeMB"`       // HTTP 请求体最大大小 (MB)
	WebSocketBufferSize int `json:"webSocketBufferSize"` // WebSocket 缓冲区大小 (bytes)
	WebSocketMaxLifetimeSec int `json:"webSocketMaxLifetimeSec"` // WebSocket 最大连接生命周期 (秒)
	IpsetConcurrency    int `json:"ipsetConcurrency"`    // ipset 并发数
	IpsetTimeoutSec     int `json:"ipsetTimeoutSec"`     // ipset 命令超时 (秒)
	AuthTimeoutSec      int `json:"authTimeoutSec"`      // 认证超时 (秒)
	HealthCheckInterval int `json:"healthCheckInterval"` // 健康检查间隔 (秒)
	HealthCheckTimeout  int `json:"healthCheckTimeout"`  // 健康检查超时 (秒)
	ShutdownTimeoutSec  int `json:"shutdownTimeoutSec"`  // 优雅关闭超时 (秒)
}

func defaultConfig() Config {
	return Config{
		Backends: []string{"127.0.0.1:9502"},
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
			Token: "123456",
		},
		Limits: LimitConfig{
			MaxConnections:      5000,
			MaxBodySizeMB:       10,
			WebSocketBufferSize: 1024 * 1024,
			WebSocketMaxLifetimeSec: 7200,
			IpsetConcurrency:    10,
			IpsetTimeoutSec:     5,
			AuthTimeoutSec:      3,
			HealthCheckInterval: 30,
			HealthCheckTimeout:  3,
			ShutdownTimeoutSec:  30,
		},
	}
}

func normalizeConfig(c *Config) {
	filteredBackends := make([]string, 0, len(c.Backends))
	seen := make(map[string]struct{}, len(c.Backends))
	for _, backend := range c.Backends {
		backend = strings.TrimSpace(backend)
		if backend == "" {
			continue
		}
		if _, exists := seen[backend]; exists {
			continue
		}
		seen[backend] = struct{}{}
		filteredBackends = append(filteredBackends, backend)
	}
	c.Backends = filteredBackends

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
	if c.Limits.AuthTimeoutSec <= 0 {
		c.Limits.AuthTimeoutSec = 3
	}
	if c.Limits.HealthCheckInterval <= 0 {
		c.Limits.HealthCheckInterval = 30
	}
	if c.Limits.HealthCheckTimeout <= 0 {
		c.Limits.HealthCheckTimeout = 3
	}
	if c.Limits.ShutdownTimeoutSec <= 0 {
		c.Limits.ShutdownTimeoutSec = 30
	}
}

func applyConfig(newCfg Config) error {
	normalizeConfig(&newCfg)
	if len(newCfg.Backends) == 0 {
		return fmt.Errorf("配置文件中 backends 不能为空")
	}

	cfg = newCfg

	backendsMu.Lock()
	backends = append(backends[:0], cfg.Backends...)
	backendsMu.Unlock()

	healthMu.Lock()
	if backendHealth == nil {
		backendHealth = make(map[string]bool)
	}

	active := make(map[string]struct{}, len(backends))
	for _, backend := range backends {
		active[backend] = struct{}{}
		if _, exists := backendHealth[backend]; !exists {
			backendHealth[backend] = true
		}
		if backendHealth[backend] {
			backendHealthMetric.WithLabelValues(backend).Set(1)
		} else {
			backendHealthMetric.WithLabelValues(backend).Set(0)
		}
	}
	for backend := range backendHealth {
		if _, exists := active[backend]; !exists {
			delete(backendHealth, backend)
			backendHealthMetric.DeleteLabelValues(backend)
		}
	}
	healthMu.Unlock()

	upgrader.ReadBufferSize = cfg.Limits.WebSocketBufferSize
	upgrader.WriteBufferSize = cfg.Limits.WebSocketBufferSize

	ipsetSem = make(chan struct{}, cfg.Limits.IpsetConcurrency)
	connSem = make(chan struct{}, cfg.Limits.MaxConnections)

	return nil
}

func init() {
	// 注册 Prometheus metrics
	prometheus.MustRegister(totalConnections)
	prometheus.MustRegister(activeConnections)
	prometheus.MustRegister(totalRequests)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(backendHealthMetric)
	prometheus.MustRegister(rateLimitRejections)
	prometheus.MustRegister(blacklistHits)
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

	log.Println("配置已加载, 后端列表:", backends)
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

	// 仅在本地回环代理场景下信任 X-Forwarded-For，避免被外部直连请求伪造
	if remoteIP.IsLoopback() {
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
	if val == 1 {
		blacklistHits.Inc()
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

	ipsetMutex.Lock()
	currentExpireAt, ok := ipsetCache[ip]
	if ok && !expireAt.After(currentExpireAt) {
		ipsetMutex.Unlock()
		return
	}
	ipsetCache[ip] = expireAt
	ipsetMutex.Unlock()

	go func(expectedExpireAt time.Time) {
		sleepFor := time.Until(expectedExpireAt)
		if sleepFor > 0 {
			time.Sleep(sleepFor)
		}
		ipsetMutex.Lock()
		current, exists := ipsetCache[ip]
		if exists && !current.After(expectedExpireAt) {
			delete(ipsetCache, ip)
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
		current, exists := ipsetCache[ip]
		if !exists || current.After(expectedExpireAt) {
			ipsetMutex.Unlock()
			return
		}
		ipsetMutex.Unlock()

		sem <- struct{}{}
		defer func() { <-sem }()

		cmdCtx, cmdCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Limits.IpsetTimeoutSec)*time.Second)
		defer cmdCancel()

		banSeconds := int(time.Until(expectedExpireAt).Seconds())
		if banSeconds <= 0 {
			return
		}

		cmd := exec.CommandContext(cmdCtx, "ipset", "add", "blacklist", ip, "timeout", fmt.Sprintf("%d", banSeconds))
		if err := cmd.Run(); err != nil {
			log.Println("ipset error:", err)
		}
	}(expireAt)
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
	key := fmt.Sprintf("conn:%s:%d", ip, timeWindow)
	val, err := rdb.Incr(ctx, key).Result()
	if err != nil {
		return false, err
	}
	if err := rdb.Expire(ctx, key, 60*time.Second).Err(); err != nil {
		return false, err
	}
	return val <= int64(cfg.RateLimit.MaxConn), nil
}

func decConn(ctx context.Context, ip string) {
	timeWindow := time.Now().Unix() / 60
	if err := rdb.Decr(ctx, fmt.Sprintf("conn:%s:%d", ip, timeWindow)).Err(); err != nil {
		log.Println("Redis error in decConn:", err)
	}
}

func allowSlidingWindow(ctx context.Context, key string, limit int, windowSec int, member string, now int64) (bool, error) {
	if limit <= 0 {
		return false, nil
	}

	windowMillis := int64(windowSec) * 1000
	if windowMillis <= 0 {
		windowMillis = 10000
	}

	start := now - windowMillis

	pipe := rdb.TxPipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(now), Member: member})
	pipe.ZRemRangeByScore(ctx, key, "0", fmt.Sprintf("%d", start))
	count := pipe.ZCard(ctx, key)
	pipe.Expire(ctx, key, time.Duration(windowSec)*time.Second)

	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}

	return count.Val() <= int64(limit), nil
}

// ---------------- 滑动窗口限流 ----------------
func allowRequestWithReason(ctx context.Context, ip string) (bool, string) {
	now := time.Now().UnixMilli()
	member := fmt.Sprintf("%d-%d", now, atomic.AddUint64(&rateLimitSeq, 1))
	windowSec := cfg.RateLimit.WindowSec

	globalAllowed, err := allowSlidingWindow(ctx, "rate:global", cfg.RateLimit.GlobalMaxRequests, windowSec, member+"-g", now)
	if err != nil {
		log.Println("Redis error in allowRequest (global):", err)
		return false, "redis"
	}
	if !globalAllowed {
		return false, "global"
	}

	ipAllowed, err := allowSlidingWindow(ctx, "rate:ip:"+ip, cfg.RateLimit.MaxRequests, windowSec, member+"-i", now)
	if err != nil {
		log.Println("Redis error in allowRequest (ip):", err)
		return false, "redis"
	}
	if !ipAllowed {
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

// ---------------- 心跳 ----------------
func heartbeat(ws *websocket.Conn, wsWriteMu *sync.Mutex, ctx context.Context, cancel context.CancelFunc) {
	const wsReadTimeout = 65 * time.Second
	ws.SetPongHandler(func(string) error {
		ws.SetReadDeadline(time.Now().Add(wsReadTimeout))
		return nil
	})
	_ = ws.SetReadDeadline(time.Now().Add(wsReadTimeout))

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				wsWriteMu.Lock()
				err := ws.WriteMessage(websocket.PingMessage, nil)
				wsWriteMu.Unlock()
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

// ---------------- 后端健康检查 ----------------
func checkBackendHealth(backend string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Limits.HealthCheckTimeout)*time.Second)
	defer cancel()

	// 尝试连接后端
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, "ws://"+backend, nil)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func startHealthCheck() {
	ticker := time.NewTicker(time.Duration(cfg.Limits.HealthCheckInterval) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		backendsMu.RLock()
		currentBackends := make([]string, len(backends))
		copy(currentBackends, backends)
		backendsMu.RUnlock()

		for _, backend := range currentBackends {
			currentHealthy := checkBackendHealth(backend)

			healthMu.Lock()
			previousHealthy := backendHealth[backend]
			backendHealth[backend] = currentHealthy
			healthMu.Unlock()

			if currentHealthy {
				backendHealthMetric.WithLabelValues(backend).Set(1)
			} else {
				backendHealthMetric.WithLabelValues(backend).Set(0)
			}

			if previousHealthy != currentHealthy {
				if currentHealthy {
					log.Println("后端恢复健康:", backend)
				} else {
					log.Println("后端变为不健康:", backend)
				}
			}
		}
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

// ---------------- 负载均衡 ----------------
func pickBackend(ip string, sticky bool) string {
	backendsMu.RLock()
	defer backendsMu.RUnlock()
	if len(backends) == 0 {
		return ""
	}

	// 获取健康的后端列表
	healthMu.RLock()
	defer healthMu.RUnlock()

	var healthyBackends []string
	for _, backend := range backends {
		if backendHealth[backend] {
			healthyBackends = append(healthyBackends, backend)
		}
	}

	if len(healthyBackends) == 0 {
		// 如果没有健康的后端，返回第一个（fallback）
		return backends[0]
	}

	if sticky && ip != "" {
		// 粘性负载均衡：基于IP哈希，确保同一IP总是路由到同一后端
		hash := 0
		for _, c := range ip {
			hash = hash*31 + int(c)
		}
		if hash < 0 {
			hash = -hash
		}
		return healthyBackends[hash%len(healthyBackends)]
	}

	// 轮询负载均衡
	idx := atomic.AddUint64(&backendIndex, 1) - 1
	return healthyBackends[idx%uint64(len(healthyBackends))]
}

// ---------------- HTTP代理处理器 ----------------
func httpProxyHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	totalRequests.WithLabelValues(r.Method, r.URL.Path).Inc()
	if !validateRequestHeaders(r.Header) {
		rateLimitRejections.WithLabelValues("http_header").Inc()
		http.Error(w, "Request Header Fields Too Large", http.StatusRequestHeaderFieldsTooLarge)
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

	// 记录指标
	totalConnections.WithLabelValues("http").Inc()
	activeConnections.WithLabelValues("http").Inc()
	defer activeConnections.WithLabelValues("http").Dec()

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
		rateLimitRejections.WithLabelValues("http_" + reason).Inc()
		http.Error(w, "Too many requests", 429)
		return
	}

	// 选择后端（非粘性，使用轮询）
	target := pickBackend(ip, false)
	if target == "" {
		http.Error(w, "No backend available", 503)
		return
	}
	targetURL, err := url.Parse("http://" + target)
	if err != nil {
		log.Println("解析目标URL失败:", err)
		http.Error(w, "Bad Gateway", 502)
		return
	}

	// 创建反向代理
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.Transport = &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
		MaxResponseHeaderBytes: 1 << 20,
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Println("代理错误:", err)
		http.Error(w, "Bad Gateway", 502)
	}

	// 添加X-Forwarded-For头
	r.Header.Set("X-Forwarded-For", ip)

	proxy.ServeHTTP(w, r)

	// 记录请求持续时间
	duration := time.Since(start).Seconds()
	requestDuration.WithLabelValues("http").Observe(duration)
}

// ---------------- WebSocket代理处理器 ----------------
func websocketProxyHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// 全局连接数限制（防止攻击时创建过多 goroutine）
	select {
	case connSem <- struct{}{}:
	default:
		http.Error(w, "Too many connections", 429)
		return
	}
	defer func() { <-connSem }()

	// 记录指标
	totalConnections.WithLabelValues("websocket").Inc()
	activeConnections.WithLabelValues("websocket").Inc()
	defer activeConnections.WithLabelValues("websocket").Dec()

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
		rateLimitRejections.WithLabelValues("ws_rate_" + reason).Inc()
		http.Error(w, "Too many requests", 429)
		return
	}

	// Header 鉴权，Upgrade 前完成
	if !auth(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
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
		rateLimitRejections.WithLabelValues("ws_conn").Inc()
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

	var clientWriteMu sync.Mutex

	// 心跳
	heartbeat(conn, &clientWriteMu, ctx, cancel)

	// 后端（粘性负载均衡）
	target := pickBackend(ip, true)
	if target == "" {
		clientWriteMu.Lock()
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "no backend available"))
		clientWriteMu.Unlock()
		return
	}
	targetConn, _, err := websocket.DefaultDialer.Dial("ws://"+target, nil)
	if err != nil {
		log.Println("后端连接失败:", target, err)
		// 标记后端为不健康
		healthMu.Lock()
		backendHealth[target] = false
		healthMu.Unlock()
		// 更新 Prometheus 指标
		backendHealthMetric.WithLabelValues(target).Set(0)
		return
	}
	defer targetConn.Close()
	maxWSMessageSize := int64(cfg.Limits.MaxBodySizeMB) * 1024 * 1024
	if maxWSMessageSize <= 0 {
		maxWSMessageSize = 10 * 1024 * 1024
	}
	conn.SetReadLimit(maxWSMessageSize)
	targetConn.SetReadLimit(maxWSMessageSize)

	var wg sync.WaitGroup
	done := make(chan struct{})
	var targetWriteMu sync.Mutex

	// client → backend
	wg.Add(1)
	go func() {
		defer wg.Done()
		const wsReadTimeout = 65 * time.Second
		for {
			select {
			case <-done:
				return
			default:
				conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
				mt, msg, err := conn.ReadMessage()
				if err != nil {
					cancel()
					return
				}

				if allowed, reason := allowRequestWithReason(ctx, ip); !allowed {
					if reason == "ip" {
						banIP(ctx, ip)
					}
					cancel()
					return
				}

				targetWriteMu.Lock()
				err = targetConn.WriteMessage(mt, msg)
				targetWriteMu.Unlock()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()

	// backend → client
	wg.Add(1)
	go func() {
		defer wg.Done()
		const backendReadTimeout = 65 * time.Second
		for {
			select {
			case <-done:
				return
			default:
				targetConn.SetReadDeadline(time.Now().Add(backendReadTimeout))
				mt, msg, err := targetConn.ReadMessage()
				if err != nil {
					cancel()
					return
				}
				clientWriteMu.Lock()
				err = conn.WriteMessage(mt, msg)
				clientWriteMu.Unlock()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()

	// 等待任一方向出错
	<-ctx.Done()
	if ctx.Err() == context.DeadlineExceeded {
		clientWriteMu.Lock()
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "connection lifetime exceeded"), time.Now().Add(time.Second))
		clientWriteMu.Unlock()
	}
	close(done)
	_ = conn.Close()
	_ = targetConn.Close()
	wg.Wait()

	// 记录请求持续时间
	duration := time.Since(start).Seconds()
	requestDuration.WithLabelValues("websocket").Observe(duration)
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

	// 启动后端健康检查
	go startHealthCheck()

	http.HandleFunc("/", proxyHandler)
	http.Handle("/metrics", promhttp.Handler())

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
