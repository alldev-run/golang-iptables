package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// 测试 getClientIP
func TestGetClientIP(t *testing.T) {
	tests := []struct {
		name     string
		header   string
		remote   string
		expected string
	}{
		{
			name:     "非回环地址不信任 X-Forwarded-For",
			header:   "1.2.3.4, 5.6.7.8",
			remote:   "9.10.11.12:1234",
			expected: "9.10.11.12",
		},
		{
			name:     "回环地址信任 X-Forwarded-For",
			header:   "1.2.3.4, 5.6.7.8",
			remote:   "127.0.0.1:1234",
			expected: "1.2.3.4",
		},
		{
			name:     "X-Forwarded-For 不存在",
			header:   "",
			remote:   "9.10.11.12:1234",
			expected: "9.10.11.12",
		},
		{
			name:     "X-Forwarded-For 为空",
			header:   "",
			remote:   "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			if tt.header != "" {
				req.Header.Set("X-Forwarded-For", tt.header)
			}
			req.RemoteAddr = tt.remote

			result := getClientIP(req)
			if result != tt.expected {
				t.Errorf("getClientIP() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestGetClientIP_TrustedProxy(t *testing.T) {
	originalCfg := cfg
	originalTrusted := trustedProxyNets
	defer func() {
		cfg = originalCfg
		trustedProxyMu.Lock()
		trustedProxyNets = originalTrusted
		trustedProxyMu.Unlock()
	}()

	newCfg := defaultConfig()
	newCfg.TrustedProxies = []string{"10.0.0.0/8", "192.168.0.1"}
	if err := applyConfig(newCfg); err != nil {
		t.Fatalf("applyConfig() error = %v", err)
	}

	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.10.10.10:1234"
	req.Header.Set("X-Forwarded-For", "1.2.3.4, 5.6.7.8")

	if got := getClientIP(req); got != "1.2.3.4" {
		t.Fatalf("trusted proxy 场景 getClientIP() = %v, want %v", got, "1.2.3.4")
	}
}

// 测试配置文件加载
func TestLoadConfig(t *testing.T) {
	// 保存原始配置文件路径
	originalConfigFile := configFile
	defer func() { configFile = originalConfigFile }()

	// 创建临时配置文件
	tempFile := "/tmp/test_config.json"
	configFile = tempFile

	// 测试正常配置
	t.Run("正常配置", func(t *testing.T) {
		testConfig := `{
			"backend": "127.0.0.1:9502",
			"redis": {
				"addr": "127.0.0.1:16379",
				"db": 14,
				"password": ""
			}
		}`

		if err := writeTestConfig(tempFile, testConfig); err != nil {
			t.Fatalf("写入测试配置失败: %v", err)
		}

		if err := loadConfig(); err != nil {
			t.Errorf("loadConfig() error = %v", err)
		}

		if cfg.Backend != "127.0.0.1:9502" {
			t.Errorf("期望 backend 为 127.0.0.1:9502，实际 %s", cfg.Backend)
		}
	})

	// 测试空 backend（会使用默认值）
	t.Run("空 backend", func(t *testing.T) {
		testConfig := `{
			"backend": "",
			"redis": {
				"addr": "127.0.0.1:16379",
				"db": 14,
				"password": ""
			}
		}`

		if err := writeTestConfig(tempFile, testConfig); err != nil {
			t.Fatalf("写入测试配置失败: %v", err)
		}

		if err := loadConfig(); err != nil {
			t.Errorf("loadConfig() error = %v", err)
		}

		if cfg.Backend != "127.0.0.1:9502" {
			t.Errorf("期望使用默认 backend 127.0.0.1:9502，实际 %s", cfg.Backend)
		}
	})

	// 测试文件不存在
	t.Run("文件不存在", func(t *testing.T) {
		configFile = "/tmp/nonexistent.json"
		if err := loadConfig(); err == nil {
			t.Error("期望返回错误，但返回 nil")
		}
	})

	// 测试 trustedProxies 非法配置
	t.Run("trustedProxies 非法配置", func(t *testing.T) {
		testConfig := `{
			"backend": "127.0.0.1:9502",
			"redis": {
				"addr": "127.0.0.1:16379",
				"db": 14,
				"password": ""
			},
			"trustedProxies": ["bad-cidr"]
		}`

		if err := writeTestConfig(tempFile, testConfig); err != nil {
			t.Fatalf("写入测试配置失败: %v", err)
		}

		if err := loadConfig(); err == nil {
			t.Error("期望 trustedProxies 非法时报错，但返回 nil")
		}
	})
}

// 辅助函数：写入测试配置
func writeTestConfig(path, content string) error {
	return writeFile(path, content)
}

// 辅助函数：写入文件
func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0644)
}

// 测试 Redis 连接数控制
func TestIncConn(t *testing.T) {
	// 使用 miniredis 模拟 Redis
	s := miniredis.RunT(t)
	defer s.Close()

	rdb = redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	cfg = defaultConfig()
	cfg.RateLimit.MaxConn = 5

	ctx := context.Background()
	ip := "1.2.3.4"

	// 测试连接数递增
	for i := 0; i < 5; i++ {
		allowed, err := incConn(ctx, ip)
		if err != nil {
			t.Fatalf("incConn() error = %v", err)
		}
		if !allowed {
			t.Errorf("第 %d 个连接应该被允许", i+1)
		}
	}

	// 第 6 个连接应该被拒绝
	allowed, err := incConn(ctx, ip)
	if err != nil {
		t.Fatalf("incConn() error = %v", err)
	}
	if allowed {
		t.Error("第 6 个连接应该被拒绝")
	}

	// 清理
	decConn(ctx, ip)
}

// 测试滑动窗口限流
func TestAllowRequest(t *testing.T) {
	// 使用 miniredis 模拟 Redis
	s := miniredis.RunT(t)
	defer s.Close()

	rdb = redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	cfg = defaultConfig()
	cfg.RateLimit.MaxRequests = 100
	cfg.RateLimit.WindowSec = 10

	ctx := context.Background()
	ip := "1.2.3.4"

	// 测试限流 - 发送 95 个请求（小于限制）
	for i := 0; i < 95; i++ {
		if !allowRequest(ctx, ip) {
			t.Errorf("第 %d 个请求应该被允许", i+1)
		}
	}

	// 验证功能正常工作
	// 注意：由于测试运行速度很快，所有请求可能在同一时间窗口内
	// 这里主要测试函数不会 panic 且能正常返回
}

func TestAllowRequestWithReason(t *testing.T) {
	t.Run("触发全局限流", func(t *testing.T) {
		s := miniredis.RunT(t)
		defer s.Close()

		rdb = redis.NewClient(&redis.Options{Addr: s.Addr()})
		cfg = defaultConfig()
		cfg.RateLimit.WindowSec = 10
		cfg.RateLimit.GlobalMaxRequests = 3
		cfg.RateLimit.MaxRequests = 100

		ctx := context.Background()
		for i := 0; i < 3; i++ {
			allowed, reason := allowRequestWithReason(ctx, fmt.Sprintf("1.2.3.%d", i+1))
			if !allowed || reason != "" {
				t.Fatalf("前 %d 次请求应通过，allowed=%v reason=%q", i+1, allowed, reason)
			}
		}

		allowed, reason := allowRequestWithReason(ctx, "9.9.9.9")
		if allowed || reason != "global" {
			t.Fatalf("应触发 global 限流，allowed=%v reason=%q", allowed, reason)
		}
	})

	t.Run("触发单IP限流", func(t *testing.T) {
		s := miniredis.RunT(t)
		defer s.Close()

		rdb = redis.NewClient(&redis.Options{Addr: s.Addr()})
		cfg = defaultConfig()
		cfg.RateLimit.WindowSec = 10
		cfg.RateLimit.GlobalMaxRequests = 100
		cfg.RateLimit.MaxRequests = 3

		ctx := context.Background()
		ip := "1.2.3.4"
		for i := 0; i < 3; i++ {
			allowed, reason := allowRequestWithReason(ctx, ip)
			if !allowed || reason != "" {
				t.Fatalf("前 %d 次请求应通过，allowed=%v reason=%q", i+1, allowed, reason)
			}
		}

		allowed, reason := allowRequestWithReason(ctx, ip)
		if allowed || reason != "ip" {
			t.Fatalf("应触发 ip 限流，allowed=%v reason=%q", allowed, reason)
		}
	})

	t.Run("Redis异常返回redis原因", func(t *testing.T) {
		rdb = redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
		cfg = defaultConfig()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		allowed, reason := allowRequestWithReason(ctx, "1.2.3.4")
		if allowed || reason != "redis" {
			t.Fatalf("Redis异常应返回 redis 原因，allowed=%v reason=%q", allowed, reason)
		}
	})
}

func TestSetBlacklistWithMaxTTL_LevelEscalationOnly(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	rdb = redis.NewClient(&redis.Options{Addr: s.Addr()})
	ctx := context.Background()
	key := "blacklist:1.2.3.4"
	levelKey := "blacklist_level:1.2.3.4"

	updated, err := setBlacklistWithMaxTTL(ctx, key, levelKey, 1*time.Minute, 1)
	if err != nil {
		t.Fatalf("首次设置 blacklist 失败: %v", err)
	}
	if !updated {
		t.Fatalf("首次设置应更新")
	}

	if err := rdb.PExpire(ctx, key, 15*time.Second).Err(); err != nil {
		t.Fatalf("缩短 key TTL 失败: %v", err)
	}
	if err := rdb.PExpire(ctx, levelKey, 15*time.Second).Err(); err != nil {
		t.Fatalf("缩短 level key TTL 失败: %v", err)
	}

	updated, err = setBlacklistWithMaxTTL(ctx, key, levelKey, 1*time.Minute, 1)
	if err != nil {
		t.Fatalf("同档位重复设置失败: %v", err)
	}
	if updated {
		t.Fatalf("同档位不应更新 TTL")
	}

	ttlSameLevel, err := rdb.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("读取同档位 TTL 失败: %v", err)
	}
	if ttlSameLevel > 20*time.Second {
		t.Fatalf("同档位 TTL 不应被回刷，当前=%s", ttlSameLevel)
	}

	updated, err = setBlacklistWithMaxTTL(ctx, key, levelKey, 5*time.Minute, 2)
	if err != nil {
		t.Fatalf("升级档位设置失败: %v", err)
	}
	if !updated {
		t.Fatalf("升级档位应更新")
	}

	ttlEscalated, err := rdb.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("读取升级后 TTL 失败: %v", err)
	}
	if ttlEscalated < 4*time.Minute {
		t.Fatalf("升级档位后 TTL 应接近 5 分钟，当前=%s", ttlEscalated)
	}

	levelVal, err := rdb.Get(ctx, levelKey).Result()
	if err != nil {
		t.Fatalf("读取 level key 失败: %v", err)
	}
	if levelVal != "2" {
		t.Fatalf("升级后 level 应为 2，当前=%s", levelVal)
	}
}

func TestBanIP_ProgressiveEscalation(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	rdb = redis.NewClient(&redis.Options{Addr: s.Addr()})
	cfg = defaultConfig()
	ipsetSem = nil

	ctx := context.Background()
	ip := "192.168.110.7"
	blacklistKey := "blacklist:" + ip
	levelKey := "blacklist_level:" + ip

	banIP(ctx, ip)
	banIP(ctx, ip)

	exists, err := rdb.Exists(ctx, blacklistKey).Result()
	if err != nil {
		t.Fatalf("检查 blacklist key 失败: %v", err)
	}
	if exists != 0 {
		t.Fatalf("前两次违规不应封禁")
	}

	banIP(ctx, ip)

	levelVal, err := rdb.Get(ctx, levelKey).Result()
	if err != nil {
		t.Fatalf("读取 level key 失败: %v", err)
	}
	if levelVal != "1" {
		t.Fatalf("第 3 次违规应进入 1 级封禁，当前 level=%s", levelVal)
	}

	if err := rdb.PExpire(ctx, blacklistKey, 20*time.Second).Err(); err != nil {
		t.Fatalf("设置同档位验证 TTL 失败: %v", err)
	}
	if err := rdb.PExpire(ctx, levelKey, 20*time.Second).Err(); err != nil {
		t.Fatalf("设置同档位验证 level TTL 失败: %v", err)
	}

	banIP(ctx, ip)

	ttlAfterSameLevel, err := rdb.PTTL(ctx, blacklistKey).Result()
	if err != nil {
		t.Fatalf("读取同档位 TTL 失败: %v", err)
	}
	if ttlAfterSameLevel > 25*time.Second {
		t.Fatalf("同档位不应回刷 TTL，当前=%s", ttlAfterSameLevel)
	}

	banIP(ctx, ip)
	banIP(ctx, ip)

	levelVal, err = rdb.Get(ctx, levelKey).Result()
	if err != nil {
		t.Fatalf("读取升级到 2 级的 level 失败: %v", err)
	}
	if levelVal != "2" {
		t.Fatalf("第 6 次违规应进入 2 级封禁，当前 level=%s", levelVal)
	}

	ttlLevel2, err := rdb.PTTL(ctx, blacklistKey).Result()
	if err != nil {
		t.Fatalf("读取 2 级 TTL 失败: %v", err)
	}
	if ttlLevel2 < 4*time.Minute {
		t.Fatalf("2 级封禁 TTL 应接近 5 分钟，当前=%s", ttlLevel2)
	}

	for i := 0; i < 4; i++ {
		banIP(ctx, ip)
	}

	levelVal, err = rdb.Get(ctx, levelKey).Result()
	if err != nil {
		t.Fatalf("读取升级到 3 级的 level 失败: %v", err)
	}
	if levelVal != "3" {
		t.Fatalf("第 10 次违规应进入 3 级封禁，当前 level=%s", levelVal)
	}

	ttlLevel3, err := rdb.PTTL(ctx, blacklistKey).Result()
	if err != nil {
		t.Fatalf("读取 3 级 TTL 失败: %v", err)
	}
	if ttlLevel3 < 9*time.Minute {
		t.Fatalf("3 级封禁 TTL 应接近 10 分钟，当前=%s", ttlLevel3)
	}
}

func TestSyncIPSetBan_StaleTaskDoesNotOverrideLatest(t *testing.T) {
	originalCfg := cfg
	originalSem := ipsetSem
	originalCache := ipsetCache
	defer func() {
		cfg = originalCfg
		ipsetSem = originalSem
		ipsetCache = originalCache
	}()

	cfg = defaultConfig()
	cfg.Limits.IpsetTimeoutSec = 1
	ipsetCache = make(map[string]time.Time)

	tmpDir := t.TempDir()
	logFile := tmpDir + "/ipset_calls.log"
	scriptPath := tmpDir + "/ipset"
	script := "#!/bin/sh\n" +
		"echo \"$@\" >> \"" + logFile + "\"\n"
	if err := os.WriteFile(logFile, []byte{}, 0o644); err != nil {
		t.Fatalf("创建 fake ipset 日志文件失败: %v", err)
	}
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("创建 fake ipset 失败: %v", err)
	}
	t.Setenv("PATH", tmpDir+":"+os.Getenv("PATH"))
	if err := exec.Command("ipset", "probe").Run(); err != nil {
		t.Fatalf("fake ipset 不可执行: %v", err)
	}
	if err := os.WriteFile(logFile, []byte{}, 0o644); err != nil {
		t.Fatalf("重置 fake ipset 日志文件失败: %v", err)
	}

	ipsetSem = make(chan struct{}, 1)
	ipsetSem <- struct{}{}

	ip := "1.2.3.4"
	syncIPSetBan(ip, 2*time.Second)
	time.Sleep(50 * time.Millisecond)
	syncIPSetBan(ip, 5*time.Second)
	time.Sleep(50 * time.Millisecond)

	<-ipsetSem

	var content []byte
	deadline := time.Now().Add(2 * time.Second)
	for {
		var err error
		content, err = os.ReadFile(logFile)
		if err != nil {
			t.Fatalf("读取 fake ipset 调用日志失败: %v", err)
		}
		if strings.TrimSpace(string(content)) != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 ipset 调用超时")
		}
		time.Sleep(20 * time.Millisecond)
	}

	trimmed := strings.TrimSpace(string(content))
	if trimmed == "" {
		t.Fatalf("预期至少有一次 ipset 调用")
	}

	lines := strings.Split(trimmed, "\n")
	if len(lines) != 1 {
		t.Fatalf("过期任务不应执行 ipset，预期 1 次调用，实际 %d 次，内容=%q", len(lines), trimmed)
	}
}

func TestIsDialFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil 错误",
			err:  nil,
			want: false,
		},
		{
			name: "net.OpError dial",
			err: &net.OpError{
				Op:  "dial",
				Err: errors.New("connection refused"),
			},
			want: true,
		},
		{
			name: "url.Error 包装 dial 错误",
			err: &url.Error{
				Op:  "Get",
				URL: "http://127.0.0.1:9503",
				Err: &net.OpError{Op: "dial", Err: errors.New("connect: connection refused")},
			},
			want: true,
		},
		{
			name: "字符串匹配 dial tcp",
			err:  errors.New("dial tcp 127.0.0.1:9503: connect: connection refused"),
			want: true,
		},
		{
			name: "非拨号错误",
			err:  errors.New("unexpected EOF"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isDialFailure(tt.err)
			if got != tt.want {
				t.Fatalf("isDialFailure() = %v, want %v, err=%v", got, tt.want, tt.err)
			}
		})
	}
}

func TestMachineCircuitStateTransitions(t *testing.T) {
	originalCfg := cfg
	originalHasPrevCPU := machineHasPrevCPU
	originalUnhealthy := machineUnhealthyStreak
	originalHealthy := machineHealthyStreak
	originalOpen := machineCircuitOpen.Load()
	originalReason, _ := machineCircuitReason.Load().(string)
	defer func() {
		cfg = originalCfg
		machineHasPrevCPU = originalHasPrevCPU
		machineUnhealthyStreak = originalUnhealthy
		machineHealthyStreak = originalHealthy
		machineCircuitOpen.Store(originalOpen)
		machineCircuitReason.Store(originalReason)
	}()

	cfg = defaultConfig()
	cfg.Limits.MachineUnhealthyThreshold = 2
	cfg.Limits.MachineRecoveryThreshold = 2
	machineHasPrevCPU = false
	machineUnhealthyStreak = 0
	machineHealthyStreak = 0
	machineCircuitOpen.Store(false)
	machineCircuitReason.Store("")

	updateMachineCircuitState(false, "cpu=99.0%")
	if open, _ := machineCircuitStatus(); open {
		t.Fatalf("第一次异常不应触发熔断")
	}

	updateMachineCircuitState(false, "cpu=99.0%")
	open, reason := machineCircuitStatus()
	if !open {
		t.Fatalf("达到异常阈值后应触发熔断")
	}
	if reason == "" {
		t.Fatalf("熔断触发后应记录原因")
	}

	updateMachineCircuitState(true, "")
	if open, _ := machineCircuitStatus(); !open {
		t.Fatalf("恢复次数不足时不应立即关闭熔断")
	}

	updateMachineCircuitState(true, "")
	if open, _ := machineCircuitStatus(); open {
		t.Fatalf("达到恢复阈值后应关闭熔断")
	}
}

// 测试心跳机制
func TestHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建一个测试用的 HTTP 服务器
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// 心跳函数需要有效的 WebSocket 连接，这里只测试 context 取消
	// 创建一个简单的测试，确保 context 取消能正常工作
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	select {
	case <-ctx.Done():
		// context 正确取消
	case <-time.After(200 * time.Millisecond):
		t.Error("context 未在预期时间内取消")
	}
}

func TestAdminBanHandler(t *testing.T) {
	originalCfg := cfg
	defer func() {
		cfg = originalCfg
	}()

	cfg = defaultConfig()
	cfg.Auth.AdminToken = "test-admin-token"

	tests := []struct {
		name           string
		method         string
		authHeader     string
		body           string
		expectedStatus int
	}{
		{
			name:           "无认证",
			method:         "POST",
			authHeader:     "",
			body:           `{"ip": "1.2.3.4"}`,
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "错误的认证token",
			method:         "POST",
			authHeader:     "Bearer wrong-token",
			body:           `{"ip": "1.2.3.4"}`,
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "正确的认证token",
			method:         "POST",
			authHeader:     "Bearer test-admin-token",
			body:           `{"ip": "1.2.3.4"}`,
			expectedStatus: http.StatusInternalServerError,
		},
		{
			name:           "X-Admin-Token认证",
			method:         "POST",
			authHeader:     "test-admin-token",
			body:           `{"ip": "1.2.3.4"}`,
			expectedStatus: http.StatusInternalServerError,
		},
		{
			name:           "缺少IP",
			method:         "POST",
			authHeader:     "Bearer test-admin-token",
			body:           `{}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "无效的IP",
			method:         "POST",
			authHeader:     "Bearer test-admin-token",
			body:           `{"ip": "invalid-ip"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "错误的HTTP方法",
			method:         "GET",
			authHeader:     "Bearer test-admin-token",
			body:           `{"ip": "1.2.3.4"}`,
			expectedStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/admin/ban", strings.NewReader(tt.body))
			if tt.authHeader != "" {
				if strings.HasPrefix(strings.ToLower(tt.authHeader), "bearer ") {
					req.Header.Set("Authorization", tt.authHeader)
				} else {
					req.Header.Set("X-Admin-Token", tt.authHeader)
				}
			}
			w := httptest.NewRecorder()
			adminBanHandler(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, w.Code)
			}
		})
	}
}

// 基准测试：getClientIP
func BenchmarkGetClientIP(b *testing.B) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Forwarded-For", "1.2.3.4, 5.6.7.8")
	req.RemoteAddr = "9.10.11.12:1234"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getClientIP(req)
	}
}
