package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
)

type ClusterConfig struct {
	Enable              bool     `json:"enable"`
	BindAddr            string   `json:"bindAddr"`
	BindPort            int      `json:"bindPort"`
	Join                []string `json:"join"`
	SourceAllowCIDRs    []string `json:"sourceAllowCidrs"`
	Secret              string   `json:"secret"`
	RetransmitMult      int      `json:"retransmitMult"`
	NodeName            string   `json:"nodeName"`
	PublishQueueSize    int      `json:"publishQueueSize"`
	PublishBatchSize    int      `json:"publishBatchSize"`
	PublishFlushMs      int      `json:"publishFlushMs"`
	PublishMaxPerSec    int      `json:"publishMaxPerSec"`
	BatchPayloadMaxSize int      `json:"batchPayloadMaxSize"` // batch 最大字节数，0 使用默认值 1200
}

func buildClusterBatchPayload(pending [][]byte, limit int) ([]byte, int, error) {
	if limit <= 0 || len(pending) == 0 {
		return nil, 0, nil
	}
	if limit > len(pending) {
		limit = len(pending)
	}

	batch := make([]json.RawMessage, 0, limit)
	used := 0
	var payload []byte

	for i := 0; i < limit; i++ {
		batch = append(batch, json.RawMessage(pending[i]))
		candidate, err := json.Marshal(clusterBatchBanEvent{Type: "batch_ban", Msgs: batch})
		if err != nil {
			return nil, 0, err
		}
		if len(candidate) > getClusterBatchPayloadMaxBytes() {
			break
		}
		payload = candidate
		used = i + 1
	}

	if used < 2 {
		return nil, 0, nil
	}

	return payload, used, nil
}

type clusterBanEvent struct {
	IP         string `json:"ip"`
	DurationMS int64  `json:"duration"`
	Level      int    `json:"level"`
	Source     string `json:"source"`
	SourceAddr string `json:"sourceAddr"`
	Timestamp  int64  `json:"ts"`
	Signature  string `json:"sig"`
}

type clusterBatchBanEvent struct {
	Type string            `json:"type"`
	Msgs []json.RawMessage `json:"msgs"`
}

const defaultClusterBatchPayloadMaxBytes = 1200

// getClusterBatchPayloadMaxBytes 获取 batch 最大字节数，可根据网络 MTU 或配置调整
// 建议: Ethernet MTU 1500 - IP Header 20 - UDP Header 8 = 1472，考虑 memberlist 开销预留约 1200
func getClusterBatchPayloadMaxBytes() int {
	maxSize := cfg.Cluster.BatchPayloadMaxSize
	if maxSize <= 0 {
		return defaultClusterBatchPayloadMaxBytes
	}
	// 防止配置过大导致 UDP 分片或超出 memberlist 限制
	const maxAllowed = 8192
	if maxSize > maxAllowed {
		return maxAllowed
	}
	return maxSize
}

type clusterBroadcast struct {
	msg    []byte
	notify chan struct{}
}

func (b *clusterBroadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *clusterBroadcast) Message() []byte {
	return b.msg
}

func (b *clusterBroadcast) Finished() {
	if b.notify != nil {
		close(b.notify)
	}
}

func runClusterPublishWorker(stop <-chan struct{}) {
	flushInterval := time.Duration(cfg.Cluster.PublishFlushMs) * time.Millisecond
	if flushInterval <= 0 {
		flushInterval = 100 * time.Millisecond
	}

	batchSize := cfg.Cluster.PublishBatchSize
	if batchSize <= 0 {
		batchSize = 32
	}

	maxPerSec := cfg.Cluster.PublishMaxPerSec
	if maxPerSec <= 0 {
		maxPerSec = 500
	}

	perTick := maxPerSec * cfg.Cluster.PublishFlushMs / 1000
	if perTick <= 0 {
		perTick = 1
	}
	if perTick > batchSize {
		perTick = batchSize
	}

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	pending := make([][]byte, 0, batchSize*2)
	flush := func(limit int) {
		if limit <= 0 || len(pending) == 0 {
			return
		}
		if limit > len(pending) {
			limit = len(pending)
		}

		qAny := clusterBroadcastQueue.Load()
		if qAny == nil {
			pending = pending[limit:]
			return
		}

		queue := qAny.(*memberlist.TransmitLimitedQueue)

		batchPayload, batchedCount, err := buildClusterBatchPayload(pending, limit)
		if err != nil {
			logThrottled("cluster_batch_marshal", 3*time.Second, "cluster 批量事件序列化失败，回退单条发送: %v", err)
		}
		if batchedCount > 1 && len(batchPayload) > 0 {
			queue.QueueBroadcast(&clusterBroadcast{msg: batchPayload})
			pending = pending[batchedCount:]
			return
		}

		for i := 0; i < limit; i++ {
			queue.QueueBroadcast(&clusterBroadcast{msg: pending[i]})
		}
		pending = pending[limit:]
	}

	for {
		select {
		case msg := <-clusterPublishQueueCh:
			if msg != nil {
				pending = append(pending, msg)
				if len(pending) >= batchSize {
					flush(perTick)
				}
			}
		case <-ticker.C:
			flush(perTick)
		case <-stop:
			for len(pending) > 0 {
				flush(batchSize)
			}
			return
		}
	}
}

type clusterDelegate struct{}

func (d *clusterDelegate) NodeMeta(limit int) []byte { return nil }

func (d *clusterDelegate) NotifyMsg(msg []byte) {
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(msg, &envelope); err == nil && envelope.Type == "batch_ban" {
		var batch clusterBatchBanEvent
		if err := json.Unmarshal(msg, &batch); err != nil {
			return
		}
		for _, raw := range batch.Msgs {
			d.handleBanEvent(raw)
		}
		return
	}

	d.handleBanEvent(msg)
}

func (d *clusterDelegate) handleBanEvent(msg []byte) {
	var evt clusterBanEvent
	if err := json.Unmarshal(msg, &evt); err != nil {
		return
	}
	if net.ParseIP(evt.IP) == nil || evt.DurationMS <= 0 {
		return
	}

	if !verifyClusterEvent(evt) {
		logThrottled("cluster_event_verify", 3*time.Second, "cluster 事件签名校验失败: source=%s ip=%s", evt.Source, evt.IP)
		return
	}
	// 从 memberlist 获取发送节点的真实地址，而非信任事件自报的 SourceAddr
	senderAddr := getClusterMemberAddr(evt.Source)
	if senderAddr == "" {
		logThrottled("cluster_event_sender", 3*time.Second, "cluster 事件发送节点不在成员列表，丢弃: source=%s ip=%s", evt.Source, evt.IP)
		return
	}
	if !isClusterSourceAllowed(senderAddr) {
		logThrottled("cluster_event_source", 3*time.Second, "cluster 事件来源不在白名单，丢弃: source=%s senderAddr=%s ip=%s", evt.Source, senderAddr, evt.IP)
		return
	}

	if evt.Source != "" && evt.Source == getClusterNodeName() {
		return
	}

	id := clusterEventID(evt)
	if !rememberClusterEvent(id) {
		return
	}

	syncIPSetBan(evt.IP, time.Duration(evt.DurationMS)*time.Millisecond)
}

func (d *clusterDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	q := clusterBroadcastQueue.Load()
	if q == nil {
		return nil
	}
	return q.(*memberlist.TransmitLimitedQueue).GetBroadcasts(overhead, limit)
}

func (d *clusterDelegate) LocalState(join bool) []byte            { return nil }
func (d *clusterDelegate) MergeRemoteState(buf []byte, join bool) {}

var (
	clusterBroadcastQueue  atomic.Value
	clusterMemberList      atomic.Value
	clusterNodeName        atomic.Value
	clusterSecretMu        sync.RWMutex
	clusterSecret          []byte
	clusterSourceAllowMu   sync.RWMutex
	clusterSourceAllowNets []*net.IPNet
	clusterSeenMu          sync.Mutex
	clusterSeenEvents      = make(map[string]int64)
	clusterStopCleanup     chan struct{}
	clusterPublishQueueCh  chan []byte
	clusterWorkerWG        sync.WaitGroup
	clusterRunning         atomic.Bool
	clusterPublishDropCnt  atomic.Uint64 // cluster publish 队列满丢弃计数
)

func startClusterMesh() {
	if !cfg.Cluster.Enable {
		return
	}
	if clusterRunning.Load() {
		return
	}

	bindAddr := cfg.Cluster.BindAddr
	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}

	nodeName := cfg.Cluster.NodeName
	if nodeName == "" {
		hostname, _ := os.Hostname()
		nodeName = hostname + ":" + strconv.Itoa(cfg.Cluster.BindPort)
	}

	conf := memberlist.DefaultLANConfig()
	conf.Name = nodeName
	conf.BindAddr = bindAddr
	conf.BindPort = cfg.Cluster.BindPort
	conf.Delegate = &clusterDelegate{}

	list, err := memberlist.Create(conf)
	if err != nil {
		log.Printf("cluster 启动失败，降级为本地模式: %v", err)
		return
	}

	queue := &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return list.NumMembers()
		},
		RetransmitMult: cfg.Cluster.RetransmitMult,
	}

	clusterBroadcastQueue.Store(queue)
	clusterMemberList.Store(list)
	clusterNodeName.Store(nodeName)

	clusterSecretMu.Lock()
	clusterSecret = []byte(cfg.Cluster.Secret)
	clusterSecretMu.Unlock()
	if len(cfg.Cluster.Secret) == 0 {
		log.Println("[WARN] cluster secret 未配置，生产环境建议强制配置以增强安全性")
	}

	allowNets, err := buildClusterSourceAllowNets(cfg.Cluster.SourceAllowCIDRs)
	if err != nil {
		log.Printf("cluster sourceAllowCidrs 配置无效，降级为不过滤: %v", err)
		allowNets = nil
	}
	if len(cfg.Cluster.SourceAllowCIDRs) == 0 {
		log.Println("[WARN] cluster sourceAllowCidrs 未配置，接受所有来源事件，生产环境建议限制内网网段")
	}
	clusterSourceAllowMu.Lock()
	clusterSourceAllowNets = allowNets
	clusterSourceAllowMu.Unlock()

	if len(cfg.Cluster.Join) > 0 {
		if _, err := list.Join(cfg.Cluster.Join); err != nil {
			log.Printf("cluster Join 失败，继续单节点运行: %v", err)
		}
	}

	clusterStopCleanup = make(chan struct{})
	clusterPublishQueueCh = make(chan []byte, cfg.Cluster.PublishQueueSize)
	clusterPublishDropCnt.Store(0)

	clusterWorkerWG.Add(1)
	go func() {
		defer clusterWorkerWG.Done()
		cleanupClusterSeenEvents(clusterStopCleanup)
	}()

	clusterWorkerWG.Add(1)
	go func() {
		defer clusterWorkerWG.Done()
		runClusterPublishWorker(clusterStopCleanup)
	}()

	clusterRunning.Store(true)
	log.Printf("cluster mesh 已启动: node=%s bind=%s:%d peers=%d", nodeName, bindAddr, cfg.Cluster.BindPort, list.NumMembers())
}

func stopClusterMesh() {
	if !clusterRunning.Load() {
		return
	}
	clusterRunning.Store(false)

	if clusterStopCleanup != nil {
		close(clusterStopCleanup)
		clusterStopCleanup = nil
	}
	clusterWorkerWG.Wait()
	// 先关闭 channel 让 send 端能检测到（select 会感知到 closed channel），而非设为 nil 导致 panic
	if clusterPublishQueueCh != nil {
		close(clusterPublishQueueCh)
	}

	if listAny := clusterMemberList.Load(); listAny != nil {
		if err := listAny.(*memberlist.Memberlist).Shutdown(); err != nil {
			log.Printf("cluster 关闭失败: %v", err)
		}
	}
	clusterBroadcastQueue.Store((*memberlist.TransmitLimitedQueue)(nil))
	clusterMemberList.Store((*memberlist.Memberlist)(nil))
	clusterSourceAllowMu.Lock()
	clusterSourceAllowNets = nil
	clusterSourceAllowMu.Unlock()
	log.Println("cluster mesh 已关闭")
}

func publishBanEvent(ip string, banDuration time.Duration, level int) {
	if banDuration <= 0 {
		return
	}

	if net.ParseIP(ip) == nil {
		return
	}

	// 本机立即下发，确保请求路径本地实时生效。
	syncIPSetBan(ip, banDuration)

	if !cfg.Cluster.Enable || !clusterRunning.Load() {
		return
	}

	evt := clusterBanEvent{
		IP:         ip,
		DurationMS: banDuration.Milliseconds(),
		Level:      level,
		Source:     getClusterNodeName(),
		SourceAddr: getClusterLocalAddr(),
		Timestamp:  time.Now().UnixNano(),
	}
	if evt.Source == "" {
		evt.Source = "unknown"
	}
	payload, ok := marshalClusterEvent(evt)
	if !ok {
		return
	}

	if !rememberClusterEvent(clusterEventID(evt)) {
		return
	}

	if clusterPublishQueueCh == nil {
		return
	}

	// 双重检查：防止 clusterRunning 检查后 channel 被关闭
	if !clusterRunning.Load() {
		return
	}

	// 捕获向已关闭 channel 发送导致的 panic（stopClusterMesh 关闭 channel 时的竞态窗口）
	defer func() {
		if r := recover(); r != nil {
			dropped := clusterPublishDropCnt.Add(1)
			logThrottled("cluster_publish_drop", 2*time.Second, "cluster 发布队列关闭，丢弃事件: ip=%s total_dropped=%d", ip, dropped)
		}
	}()

	select {
	case clusterPublishQueueCh <- payload:
	default:
		dropped := clusterPublishDropCnt.Add(1)
		logThrottled("cluster_publish_drop", 2*time.Second, "cluster 发布队列拥堵，丢弃事件: ip=%s total_dropped=%d", ip, dropped)
	}
}

func getClusterNodeName() string {
	if val := clusterNodeName.Load(); val != nil {
		if s, ok := val.(string); ok {
			return s
		}
	}
	return ""
}

func getClusterLocalAddr() string {
	listAny := clusterMemberList.Load()
	if listAny == nil {
		return ""
	}
	node := listAny.(*memberlist.Memberlist).LocalNode()
	if node == nil || node.Addr == nil {
		return ""
	}
	return node.Addr.String()
}

// getClusterMemberAddr 通过节点名称从 memberlist 获取实际地址，防止 SourceAddr 被伪造
func getClusterMemberAddr(nodeName string) string {
	listAny := clusterMemberList.Load()
	if listAny == nil {
		return ""
	}
	list := listAny.(*memberlist.Memberlist)
	for _, member := range list.Members() {
		if member.Name == nodeName {
			if member.Addr != nil {
				return member.Addr.String()
			}
			return ""
		}
	}
	return ""
}

func marshalClusterEvent(evt clusterBanEvent) ([]byte, bool) {
	raw := evt
	raw.Signature = ""
	data, err := json.Marshal(raw)
	if err != nil {
		logThrottled("cluster_event_marshal", 3*time.Second, "cluster 事件序列化失败: %v", err)
		return nil, false
	}
	evt.Signature = signClusterEvent(data)
	finalData, err := json.Marshal(evt)
	if err != nil {
		logThrottled("cluster_event_marshal_final", 3*time.Second, "cluster 事件最终序列化失败: %v", err)
		return nil, false
	}
	return finalData, true
}

func verifyClusterEvent(evt clusterBanEvent) bool {
	clusterSecretMu.RLock()
	secret := clusterSecret
	clusterSecretMu.RUnlock()

	if len(secret) == 0 {
		return true
	}

	raw := evt
	raw.Signature = ""
	data, err := json.Marshal(raw)
	if err != nil {
		return false
	}
	expected := signWithSecret(secret, data)
	return hmac.Equal([]byte(expected), []byte(evt.Signature))
}

func signClusterEvent(data []byte) string {
	clusterSecretMu.RLock()
	secret := clusterSecret
	clusterSecretMu.RUnlock()
	if len(secret) == 0 {
		return ""
	}
	return signWithSecret(secret, data)
}

func signWithSecret(secret []byte, data []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(data)
	return hex.EncodeToString(mac.Sum(nil))
}

func clusterEventID(evt clusterBanEvent) string {
	return fmt.Sprintf("%s-%d-%d", evt.IP, evt.DurationMS, evt.Timestamp)
}

func rememberClusterEvent(id string) bool {
	now := time.Now().Unix()
	clusterSeenMu.Lock()
	defer clusterSeenMu.Unlock()
	if _, exists := clusterSeenEvents[id]; exists {
		return false
	}
	clusterSeenEvents[id] = now
	return true
}

func buildClusterSourceAllowNets(cidrs []string) ([]*net.IPNet, error) {
	if len(cidrs) == 0 {
		return nil, nil
	}

	nets := make([]*net.IPNet, 0, len(cidrs))
	for _, entry := range cidrs {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		_, ipNet, err := net.ParseCIDR(entry)
		if err != nil {
			return nil, fmt.Errorf("invalid sourceAllowCidrs entry %q: %w", entry, err)
		}
		nets = append(nets, ipNet)
	}
	if len(nets) == 0 {
		return nil, nil
	}
	return nets, nil
}

func isClusterSourceAllowed(sourceAddr string) bool {
	clusterSourceAllowMu.RLock()
	allowNets := clusterSourceAllowNets
	clusterSourceAllowMu.RUnlock()

	if len(allowNets) == 0 {
		return true
	}

	ip := net.ParseIP(strings.TrimSpace(sourceAddr))
	if ip == nil {
		return false
	}

	for _, ipNet := range allowNets {
		if ipNet.Contains(ip) {
			return true
		}
	}
	return false
}

func cleanupClusterSeenEvents(stop <-chan struct{}) {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			expireBefore := time.Now().Add(-10 * time.Minute).Unix()
			clusterSeenMu.Lock()
			for id, ts := range clusterSeenEvents {
				if ts < expireBefore {
					delete(clusterSeenEvents, id)
				}
			}
			clusterSeenMu.Unlock()
		case <-stop:
			return
		}
	}
}
