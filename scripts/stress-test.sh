#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# 默认配置
TARGET_URL="${TARGET_URL:-http://192.168.110.8:9999}"
ADMIN_TOKEN="${ADMIN_TOKEN:-CHANGE_ME_ADMIN_TOKEN}"
TEST_DURATION="${TEST_DURATION:-30}"
THREADS="${THREADS:-4}"
CONNECTIONS="${CONNECTIONS:-100}"
ADMIN_BAN_URL="${ADMIN_BAN_URL:-${TARGET_URL}/admin/ban}"
ADMIN_BENCH_TOTAL="${ADMIN_BENCH_TOTAL:-10000}"
ADMIN_BENCH_CONCURRENCY="${ADMIN_BENCH_CONCURRENCY:-100}"
ADMIN_BENCH_MODE="${ADMIN_BENCH_MODE:-unban}"
ADMIN_BENCH_IP_MODE="${ADMIN_BENCH_IP_MODE:-same}"
ADMIN_BENCH_BASE_IP="${ADMIN_BENCH_BASE_IP:-198.51.100.10}"
ADMIN_BENCH_MULTI_IP_POOL="${ADMIN_BENCH_MULTI_IP_POOL:-200}"
ADMIN_BENCH_BAN_DURATION="${ADMIN_BENCH_BAN_DURATION:-600}"
ADMIN_BENCH_SUCCESS_RATE_THRESHOLD="${ADMIN_BENCH_SUCCESS_RATE_THRESHOLD:-99.90}"
ADMIN_BENCH_FAIL_ON_THRESHOLD="${ADMIN_BENCH_FAIL_ON_THRESHOLD:-false}"
ADMIN_BENCH_SUCCESS_RPS_TARGET="${ADMIN_BENCH_SUCCESS_RPS_TARGET:-1000}"
ADMIN_BENCH_TOKEN_HEADER="${ADMIN_BENCH_TOKEN_HEADER:-authorization}"
ADMIN_BENCH_CONNECT_TIMEOUT_SEC="${ADMIN_BENCH_CONNECT_TIMEOUT_SEC:-1}"
ADMIN_BENCH_MAX_TIME_SEC="${ADMIN_BENCH_MAX_TIME_SEC:-3}"
ADMIN_BENCH_IP_VERSION="${ADMIN_BENCH_IP_VERSION:-ipv4}"
DIAG_URL="${DIAG_URL:-${TARGET_URL}/}"
DIAG_CONCURRENCY="${DIAG_CONCURRENCY:-100}"
DIAG_DURATION="${DIAG_DURATION:-30}"
DIAG_FAIL_FAST="${DIAG_FAIL_FAST:-true}"
DIAG_STOP_ON_CODES="${DIAG_STOP_ON_CODES:-403,429,503}"
DIAG_CONNECT_TIMEOUT_SEC="${DIAG_CONNECT_TIMEOUT_SEC:-1}"
DIAG_MAX_TIME_SEC="${DIAG_MAX_TIME_SEC:-2}"
WS_URL="${WS_URL:-}"
REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:6379}"
REDIS_DB="${REDIS_DB:-14}"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"
REDIS_BAN_QUEUE="${REDIS_BAN_QUEUE:-admin:ban}"
REDIS_UNBAN_QUEUE="${REDIS_UNBAN_QUEUE:-admin:unban}"
REDIS_QUEUE_TEST_TOTAL="${REDIS_QUEUE_TEST_TOTAL:-100}"
REDIS_QUEUE_TEST_CONCURRENCY="${REDIS_QUEUE_TEST_CONCURRENCY:-10}"
REDIS_QUEUE_TEST_MODE="${REDIS_QUEUE_TEST_MODE:-ban}"
REDIS_QUEUE_TEST_IP_MODE="${REDIS_QUEUE_TEST_IP_MODE:-same}"
REDIS_QUEUE_TEST_BASE_IP="${REDIS_QUEUE_TEST_BASE_IP:-198.51.100.20}"
REDIS_QUEUE_TEST_MULTI_IP_POOL="${REDIS_QUEUE_TEST_MULTI_IP_POOL:-50}"
REDIS_QUEUE_TEST_BAN_DURATION="${REDIS_QUEUE_TEST_BAN_DURATION:-600}"
REDIS_QUEUE_TEST_IP_VERSION="${REDIS_QUEUE_TEST_IP_VERSION:-ipv4}"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    printf "%b\n" "${GREEN}[INFO]${NC} $1"
}

build_ws_url() {
    if [ -n "$WS_URL" ]; then
        printf "%s" "$WS_URL"
        return
    fi

    case "$TARGET_URL" in
        https://*)
            printf "wss://%s" "${TARGET_URL#https://}"
            ;;
        http://*)
            printf "ws://%s" "${TARGET_URL#http://}"
            ;;
        *)
            printf "ws://%s" "$TARGET_URL"
            ;;
    esac
}

# 场景8: Admin Ban/Unban 压测
test_admin_ban_benchmark() {
    log_info "=== 场景8: Admin Ban/Unban 压测 ==="

    if [ "$ADMIN_BENCH_MODE" != "ban" ] && [ "$ADMIN_BENCH_MODE" != "unban" ]; then
        log_error "ADMIN_BENCH_MODE 仅支持 ban 或 unban，当前值: $ADMIN_BENCH_MODE"
        return 1
    fi
    if [ "$ADMIN_BENCH_IP_MODE" != "same" ] && [ "$ADMIN_BENCH_IP_MODE" != "multi" ]; then
        log_error "ADMIN_BENCH_IP_MODE 仅支持 same 或 multi，当前值: $ADMIN_BENCH_IP_MODE"
        return 1
    fi
    if ! [[ "$ADMIN_BENCH_MULTI_IP_POOL" =~ ^[1-9][0-9]*$ ]]; then
        log_error "ADMIN_BENCH_MULTI_IP_POOL 必须为正整数，当前值: $ADMIN_BENCH_MULTI_IP_POOL"
        return 1
    fi
    if [ "$ADMIN_BENCH_TOKEN_HEADER" != "authorization" ] && [ "$ADMIN_BENCH_TOKEN_HEADER" != "x-admin-token" ]; then
        log_error "ADMIN_BENCH_TOKEN_HEADER 仅支持 authorization 或 x-admin-token，当前值: $ADMIN_BENCH_TOKEN_HEADER"
        return 1
    fi

    log_info "URL: $ADMIN_BAN_URL"
    log_info "总请求: $ADMIN_BENCH_TOTAL"
    log_info "并发: $ADMIN_BENCH_CONCURRENCY"
    log_info "模式: $ADMIN_BENCH_MODE"
    log_info "IP模式: $ADMIN_BENCH_IP_MODE"
    if [ "$ADMIN_BENCH_IP_MODE" = "multi" ]; then
        log_info "多IP池: $ADMIN_BENCH_MULTI_IP_POOL"
        if [ "$ADMIN_BENCH_MULTI_IP_POOL" -lt "$ADMIN_BENCH_TOTAL" ]; then
            log_warn "多IP池小于总请求，将出现重复IP（非最坏情况）"
        fi
    fi
    log_info "TokenHeader: $ADMIN_BENCH_TOKEN_HEADER"
    log_info "CurlTimeout: connect=${ADMIN_BENCH_CONNECT_TIMEOUT_SEC}s total=${ADMIN_BENCH_MAX_TIME_SEC}s"

    local tmp_file
    local times_file
    local token_header
    local connect_timeout_sec
    local max_time_sec
    local bench_start_ts
    local bench_end_ts
    local bench_elapsed_sec
    tmp_file=$(mktemp)
    times_file=$(mktemp)
    token_header="$ADMIN_BENCH_TOKEN_HEADER"
    connect_timeout_sec="$ADMIN_BENCH_CONNECT_TIMEOUT_SEC"
    max_time_sec="$ADMIN_BENCH_MAX_TIME_SEC"
    bench_start_ts=$(date +%s)

    seq "$ADMIN_BENCH_TOTAL" | xargs -P"$ADMIN_BENCH_CONCURRENCY" -I{} bash -c '
        idx="$1"
        base_ip="$2"
        mode="$3"
        ban_duration="$4"
        target_url="$5"
        token="$6"
        ip_mode="$7"
        multi_ip_pool="${8}"
        token_header="$9"
        connect_timeout_sec="${10}"
        max_time_sec="${11}"
        ip_version="${12}"

        if [ "$ip_mode" = "multi" ]; then
            seq_idx=$(( (idx - 1) % multi_ip_pool ))
            if [ "$ip_version" = "ipv6" ]; then
                # IPv6 multi IP generation
                h1=$(( 0x2001 + (seq_idx / 65536) % 0x1000 ))
                h2=$(( 0x0db8 ))
                h3=$(( (seq_idx / 256) % 65536 ))
                h4=$(( seq_idx % 65536 ))
                req_ip="${h1}:${h2}:${h3}:${h4}::1"
            else
                # IPv4 multi IP generation
                octet2=$(( (seq_idx / 64516) % 253 + 1 ))
                octet3=$(( (seq_idx / 254) % 254 + 1 ))
                octet4=$(( seq_idx % 254 + 1 ))
                req_ip="198.${octet2}.${octet3}.${octet4}"
            fi
        else
            req_ip="$base_ip"
        fi

        if [ "$mode" = "ban" ]; then
            req_duration="$ban_duration"
            req_reason="bench-ban"
        else
            req_duration=0
            req_reason="bench-unban"
        fi

        payload=$(printf "{\"ip\":\"%s\",\"duration\":%s,\"reason\":\"%s\"}" "$req_ip" "$req_duration" "$req_reason")
        if [ "$token_header" = "x-admin-token" ]; then
            result=$(curl -s -o /dev/null \
                --connect-timeout "$connect_timeout_sec" \
                --max-time "$max_time_sec" \
                -X POST "$target_url" \
                -H "X-Admin-Token: $token" \
                -H "Content-Type: application/json" \
                -d "$payload" \
                -w "%{time_total} %{http_code}" 2>/dev/null || true)
        else
            result=$(curl -s -o /dev/null \
                --connect-timeout "$connect_timeout_sec" \
                --max-time "$max_time_sec" \
                -X POST "$target_url" \
                -H "Authorization: Bearer $token" \
                -H "Content-Type: application/json" \
                -d "$payload" \
                -w "%{time_total} %{http_code}" 2>/dev/null || true)
        fi

        if [ -z "$result" ]; then
            printf "0.000000 000\n"
        else
            printf "%s\n" "$result"
        fi
    ' _ {} "$ADMIN_BENCH_BASE_IP" "$ADMIN_BENCH_MODE" "$ADMIN_BENCH_BAN_DURATION" "$ADMIN_BAN_URL" "$ADMIN_TOKEN" "$ADMIN_BENCH_IP_MODE" "$ADMIN_BENCH_MULTI_IP_POOL" "$token_header" "$connect_timeout_sec" "$max_time_sec" "$ADMIN_BENCH_IP_VERSION" >> "$tmp_file"

    bench_end_ts=$(date +%s)
    bench_elapsed_sec=$((bench_end_ts - bench_start_ts))
    if [ "$bench_elapsed_sec" -le 0 ]; then
        bench_elapsed_sec=1
    fi

    if [ ! -s "$tmp_file" ]; then
        log_error "压测结果为空，请检查目标服务与参数"
        rm -f "$tmp_file" "$times_file"
        return 1
    fi

    local count
    count=$(wc -l < "$tmp_file" | tr -d ' ')
    awk '{print $1}' "$tmp_file" | sort -n > "$times_file"

    local avg max min p95 p99 p95_index p99_index
    avg=$(awk '{sum+=$1} END {if (NR>0) printf "%.6f", sum/NR; else print "0"}' "$tmp_file")
    max=$(awk 'BEGIN{max=0}{if($1>max)max=$1}END{printf "%.6f", max}' "$tmp_file")
    min=$(awk 'BEGIN{min=999999}{if($1<min)min=$1}END{if(min==999999)min=0; printf "%.6f", min}' "$tmp_file")

    p95_index=$(( (count * 95 + 99) / 100 ))
    p99_index=$(( (count * 99 + 99) / 100 ))
    p95=$(sed -n "${p95_index}p" "$times_file")
    p99=$(sed -n "${p99_index}p" "$times_file")
    p95=${p95:-0}
    p99=${p99:-0}

    local ok_count busy_count unauthorized_count network_fail_count success_rate
    local failed_count total_rps success_rps failed_rps
    ok_count=$(awk '$2 ~ /^2[0-9][0-9]$/ {n++} END {print n+0}' "$tmp_file")
    busy_count=$(awk '$2=="503" {n++} END {print n+0}' "$tmp_file")
    unauthorized_count=$(awk '$2=="401" {n++} END {print n+0}' "$tmp_file")
    network_fail_count=$(awk '$2=="000" {n++} END {print n+0}' "$tmp_file")
    success_rate=$(awk -v ok="$ok_count" -v total="$count" 'BEGIN {if (total>0) printf "%.4f", (ok*100.0)/total; else print "0.0000"}')
    failed_count=$((count - ok_count))
    total_rps=$(awk -v total="$count" -v sec="$bench_elapsed_sec" 'BEGIN {if (sec>0) printf "%.2f", total/sec; else print "0.00"}')
    success_rps=$(awk -v ok="$ok_count" -v sec="$bench_elapsed_sec" 'BEGIN {if (sec>0) printf "%.2f", ok/sec; else print "0.00"}')
    failed_rps=$(awk -v fail="$failed_count" -v sec="$bench_elapsed_sec" 'BEGIN {if (sec>0) printf "%.2f", fail/sec; else print "0.00"}')

    echo "------ Result ------"
    echo "Requests: $count"
    echo "Concurrency: $ADMIN_BENCH_CONCURRENCY"
    echo "Mode: $ADMIN_BENCH_MODE"
    echo "IP Mode: $ADMIN_BENCH_IP_MODE"
    echo "Avg Time: ${avg} s"
    echo "P95 Time: ${p95} s"
    echo "P99 Time: ${p99} s"
    echo "Max Time: ${max} s"
    echo "Min Time: ${min} s"
    echo "HTTP 2xx: $ok_count"
    echo "HTTP 503: $busy_count"
    echo "HTTP 401: $unauthorized_count"
    echo "HTTP 000: $network_fail_count"
    echo "Success Rate (2xx): ${success_rate}%"
    echo "Real Elapsed: ${bench_elapsed_sec} s"
    echo "Total RPS (real): ${total_rps}"
    echo "Success RPS (real): ${success_rps}"
    echo "Failed RPS (real): ${failed_rps}"

    if awk -v rps="$success_rps" -v target="$ADMIN_BENCH_SUCCESS_RPS_TARGET" 'BEGIN {exit !(rps+0 >= target+0)}'; then
        log_info "吞吐目标通过: Success RPS=${success_rps} (目标 ${ADMIN_BENCH_SUCCESS_RPS_TARGET})"
    else
        log_warn "吞吐目标未达标: Success RPS=${success_rps} (目标 ${ADMIN_BENCH_SUCCESS_RPS_TARGET})"
    fi

    echo "------ HTTP Code Distribution ------"
    awk '{codes[$2]++} END {for (code in codes) printf "%s %d\n", code, codes[code]}' "$tmp_file" | sort -n

    if awk -v rate="$success_rate" -v threshold="$ADMIN_BENCH_SUCCESS_RATE_THRESHOLD" 'BEGIN {exit !(rate+0 < threshold+0)}'; then
        log_error "成功率告警: 2xx 成功率 ${success_rate}% 低于阈值 ${ADMIN_BENCH_SUCCESS_RATE_THRESHOLD}%"
        if [ "$ADMIN_BENCH_FAIL_ON_THRESHOLD" = "true" ]; then
            rm -f "$tmp_file" "$times_file"
            return 2
        fi
    else
        log_info "成功率通过: 2xx 成功率 ${success_rate}% (阈值 ${ADMIN_BENCH_SUCCESS_RATE_THRESHOLD}%)"
    fi

    rm -f "$tmp_file" "$times_file"
}

log_warn() {
    printf "%b\n" "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    printf "%b\n" "${RED}[ERROR]${NC} $1"
}

# 检查工具
check_tool() {
    local tool="$1"
    if ! command -v "$tool" &> /dev/null; then
        log_error "$tool 未安装，请先安装: brew install $tool (macOS) 或 apt install $tool (Linux)"
        return 1
    fi
    return 0
}

# 场景1: 单IP高频请求测试
test_single_ip_high_rate() {
    log_info "=== 场景1: 单IP高频请求测试 ==="
    log_info "目标: 验证单IP限流是否生效 (6 QPS/IP)"
    log_info "参数: 1连接, 30秒, 持续请求"

    if check_tool "wrk"; then
        wrk -t1 -c1 -d"${TEST_DURATION}s" "${TARGET_URL}/"
    elif check_tool "ab"; then
        ab -n 1000 -c 1 -t "${TEST_DURATION}" "${TARGET_URL}/"
    else
        log_error "需要 wrk 或 ab 工具"
        return 1
    fi
}

# 场景2: 单IP并发连接测试
test_single_ip_concurrent() {
    log_info "=== 场景2: 单IP并发连接测试 ==="
    log_info "目标: 验证单IP并发连接限制 (2连接/IP)"
    log_info "参数: 10连接, 30秒"

    if check_tool "wrk"; then
        wrk -t4 -c10 -d"${TEST_DURATION}s" "${TARGET_URL}/"
    elif check_tool "ab"; then
        ab -n 1000 -c 10 -t "${TEST_DURATION}" "${TARGET_URL}/"
    else
        log_error "需要 wrk 或 ab 工具"
        return 1
    fi
}

# 场景3: 全局DDoS模拟
test_global_ddos() {
    log_info "=== 场景3: 全局DDoS模拟 ==="
    log_info "目标: 验证全局限流是否生效 (250 QPS)"
    log_info "参数: ${CONNECTIONS}连接, ${THREADS}线程, ${TEST_DURATION}秒"

    if check_tool "wrk"; then
        wrk -t"${THREADS}" -c"${CONNECTIONS}" -d"${TEST_DURATION}s" "${TARGET_URL}/"
    elif check_tool "ab"; then
        ab -n 10000 -c "${CONNECTIONS}" -t "${TEST_DURATION}" "${TARGET_URL}/"
    else
        log_error "需要 wrk 或 ab 工具"
        return 1
    fi
}

# 场景4: 渐进封禁测试
test_progressive_ban() {
    log_info "=== 场景4: 渐进封禁测试 ==="
    log_info "目标: 验证渐进封禁是否触发 (第3次违规触发1分钟封禁)"
    log_info "参数: 连续触发违规"

    local test_ip="198.51.100.99"
    local ban_duration=600

    log_info "手动封禁测试IP: ${test_ip}"
    curl -s -X POST "${TARGET_URL}/admin/ban" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"ip\":\"${test_ip}\",\"duration\":${ban_duration},\"reason\":\"stress-test\"}"

    log_info "等待5秒后尝试访问封禁IP"
    sleep 5

    log_info "测试封禁是否生效"
    for i in {1..10}; do
        response=$(curl -s -o /dev/null -w "%{http_code}" "${TARGET_URL}/")
        if [ "$response" = "429" ]; then
            log_info "第 $i 次请求返回 429 (限流生效)"
        else
            log_warn "第 $i 次请求返回 $response (未触发限流)"
        fi
        sleep 1
    done
}

# 场景5: 集群封禁传播测试
test_cluster_ban_propagation() {
    log_info "=== 场景5: 集群封禁传播测试 ==="
    log_info "目标: 验证集群内封禁传播延迟"
    log_info "参数: 需要配置集群节点"

    local node1_url="${NODE1_URL:-http://127.0.0.1:18081}"
    local node2_url="${NODE2_URL:-http://127.0.0.1:18082}"
    local node3_url="${NODE3_URL:-http://127.0.0.1:18083}"
    local test_ip="203.0.113.88"

    log_info "从 node1 触发封禁: ${test_ip}"
    curl -s -X POST "${node1_url}/admin/ban" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"ip\":\"${test_ip}\",\"duration\":600,\"reason\":\"cluster-test\"}"

    log_info "等待2秒后检查其他节点"
    sleep 2

    for node_url in "$node2_url" "$node3_url"; do
        log_info "检查节点: ${node_url}"
        response=$(curl -s -o /dev/null -w "%{http_code}" "${node_url}/admin/ban" \
            -H "Authorization: Bearer ${ADMIN_TOKEN}" \
            -H "Content-Type: application/json" \
            -d "{\"ip\":\"${test_ip}\"}")
        log_info "响应状态码: ${response}"
    done
}

# 场景6: WebSocket 连接测试
test_websocket() {
    log_info "=== 场景6: WebSocket 连接测试 ==="
    log_info "目标: 验证 WebSocket 连接限制"

    local ws_url
    ws_url="$(build_ws_url)"
    log_info "WebSocket URL: ${ws_url}"

    if ! command -v websocat &> /dev/null; then
        log_warn "websocat 未安装，跳过 WebSocket 测试"
        log_warn "安装: brew install websocat (macOS) 或 apt install websocat (Linux)"
        return 0
    fi

    log_info "测试 WebSocket 连接"
    for i in {1..5}; do
        websocat "$ws_url" --exit-on-eof &
        log_info "启动 WebSocket 连接 $i"
        sleep 0.5
    done

    log_info "等待5秒"
    sleep 5

    log_info "尝试第6个连接（应被限流）"
    websocat "$ws_url" --exit-on-eof || log_warn "第6个连接被拒绝（符合预期）"
}

# 场景7: 本地持久化恢复测试
test_local_persistence() {
    log_info "=== 场景7: 本地持久化恢复测试 ==="
    log_info "目标: 验证本地短期持久化恢复"

    local test_ip="192.0.2.77"
    local persist_file="./data/local_bans.json"

    log_info "手动封禁测试IP: ${test_ip}"
    curl -s -X POST "${TARGET_URL}/admin/ban" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"ip\":\"${test_ip}\",\"duration\":600,\"reason\":\"persist-test\"}"

    log_info "等待5秒让持久化刷盘"
    sleep 5

    if [ -f "$persist_file" ]; then
        log_info "持久化文件存在: $persist_file"
        log_info "文件内容预览:"
        head -20 "$persist_file"
    else
        log_warn "持久化文件不存在: $persist_file"
    fi
}

# 场景10: Redis 队列封禁/解封测试
test_redis_queue() {
    log_info "=== 场景10: Redis 队列封禁/解封测试 ==="

    if ! command -v redis-cli &> /dev/null; then
        log_error "redis-cli 未安装，请先安装: brew install redis (macOS) 或 apt install redis-tools (Linux)"
        return 1
    fi

    if [ "$REDIS_QUEUE_TEST_MODE" != "ban" ] && [ "$REDIS_QUEUE_TEST_MODE" != "unban" ]; then
        log_error "REDIS_QUEUE_TEST_MODE 仅支持 ban 或 unban，当前值: $REDIS_QUEUE_TEST_MODE"
        return 1
    fi
    if [ "$REDIS_QUEUE_TEST_IP_MODE" != "same" ] && [ "$REDIS_QUEUE_TEST_IP_MODE" != "multi" ]; then
        log_error "REDIS_QUEUE_TEST_IP_MODE 仅支持 same 或 multi，当前值: $REDIS_QUEUE_TEST_IP_MODE"
        return 1
    fi
    if ! [[ "$REDIS_QUEUE_TEST_MULTI_IP_POOL" =~ ^[1-9][0-9]*$ ]]; then
        log_error "REDIS_QUEUE_TEST_MULTI_IP_POOL 必须为正整数，当前值: $REDIS_QUEUE_TEST_MULTI_IP_POOL"
        return 1
    fi

    local redis_args
    if [ -n "$REDIS_PASSWORD" ]; then
        redis_args="-a $REDIS_PASSWORD"
    fi
    redis_args="$redis_args -h $(echo "$REDIS_ADDR" | cut -d: -f1) -p $(echo "$REDIS_ADDR" | cut -d: -f2) -n $REDIS_DB"

    log_info "Redis: $REDIS_ADDR (DB: $REDIS_DB)"
    log_info "Ban Queue: $REDIS_BAN_QUEUE"
    log_info "Unban Queue: $REDIS_UNBAN_QUEUE"
    log_info "总请求: $REDIS_QUEUE_TEST_TOTAL"
    log_info "并发: $REDIS_QUEUE_TEST_CONCURRENCY"
    log_info "模式: $REDIS_QUEUE_TEST_MODE"
    log_info "IP模式: $REDIS_QUEUE_TEST_IP_MODE"
    if [ "$REDIS_QUEUE_TEST_IP_MODE" = "multi" ]; then
        log_info "多IP池: $REDIS_QUEUE_TEST_MULTI_IP_POOL"
    fi

    local queue_name
    if [ "$REDIS_QUEUE_TEST_MODE" = "ban" ]; then
        queue_name="$REDIS_BAN_QUEUE"
    else
        queue_name="$REDIS_UNBAN_QUEUE"
    fi

    log_info "目标队列: $queue_name"

    local tmp_file
    local times_file
    local ip_file
    local test_start_ts
    local test_end_ts
    local test_elapsed_sec
    tmp_file=$(mktemp)
    times_file=$(mktemp)
    ip_file=$(mktemp)
    test_start_ts=$(date +%s)

    seq "$REDIS_QUEUE_TEST_TOTAL" | xargs -P"$REDIS_QUEUE_TEST_CONCURRENCY" -I{} bash -c '
        idx="$1"
        base_ip="$2"
        mode="$3"
        ban_duration="$4"
        ip_mode="$5"
        multi_ip_pool="${6}"
        queue_name="$7"
        redis_args="$8"
        ip_version="${10}"

        if [ "$ip_mode" = "multi" ]; then
            seq_idx=$(( (idx - 1) % multi_ip_pool ))
            if [ "$ip_version" = "ipv6" ]; then
                # IPv6 multi IP generation
                h1=$(( 0x2001 + (seq_idx / 65536) % 0x1000 ))
                h2=$(( 0x0db8 ))
                h3=$(( (seq_idx / 256) % 65536 ))
                h4=$(( seq_idx % 65536 ))
                req_ip="${h1}:${h2}:${h3}:${h4}::1"
            else
                # IPv4 multi IP generation
                octet2=$(( (seq_idx / 64516) % 253 + 1 ))
                octet3=$(( (seq_idx / 254) % 254 + 1 ))
                octet4=$(( seq_idx % 254 + 1 ))
                req_ip="198.${octet2}.${octet3}.${octet4}"
            fi
        else
            req_ip="$base_ip"
        fi

        start_time=$(date +%s.%N)
        if [ "$mode" = "ban" ]; then
            payload=$(printf "{\"ip\":\"%s\",\"duration\":%s,\"reason\":\"redis-queue-test\"}" "$req_ip" "$ban_duration")
        else
            payload=$(printf "{\"ip\":\"%s\"}" "$req_ip")
        fi

        if result=$(redis-cli $redis_args PUBLISH "$queue_name" "$payload" 2>&1); then
            end_time=$(date +%s.%N)
            elapsed=$(echo "$end_time - $start_time" | bc)
            printf "%.6f 200\n" "$elapsed"
            printf "%s\n" "$req_ip" >> "$9"
        else
            end_time=$(date +%s.%N)
            elapsed=$(echo "$end_time - $start_time" | bc)
            printf "%.6f 000\n" "$elapsed"
        fi
    ' _ {} "$REDIS_QUEUE_TEST_BASE_IP" "$REDIS_QUEUE_TEST_MODE" "$REDIS_QUEUE_TEST_BAN_DURATION" "$REDIS_QUEUE_TEST_IP_MODE" "$REDIS_QUEUE_TEST_MULTI_IP_POOL" "$queue_name" "$redis_args" "$ip_file" "$REDIS_QUEUE_TEST_IP_VERSION" >> "$tmp_file"

    test_end_ts=$(date +%s)
    test_elapsed_sec=$((test_end_ts - test_start_ts))
    if [ "$test_elapsed_sec" -le 0 ]; then
        test_elapsed_sec=1
    fi

    if [ ! -s "$tmp_file" ]; then
        log_error "压测结果为空，请检查 Redis 连接与参数"
        rm -f "$tmp_file" "$times_file" "$ip_file"
        return 1
    fi

    local count
    count=$(wc -l < "$tmp_file" | tr -d ' ')
    awk '{print $1}' "$tmp_file" | sort -n > "$times_file"

    local avg max min p95 p99 p95_index p99_index
    avg=$(awk '{sum+=$1} END {if (NR>0) printf "%.6f", sum/NR; else print "0"}' "$tmp_file")
    max=$(awk 'BEGIN{max=0}{if($1>max)max=$1}END{printf "%.6f", max}' "$tmp_file")
    min=$(awk 'BEGIN{min=999999}{if($1<min)min=$1}END{if(min==999999)min=0; printf "%.6f", min}' "$tmp_file")

    p95_index=$(( (count * 95 + 99) / 100 ))
    p99_index=$(( (count * 99 + 99) / 100 ))
    p95=$(sed -n "${p95_index}p" "$times_file")
    p99=$(sed -n "${p99_index}p" "$times_file")
    p95=${p95:-0}
    p99=${p99:-0}

    local ok_count fail_count success_rate
    local total_rps success_rps failed_rps
    ok_count=$(awk '$2=="200" {n++} END {print n+0}' "$tmp_file")
    fail_count=$(awk '$2=="000" {n++} END {print n+0}' "$tmp_file")
    success_rate=$(awk -v ok="$ok_count" -v total="$count" 'BEGIN {if (total>0) printf "%.4f", (ok*100.0)/total; else print "0.0000"}')
    total_rps=$(awk -v total="$count" -v sec="$test_elapsed_sec" 'BEGIN {if (sec>0) printf "%.2f", total/sec; else print "0.00"}')
    success_rps=$(awk -v ok="$ok_count" -v sec="$test_elapsed_sec" 'BEGIN {if (sec>0) printf "%.2f", ok/sec; else print "0.00"}')
    failed_rps=$(awk -v fail="$fail_count" -v sec="$test_elapsed_sec" 'BEGIN {if (sec>0) printf "%.2f", fail/sec; else print "0.00"}')

    echo "------ Result ------"
    echo "Requests: $count"
    echo "Concurrency: $REDIS_QUEUE_TEST_CONCURRENCY"
    echo "Mode: $REDIS_QUEUE_TEST_MODE"
    echo "IP Mode: $REDIS_QUEUE_TEST_IP_MODE"
    echo "Queue: $queue_name"
    echo "Avg Time: ${avg} s"
    echo "P95 Time: ${p95} s"
    echo "P99 Time: ${p99} s"
    echo "Max Time: ${max} s"
    echo "Min Time: ${min} s"
    echo "Success (200): $ok_count"
    echo "Failed (000): $fail_count"
    echo "Success Rate: ${success_rate}%"
    echo "Real Elapsed: ${test_elapsed_sec} s"
    echo "Total RPS (real): ${total_rps}"
    echo "Success RPS (real): ${success_rps}"
    echo "Failed RPS (real): ${failed_rps}"

    rm -f "$tmp_file" "$times_file"

    log_info "等待5秒让队列消息被处理"
    sleep 5

    log_info "验证 IP 处理情况"
    local sent_count
    local unique_count
    sent_count=$(wc -l < "$ip_file" | tr -d ' ')
    unique_count=$(sort -u "$ip_file" | wc -l | tr -d ' ')
    log_info "发送的 IP 总数: $sent_count"
    log_info "唯一 IP 数量: $unique_count"

    if [ "$REDIS_QUEUE_TEST_MODE" = "ban" ]; then
        log_info "封禁模式：无法直接查询封禁状态，请检查服务端日志"
        log_info "查找日志: 'redis queue ban' 和 'ipset add'"
    else
        log_info "解封模式：无法直接查询解封状态，请检查服务端日志"
        log_info "查找日志: 'redis queue unban' 和 'ipset del'"
    fi

    rm -f "$ip_file"
}

diagnostic_probe() {
    local url="$1"
    local concurrency="$2"
    local duration="$3"
    local total_requests
    local tmp_file
    local stop_file
    local stop_reason_file
    local xargs_err_file
    local xargs_status
    local connect_timeout_sec
    local max_time_sec

    log_info "=== 诊断模式压测 ==="
    log_info "URL: $url"
    log_info "并发: $concurrency / 时长: ${duration}s"
    log_info "FailFast: $DIAG_FAIL_FAST / StopCodes: $DIAG_STOP_ON_CODES"
    log_info "CurlTimeout: connect=${DIAG_CONNECT_TIMEOUT_SEC}s total=${DIAG_MAX_TIME_SEC}s"

    connect_timeout_sec="$DIAG_CONNECT_TIMEOUT_SEC"
    max_time_sec="$DIAG_MAX_TIME_SEC"

    total_requests=$((concurrency * duration))
    if [ "$total_requests" -le 0 ]; then
        log_error "诊断参数无效: total_requests=$total_requests"
        return 1
    fi

    tmp_file=$(mktemp)
    stop_file=$(mktemp)
    stop_reason_file=$(mktemp)
    xargs_err_file=$(mktemp)
    rm -f "$stop_file" "$stop_reason_file"

    set +e
    seq "$total_requests" | xargs -P"$concurrency" -I{} bash -c '
        idx="$1"
        url="$2"
        fail_fast="$3"
        stop_codes="$4"
        stop_file="$5"
        stop_reason_file="$6"
        connect_timeout_sec="$7"
        max_time_sec="$8"

        if [ -f "$stop_file" ]; then
            exit 0
        fi

        result=$(curl -s -o /dev/null \
            --connect-timeout "$connect_timeout_sec" \
            --max-time "$max_time_sec" \
            -w "%{http_code} %{time_total} %{errormsg}\n" \
            "$url" 2>&1 || true)

        if [ -z "$result" ]; then
            result="000 0 error"
        fi

        echo "$result"

        code="${result%% *}"
        if [ "$fail_fast" = "true" ]; then
            IFS=, read -r -a codes <<< "$stop_codes"
            for stop_code in "${codes[@]}"; do
                if [ "$code" = "$stop_code" ]; then
                    if [ ! -f "$stop_file" ]; then
                        : > "$stop_file"
                        printf "idx=%s code=%s result=%s\n" "$idx" "$code" "$result" > "$stop_reason_file"
                    fi
                    exit 255
                fi
            done
        fi
    ' _ {} "$url" "$DIAG_FAIL_FAST" "$DIAG_STOP_ON_CODES" "$stop_file" "$stop_reason_file" "$connect_timeout_sec" "$max_time_sec" >> "$tmp_file" 2>"$xargs_err_file"
    xargs_status=$?
    set -e

    if [ "$xargs_status" -ne 0 ] && [ ! -f "$stop_file" ]; then
        if [ -s "$xargs_err_file" ]; then
            log_warn "诊断压测命令异常退出，exit=$xargs_status, err=$(tr '\n' ' ' < "$xargs_err_file")"
        else
            log_warn "诊断压测命令异常退出，exit=$xargs_status"
        fi
    fi

    if [ -f "$stop_file" ]; then
        log_warn "命中停止条件，已提前结束诊断压测"
        if [ -s "$stop_reason_file" ]; then
            log_warn "首次触发详情: $(cat "$stop_reason_file")"
        fi
    elif [ "$DIAG_FAIL_FAST" = "true" ]; then
        log_info "未命中停止码(${DIAG_STOP_ON_CODES})，已完成全部请求"
    fi

    echo "---- HTTP 状态码分布 ----"
    awk '{print $1}' "$tmp_file" | sort | uniq -c | sort -nr

    echo "---- 失败类型 ----"
    awk '$1=="000"{print}' "$tmp_file" | head

    rm -f "$tmp_file" "$stop_file" "$stop_reason_file" "$xargs_err_file"
}

# 打印使用说明
print_usage() {
    cat <<EOF
用法: $0 [场景编号] [选项]

建议使用: bash $0 <场景编号>

场景:
  1  单IP高频请求测试
  2  单IP并发连接测试
  3  全局DDoS模拟
  4  渐进封禁测试
  5  集群封禁传播测试
  6  WebSocket连接测试
  7  本地持久化恢复测试
  8  Admin Ban/Unban 压测
  9  HTTP诊断压测
  10 Redis队列封禁/解封测试
  all 运行所有场景

环境变量:
  TARGET_URL        目标URL (默认: http://192.168.110.8:9999)
  ADMIN_TOKEN       管理员Token (默认: CHANGE_ME_ADMIN_TOKEN)
  TEST_DURATION     测试时长秒 (默认: 30)
  THREADS           线程数 (默认: 4)
  CONNECTIONS       连接数 (默认: 100)
  ADMIN_BAN_URL     admin ban 接口地址 (默认: TARGET_URL/admin/ban)
  ADMIN_BENCH_TOTAL 压测请求总数 (默认: 10000)
  ADMIN_BENCH_CONCURRENCY 压测并发数 (默认: 100)
  ADMIN_BENCH_MODE  压测模式 ban|unban (默认: unban)
  ADMIN_BENCH_IP_MODE IP模式 same|multi (默认: same)
  ADMIN_BENCH_BASE_IP same 模式固定IP (默认: 198.51.100.10)
  ADMIN_BENCH_MULTI_IP_POOL multi 模式IP池大小 (默认: 200)
  ADMIN_BENCH_BAN_DURATION ban 模式时长秒 (默认: 600)
  ADMIN_BENCH_IP_VERSION IP版本 ipv4|ipv6 (默认: ipv4)
  ADMIN_BENCH_SUCCESS_RATE_THRESHOLD 2xx成功率阈值百分比 (默认: 99.90)
  ADMIN_BENCH_FAIL_ON_THRESHOLD 低于阈值时是否返回非零 true|false (默认: false)
  ADMIN_BENCH_SUCCESS_RPS_TARGET 成功吞吐目标(req/s)，用于达标提示 (默认: 1000)
  ADMIN_BENCH_TOKEN_HEADER admin 鉴权头 authorization|x-admin-token (默认: authorization)
  ADMIN_BENCH_CONNECT_TIMEOUT_SEC admin 压测连接超时秒 (默认: 1)
  ADMIN_BENCH_MAX_TIME_SEC admin 压测总超时秒 (默认: 3)
  DIAG_URL          诊断压测目标URL (默认: TARGET_URL/)
  DIAG_CONCURRENCY  诊断压测并发 (默认: 100)
  DIAG_DURATION     诊断压测时长秒 (默认: 30)
  DIAG_FAIL_FAST    诊断压测命中停止码时提前结束 true|false (默认: true)
  DIAG_STOP_ON_CODES 诊断压测停止码列表，逗号分隔 (默认: 403,429,503)
  DIAG_CONNECT_TIMEOUT_SEC 诊断请求连接超时秒 (默认: 1)
  DIAG_MAX_TIME_SEC 诊断请求总超时秒 (默认: 2)
  WS_URL            WebSocket 测试地址 (默认: 按 TARGET_URL 自动推导)
  REDIS_ADDR        Redis 服务器地址 (默认: 127.0.0.1:6379)
  REDIS_DB          Redis 数据库编号 (默认: 14)
  REDIS_PASSWORD    Redis 密码 (默认: 空)
  REDIS_BAN_QUEUE   Redis 封禁队列名 (默认: admin:ban)
  REDIS_UNBAN_QUEUE Redis 解封队列名 (默认: admin:unban)
  REDIS_QUEUE_TEST_TOTAL Redis 队列测试请求总数 (默认: 100)
  REDIS_QUEUE_TEST_CONCURRENCY Redis 队列测试并发数 (默认: 10)
  REDIS_QUEUE_TEST_MODE Redis 队列测试模式 ban|unban (默认: ban)
  REDIS_QUEUE_TEST_IP_MODE Redis 队列测试 IP模式 same|multi (默认: same)
  REDIS_QUEUE_TEST_BASE_IP Redis 队列测试 same 模式固定IP (默认: 198.51.100.20)
  REDIS_QUEUE_TEST_MULTI_IP_POOL Redis 队列测试 multi 模式IP池大小 (默认: 50)
  REDIS_QUEUE_TEST_BAN_DURATION Redis 队列测试 ban 模式时长秒 (默认: 600)
  REDIS_QUEUE_TEST_IP_VERSION Redis 队列测试 IP版本 ipv4|ipv6 (默认: ipv4)
  NODE1_URL         节点1 URL (集群测试用)
  NODE2_URL         节点2 URL (集群测试用)
  NODE3_URL         节点3 URL (集群测试用)

示例:
  # 运行单IP高频测试
  $0 1

  # 运行全局DDoS测试，自定义参数
  TARGET_URL=http://127.0.0.1:8080 CONNECTIONS=200 THREADS=8 $0 3

  # 运行所有场景
  $0 all

  # admin unban 压测（单IP）
  ADMIN_BENCH_MODE=unban ADMIN_BENCH_IP_MODE=same $0 8

  # admin ban 压测（多IP）
  ADMIN_BENCH_MODE=ban ADMIN_BENCH_IP_MODE=multi ADMIN_BENCH_TOTAL=20000 ADMIN_BENCH_CONCURRENCY=200 $0 8

  # admin ban 压测（IPv6 多IP）
  ADMIN_BENCH_MODE=ban ADMIN_BENCH_IP_MODE=multi ADMIN_BENCH_IP_VERSION=ipv6 ADMIN_BENCH_BASE_IP=2001:db8::1 $0 8

  # Redis 队列测试（IPv6）
  REDIS_QUEUE_TEST_MODE=ban REDIS_QUEUE_TEST_IP_MODE=multi REDIS_QUEUE_TEST_IP_VERSION=ipv6 REDIS_QUEUE_TEST_BASE_IP=2001:db8::1 $0 10

  # admin 压测使用 X-Admin-Token 头
  ADMIN_BENCH_TOKEN_HEADER=x-admin-token $0 8

  # 成功率阈值告警（低于99.95%直接返回非零）
  ADMIN_BENCH_SUCCESS_RATE_THRESHOLD=99.95 ADMIN_BENCH_FAIL_ON_THRESHOLD=true $0 8

  # 诊断压测
  DIAG_URL=http://127.0.0.1:8080/healthz DIAG_CONCURRENCY=200 DIAG_DURATION=20 $0 9

  # 诊断压测，命中 401/403/429 立即停止
  DIAG_FAIL_FAST=true DIAG_STOP_ON_CODES=401,403,429 $0 9

  # 诊断压测，缩短 curl 挂起时间
  DIAG_CONNECT_TIMEOUT_SEC=1 DIAG_MAX_TIME_SEC=1.5 $0 9

  # Redis 队列封禁测试（单IP）
  REDIS_QUEUE_TEST_MODE=ban REDIS_QUEUE_TEST_IP_MODE=same $0 10

  # Redis 队列解封测试（多IP）
  REDIS_QUEUE_TEST_MODE=unban REDIS_QUEUE_TEST_IP_MODE=multi REDIS_QUEUE_TEST_TOTAL=500 REDIS_QUEUE_TEST_CONCURRENCY=20 $0 10

  # Redis 队列测试，自定义 Redis 地址和队列名
  REDIS_ADDR=192.168.1.100:6379 REDIS_DB=0 REDIS_BAN_QUEUE=my:ban:queue REDIS_UNBAN_QUEUE=my:unban:queue $0 10

依赖工具:
  - wrk (推荐): brew install wrk / apt install wrk
  - ab: brew install httpd / apt install apache2-utils
  - websocat (WebSocket测试): brew install websocat / apt install websocat
  - redis-cli (Redis队列测试): brew install redis / apt install redis-tools
EOF
}

# 主函数
main() {
    local scenario="${1:-}"

    if [ -z "$scenario" ]; then
        print_usage
        exit 1
    fi

    log_info "开始压测: 场景 $scenario"
    log_info "目标URL: $TARGET_URL"
    log_info "测试时长: ${TEST_DURATION}s"

    case "$scenario" in
        1)
            test_single_ip_high_rate
            ;;
        2)
            test_single_ip_concurrent
            ;;
        3)
            test_global_ddos
            ;;
        4)
            test_progressive_ban
            ;;
        5)
            test_cluster_ban_propagation
            ;;
        6)
            test_websocket
            ;;
        7)
            test_local_persistence
            ;;
        8)
            test_admin_ban_benchmark
            ;;
        9)
            diagnostic_probe "$DIAG_URL" "$DIAG_CONCURRENCY" "$DIAG_DURATION"
            ;;
        10)
            test_redis_queue
            ;;

        all)
            test_single_ip_high_rate
            echo
            test_single_ip_concurrent
            echo
            test_global_ddos
            echo
            test_progressive_ban
            echo
            test_websocket
            echo
            test_local_persistence
            ;;
        *)
            log_error "无效场景编号: $scenario"
            print_usage
            exit 1
            ;;
    esac

    log_info "压测完成"
}

main "$@"
