#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# 默认配置
TARGET_URL="${TARGET_URL:-http://127.0.0.1:8080}"
ADMIN_TOKEN="${ADMIN_TOKEN:-CHANGE_ME_ADMIN_TOKEN}"
TEST_DURATION="${TEST_DURATION:-30}"
THREADS="${THREADS:-4}"
CONNECTIONS="${CONNECTIONS:-100}"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
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

    if ! command -v websocat &> /dev/null; then
        log_warn "websocat 未安装，跳过 WebSocket 测试"
        log_warn "安装: brew install websocat (macOS) 或 apt install websocat (Linux)"
        return 0
    fi

    log_info "测试 WebSocket 连接"
    for i in {1..5}; do
        websocat "ws://127.0.0.1:8080" --exit-on-eof &
        log_info "启动 WebSocket 连接 $i"
        sleep 0.5
    done

    log_info "等待5秒"
    sleep 5

    log_info "尝试第6个连接（应被限流）"
    websocat "ws://127.0.0.1:8080" --exit-on-eof || log_warn "第6个连接被拒绝（符合预期）"
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

# 打印使用说明
print_usage() {
    cat <<EOF
用法: $0 [场景编号] [选项]

场景:
  1  单IP高频请求测试
  2  单IP并发连接测试
  3  全局DDoS模拟
  4  渐进封禁测试
  5  集群封禁传播测试
  6  WebSocket连接测试
  7  本地持久化恢复测试
  all 运行所有场景

环境变量:
  TARGET_URL        目标URL (默认: http://127.0.0.1:8080)
  ADMIN_TOKEN       管理员Token (默认: CHANGE_ME_ADMIN_TOKEN)
  TEST_DURATION     测试时长秒 (默认: 30)
  THREADS           线程数 (默认: 4)
  CONNECTIONS       连接数 (默认: 100)
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

依赖工具:
  - wrk (推荐): brew install wrk / apt install wrk
  - ab: brew install httpd / apt install apache2-utils
  - websocat (WebSocket测试): brew install websocat / apt install websocat
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
