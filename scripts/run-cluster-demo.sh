#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TMP_DIR="$(mktemp -d /tmp/golang-iptables-cluster.XXXXXX)"

PORT1="${PORT1:-18081}"
PORT2="${PORT2:-18082}"
PORT3="${PORT3:-18083}"

GOSSIP1="${GOSSIP1:-7946}"
GOSSIP2="${GOSSIP2:-7947}"
GOSSIP3="${GOSSIP3:-7948}"

SEED_ADDR="127.0.0.1:${GOSSIP1}"
CLUSTER_SECRET="${CLUSTER_SECRET:-demo-cluster-secret-change-me}"
ADMIN_TOKEN="${ADMIN_TOKEN:-demo-admin-token}"
BACKEND_ADDR="${BACKEND_ADDR:-127.0.0.1:9502}"

PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
  rm -rf "$TMP_DIR"
}

trap cleanup EXIT INT TERM

write_node_config() {
  local file="$1"
  local gossip_port="$2"
  local join_json="$3"
  local persist_file="$4"

  cat >"$file" <<EOF
{
  "backend": "${BACKEND_ADDR}",
  "redis": {
    "addr": "127.0.0.1:6379",
    "db": 14,
    "password": ""
  },
  "cluster": {
    "enable": true,
    "bindAddr": "127.0.0.1",
    "bindPort": ${gossip_port},
    "join": ${join_json},
    "secret": "${CLUSTER_SECRET}",
    "retransmitMult": 3,
    "nodeName": "node-${gossip_port}"
  },
  "rateLimit": {
    "maxRequests": 20,
    "globalMaxRequests": 100,
    "windowSec": 10,
    "maxConn": 5
  },
  "auth": {
    "token": "demo-token",
    "adminToken": "${ADMIN_TOKEN}"
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
    "webSocketMaxLifetimeSec": 600,
    "ipsetConcurrency": 10,
    "ipsetTimeoutSec": 3,
    "machineHealthCheckSec": 2,
    "machineMaxCPUPercent": 100,
    "machineMaxLoadPerCpu": 2,
    "machineMaxMemoryPercent": 100,
    "machineUnhealthyThreshold": 3,
    "machineRecoveryThreshold": 3,
    "ipsetSyncIntervalSec": 30,
    "ipsetCacheMaxEntries": 200000,
    "ipsetSyncMaxPerRound": 50000,
    "blacklistIpKeyMaxLen": 64,
    "localBanPersistEnabled": true,
    "localBanPersistFile": "${persist_file}",
    "localBanPersistWindowSec": 1800,
    "localBanPersistFlushMs": 1000,
    "localBanPersistMaxEntries": 50000,
    "authTimeoutSec": 3,
    "shutdownTimeoutSec": 15
  }
}
EOF
}

start_node() {
  local name="$1"
  local port="$2"
  local config="$3"
  local log_file="$4"

  (
    cd "$ROOT_DIR"
    go run main.go "$port" "$config"
  ) >"$log_file" 2>&1 &

  local pid=$!
  PIDS+=("$pid")
  echo "[$name] pid=$pid port=$port config=$config log=$log_file"
}

CFG1="$TMP_DIR/node1.json"
CFG2="$TMP_DIR/node2.json"
CFG3="$TMP_DIR/node3.json"

LOG1="$TMP_DIR/node1.log"
LOG2="$TMP_DIR/node2.log"
LOG3="$TMP_DIR/node3.log"

PERSIST1="$TMP_DIR/node1-local-bans.json"
PERSIST2="$TMP_DIR/node2-local-bans.json"
PERSIST3="$TMP_DIR/node3-local-bans.json"

write_node_config "$CFG1" "$GOSSIP1" "[]" "$PERSIST1"
write_node_config "$CFG2" "$GOSSIP2" "[\"${SEED_ADDR}\"]" "$PERSIST2"
write_node_config "$CFG3" "$GOSSIP3" "[\"${SEED_ADDR}\"]" "$PERSIST3"

start_node "node1" "$PORT1" "$CFG1" "$LOG1"
sleep 1
start_node "node2" "$PORT2" "$CFG2" "$LOG2"
start_node "node3" "$PORT3" "$CFG3" "$LOG3"

echo
echo "Cluster demo started."
echo "- node1 http: http://127.0.0.1:${PORT1}"
echo "- node2 http: http://127.0.0.1:${PORT2}"
echo "- node3 http: http://127.0.0.1:${PORT3}"
echo "- logs dir : ${TMP_DIR}"
echo "- local ban snapshots:"
echo "  - node1: ${PERSIST1}"
echo "  - node2: ${PERSIST2}"
echo "  - node3: ${PERSIST3}"
echo
echo "Try a manual ban on node1:"
echo "curl -s -X POST http://127.0.0.1:${PORT1}/admin/ban \\
  -H 'Authorization: Bearer ${ADMIN_TOKEN}' \\
  -H 'Content-Type: application/json' \\
  -d '{\"ip\":\"1.2.3.4\",\"duration\":600,\"reason\":\"cluster-demo\"}'"
echo
echo "Then watch logs on other nodes:"
echo "tail -f ${LOG2} ${LOG3}"
echo
echo "To verify restart recovery (example for node2):"
echo "1) kill \\$(pgrep -f 'main.go ${PORT2} ${CFG2}')"
echo "2) (cd ${ROOT_DIR} && go run main.go ${PORT2} ${CFG2})"
echo "3) check ${PERSIST2} and node2 logs for local ban restore"
echo
echo "Press Ctrl+C to stop all nodes and clean temp files."

wait
