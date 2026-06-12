#!/bin/bash
# TIDAL Worker 部署脚本
# 用法: ./deploy.sh

set -e

REMOTE="root@117.55.199.208"
REMOTE_DIR="/opt/tidal-dl"
LOCAL_DIR="$(cd "$(dirname "$0")" && pwd)/tidal-dl"

echo "=== 1. 同步代码 ==="
rsync -avz "$LOCAL_DIR/server/" "$REMOTE:$REMOTE_DIR/server/" --exclude='__pycache__'
rsync -avz "$LOCAL_DIR/worker/" "$REMOTE:$REMOTE_DIR/worker/" --exclude='__pycache__'
rsync -avz "$LOCAL_DIR/web/dist/" "$REMOTE:$REMOTE_DIR/web/dist/"
echo "✅ 代码同步完成"

echo ""
echo "=== 2. 安装依赖并重启 Server ==="
ssh "$REMOTE" 'bash -s' <<'SERVEREOF'
  cd /opt/tidal-dl/server
  /opt/tidal-dl/venv/bin/pip install -r requirements.txt
  fuser -k 8000/tcp 2>/dev/null || true
  sleep 1
  nohup /opt/tidal-dl/venv/bin/python -u main.py > /opt/tidal-dl/server.log 2>&1 &
  echo $! > /opt/tidal-dl/server.pid
  sleep 2
  if curl -sf http://127.0.0.1:8000/api/health > /dev/null; then
    echo "✅ Server 启动成功 (PID=$(cat /opt/tidal-dl/server.pid))"
  else
    echo "❌ Server 启动失败"
    tail -10 /opt/tidal-dl/server.log
    exit 1
  fi
SERVEREOF

echo ""
echo "=== 3. 重启 Worker ==="
ssh "$REMOTE" 'bash -s' <<'WORKEREOF'
  # 杀掉所有旧 Worker
  OLD_PIDS=$(ps aux | grep 'python.*main.py.*8000' | grep -v grep | awk '{print $2}')
  if [ -n "$OLD_PIDS" ]; then
    echo "杀掉旧 Worker: $OLD_PIDS"
    echo "$OLD_PIDS" | xargs kill 2>/dev/null || true
    sleep 3
    # 强杀残留
    REMAINING=$(ps aux | grep 'python.*main.py.*8000' | grep -v grep | awk '{print $2}')
    if [ -n "$REMAINING" ]; then
      echo "强制杀掉残留: $REMAINING"
      echo "$REMAINING" | xargs kill -9 2>/dev/null || true
      sleep 1
    fi
  fi
  # 也杀掉残留的 bash wrapper
  ps aux | grep 'bash.*worker' | grep -v grep | awk '{print $2}' | xargs kill 2>/dev/null || true

  COUNT=$(ps aux | grep 'python.*main.py.*8000' | grep -v grep | wc -l)
  if [ "$COUNT" -gt 0 ]; then
    echo "❌ 仍有 $COUNT 个旧 Worker，放弃"
    exit 1
  fi
  echo "✅ 旧 Worker 已全部清除"

  # 启动新 Worker
  cd /opt/tidal-dl/worker
  nohup /opt/tidal-dl/venv/bin/python -u main.py \
    --server http://127.0.0.1:8000 \
    > /opt/tidal-dl/worker.log 2>&1 &
  NEW_PID=$!
  echo $NEW_PID > /opt/tidal-dl/worker.pid
  sleep 5

  # 验证
  if kill -0 $NEW_PID 2>/dev/null; then
    echo "✅ Worker 启动成功 (PID=$NEW_PID)"
  else
    echo "❌ Worker 启动失败"
    tail -20 /opt/tidal-dl/worker.log
    exit 1
  fi

  # 最终确认只有1个
  FINAL=$(ps aux | grep 'python.*main.py.*8000' | grep -v grep | wc -l)
  echo "当前 Worker 进程数: $FINAL"

  echo ""
  echo "=== Worker 日志 ==="
  tail -10 /opt/tidal-dl/worker.log
WORKEREOF

echo ""
echo "=== 部署完成 ==="
