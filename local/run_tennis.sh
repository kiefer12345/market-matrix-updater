#!/bin/bash
# 运行一次 Cheltenham 网球场空位抓取 (由 cron 每 30 分钟调用, 也可以手动运行)
# 结果写到仓库根目录的 tennis_data.json / tennis_data.js, 双击 tennis.html 查看
set -uo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG="$REPO_DIR/local/tennis.log"

# 日志只保留最近 2000 行
if [ -f "$LOG" ]; then
    tail -n 2000 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
fi
# 从 cron 运行时 (没有终端) 输出写进日志; 手动运行时直接打印到屏幕
[ -t 1 ] || exec >> "$LOG" 2>&1

PY="$REPO_DIR/.venv/bin/python"
[ -x "$PY" ] || PY="$(command -v python3)"

cd "$REPO_DIR"
echo "=== $(date '+%Y-%m-%d %H:%M:%S') ==="
"$PY" tennis_court_finder.py
