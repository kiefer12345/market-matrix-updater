#!/bin/bash
# 在本机 (macOS / Linux) 安装定时任务: 每 30 分钟抓取一次 Cheltenham 网球场空位
#   安装:  ./local/install_cron.sh
#   卸载:  ./local/install_cron.sh --uninstall
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RUN="$REPO_DIR/local/run_tennis.sh"
MARK="# cheltenham-tennis"

if ! command -v crontab >/dev/null; then
    echo "❌ 没找到 crontab 命令 (Linux 上可以用 sudo apt install cron 安装)" >&2
    exit 1
fi

if [ "${1:-}" = "--uninstall" ]; then
    (crontab -l 2>/dev/null | grep -vF "$MARK" || true) | crontab -
    echo "✅ 已删除定时任务"
    exit 0
fi

if ! command -v python3 >/dev/null; then
    echo "❌ 没找到 python3, 请先安装 Python 3.9 以上版本" >&2
    exit 1
fi

echo "📦 创建 Python 虚拟环境 (.venv) 并安装依赖..."
python3 -m venv "$REPO_DIR/.venv"
"$REPO_DIR/.venv/bin/pip" install -q --upgrade pip requests

chmod +x "$RUN"

echo "🎾 先运行一次..."
"$RUN" || echo "⚠️  这次运行有场馆获取失败, 详见上面的输出或 local/tennis.log"

# 写入 crontab (先删掉旧的同名任务, 可重复运行)
(crontab -l 2>/dev/null | grep -vF "$MARK" || true; echo "*/30 * * * * \"$RUN\" $MARK") | crontab -

echo
echo "✅ 已安装: 每 30 分钟自动更新 (电脑睡眠时会跳过, 醒来后下个整点/半点继续)"
echo "   看板:  $REPO_DIR/tennis.html   (双击用浏览器打开, 开着会每 10 分钟自动刷新)"
echo "   日志:  $REPO_DIR/local/tennis.log"
echo "   卸载:  $0 --uninstall"

if [ "$(uname)" = "Darwin" ]; then
    case "$REPO_DIR" in
        "$HOME/Documents"*|"$HOME/Desktop"*|"$HOME/Downloads"*)
            echo
            echo "⚠️  仓库在 Documents/Desktop/Downloads 里, macOS 默认不允许 cron 访问这些目录。"
            echo "   请到 系统设置 → 隐私与安全性 → 完全磁盘访问权限, 点 + 并按 Cmd+Shift+G 输入 /usr/sbin/cron 添加,"
            echo "   或者把仓库移到其它目录 (比如 ~/code) 后重新运行本脚本。"
            ;;
    esac
    open "$REPO_DIR/tennis.html"
elif command -v xdg-open >/dev/null; then
    xdg-open "$REPO_DIR/tennis.html" >/dev/null 2>&1 || true
fi
