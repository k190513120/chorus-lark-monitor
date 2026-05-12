#!/bin/bash
# 一键部署群聊消息统计到 Mac mini
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LA_DIR="$HOME/Library/LaunchAgents"

echo "=== 群聊消息统计 · Mac mini 部署 ==="
echo ""

# 1. 检查 .env
if [[ ! -f "$PROJECT_DIR/.env" ]]; then
  echo "⚠️  未找到 .env，从模板创建..."
  cp "$SCRIPT_DIR/.env.example" "$PROJECT_DIR/.env"
  echo "   请编辑 $PROJECT_DIR/.env 填入凭据后重新运行"
  exit 1
fi

# 检查 .env 是否有值
source "$PROJECT_DIR/.env"
if [[ -z "${LARK_APP_ID:-}" || -z "${LARK_APP_SECRET:-}" || -z "${LARK_BASE_URL:-}" ]]; then
  echo "❌ .env 中 LARK_APP_ID / LARK_APP_SECRET / LARK_BASE_URL 未填写"
  exit 1
fi
echo "✅ .env 配置正常"

# 2. 创建日志目录
mkdir -p "$PROJECT_DIR/logs"

# 3. 卸载旧版（如有）
for plist in com.feishu-chat.daily-sync com.feishu-chat.group-join com.feishu-chat.export-web com.feishu-chat.bulk-stats com.feishu-chat.web; do
  if launchctl list "$plist" &>/dev/null; then
    launchctl unload "$LA_DIR/$plist.plist" 2>/dev/null || true
    echo "  卸载旧版: $plist"
  fi
  rm -f "$LA_DIR/$plist.plist"
done

# 4. 安装 plist
for plist in com.feishu-chat.daily-sync com.feishu-chat.group-join com.feishu-chat.export-web com.feishu-chat.bulk-stats com.feishu-chat.web; do
  cp "$SCRIPT_DIR/$plist.plist" "$LA_DIR/"
  launchctl load "$LA_DIR/$plist.plist"
  echo "  ✅ 已加载: $plist"
done

echo ""
echo "=== 部署完成 ==="
echo ""
echo "定时任务："
echo "  每日 10:00  群消息增量同步"
echo "  每日 10:30  导出前端数据"
echo "  每日 20:00  刷新群发消息已读统计"
echo "  每日 22:00  拉机器人进外部群"
echo ""
echo "常驻服务："
echo "  群聊监控台  http://127.0.0.1:5678/"
echo ""
echo "验证："
echo "  launchctl list | grep feishu-chat"
echo "  tail -f $PROJECT_DIR/logs/daily-sync.out.log"
