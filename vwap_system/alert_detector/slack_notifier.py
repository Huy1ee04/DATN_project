"""
Slack Notifier — Gửi cảnh báo CRITICAL qua Slack Incoming Webhook.

Cách lấy Webhook URL:
1. Vào https://api.slack.com/apps → Create New App → From scratch
2. Chọn workspace → Features → Incoming Webhooks → Activate
3. Add New Webhook to Workspace → chọn channel (ví dụ #stock-alerts)
4. Copy Webhook URL, paste vào .env: SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
"""

import os
import json
import logging
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import URLError

logger = logging.getLogger('slack_notifier')

# ─── Emoji & Color mapping ────────────────────────────────────

SEVERITY_COLOR = {
    'CRITICAL': '#ff0000',   # Đỏ
    'WARNING': '#ffa800',    # Cam
    'INFO': '#36a64f',       # Xanh lá
}

ALERT_TYPE_EMOJI = {
    'COMBINED_PANIC_SELL': '🔴',
    'COMBINED_STRONG_BREAKOUT': '🟢',
    'COMBINED_OVERSOLD_BOUNCE': '🟡',
    'COMBINED_OVERBOUGHT_RISK': '⚠️',
    'COMBINED_UNUSUAL_VOLUME': '🟠',
    'COMBINED_OVERSOLD': '🔵',
    'VWAP_BREAKOUT_UP': '📈',
    'VWAP_BREAKDOWN': '📉',
    'RSI_OVERBOUGHT': '🔴',
    'RSI_OVERSOLD': '🟢',
    'VOLUME_SPIKE': '🔶',
}


class SlackNotifier:
    """Gửi cảnh báo qua Slack Incoming Webhook."""

    def __init__(self, webhook_url: str | None = None):
        self.webhook_url = webhook_url or os.getenv('SLACK_WEBHOOK_URL', '')
        self.enabled = bool(self.webhook_url)
        if self.enabled:
            logger.info("Slack notifier enabled")
        else:
            logger.info("Slack notifier disabled (SLACK_WEBHOOK_URL not set)")

    def send_alert(self, alert) -> bool:
        """
        Gửi 1 alert lên Slack. Chỉ gửi nếu severity == CRITICAL.
        Returns True nếu gửi thành công.
        """
        if not self.enabled:
            return False

        if alert.severity != 'CRITICAL':
            return False

        emoji = ALERT_TYPE_EMOJI.get(alert.alert_type, '🚨')
        color = SEVERITY_COLOR.get(alert.severity, '#ff0000')

        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} CRITICAL ALERT — {alert.symbol}",
                        "emoji": True
                    }
                },
            ],
            "attachments": [
                {
                    "color": color,
                    "blocks": [
                        {
                            "type": "section",
                            "fields": [
                                {"type": "mrkdwn", "text": f"*Loại:*\n`{alert.alert_type}`"},
                                {"type": "mrkdwn", "text": f"*Rule:*\n`{alert.rule_name}`"},
                                {"type": "mrkdwn", "text": f"*Giá:*\n`{alert.price:,.2f}`"},
                                {"type": "mrkdwn", "text": f"*Chỉ báo:*\n`{alert.indicator_value:.2f}`"},
                                {"type": "mrkdwn", "text": f"*Ngưỡng:*\n`{alert.threshold:.2f}`"},
                                {"type": "mrkdwn", "text": f"*Thời gian:*\n`{alert.alert_time.strftime('%H:%M:%S')}`"},
                            ]
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"💬 *{alert.message}*"
                            }
                        },
                        {
                            "type": "context",
                            "elements": [
                                {
                                    "type": "mrkdwn",
                                    "text": f"📡 Multi-Signal Alert Engine • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        return self._post(payload)

    def _post(self, payload: dict) -> bool:
        """POST JSON payload tới Slack webhook."""
        try:
            data = json.dumps(payload).encode('utf-8')
            req = Request(
                self.webhook_url,
                data=data,
                headers={'Content-Type': 'application/json'},
                method='POST',
            )
            with urlopen(req, timeout=5) as resp:
                if resp.status == 200:
                    logger.info("Slack notification sent ✓")
                    return True
                else:
                    logger.warning(f"Slack returned status {resp.status}")
                    return False
        except URLError as e:
            logger.error(f"Slack send failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Slack unexpected error: {e}")
            return False
