"""
slack_alert.py

Module gửi thông báo Slack cho Airflow DAGs.
Sử dụng Incoming Webhook — chỉ cần HTTP POST, không cần thêm provider package.

Cách dùng trong DAG:
    from utils.slack_alert import slack_on_failure, slack_on_success

    default_args = {
        "on_failure_callback": slack_on_failure,
    }

    # Gắn on_success cho task cuối cùng (không nên gắn vào default_args
    # vì sẽ gửi thông báo cho MỌI task thành công).
    last_task.on_success_callback = slack_on_success

Biến môi trường:
    SLACK_AIRFLOW_WEBHOOK  — Webhook URL cho channel #pipeline-alerts
"""

from __future__ import annotations

import json
import logging
import os
import traceback
from datetime import datetime, timezone, timedelta
from urllib.request import Request, urlopen
from urllib.error import URLError

logger = logging.getLogger(__name__)

# ── Timezone Việt Nam ─────────────────────────────────────────────────
ICT = timezone(timedelta(hours=7))

# ── Webhook URL ───────────────────────────────────────────────────────
# Callback chạy trong Airflow scheduler/worker process — KHÔNG tự có
# biến .env (chỉ BashOperator mới source .env).
# → Đọc trực tiếp file .env mounted tại /opt/airflow/.env.

_ENV_FILE = "/opt/airflow/.env"


def _load_env_var(key: str, fallback: str = "") -> str:
    """Đọc biến môi trường từ file .env, fallback sang os.environ."""
    try:
        with open(_ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                if k.strip() == key:
                    return v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return os.environ.get(key, fallback)


_WEBHOOK_URL = _load_env_var("SLACK_AIRFLOW_WEBHOOK")

# ── Slack text block char limit ───────────────────────────────────────
_MAX_TEXT_LEN = 2900  # Slack block text limit ~3000, giữ buffer


def _send_slack_message(blocks: list[dict], text: str = "") -> bool:
    """Gửi message tới Slack via Incoming Webhook."""
    webhook_url = _WEBHOOK_URL
    if not webhook_url:
        logger.warning(
            "SLACK_AIRFLOW_WEBHOOK chưa được set — bỏ qua thông báo Slack."
        )
        return False

    payload = json.dumps({"text": text, "blocks": blocks}).encode("utf-8")
    req = Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                logger.info("Slack notification sent successfully.")
                return True
            logger.warning("Slack returned status %d", resp.status)
            return False
    except URLError as exc:
        logger.error("Failed to send Slack notification: %s", exc)
        return False


def _format_duration(start_date, end_date) -> str:
    """Format khoảng thời gian thành chuỗi dễ đọc."""
    if not start_date or not end_date:
        return "N/A"
    delta = end_date - start_date
    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        return "N/A"
    if total_seconds < 60:
        return f"{total_seconds}s"
    minutes, seconds = divmod(total_seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m"


def _get_log_url(context: dict) -> str:
    """Tạo link tới Airflow log cho task."""
    ti = context.get("task_instance")
    if not ti:
        return ""

    # Đọc base URL từ biến môi trường hoặc fallback về localhost
    base_url = _load_env_var("AIRFLOW_WEBSERVER_BASE_URL", "http://localhost:8083")

    import urllib.parse
    params = {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "execution_date": ti.execution_date.isoformat(),
        "try_number": ti.try_number,
    }
    return f"{base_url}/log?{urllib.parse.urlencode(params)}"


def _get_traceback(context: dict) -> str:
    """Lấy full traceback từ exception context."""
    exception = context.get("exception")
    if not exception:
        return "No error details available"

    # Thử lấy full traceback
    try:
        tb_lines = traceback.format_exception(
            type(exception), exception, exception.__traceback__
        )
        full_tb = "".join(tb_lines)
    except Exception:
        full_tb = str(exception)

    # Truncate nếu quá dài (Slack block limit ~3000 chars)
    if len(full_tb) > _MAX_TEXT_LEN:
        # Giữ phần đầu và phần cuối (error message thường ở cuối)
        head = full_tb[:500]
        tail = full_tb[-(_MAX_TEXT_LEN - 550) :]
        full_tb = f"{head}\n... (truncated) ...\n{tail}"

    return full_tb


def _get_task_summary(context: dict) -> str:
    """Lấy danh sách tất cả tasks trong DAG run với status + duration."""
    dag_run = context.get("dag_run")
    if not dag_run:
        return ""

    try:
        task_instances = dag_run.get_task_instances()
    except Exception:
        return ""

    if not task_instances:
        return ""

    # Sắp xếp theo start_date
    task_instances.sort(key=lambda t: (t.start_date or datetime.min.replace(tzinfo=ICT)))

    # Utility tasks — không hiển thị (chỉ là checkpoint, không làm việc thực)
    _SKIP_TASKS = {"pipeline_done", "all_masters_ready", "ingestion_done",
                   "all_s1_done", "backfill_start"}

    lines = []
    n_success = 0
    n_failed = 0
    n_other = 0

    for ti in task_instances:
        # Bỏ qua utility tasks
        if ti.task_id in _SKIP_TASKS:
            continue
        # Bỏ qua ingest_done_*, day_done_* (backfill EmptyOperators)
        if ti.task_id.startswith(("ingest_done_", "day_done_")):
            continue

        state = ti.state or "pending"
        dur = _format_duration(ti.start_date, ti.end_date)

        # Icon theo state
        if state == "success":
            icon = "✅"
            n_success += 1
        elif state == "failed":
            icon = "❌"
            n_failed += 1
        elif state == "running":
            # Task đang chạy callback on_success → coi như success
            icon = "✅"
            n_success += 1
            dur = _format_duration(ti.start_date, datetime.now(timezone.utc))
        elif state == "skipped":
            icon = "⏭️"
            n_other += 1
        else:
            icon = "⬜"
            n_other += 1

        lines.append(f"{icon} `{ti.task_id}` — {dur}")

    summary = "\n".join(lines)

    # Truncate nếu quá dài
    if len(summary) > _MAX_TEXT_LEN:
        summary = summary[:_MAX_TEXT_LEN] + "\n... (truncated)"

    # Header line
    total = n_success + n_failed + n_other
    header = f"*Tasks: {n_success}/{total} success*"
    if n_failed > 0:
        header += f" • {n_failed} failed"

    return f"{header}\n{summary}"


# ═══════════════════════════════════════════════════════════════════════
# CALLBACKS
# ═══════════════════════════════════════════════════════════════════════


def slack_on_failure(context: dict) -> None:
    """Callback khi task FAIL — gửi thông báo lỗi + traceback chi tiết."""
    ti = context.get("task_instance")
    dag_id = ti.dag_id if ti else "unknown"
    task_id = ti.task_id if ti else "unknown"
    execution_date = context.get("execution_date", "")
    now_ict = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S ICT")

    # Duration
    duration = _format_duration(
        getattr(ti, "start_date", None),
        getattr(ti, "end_date", None),
    )

    # Retry info
    try_number = getattr(ti, "try_number", 1)
    max_tries = getattr(ti, "max_tries", 0) + 1  # max_tries is 0-indexed

    # Full traceback
    tb = _get_traceback(context)

    log_url = _get_log_url(context)

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "🔴 TASK FAILED", "emoji": True},
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*📋 DAG:*\n`{dag_id}`"},
                {"type": "mrkdwn", "text": f"*🔧 Task:*\n`{task_id}`"},
                {"type": "mrkdwn", "text": f"*📅 Execution Date:*\n`{execution_date}`"},
                {"type": "mrkdwn", "text": f"*⏱ Duration:*\n`{duration}`"},
                {"type": "mrkdwn", "text": f"*🔁 Retry:*\n`{try_number}/{max_tries}`"},
                {"type": "mrkdwn", "text": f"*🕐 Time:*\n`{now_ict}`"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*💥 Traceback:*\n```{tb}```",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"🔗 *<{log_url}|Click here to View Full Log on Airflow>*",
            },
        },
    ]

    _send_slack_message(
        blocks,
        text=f"🔴 FAILED: {dag_id} / {task_id}",
    )


def slack_on_success(context: dict) -> None:
    """Callback khi DAG hoàn thành — hiển thị danh sách tasks + duration."""
    ti = context.get("task_instance")
    dag_run = context.get("dag_run")
    dag_id = ti.dag_id if ti else "unknown"
    execution_date = context.get("execution_date", "")
    now_ict = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S ICT")

    # Tổng thời gian DAG run
    dag_duration = "N/A"
    if dag_run:
        dag_duration = _format_duration(
            getattr(dag_run, "start_date", None),
            datetime.now(timezone.utc),
        )

    # Danh sách tasks
    task_summary = _get_task_summary(context)

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "🟢 DAG COMPLETED", "emoji": True},
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*📋 DAG:*\n`{dag_id}`"},
                {"type": "mrkdwn", "text": f"*📅 Execution Date:*\n`{execution_date}`"},
                {"type": "mrkdwn", "text": f"*⏱ Total Duration:*\n`{dag_duration}`"},
                {"type": "mrkdwn", "text": f"*🕐 Completed:*\n`{now_ict}`"},
            ],
        },
    ]

    # Thêm task summary nếu có
    if task_summary:
        blocks.append({"type": "divider"})
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": task_summary},
            }
        )

    _send_slack_message(
        blocks,
        text=f"🟢 COMPLETED: {dag_id}",
    )


def slack_dag_start(context: dict) -> None:
    """Callback khi DAG bắt đầu — gắn cho task đầu tiên."""
    ti = context.get("task_instance")
    dag_id = ti.dag_id if ti else "unknown"
    execution_date = context.get("execution_date", "")
    now_ict = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S ICT")

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"🚀 *DAG Started:* `{dag_id}`\n"
                    f"📅 Execution: `{execution_date}`  •  🕐 {now_ict}"
                ),
            },
        },
    ]

    _send_slack_message(
        blocks,
        text=f"🚀 STARTED: {dag_id}",
    )
