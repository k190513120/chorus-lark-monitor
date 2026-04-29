#!/usr/bin/env python3
import argparse
import json
import os
import re
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo


CHAT_TABLE_NAME = "机器人群列表"
MEMBER_TABLE_NAME = "机器人群成员记录"
MESSAGE_TABLE_NAME = "机器人群消息记录"
SELECT_COLOR_BLUE = {"hue": "Blue", "lightness": "Light"}
SELECT_COLOR_GREEN = {"hue": "Green", "lightness": "Light"}
SELECT_COLOR_ORANGE = {"hue": "Orange", "lightness": "Light"}

CHAT_FIELD_DEFS = [
    {"name": "群ID", "type": "text"},
    {"name": "群名称", "type": "text"},
    {"name": "群描述", "type": "text"},
    {"name": "群类型", "type": "text"},
    {
        "name": "群聊类型",
        "type": "select",
        "multiple": False,
        "options": [
            {"name": "私有群", **SELECT_COLOR_BLUE},
            {"name": "公开群", **SELECT_COLOR_GREEN},
        ],
    },
    {"name": "群模式", "type": "text"},
    {"name": "群状态", "type": "text"},
    {"name": "是否外部群", "type": "text"},
    {"name": "官方群标签", "type": "text"},
    {"name": "群标签", "type": "text"},
    {"name": "群主ID", "type": "text"},
    {"name": "群主ID类型", "type": "text"},
    {"name": "群主", "type": "user", "multiple": False},
    {"name": "用户数", "type": "text"},
    {"name": "机器人数量", "type": "text"},
    {"name": "成员总数", "type": "text"},
    {"name": "成员摘要", "type": "text"},
    {"name": "进群链接", "type": "text"},
    {"name": "链接是否永久有效", "type": "text"},
    {"name": "链接过期时间", "type": "text"},
    {"name": "进群链接状态", "type": "text"},
    {"name": "租户Key", "type": "text"},
    {"name": "群头像", "type": "text"},
    {"name": "同步批次", "type": "text"},
    {"name": "同步时间", "type": "text"},
]

BASE_MEMBER_FIELD_DEFS = [
    {"name": "群ID", "type": "text"},
    {"name": "群名称", "type": "text"},
    {"name": "关联群组", "type": "link", "link_table": "__CHAT_TABLE_ID__", "bidirectional": False},
    {"name": "成员ID", "type": "text"},
    {"name": "成员ID类型", "type": "text"},
    {"name": "成员", "type": "user", "multiple": False},
    {"name": "成员姓名", "type": "text"},
    {"name": "成员租户Key", "type": "text"},
    {"name": "同步批次", "type": "text"},
]

BASE_MESSAGE_FIELD_DEFS = [
    {"name": "消息ID", "type": "text"},
    {"name": "群ID", "type": "text"},
    {"name": "群名称", "type": "text"},
    {"name": "关联群组", "type": "link", "link_table": "__CHAT_TABLE_ID__", "bidirectional": False},
    {"name": "消息类型", "type": "text"},
    {"name": "发送时间", "type": "text"},
    {"name": "更新时间", "type": "text"},
    {"name": "发送者ID", "type": "text"},
    {"name": "发送者ID类型", "type": "text"},
    {"name": "发送者", "type": "user", "multiple": False},
    {"name": "发送者类型", "type": "text"},
    {"name": "发送者租户Key", "type": "text"},
    {"name": "是否已删除", "type": "text"},
    {"name": "是否已编辑", "type": "text"},
    {"name": "线程ID", "type": "text"},
    {"name": "消息内容", "type": "text"},
    {"name": "提取消息内容", "type": "text"},
    {"name": "消息体JSON", "type": "text"},
    {"name": "同步批次", "type": "text"},
]

CHAT_FIELDS = [field["name"] for field in CHAT_FIELD_DEFS]

CHAT_GROUPCHAT_FIELD_NAME = "群组字段-允许添加多个群组"


class FeishuAPIError(RuntimeError):
    pass


TOKEN_EXPIRED_CODES = {99991663, 99991664, 99991668, 99991677}
TOKEN_REFRESH_INTERVAL_SECONDS = 5400  # refresh every 1.5h, tokens expire at 2h


def int_value(value: object, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def chat_scope_signature(max_chats: Optional[int], skip_chats: int) -> str:
    if max_chats is None and skip_chats == 0:
        return "all"
    return f"skip:{skip_chats}:max:{max_chats if max_chats is not None else 'all'}"


class FeishuClient:
    def __init__(self, app_id: str, app_secret: str, verbose: bool = False) -> None:
        self.app_id = app_id
        self.app_secret = app_secret
        self.verbose = verbose
        self.base_url = "https://open.feishu.cn"
        self.tenant_access_token: Optional[str] = None
        self.token_acquired_at: float = 0.0
        self.v1_field_cache: Dict[str, List[Dict[str, object]]] = {}

    def authenticate(self) -> None:
        data = self.request(
            "POST",
            "/open-apis/auth/v3/tenant_access_token/internal",
            data={
                "app_id": self.app_id,
                "app_secret": self.app_secret,
            },
            auth_required=False,
            return_full_payload=True,
        )
        token = data.get("tenant_access_token")
        if not token:
            raise FeishuAPIError("tenant_access_token missing in auth response")
        self.tenant_access_token = token
        self.token_acquired_at = time.time()
        if self.verbose:
            print("[debug] tenant_access_token refreshed", file=sys.stderr)

    def identity_summary(self) -> str:
        return f"bot(app_id={self.app_id}) via tenant_access_token/internal"

    def _maybe_refresh_token(self) -> None:
        if not self.tenant_access_token:
            return
        if time.time() - self.token_acquired_at >= TOKEN_REFRESH_INTERVAL_SECONDS:
            self.authenticate()

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, object]] = None,
        data: Optional[Dict[str, object]] = None,
        auth_required: bool = True,
        retries: int = 3,
        return_full_payload: bool = False,
    ) -> Dict[str, object]:
        if auth_required:
            if not self.tenant_access_token:
                raise FeishuAPIError("client is not authenticated")
            self._maybe_refresh_token()

        query = ""
        if params:
            query = "?" + urllib.parse.urlencode(params, doseq=True)
        url = self.base_url + path + query

        body = None
        if data is not None:
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")

        last_error: Optional[Exception] = None
        token_refreshed_on_error = False
        for attempt in range(1, retries + 1):
            headers = {
                "Content-Type": "application/json; charset=utf-8",
            }
            if auth_required:
                headers["Authorization"] = f"Bearer {self.tenant_access_token}"
            try:
                req = urllib.request.Request(url, data=body, headers=headers, method=method)
                with urllib.request.urlopen(req, timeout=60) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                code = int(payload.get("code") or 0)
                if code != 0:
                    if (
                        auth_required
                        and code in TOKEN_EXPIRED_CODES
                        and not token_refreshed_on_error
                    ):
                        self.authenticate()
                        token_refreshed_on_error = True
                        continue
                    raise FeishuAPIError(
                        f"{method} {path} failed: code={code} msg={payload.get('msg')}"
                    )
                if self.verbose:
                    print(f"[debug] {method} {path} ok", file=sys.stderr)
                return payload if return_full_payload else payload.get("data", {})
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                if auth_required and not token_refreshed_on_error:
                    try:
                        err_payload = json.loads(detail)
                        if int(err_payload.get("code") or 0) in TOKEN_EXPIRED_CODES:
                            self.authenticate()
                            token_refreshed_on_error = True
                            continue
                    except (json.JSONDecodeError, ValueError, TypeError):
                        pass
                last_error = FeishuAPIError(f"{method} {path} http {exc.code}: {detail}")
            except Exception as exc:  # noqa: BLE001
                last_error = exc

            if attempt < retries:
                time.sleep(min(2 ** (attempt - 1), 5))
            else:
                raise last_error or FeishuAPIError(f"{method} {path} failed")

        raise FeishuAPIError(f"{method} {path} failed")

    def list_chats(
        self,
        page_size: int = 100,
        max_chats: Optional[int] = None,
        skip_chats: int = 0,
        created_desc: bool = False,
    ) -> List[Dict[str, object]]:
        items: List[Dict[str, object]] = []
        page_token = None
        while True:
            params = {
                "page_size": min(page_size, 100),
                "sort_type": "ByCreateTimeAsc",
            }
            if page_token:
                params["page_token"] = page_token
            data = self.request("GET", "/open-apis/im/v1/chats", params=params)
            batch = list(data.get("items") or [])
            items.extend(batch)
            if not data.get("has_more"):
                break
            page_token = data.get("page_token")
            if not page_token:
                break

        if created_desc:
            if any(int_value(item.get("create_time")) for item in items):
                items.sort(key=lambda item: int_value(item.get("create_time")), reverse=True)
            else:
                items.reverse()
        if skip_chats:
            items = items[skip_chats:]
        if max_chats is not None:
            items = items[:max_chats]
        return items

    def iter_messages(
        self,
        chat_id: str,
        *,
        page_size: int = 50,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        max_messages: Optional[int] = None,
    ) -> Iterable[Dict[str, object]]:
        yielded = 0
        page_token = None
        while True:
            params: Dict[str, object] = {
                "card_msg_content_type": "raw_card_content",
                "container_id": chat_id,
                "container_id_type": "chat",
                "page_size": min(page_size, 50),
                "sort_type": "ByCreateTimeAsc",
            }
            if start_time is not None:
                params["start_time"] = start_time
            if end_time is not None:
                params["end_time"] = end_time
            if page_token:
                params["page_token"] = page_token
            data = self.request("GET", "/open-apis/im/v1/messages", params=params)
            batch = list(data.get("items") or [])
            for item in batch:
                yield item
                yielded += 1
                if max_messages is not None and yielded >= max_messages:
                    return
            if not data.get("has_more"):
                return
            page_token = data.get("page_token")
            if not page_token:
                return

    def get_chat_detail(self, chat_id: str) -> Dict[str, object]:
        return self.request(
            "GET",
            f"/open-apis/im/v1/chats/{chat_id}",
            params={"user_id_type": "open_id"},
        )

    def get_chat_share_link(self, chat_id: str, validity_period: str = "year") -> Dict[str, object]:
        return self.request(
            "POST",
            f"/open-apis/im/v1/chats/{chat_id}/link",
            data={"validity_period": validity_period},
        )

    def list_chat_members(self, chat_id: str, page_size: int = 100) -> Dict[str, object]:
        items: List[Dict[str, object]] = []
        page_token = None
        member_total = 0
        while True:
            params: Dict[str, object] = {
                "member_id_type": "open_id",
                "page_size": min(page_size, 100),
            }
            if page_token:
                params["page_token"] = page_token
            data = self.request(
                "GET",
                f"/open-apis/im/v1/chats/{chat_id}/members",
                params=params,
            )
            batch = list(data.get("items") or [])
            items.extend(batch)
            member_total = int(data.get("member_total") or member_total or 0)
            if not data.get("has_more"):
                return {"items": items, "member_total": member_total}
            page_token = data.get("page_token")
            if not page_token:
                return {"items": items, "member_total": member_total}

    def list_tables(self, base_token: str) -> List[Dict[str, object]]:
        items: List[Dict[str, object]] = []
        offset = 0
        limit = 100
        while True:
            data = self.request(
                "GET",
                f"/open-apis/base/v3/bases/{base_token}/tables",
                params={"offset": offset, "limit": limit},
            )
            batch = list(data.get("items") or data.get("tables") or [])
            items.extend(batch)
            if not batch or len(batch) < limit:
                return items
            offset += len(batch)

    def list_fields_v1(self, app_token: str, table_id: str) -> List[Dict[str, object]]:
        cache_key = f"{app_token}:{table_id}"
        if cache_key in self.v1_field_cache:
            return self.v1_field_cache[cache_key]

        items: List[Dict[str, object]] = []
        page_token: Optional[str] = None
        while True:
            params: Dict[str, object] = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token
            data = self.request(
                "GET",
                f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields",
                params=params,
            )
            batch = list(data.get("items") or [])
            items.extend(batch)
            if not data.get("has_more"):
                self.v1_field_cache[cache_key] = items
                return items
            page_token = data.get("page_token")
            if not page_token:
                self.v1_field_cache[cache_key] = items
                return items

    def get_groupchat_field_names(self, app_token: str, table_id: str) -> List[str]:
        fields = self.list_fields_v1(app_token, table_id)
        return [
            stringify(field.get("field_name"))
            for field in fields
            if int(field.get("type") or 0) == 23 or stringify(field.get("ui_type")) == "GroupChat"
        ]

    def ensure_groupchat_field_v1(
        self,
        app_token: str,
        table_id: str,
        field_name: str,
        *,
        multiple: bool = True,
    ) -> None:
        for field in self.list_fields_v1(app_token, table_id):
            if stringify(field.get("field_name")) == field_name:
                return
        response = self.request(
            "POST",
            f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields",
            data={
                "field_name": field_name,
                "is_primary": False,
                "property": {"multiple": multiple},
                "type": 23,
                "ui_type": "GroupChat",
            },
        )
        cache_key = f"{app_token}:{table_id}"
        self.v1_field_cache.pop(cache_key, None)
        created = response.get("field") if isinstance(response, dict) else None
        if isinstance(created, dict) and created.get("field_name"):
            self.v1_field_cache[cache_key] = list(self.list_fields_v1(app_token, table_id))
            if not any(
                stringify(field.get("field_name")) == field_name
                for field in self.v1_field_cache[cache_key]
            ):
                self.v1_field_cache[cache_key].append(created)

    def batch_update_groupchat_fields_v1(
        self,
        app_token: str,
        table_id: str,
        record_id_list: List[str],
        chat_id: str,
        chat_name: str,
    ) -> None:
        if not record_id_list:
            return
        field_names = self.get_groupchat_field_names(app_token, table_id)
        if not field_names:
            return
        group_value = [{"id": chat_id, "name": chat_name}] if chat_name else [{"id": chat_id}]
        fields_patch = {field_name: group_value for field_name in field_names}
        records = [{"record_id": record_id, "fields": fields_patch} for record_id in record_id_list]
        self.request(
            "POST",
            f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update",
            data={"records": records},
        )

    def create_table(self, base_token: str, table_name: str) -> str:
        data = self.request(
            "POST",
            f"/open-apis/base/v3/bases/{base_token}/tables",
            data={"name": table_name},
        )
        for key in ("table_id", "id"):
            if data.get(key):
                return str(data[key])
        table = data.get("table") or {}
        if isinstance(table, dict):
            table_id = table_id_of(table)
            if table_id:
                return table_id
        for item in self.list_tables(base_token):
            if item.get("name") == table_name or item.get("table_name") == table_name:
                table_id = table_id_of(item)
                if table_id:
                    return table_id
        raise FeishuAPIError(f"table {table_name} created but table_id not found")

    def delete_table(self, base_token: str, table_id: str) -> None:
        self.request("DELETE", f"/open-apis/base/v3/bases/{base_token}/tables/{table_id}")

    def list_fields(self, base_token: str, table_id: str) -> List[Dict[str, object]]:
        items: List[Dict[str, object]] = []
        offset = 0
        limit = 200
        while True:
            data = self.request(
                "GET",
                f"/open-apis/base/v3/bases/{base_token}/tables/{table_id}/fields",
                params={"offset": offset, "limit": limit},
            )
            batch = list(data.get("items") or data.get("fields") or [])
            items.extend(batch)
            if not batch or len(batch) < limit:
                return items
            offset += len(batch)

    def update_field(self, base_token: str, table_id: str, field_id: str, payload: Dict[str, object]) -> None:
        self.request(
            "PUT",
            f"/open-apis/base/v3/bases/{base_token}/tables/{table_id}/fields/{field_id}",
            data=payload,
        )

    def create_field(self, base_token: str, table_id: str, field_def: Dict[str, object]) -> None:
        self.request(
            "POST",
            f"/open-apis/base/v3/bases/{base_token}/tables/{table_id}/fields",
            data=field_def,
        )

    def batch_create_records(
        self,
        base_token: str,
        table_id: str,
        fields: List[str],
        rows: List[List[object]],
    ) -> Dict[str, object]:
        if not rows:
            return {}
        return self.request(
            "POST",
            f"/open-apis/base/v3/bases/{base_token}/tables/{table_id}/records/batch_create",
            data={"fields": fields, "rows": rows},
        )

    def list_existing_text_values(
        self,
        app_token: str,
        table_id: str,
        field_name: str,
    ) -> set:
        values: set = set()
        page_token: Optional[str] = None
        while True:
            params: Dict[str, object] = {
                "page_size": 500,
                "field_names": json.dumps([field_name], ensure_ascii=False),
            }
            if page_token:
                params["page_token"] = page_token
            data = self.request(
                "GET",
                f"/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
                params=params,
            )
            for item in data.get("items") or []:
                cell = (item.get("fields") or {}).get(field_name)
                text_value = _extract_bitable_text(cell)
                if text_value:
                    values.add(text_value)
            if not data.get("has_more"):
                return values
            page_token = data.get("page_token")
            if not page_token:
                return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch sync Feishu bot chats and chat messages into a Feishu Base."
    )
    parser.add_argument("--app-id", default=os.getenv("LARK_APP_ID"))
    parser.add_argument("--app-secret", default=os.getenv("LARK_APP_SECRET"))
    parser.add_argument(
        "--base-url",
        default=os.getenv("LARK_BASE_URL"),
        help="Full Base URL such as https://xxx.larkoffice.com/base/<token>?table=...",
    )
    parser.add_argument("--chat-table-name", default=CHAT_TABLE_NAME)
    parser.add_argument("--member-table-name", default=MEMBER_TABLE_NAME)
    parser.add_argument("--message-table-name", default=MESSAGE_TABLE_NAME)
    parser.add_argument("--chat-page-size", type=int, default=100)
    parser.add_argument("--message-page-size", type=int, default=50)
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument(
        "--sync-timezone",
        default=os.getenv("SYNC_TIMEZONE", "Asia/Shanghai"),
        help="Timezone used for date-only windows and scheduled daily baselines.",
    )
    parser.add_argument(
        "--scheduled-daily",
        action="store_true",
        help="Run as a daily incremental sync using a local state file.",
    )
    parser.add_argument(
        "--scheduled-baseline",
        choices=["today", "state"],
        default=os.getenv("SCHEDULED_BASELINE", "today"),
        help=(
            "When scheduled state does not match the current chat scope, "
            "'today' starts from local midnight once; 'state' only follows the saved state."
        ),
    )
    parser.add_argument(
        "--state-file",
        default=os.path.join(os.path.dirname(__file__), ".sync_state.json"),
        help="Local state file for scheduled incremental sync.",
    )
    parser.add_argument(
        "--initial-lookback-hours",
        type=int,
        default=24,
        help="If no state exists, scheduled mode will sync this many hours backward from now.",
    )
    parser.add_argument(
        "--refresh-metadata-tables",
        action="store_true",
        help="Rebuild chat/member snapshot tables while keeping message history table.",
    )
    parser.add_argument(
        "--skip-share-links",
        action="store_true",
        help="Do not request per-chat share links. Useful for full-scope daily syncs.",
    )
    parser.add_argument(
        "--fast-metadata",
        action="store_true",
        help="Use the chat list payload for chat metadata instead of requesting every chat detail.",
    )
    parser.add_argument(
        "--skip-groupchat-field-updates",
        action="store_true",
        help="Skip optional native GroupChat field backfill; text fields and linked records are still written.",
    )
    parser.add_argument(
        "--sync-batch-size",
        type=int,
        default=200,
        help="Number of chats to read before batch-writing records. Max write batch remains 200.",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="Read-only check: list chats and sample chat members/messages without writing Base records.",
    )
    parser.add_argument(
        "--smoke-test-detail-chats",
        type=int,
        default=5,
        help="How many chats to sample in --smoke-test mode.",
    )
    parser.add_argument(
        "--chat-order",
        choices=["created_asc", "created_desc"],
        default="created_asc",
        help="Order chats by creation time.",
    )
    parser.add_argument("--skip-chats", type=int, default=0)
    parser.add_argument("--max-chats", type=int)
    parser.add_argument("--max-messages-per-chat", type=int)
    parser.add_argument(
        "--recreate-tables",
        action="store_true",
        help="Delete same-name sync tables first, then rebuild them.",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    missing = []
    if not args.app_id:
        missing.append("--app-id or LARK_APP_ID")
    if not args.app_secret:
        missing.append("--app-secret or LARK_APP_SECRET")
    if not args.base_url:
        missing.append("--base-url or LARK_BASE_URL")
    if missing:
        parser.error("missing required inputs: " + ", ".join(missing))
    return args


def parse_base_token(base_url: str) -> str:
    if "/base/" not in base_url:
        return base_url.strip()
    parsed = urllib.parse.urlparse(base_url)
    marker = "/base/"
    path = parsed.path
    start = path.find(marker)
    if start == -1:
        raise ValueError(f"unable to parse base token from {base_url}")
    token = path[start + len(marker):].strip("/")
    if not token:
        raise ValueError(f"unable to parse base token from {base_url}")
    return token


def load_timezone(name: str) -> tzinfo:
    try:
        return ZoneInfo(name)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"unsupported timezone: {name}") from exc


def today_start_epoch_seconds(tzinfo: tzinfo) -> int:
    now = datetime.now(tzinfo)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(start.timestamp())


def parse_time_to_epoch_seconds(
    value: Optional[str],
    *,
    end_of_day: bool = False,
    tzinfo: Optional[tzinfo] = None,
) -> Optional[int]:
    if not value:
        return None
    local_tz = tzinfo or datetime.now().astimezone().tzinfo or timezone(timedelta(hours=8))

    dt: Optional[datetime] = None
    value = value.strip()
    if len(value) == 10 and value.count("-") == 2:
        dt = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=local_tz)
        if end_of_day:
            dt = dt + timedelta(days=1) - timedelta(seconds=1)
    else:
        normalized = value.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"unsupported time format: {value}") from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=local_tz)
    return int(dt.timestamp())


def epoch_seconds_to_iso(value: int, tzinfo: Optional[tzinfo] = None) -> str:
    dt = datetime.fromtimestamp(value, tz=tzinfo or datetime.now().astimezone().tzinfo)
    return dt.isoformat()


def load_state(path: str) -> Dict[str, object]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def save_state(path: str, payload: Dict[str, object]) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    shutil.move(tmp_path, path)


def epoch_millis_to_text(value: object, tzinfo: Optional[tzinfo] = None) -> str:
    if value in (None, ""):
        return ""
    try:
        millis = int(str(value))
    except ValueError:
        return str(value)
    dt = datetime.fromtimestamp(millis / 1000, tz=tzinfo or datetime.now().astimezone().tzinfo)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def epoch_seconds_to_text(value: object, tzinfo: Optional[tzinfo] = None) -> str:
    if value in (None, ""):
        return ""
    try:
        seconds = int(str(value))
    except ValueError:
        return str(value)
    dt = datetime.fromtimestamp(seconds, tz=tzinfo or datetime.now().astimezone().tzinfo)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def compact_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _extract_bitable_text(cell: object) -> str:
    if cell in (None, ""):
        return ""
    if isinstance(cell, str):
        return cell.strip()
    if isinstance(cell, list):
        parts: List[str] = []
        for seg in cell:
            if isinstance(seg, dict):
                text = seg.get("text") or seg.get("value")
                if isinstance(text, str):
                    parts.append(text)
            elif isinstance(seg, str):
                parts.append(seg)
        return "".join(parts).strip()
    if isinstance(cell, dict):
        text = cell.get("text") or cell.get("value")
        if isinstance(text, str):
            return text.strip()
    return str(cell).strip()


def stringify(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return compact_json(value)
    return str(value)


def collect_post_text(node: object, output: List[str]) -> None:
    if isinstance(node, dict):
        text = node.get("text")
        if isinstance(text, str) and text.strip():
            output.append(text.strip())
        for value in node.values():
            collect_post_text(value, output)
    elif isinstance(node, list):
        for item in node:
            collect_post_text(item, output)


def uniq_keep_order(values: List[str]) -> List[str]:
    seen = set()
    result = []
    for value in values:
        value = value.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def render_post_node(node: object) -> str:
    if not isinstance(node, dict):
        return stringify(node)
    tag = stringify(node.get("tag"))
    nested_text = node.get("text")
    if isinstance(nested_text, dict):
        nested_parts: List[str] = []
        extract_text_fragments(nested_text, nested_parts)
        rendered_nested = "\n".join(uniq_keep_order(nested_parts))
        if rendered_nested:
            return rendered_nested
    if tag == "text":
        return stringify(node.get("text"))
    if tag in {"plain_text", "lark_md", "markdown", "md"}:
        return stringify(node.get("content") or node.get("text"))
    if tag == "a":
        text = stringify(node.get("text"))
        href = stringify(node.get("href"))
        return text or href
    if tag == "at":
        return "@" + (stringify(node.get("user_name")) or stringify(node.get("name")) or "某人")
    if tag == "img":
        return "[图片]"
    if tag == "emotion":
        emoji = stringify(node.get("emoji_type"))
        return f"[表情:{emoji}]" if emoji else "[表情]"
    if tag == "media":
        return "[媒体]"
    if tag == "file":
        name = stringify(node.get("file_name"))
        return f"[文件:{name}]" if name else "[文件]"
    if isinstance(node.get("content"), str):
        return stringify(node.get("content"))
    return stringify(node.get("text") or node)


def render_post_content(parsed: Dict[str, object]) -> str:
    lines: List[str] = []
    title = stringify(parsed.get("title"))
    if title:
        lines.append(title)
    for paragraph in parsed.get("content") or []:
        if not isinstance(paragraph, list):
            continue
        line = "".join(render_post_node(node) for node in paragraph).strip()
        if line:
            lines.append(line)
    return "\n".join(uniq_keep_order(lines))


def extract_text_fragments(node: object, output: List[str]) -> None:
    if isinstance(node, dict):
        if "tag" in node:
            rendered = render_post_node(node)
            if rendered:
                output.append(rendered)
            return
        for key in ("text", "title", "content", "name", "label", "value"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                output.append(value.strip())
        for value in node.values():
            extract_text_fragments(value, output)
    elif isinstance(node, list):
        for item in node:
            extract_text_fragments(item, output)
    elif isinstance(node, str) and node.strip():
        output.append(node.strip())


def render_system_content(parsed: Dict[str, object]) -> str:
    template = stringify(parsed.get("template"))
    if not template:
        texts: List[str] = []
        extract_text_fragments(parsed, texts)
        return "\n".join(uniq_keep_order(texts))
    for key, value in parsed.items():
        if key == "template":
            continue
        if isinstance(value, list):
            replacement = "、".join(stringify(item) for item in value if stringify(item))
        elif isinstance(value, dict):
            replacement = ""
        else:
            replacement = stringify(value)
        template = template.replace("{" + key + "}", replacement)
    template = re.sub(r"\s+", " ", template).strip()
    return template


def render_interactive_content(parsed: Dict[str, object]) -> str:
    texts: List[str] = []
    extract_text_fragments(parsed, texts)
    rendered = "\n".join(uniq_keep_order(texts))
    return rendered


def render_message_content(msg_type: str, body: Dict[str, object]) -> str:
    content = body.get("content")
    if not isinstance(content, str) or not content:
        return ""
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        return content

    if msg_type == "text":
        return stringify(parsed.get("text"))
    if msg_type == "post":
        rendered = render_post_content(parsed)
        return rendered or compact_json(parsed)
    if msg_type == "image":
        return "[图片]"
    if msg_type in {"file", "audio", "video"}:
        file_name = stringify(parsed.get("file_name"))
        if msg_type == "audio":
            return f"[语音:{file_name}]" if file_name else "[语音]"
        if msg_type == "video":
            return f"[视频:{file_name}]" if file_name else "[视频]"
        return f"[文件:{file_name}]" if file_name else "[文件]"
    if msg_type == "system":
        rendered = render_system_content(parsed)
        return rendered or compact_json(parsed)
    if msg_type == "interactive":
        rendered = render_interactive_content(parsed)
        return rendered or compact_json(parsed)

    texts: List[str] = []
    extract_text_fragments(parsed, texts)
    rendered = "\n".join(uniq_keep_order(texts))
    return rendered or compact_json(parsed)


def find_by_name(items: List[Dict[str, object]], key_names: List[str], target: str) -> Optional[Dict[str, object]]:
    for item in items:
        for key in key_names:
            if item.get(key) == target:
                return item
    return None


def table_id_of(table: Dict[str, object]) -> Optional[str]:
    value = table.get("table_id") or table.get("id")
    return str(value) if value else None


def field_id_of(field: Dict[str, object]) -> Optional[str]:
    value = field.get("field_id") or field.get("id")
    return str(value) if value else None


def user_cell(open_id: str, id_type: str) -> Optional[List[Dict[str, str]]]:
    if open_id and id_type == "open_id":
        return [{"id": open_id}]
    return None


def chat_type_label(chat_type: object) -> Optional[str]:
    mapping = {
        "private": "私有群",
        "public": "公开群",
    }
    return mapping.get(stringify(chat_type))


def materialize_field_defs(field_defs: List[Dict[str, object]], chat_table_id: Optional[str] = None) -> List[Dict[str, object]]:
    result: List[Dict[str, object]] = []
    for field_def in field_defs:
        copied = dict(field_def)
        if copied.get("type") == "link" and copied.get("link_table") == "__CHAT_TABLE_ID__":
            if not chat_table_id:
                raise FeishuAPIError("chat table id is required for link fields")
            copied["link_table"] = chat_table_id
        result.append(copied)
    return result


def ensure_table_and_fields(
    client: FeishuClient,
    base_token: str,
    table_name: str,
    field_defs: List[Dict[str, object]],
    *,
    recreate_tables: bool,
) -> str:
    tables = client.list_tables(base_token)
    table = find_by_name(tables, ["name", "table_name"], table_name)
    if table and recreate_tables:
        table_id = table_id_of(table)
        if not table_id:
            raise FeishuAPIError(f"table id missing for {table_name}")
        client.delete_table(base_token, table_id)
        table = None

    if not table:
        table_id = client.create_table(base_token, table_name)
    else:
        table_id = table_id_of(table)
        if not table_id:
            raise FeishuAPIError(f"table id missing for {table_name}")

    fields = client.list_fields(base_token, table_id)
    existing = {str(field.get("name")): field for field in fields if field.get("name")}

    for field_def in field_defs:
        field_name = str(field_def["name"])
        if field_name not in existing:
            client.create_field(base_token, table_id, field_def)

    return table_id


def build_chat_row(
    chat: Dict[str, object],
    detail: Dict[str, object],
    share_link: Dict[str, object],
    members: List[Dict[str, object]],
    member_total: int,
    sync_run_id: str,
    sync_time_text: str,
    sync_tz: tzinfo,
) -> List[str]:
    link_error = stringify(share_link.get("_error"))
    owner_id = stringify(detail.get("owner_id") or chat.get("owner_id"))
    owner_id_type = stringify(detail.get("owner_id_type") or chat.get("owner_id_type"))
    return [
        stringify(detail.get("chat_id") or chat.get("chat_id")),
        stringify(detail.get("name") or chat.get("name")),
        stringify(detail.get("description") or chat.get("description")),
        stringify(detail.get("chat_type")),
        chat_type_label(detail.get("chat_type")),
        stringify(detail.get("chat_mode")),
        stringify(detail.get("chat_status") or chat.get("chat_status")),
        stringify(detail.get("external") if detail else chat.get("external")),
        stringify(detail.get("chat_tag")),
        stringify(detail.get("labels")),
        owner_id,
        owner_id_type,
        user_cell(owner_id, owner_id_type),
        stringify(detail.get("user_count")),
        stringify(detail.get("bot_count")),
        stringify(member_total or detail.get("user_count")),
        build_member_summary(members, member_total),
        stringify(share_link.get("share_link")),
        stringify(share_link.get("is_permanent")),
        epoch_seconds_to_text(share_link.get("expire_time"), sync_tz),
        link_error,
        stringify(detail.get("tenant_key") or chat.get("tenant_key")),
        stringify(detail.get("avatar") or chat.get("avatar")),
        sync_run_id,
        sync_time_text,
    ]


def build_member_rows(
    chat_id: str,
    chat_name: str,
    chat_record_id: str,
    members: List[Dict[str, object]],
    sync_run_id: str,
) -> List[List[object]]:
    rows = []
    for member in members:
        member_id = stringify(member.get("member_id"))
        member_id_type = stringify(member.get("member_id_type"))
        rows.append(
            [
                chat_id,
                chat_name,
                [{"id": chat_record_id}],
                member_id,
                member_id_type,
                user_cell(member_id, member_id_type),
                stringify(member.get("name")),
                stringify(member.get("tenant_key")),
                sync_run_id,
            ]
        )
    return rows


def build_message_row(
    message: Dict[str, object],
    chat_name: str,
    chat_record_id: str,
    sync_run_id: str,
    sync_tz: tzinfo,
) -> List[object]:
    sender = message.get("sender") if isinstance(message.get("sender"), dict) else {}
    body = message.get("body") if isinstance(message.get("body"), dict) else {}
    raw_body = compact_json(body) if body else ""
    rendered_content = render_message_content(stringify(message.get("msg_type")), body)
    sender_id = stringify(sender.get("id"))
    sender_id_type = stringify(sender.get("id_type"))
    sender_type = stringify(sender.get("sender_type"))
    return [
        stringify(message.get("message_id")),
        stringify(message.get("chat_id")),
        chat_name,
        [{"id": chat_record_id}],
        stringify(message.get("msg_type")),
        epoch_millis_to_text(message.get("create_time"), sync_tz),
        epoch_millis_to_text(message.get("update_time"), sync_tz),
        sender_id,
        sender_id_type,
        user_cell(sender_id, sender_id_type) if sender_type == "user" else None,
        sender_type,
        stringify(sender.get("tenant_key")),
        stringify(message.get("deleted")),
        stringify(message.get("updated")),
        stringify(message.get("thread_id")),
        rendered_content,
        rendered_content,
        raw_body,
        sync_run_id,
    ]


def chunked(rows: List[List[str]], size: int) -> Iterable[List[List[str]]]:
    for idx in range(0, len(rows), size):
        yield rows[idx:idx + size]


def build_member_summary(members: List[Dict[str, object]], member_total: int, limit: int = 20) -> str:
    names = [stringify(member.get("name")) for member in members if stringify(member.get("name"))]
    preview = names[:limit]
    if member_total > limit:
        preview.append(f"... 共{member_total}人")
    return "、".join(preview)


def main() -> int:
    args = parse_args()
    base_token = parse_base_token(args.base_url)
    sync_tz = load_timezone(args.sync_timezone)
    scheduled_window = None
    start_time = parse_time_to_epoch_seconds(args.start, tzinfo=sync_tz)
    end_time = parse_time_to_epoch_seconds(args.end, end_of_day=True, tzinfo=sync_tz)
    if args.scheduled_daily:
        state = load_state(args.state_file)
        now_ts = int(time.time())
        previous_end = int_value(state.get("last_success_end_time"))
        current_chat_scope = chat_scope_signature(args.max_chats, args.skip_chats)
        previous_chat_scope = stringify(state.get("chat_scope"))
        baseline_start = today_start_epoch_seconds(sync_tz)
        baseline_reason = "explicit_start" if start_time is not None else ""

        if start_time is not None:
            pass
        elif (
            args.scheduled_baseline == "today"
            and previous_chat_scope != current_chat_scope
        ):
            start_time = baseline_start
            baseline_reason = "chat_scope_changed_or_missing"
        elif previous_end > 0:
            start_time = previous_end + 1
            baseline_reason = "state"
        elif args.scheduled_baseline == "today":
            start_time = baseline_start
            baseline_reason = "missing_state_today"
        else:
            start_time = max(0, now_ts - args.initial_lookback_hours * 3600)
            baseline_reason = "missing_state_lookback"
        if end_time is None:
            end_time = now_ts
        args.chat_order = "created_desc"
        scheduled_window = {
            "state_file": args.state_file,
            "last_success_end_time": previous_end,
            "current_start_time": start_time,
            "current_end_time": end_time,
            "chat_scope": current_chat_scope,
            "previous_chat_scope": previous_chat_scope,
            "scheduled_baseline": args.scheduled_baseline,
            "baseline_reason": baseline_reason,
            "sync_timezone": args.sync_timezone,
        }
    sync_run_id = datetime.now(sync_tz).strftime("%Y%m%d%H%M%S")
    sync_time_text = datetime.now(sync_tz).strftime("%Y-%m-%d %H:%M:%S")

    client = FeishuClient(args.app_id, args.app_secret, verbose=args.verbose)
    client.authenticate()

    print(
        f"正在使用 {client.identity_summary()} 获取该应用机器人所在群聊...",
        file=sys.stderr,
    )
    if args.scheduled_daily:
        print(
            "定时模式增量窗口: "
            f"{epoch_seconds_to_iso(start_time, sync_tz)} -> {epoch_seconds_to_iso(end_time, sync_tz)}",
            file=sys.stderr,
        )
    chats = client.list_chats(
        page_size=args.chat_page_size,
        max_chats=args.max_chats,
        skip_chats=args.skip_chats,
        created_desc=args.chat_order == "created_desc",
    )
    print(f"共获取到 {len(chats)} 个群聊。", file=sys.stderr)

    if args.smoke_test:
        sampled_chats = []
        for chat in chats[:max(0, args.smoke_test_detail_chats)]:
            chat_id = stringify(chat.get("chat_id"))
            detail = client.get_chat_detail(chat_id)
            members_payload = client.list_chat_members(chat_id)
            messages = list(
                client.iter_messages(
                    chat_id,
                    page_size=args.message_page_size,
                    start_time=start_time,
                    end_time=end_time,
                    max_messages=args.max_messages_per_chat,
                )
            )
            sampled_chats.append(
                {
                    "chat_id": chat_id,
                    "name": stringify(detail.get("name") or chat.get("name")),
                    "member_count": len(members_payload.get("items") or []),
                    "member_total": int(members_payload.get("member_total") or 0),
                    "message_count_in_window": len(messages),
                }
            )
        print(
            json.dumps(
                {
                    "smoke_test": True,
                    "identity_source": client.identity_summary(),
                    "chat_count": len(chats),
                    "chat_scope": chat_scope_signature(args.max_chats, args.skip_chats),
                    "sync_timezone": args.sync_timezone,
                    "start_time": start_time,
                    "start_time_iso": epoch_seconds_to_iso(start_time, sync_tz) if start_time else None,
                    "end_time": end_time,
                    "end_time_iso": epoch_seconds_to_iso(end_time, sync_tz) if end_time else None,
                    "sampled_chat_count": len(sampled_chats),
                    "sampled_chats": sampled_chats,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    chat_table_id = ensure_table_and_fields(
        client,
        base_token,
        args.chat_table_name,
        CHAT_FIELD_DEFS,
        recreate_tables=args.recreate_tables or args.refresh_metadata_tables,
    )
    client.ensure_groupchat_field_v1(base_token, chat_table_id, CHAT_GROUPCHAT_FIELD_NAME)
    member_field_defs = materialize_field_defs(BASE_MEMBER_FIELD_DEFS, chat_table_id)
    message_field_defs = materialize_field_defs(BASE_MESSAGE_FIELD_DEFS, chat_table_id)
    member_fields = [field["name"] for field in member_field_defs]
    message_fields = [field["name"] for field in message_field_defs]
    member_table_id = ensure_table_and_fields(
        client,
        base_token,
        args.member_table_name,
        member_field_defs,
        recreate_tables=args.recreate_tables or args.refresh_metadata_tables,
    )
    message_table_id = ensure_table_and_fields(
        client,
        base_token,
        args.message_table_name,
        message_field_defs,
        recreate_tables=args.recreate_tables,
    )

    if args.recreate_tables:
        existing_message_ids: set = set()
    else:
        print("正在预读消息表已有 消息ID 用于去重...", file=sys.stderr)
        existing_message_ids = client.list_existing_text_values(
            base_token, message_table_id, "消息ID"
        )
        print(f"消息表中已有 {len(existing_message_ids)} 条消息ID。", file=sys.stderr)

    total_messages = 0
    total_members = 0
    total_chat_rows = 0
    processed_chats = 0
    batch_size = max(1, min(args.sync_batch_size, 200))
    for chat_batch in chunked(chats, batch_size):
        batch_payloads: List[Dict[str, object]] = []
        batch_start = processed_chats + 1
        batch_end = processed_chats + len(chat_batch)
        print(
            f"[{batch_start}-{batch_end}/{len(chats)}] 正在读取群、成员和消息...",
            file=sys.stderr,
        )

        for chat in chat_batch:
            chat_id = stringify(chat.get("chat_id"))
            detail = dict(chat) if args.fast_metadata else client.get_chat_detail(chat_id)
            members_payload = client.list_chat_members(chat_id)
            members = list(members_payload.get("items") or [])
            member_total = int(members_payload.get("member_total") or len(members))
            if args.skip_share_links:
                share_link = {}
            else:
                try:
                    share_link = client.get_chat_share_link(chat_id)
                except Exception as exc:  # noqa: BLE001
                    share_link = {"_error": str(exc)}

            chat_name = stringify(detail.get("name") or chat.get("name"))
            chat_row = build_chat_row(
                chat,
                detail,
                share_link,
                members,
                member_total,
                sync_run_id,
                sync_time_text,
                sync_tz,
            )
            messages = list(
                client.iter_messages(
                    chat_id,
                    page_size=args.message_page_size,
                    start_time=start_time,
                    end_time=end_time,
                    max_messages=args.max_messages_per_chat,
                )
            )
            batch_payloads.append(
                {
                    "chat_id": chat_id,
                    "chat_name": chat_name,
                    "chat_row": chat_row,
                    "members": members,
                    "messages": messages,
                }
            )

        chat_rows = [payload["chat_row"] for payload in batch_payloads]
        chat_result = client.batch_create_records(base_token, chat_table_id, CHAT_FIELDS, chat_rows)
        chat_record_id_list = [str(record_id) for record_id in (chat_result.get("record_id_list") or [])]
        if len(chat_record_id_list) != len(batch_payloads):
            raise FeishuAPIError(
                f"expected {len(batch_payloads)} chat record ids, got {len(chat_record_id_list)}"
            )

        member_rows_for_batch: List[List[object]] = []
        message_rows_for_batch: List[List[object]] = []
        batch_member_count = 0
        batch_message_count = 0
        batch_skipped_count = 0

        for payload, chat_record_id in zip(batch_payloads, chat_record_id_list):
            chat_id = str(payload["chat_id"])
            chat_name = str(payload["chat_name"])
            if not args.skip_groupchat_field_updates:
                client.batch_update_groupchat_fields_v1(
                    base_token,
                    chat_table_id,
                    [chat_record_id],
                    chat_id,
                    chat_name,
                )

            member_rows = build_member_rows(
                chat_id,
                chat_name,
                chat_record_id,
                list(payload["members"]),
                sync_run_id,
            )
            payload["member_rows"] = member_rows
            member_rows_for_batch.extend(member_rows)
            batch_member_count += len(member_rows)

            message_rows: List[List[object]] = []
            for message in list(payload["messages"]):
                message_id = stringify(message.get("message_id"))
                if message_id and message_id in existing_message_ids:
                    batch_skipped_count += 1
                    continue
                if message_id:
                    existing_message_ids.add(message_id)
                message_rows.append(build_message_row(message, chat_name, chat_record_id, sync_run_id, sync_tz))
                batch_message_count += 1
            payload["message_rows"] = message_rows
            message_rows_for_batch.extend(message_rows)

        if args.skip_groupchat_field_updates:
            for batch in chunked(member_rows_for_batch, 200):
                client.batch_create_records(base_token, member_table_id, member_fields, batch)
            for batch in chunked(message_rows_for_batch, 200):
                client.batch_create_records(base_token, message_table_id, message_fields, batch)
        else:
            for payload, chat_record_id in zip(batch_payloads, chat_record_id_list):
                chat_id = str(payload["chat_id"])
                chat_name = str(payload["chat_name"])
                member_rows = list(payload.get("member_rows") or [])
                for batch in chunked(member_rows, 200):
                    member_result = client.batch_create_records(base_token, member_table_id, member_fields, batch)
                    client.batch_update_groupchat_fields_v1(
                        base_token,
                        member_table_id,
                        [str(record_id) for record_id in (member_result.get("record_id_list") or [])],
                        chat_id,
                        chat_name,
                    )

                for batch in chunked(list(payload.get("message_rows") or []), 200):
                    message_result = client.batch_create_records(base_token, message_table_id, message_fields, batch)
                    client.batch_update_groupchat_fields_v1(
                        base_token,
                        message_table_id,
                        [str(record_id) for record_id in (message_result.get("record_id_list") or [])],
                        chat_id,
                        chat_name,
                    )

        total_chat_rows += len(batch_payloads)
        total_members += batch_member_count
        total_messages += batch_message_count
        processed_chats += len(chat_batch)
        skipped_text = f"，跳过 {batch_skipped_count} 条已存在消息" if batch_skipped_count else ""
        print(
            f"    批次已写入：群 {len(batch_payloads)} 个，成员 {batch_member_count} 人，消息 {batch_message_count} 条{skipped_text}。",
            file=sys.stderr,
        )

    result = {
        "base_token": base_token,
        "identity_source": client.identity_summary(),
        "chat_table_name": args.chat_table_name,
        "member_table_name": args.member_table_name,
        "message_table_name": args.message_table_name,
        "sync_run_id": sync_run_id,
        "skip_chats": args.skip_chats,
        "chat_count": total_chat_rows,
        "member_count": total_members,
        "message_count": total_messages,
        "chat_order": args.chat_order,
        "chat_scope": chat_scope_signature(args.max_chats, args.skip_chats),
        "sync_timezone": args.sync_timezone,
        "start_time": start_time,
        "end_time": end_time,
    }
    if scheduled_window:
        result["scheduled_window"] = scheduled_window
        save_state(
            args.state_file,
            {
                "last_success_end_time": end_time,
                "last_success_end_iso": epoch_seconds_to_iso(end_time, sync_tz),
                "last_sync_run_id": sync_run_id,
                "identity_source": client.identity_summary(),
                "base_token": base_token,
                "chat_scope": chat_scope_signature(args.max_chats, args.skip_chats),
                "scheduled_baseline": args.scheduled_baseline,
                "sync_timezone": args.sync_timezone,
            },
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("同步已取消。", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:  # noqa: BLE001
        print(f"同步失败: {exc}", file=sys.stderr)
        raise SystemExit(1)
