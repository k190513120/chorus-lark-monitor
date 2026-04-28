#!/usr/bin/env python3
"""Ensure this app's bot is present in external chats visible to authorized users.

The script supports two token sources:

1. A group-join proxy, such as the Cloudflare Worker in
   `/Users/bytedance/Desktop/wechat_bot/cloudflare/feishu-bot-proxy`.
2. A local JSON token pool compatible with `.feishu_group_join_user_tokens.json`.

It is dry-run by default. Pass `--apply` to actually invite the bot.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


FEISHU_OPEN_API_BASE = "https://open.feishu.cn/open-apis"
DEFAULT_TOKEN_POOL = ".feishu_group_join_user_tokens.json"
TOKEN_REFRESH_SKEW_SECONDS = 300
TOKEN_EXPIRED_CODES = {99991663, 99991664, 99991668, 99991677}


class FeishuAPIError(RuntimeError):
    def __init__(self, message: str, code: Optional[int] = None, payload: Optional[dict] = None) -> None:
        super().__init__(message)
        self.code = code
        self.payload = payload or {}


@dataclass
class AuthorizedUser:
    id: str
    open_id: str = ""
    name: str = ""
    label: str = ""
    scope: str = ""
    expires_at: float = 0.0
    has_refresh_token: bool = False

    @property
    def display(self) -> str:
        bits = [self.name or self.label or self.open_id or self.id]
        if self.id and self.id not in bits:
            bits.append(self.id)
        return " / ".join(bit for bit in bits if bit)


@dataclass
class TargetChat:
    chat_id: str
    name: str
    external: bool
    chat_status: str
    user_id: str
    user_display: str


def bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def scalar_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def normalize_epoch_seconds(value: object) -> float:
    if value in (None, ""):
        return 0.0
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return 0.0
    if ts > 10_000_000_000:
        return ts / 1000.0
    return ts


def short_error(exc: BaseException) -> str:
    text = str(exc).replace("\n", " ").strip()
    return text[:500]


def encode_params(params: Optional[Dict[str, object]]) -> str:
    if not params:
        return ""
    clean = {k: v for k, v in params.items() if v not in (None, "")}
    return urllib.parse.urlencode(clean, doseq=True)


def http_json(
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, object]] = None,
    data: Optional[Dict[str, object]] = None,
    bearer_token: str = "",
    timeout: int = 60,
    retries: int = 3,
) -> Dict[str, object]:
    query = encode_params(params)
    if query:
        joiner = "&" if "?" in url else "?"
        url = f"{url}{joiner}{query}"

    body = None
    if data is not None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "chorus-lark-monitor/ensure-bot-in-external-chats",
    }
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"

    last_error: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                text = resp.read().decode("utf-8")
            return json.loads(text) if text else {}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            last_error = FeishuAPIError(f"HTTP {exc.code}: {detail[:500]}")
        except Exception as exc:  # noqa: BLE001
            last_error = exc

        if attempt < retries:
            time.sleep(min(2 ** (attempt - 1), 5))

    raise last_error or RuntimeError(f"{method} {url} failed")


class FeishuClient:
    def __init__(self, app_id: str, app_secret: str, verbose: bool = False) -> None:
        self.app_id = app_id
        self.app_secret = app_secret
        self.verbose = verbose
        self.tenant_access_token = ""
        self.tenant_token_acquired_at = 0.0

    def _feishu_json(
        self,
        method: str,
        path: str,
        *,
        access_token: str,
        params: Optional[Dict[str, object]] = None,
        data: Optional[Dict[str, object]] = None,
        retries: int = 3,
    ) -> Dict[str, object]:
        token_refreshed = False
        for _ in range(retries):
            payload = http_json(
                method,
                f"{FEISHU_OPEN_API_BASE}{path}",
                params=params,
                data=data,
                bearer_token=access_token,
                retries=1,
            )
            code = int(payload.get("code") or 0)
            if code == 0:
                if self.verbose:
                    print(f"[debug] {method} {path} ok", file=sys.stderr)
                return dict(payload.get("data") or {})

            if code in TOKEN_EXPIRED_CODES and access_token == self.tenant_access_token and not token_refreshed:
                self.authenticate_tenant()
                access_token = self.tenant_access_token
                token_refreshed = True
                continue

            raise FeishuAPIError(
                f"{method} {path} failed: code={code} msg={payload.get('msg')}",
                code=code,
                payload=payload,
            )
        raise FeishuAPIError(f"{method} {path} failed after retries")

    def authenticate_tenant(self) -> str:
        payload = http_json(
            "POST",
            f"{FEISHU_OPEN_API_BASE}/auth/v3/tenant_access_token/internal",
            data={"app_id": self.app_id, "app_secret": self.app_secret},
        )
        token = payload.get("tenant_access_token") or payload.get("data", {}).get("tenant_access_token")
        if not token:
            raise FeishuAPIError(f"tenant_access_token missing: code={payload.get('code')} msg={payload.get('msg')}")
        self.tenant_access_token = scalar_text(token)
        self.tenant_token_acquired_at = time.time()
        if self.verbose:
            print("[debug] tenant_access_token refreshed", file=sys.stderr)
        return self.tenant_access_token

    def _tenant_token(self) -> str:
        if not self.tenant_access_token or time.time() - self.tenant_token_acquired_at > 5400:
            return self.authenticate_tenant()
        return self.tenant_access_token

    def get_app_access_token(self) -> str:
        payload = http_json(
            "POST",
            f"{FEISHU_OPEN_API_BASE}/auth/v3/app_access_token/internal",
            data={"app_id": self.app_id, "app_secret": self.app_secret},
        )
        token = payload.get("app_access_token") or payload.get("data", {}).get("app_access_token")
        if not token:
            raise FeishuAPIError(f"app_access_token missing: code={payload.get('code')} msg={payload.get('msg')}")
        return scalar_text(token)

    def refresh_user_token(self, refresh_token: str, fallback: Dict[str, object]) -> Dict[str, object]:
        """Refresh a local user token. Try OAuth v2 first, then OIDC v1."""
        v2_payload: Dict[str, object] = {}
        try:
            v2_payload = http_json(
                "POST",
                f"{FEISHU_OPEN_API_BASE}/authen/v2/oauth/token",
                data={
                    "grant_type": "refresh_token",
                    "client_id": self.app_id,
                    "client_secret": self.app_secret,
                    "refresh_token": refresh_token,
                },
                retries=1,
            )
            if int(v2_payload.get("code") or 0) == 0:
                return token_record(dict(v2_payload.get("data") or v2_payload), fallback=fallback)
        except Exception:
            v2_payload = {}

        app_access_token = self.get_app_access_token()
        v1_payload = http_json(
            "POST",
            f"{FEISHU_OPEN_API_BASE}/authen/v1/oidc/refresh_access_token",
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            bearer_token=app_access_token,
        )
        if int(v1_payload.get("code") or 0) != 0:
            raise FeishuAPIError(
                f"user token refresh failed: code={v2_payload.get('code', '?')}/{v1_payload.get('code')} "
                f"msg={v2_payload.get('msg') or v1_payload.get('msg')}"
            )
        return token_record(dict(v1_payload.get("data") or {}), fallback=fallback)

    def list_chats(self, access_token: str, *, tenant_token: bool = False) -> List[Dict[str, object]]:
        items: List[Dict[str, object]] = []
        page_token = ""
        while True:
            data = self._feishu_json(
                "GET",
                "/im/v1/chats",
                access_token=self._tenant_token() if tenant_token else access_token,
                params={
                    "user_id_type": "open_id",
                    "page_size": 100,
                    "page_token": page_token,
                },
            )
            batch = list(data.get("items") or [])
            items.extend(batch)
            if not data.get("has_more"):
                return items
            page_token = scalar_text(data.get("page_token"))
            if not page_token:
                return items

    def list_bot_chats(self) -> List[Dict[str, object]]:
        return self.list_chats(self._tenant_token(), tenant_token=True)

    def add_bot_to_chat(self, user_access_token: str, chat_id: str) -> Tuple[bool, str]:
        try:
            payload = http_json(
                "POST",
                f"{FEISHU_OPEN_API_BASE}/im/v1/chats/{urllib.parse.quote(chat_id)}/members",
                params={"member_id_type": "app_id"},
                data={"id_list": [self.app_id]},
                bearer_token=user_access_token,
            )
        except Exception as exc:  # noqa: BLE001
            return False, short_error(exc)

        code = int(payload.get("code") or 0)
        if code == 0:
            data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
            pending = data.get("pending_approval_id_list") if isinstance(data, dict) else None
            if pending:
                return True, "pending_approval"
            return True, "added"

        msg = scalar_text(payload.get("msg"))
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        detail_bits = []
        for key in ("invalid_id_list", "not_existed_id_list", "pending_approval_id_list"):
            if isinstance(data, dict) and data.get(key):
                detail_bits.append(f"{key}={data[key]}")
        detail = f" {' '.join(detail_bits)}" if detail_bits else ""
        if "already" in msg.lower() or "exist" in msg.lower() or "已在" in msg:
            return True, f"already_in_chat code={code} msg={msg}{detail}"
        return False, f"code={code} msg={msg}{detail}"


def token_record(token_data: Dict[str, object], fallback: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    fallback = fallback or {}
    access_token = token_data.get("access_token") or token_data.get("user_access_token")
    if not access_token:
        raise FeishuAPIError("token response missing access_token")

    expires_in = int(token_data.get("expires_in") or token_data.get("expire") or 7200)
    record = {
        "access_token": access_token,
        "refresh_token": token_data.get("refresh_token") or fallback.get("refresh_token", ""),
        "expires_at": time.time() + max(expires_in - TOKEN_REFRESH_SKEW_SECONDS, 0),
        "open_id": token_data.get("open_id") or fallback.get("open_id", ""),
        "union_id": token_data.get("union_id") or fallback.get("union_id", ""),
        "user_id": token_data.get("user_id") or fallback.get("user_id", ""),
        "name": token_data.get("name") or fallback.get("name", ""),
        "label": fallback.get("label", ""),
        "scope": token_data.get("scope") or fallback.get("scope", ""),
        "updated_at": time.time(),
    }
    refresh_expires_in = token_data.get("refresh_token_expires_in") or token_data.get("refresh_expires_in")
    if refresh_expires_in:
        record["refresh_expires_at"] = time.time() + int(refresh_expires_in)
    elif fallback.get("refresh_expires_at"):
        record["refresh_expires_at"] = fallback["refresh_expires_at"]
    return record


class LocalTokenPool:
    def __init__(self, path: str, client: FeishuClient, save_updates: bool = True) -> None:
        self.path = path
        self.client = client
        self.save_updates = save_updates
        self.data: Dict[str, object] = {}

    def load(self) -> Dict[str, object]:
        if self.data:
            return self.data
        if not self.path or not os.path.exists(self.path):
            self.data = {"users": {}}
            return self.data
        with open(self.path, encoding="utf-8") as f:
            loaded = json.load(f)
        if not isinstance(loaded, dict):
            raise RuntimeError(f"token pool must be a JSON object: {self.path}")
        loaded.setdefault("users", {})
        self.data = loaded
        return self.data

    def save(self) -> None:
        if not self.save_updates:
            return
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
            f.write("\n")
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, self.path)

    def list_users(self) -> List[AuthorizedUser]:
        users = self.load().get("users", {})
        if not isinstance(users, dict):
            raise RuntimeError(f"token pool users must be an object: {self.path}")
        out: List[AuthorizedUser] = []
        for user_id, record in users.items():
            if not isinstance(record, dict):
                continue
            out.append(
                AuthorizedUser(
                    id=scalar_text(user_id),
                    open_id=scalar_text(record.get("open_id") or user_id),
                    name=scalar_text(record.get("name") or record.get("en_name")),
                    label=scalar_text(record.get("label")),
                    scope=scalar_text(record.get("scope")),
                    expires_at=normalize_epoch_seconds(record.get("expires_at")),
                    has_refresh_token=bool(record.get("refresh_token")),
                )
            )
        return out

    def get_access_token(self, user_id: str) -> str:
        data = self.load()
        users = data.setdefault("users", {})
        if not isinstance(users, dict):
            raise RuntimeError(f"token pool users must be an object: {self.path}")
        record = users.get(user_id)
        if not isinstance(record, dict):
            raise RuntimeError(f"authorized user not found in local token pool: {user_id}")

        if time.time() < normalize_epoch_seconds(record.get("expires_at")) and record.get("access_token"):
            return scalar_text(record["access_token"])

        refresh_token = scalar_text(record.get("refresh_token"))
        if not refresh_token:
            raise RuntimeError(f"authorized user token expired without refresh_token: {user_id}")

        refreshed = self.client.refresh_user_token(refresh_token, fallback=record)
        refreshed["open_id"] = refreshed.get("open_id") or record.get("open_id") or user_id
        refreshed["label"] = record.get("label", "")
        users[user_id] = refreshed
        self.save()
        return scalar_text(refreshed["access_token"])


class ProxyTokenSource:
    def __init__(self, proxy_url: str, admin_token: str) -> None:
        self.proxy_url = proxy_url.rstrip("/")
        self.admin_token = admin_token

    def _get(self, path: str, params: Optional[Dict[str, object]] = None) -> Dict[str, object]:
        return http_json(
            "GET",
            f"{self.proxy_url}{path}",
            params=params,
            bearer_token=self.admin_token,
            retries=2,
        )

    def list_users(self) -> List[AuthorizedUser]:
        data = self._get("/group-join/users")
        users = data.get("users") if isinstance(data, dict) else []
        if not isinstance(users, list):
            raise RuntimeError("proxy /group-join/users returned an invalid payload")
        out: List[AuthorizedUser] = []
        for item in users:
            if not isinstance(item, dict):
                continue
            user_id = scalar_text(item.get("id") or item.get("open_id") or item.get("union_id") or item.get("user_id"))
            if not user_id:
                continue
            out.append(
                AuthorizedUser(
                    id=user_id,
                    open_id=scalar_text(item.get("open_id")),
                    name=scalar_text(item.get("name") or item.get("en_name")),
                    label=scalar_text(item.get("label")),
                    scope=scalar_text(item.get("scope")),
                    expires_at=normalize_epoch_seconds(item.get("expires_at")),
                    has_refresh_token=bool(item.get("has_refresh_token")),
                )
            )
        return out

    def get_access_token(self, user_id: str) -> str:
        data = self._get("/group-join/token", {"id": user_id})
        token = data.get("access_token") if isinstance(data, dict) else ""
        if not token:
            raise RuntimeError(f"proxy did not return access_token for user {user_id}")
        return scalar_text(token)


def choose_token_source(args: argparse.Namespace, client: FeishuClient) -> object:
    proxy_url = args.proxy_url or os.getenv("GROUP_JOIN_PROXY_URL") or os.getenv("FEISHU_GROUP_JOIN_PROXY_URL")
    admin_token = args.admin_token or os.getenv("GROUP_JOIN_ADMIN_TOKEN") or os.getenv("FEISHU_GROUP_JOIN_ADMIN_TOKEN")
    if proxy_url:
        if not admin_token:
            raise RuntimeError("--proxy-url requires --admin-token or GROUP_JOIN_ADMIN_TOKEN")
        return ProxyTokenSource(proxy_url, admin_token)

    token_pool = args.token_pool or os.getenv("GROUP_JOIN_USER_TOKEN_POOL") or DEFAULT_TOKEN_POOL
    return LocalTokenPool(token_pool, client, save_updates=not args.no_save_token_pool)


def chat_name(chat: Dict[str, object]) -> str:
    return scalar_text(chat.get("name") or chat.get("chat_name") or "(no name)")


def chat_id(chat: Dict[str, object]) -> str:
    return scalar_text(chat.get("chat_id") or chat.get("id"))


def is_target_chat(chat: Dict[str, object], include_internal: bool) -> bool:
    if not chat_id(chat):
        return False
    if include_internal:
        return True
    return bool(chat.get("external"))


def limited(items: Iterable[AuthorizedUser], limit: int) -> List[AuthorizedUser]:
    out = list(items)
    return out[:limit] if limit > 0 else out


def print_users(users: List[AuthorizedUser]) -> None:
    if not users:
        print("No authorized users found.")
        return
    print(f"Authorized users: {len(users)}")
    print(f"{'id':<34} {'name/label':<24} {'refresh':<8} {'expires_at':<19} scope")
    print("-" * 110)
    for user in users:
        expires = "-"
        if user.expires_at:
            expires = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(user.expires_at))
        label = (user.name or user.label or user.open_id or "-")[:24]
        refresh = "yes" if user.has_refresh_token else "no"
        print(f"{user.id:<34} {label:<24} {refresh:<8} {expires:<19} {user.scope}")


def collect_targets(
    client: FeishuClient,
    token_source: object,
    users: List[AuthorizedUser],
    *,
    include_internal: bool,
    limit_chats: int,
    fail_fast: bool,
) -> Tuple[List[TargetChat], Dict[str, int], List[str]]:
    print("Listing chats the current bot is already in...")
    bot_chats = client.list_bot_chats()
    bot_chat_ids = {chat_id(item) for item in bot_chats if chat_id(item)}
    print(f"Bot is already in {len(bot_chat_ids)} chat(s).")

    targets: List[TargetChat] = []
    seen_target_ids = set(bot_chat_ids)
    stats = {
        "users": len(users),
        "user_chats": 0,
        "candidate_chats": 0,
        "already_has_bot": 0,
        "deduped": 0,
        "targets": 0,
        "user_errors": 0,
    }
    errors: List[str] = []

    for idx, user in enumerate(users, start=1):
        print(f"[{idx}/{len(users)}] Listing chats for {user.display}...")
        try:
            access_token = token_source.get_access_token(user.id)  # type: ignore[attr-defined]
            chats = client.list_chats(access_token)
        except Exception as exc:  # noqa: BLE001
            stats["user_errors"] += 1
            message = f"{user.display}: {short_error(exc)}"
            errors.append(message)
            print(f"  ERROR {message}", file=sys.stderr)
            if fail_fast:
                raise
            continue

        candidates = [item for item in chats if is_target_chat(item, include_internal)]
        stats["user_chats"] += len(chats)
        stats["candidate_chats"] += len(candidates)

        user_targets = 0
        for item in candidates:
            cid = chat_id(item)
            if cid in bot_chat_ids:
                stats["already_has_bot"] += 1
                continue
            if cid in seen_target_ids:
                stats["deduped"] += 1
                continue
            target = TargetChat(
                chat_id=cid,
                name=chat_name(item),
                external=bool(item.get("external")),
                chat_status=scalar_text(item.get("chat_status") or "-"),
                user_id=user.id,
                user_display=user.display,
            )
            targets.append(target)
            seen_target_ids.add(cid)
            user_targets += 1
            if limit_chats > 0 and len(targets) >= limit_chats:
                break
        print(f"  total={len(chats)} candidate={len(candidates)} new_targets={user_targets}")
        if limit_chats > 0 and len(targets) >= limit_chats:
            break

    stats["targets"] = len(targets)
    return targets, stats, errors


def apply_targets(
    client: FeishuClient,
    token_source: object,
    targets: List[TargetChat],
    *,
    sleep_seconds: float,
    fail_fast: bool,
) -> Tuple[int, int]:
    ok_count = 0
    fail_count = 0
    token_cache: Dict[str, str] = {}

    print(f"Applying to {len(targets)} chat(s)...")
    for idx, target in enumerate(targets, start=1):
        try:
            token = token_cache.get(target.user_id)
            if not token:
                token = token_source.get_access_token(target.user_id)  # type: ignore[attr-defined]
                token_cache[target.user_id] = token
            ok, message = client.add_bot_to_chat(token, target.chat_id)
        except Exception as exc:  # noqa: BLE001
            ok = False
            message = short_error(exc)

        tag = "OK" if ok else "FAIL"
        print(f"[{idx}/{len(targets)}] {tag:<4} {target.chat_id} {target.name[:40]:<40} {message}")
        if ok:
            ok_count += 1
        else:
            fail_count += 1
            if fail_fast:
                raise RuntimeError(f"failed to add bot to {target.chat_id}: {message}")
        if sleep_seconds > 0 and idx < len(targets):
            time.sleep(sleep_seconds)

    return ok_count, fail_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--app-id", default=os.getenv("LARK_APP_ID") or os.getenv("FEISHU_APP_ID"))
    parser.add_argument("--app-secret", default=os.getenv("LARK_APP_SECRET") or os.getenv("FEISHU_APP_SECRET"))
    parser.add_argument("--proxy-url", help="group-join proxy base URL, e.g. https://feishu-bot.example.com")
    parser.add_argument("--admin-token", help="admin token for the group-join proxy")
    parser.add_argument("--token-pool", help=f"local user token pool path (default: {DEFAULT_TOKEN_POOL})")
    parser.add_argument("--no-save-token-pool", action="store_true", help="do not write refreshed local user tokens back to disk")
    parser.add_argument("--user-id", action="append", default=[], help="only process this authorized user id/open_id; repeatable")
    parser.add_argument("--limit-users", type=int, default=0, help="process at most N authorized users")
    parser.add_argument("--limit-chats", type=int, default=0, help="process at most N target chats")
    parser.add_argument("--include-internal", action="store_true", help="also process internal chats (default: external chats only)")
    parser.add_argument("--apply", action="store_true", help="actually invite the bot (default: dry-run)")
    parser.add_argument("--sleep-seconds", type=float, default=float(os.getenv("EXTERNAL_GROUP_JOIN_SLEEP_SECONDS", "0.2")))
    parser.add_argument("--fail-fast", action="store_true", help="stop on the first user/chat error")
    parser.add_argument("--list-authorized-users", action="store_true", help="list authorized users without printing tokens")
    parser.add_argument("--check-bot-access", action="store_true", help="only verify that app credentials can list bot chats")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    missing = []
    if not args.app_id:
        missing.append("--app-id or LARK_APP_ID/FEISHU_APP_ID")
    if not args.app_secret:
        missing.append("--app-secret or LARK_APP_SECRET/FEISHU_APP_SECRET")
    if missing:
        print("ERROR: missing " + ", ".join(missing), file=sys.stderr)
        return 2

    client = FeishuClient(args.app_id, args.app_secret, verbose=args.verbose)

    try:
        if args.check_bot_access:
            chats = client.list_bot_chats()
            external_count = sum(1 for item in chats if item.get("external"))
            print(f"Bot chat access OK: total={len(chats)} external={external_count}")
            return 0

        token_source = choose_token_source(args, client)
        users = token_source.list_users()  # type: ignore[attr-defined]
        if args.user_id:
            wanted = set(args.user_id)
            exact_ids = {user.id for user in users}
            users = [
                user
                for user in users
                if user.id in wanted or (user.open_id in wanted and user.open_id not in exact_ids)
            ]
        users = limited(users, args.limit_users)

        if args.list_authorized_users:
            print_users(users)
            return 0

        if not users:
            print("No authorized users to process.")
            return 0

        targets, stats, errors = collect_targets(
            client,
            token_source,
            users,
            include_internal=args.include_internal,
            limit_chats=args.limit_chats,
            fail_fast=args.fail_fast,
        )

        print("\nSummary:")
        print(
            "  users={users} user_errors={user_errors} user_chats={user_chats} "
            "candidate_chats={candidate_chats} already_has_bot={already_has_bot} "
            "deduped={deduped} targets={targets}".format(**stats)
        )
        if errors:
            print("  user error samples:")
            for message in errors[:5]:
                print(f"    - {message}")

        if targets:
            print("\nTarget chats:")
            print(f"{'chat_id':<34} {'ext':<4} {'status':<10} {'authorized_by':<28} name")
            print("-" * 120)
            for target in targets[:50]:
                ext = "yes" if target.external else "no"
                print(
                    f"{target.chat_id:<34} {ext:<4} {target.chat_status:<10} "
                    f"{target.user_display[:28]:<28} {target.name}"
                )
            if len(targets) > 50:
                print(f"... {len(targets) - 50} more target chat(s)")

        if not args.apply:
            print("\n[dry-run] No chats were modified. Re-run with --apply to invite the bot.")
            return 1 if errors and stats["user_chats"] == 0 else 0

        if not targets:
            print("\nNothing to apply.")
            return 0 if not errors else 1

        ok_count, fail_count = apply_targets(
            client,
            token_source,
            targets,
            sleep_seconds=args.sleep_seconds,
            fail_fast=args.fail_fast,
        )
        print(f"\nDone. added_or_present={ok_count} failed={fail_count}")
        return 0 if fail_count == 0 and not (errors and stats["user_chats"] == 0) else 1
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {short_error(exc)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
