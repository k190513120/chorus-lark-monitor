#!/usr/bin/env python3
"""飞书 Docx 写作工具库 - 把多个 script 里的 boilerplate 集中。

设计哲学：
  - block_type cheatsheet 一处集中：1=page, 2=text, 3..6=heading, 12=bullet,
    14=code, 22=divider, 27=image, 43=board
  - 流式 builder API：doc.builder().h1(...).p(...).svg(...).append()
  - SVG 默认走原生画板（block_type=43 + svg node），不用 image+token PATCH
  - 鉴权：从 LARK_APP_ID / LARK_APP_SECRET env 自动拿 tenant_access_token

典型用法：
    from lark_docx import LarkDocxClient

    doc = LarkDocxClient.from_env("VqlCdpASboikidxVuTMcth1rnAh")
    doc.builder() \\
        .h1("标题") \\
        .p("正文段落") \\
        .h2("二级标题") \\
        .bullet("第一条").bullet("第二条") \\
        .svg(open("arch.svg").read()) \\
        .code("print('hi')", lang="python") \\
        .divider() \\
        .rewrite()        # 清空文档后重写
        # 或 .append()    # 追加到末尾

    # 或直接构造 block 列表
    from lark_docx import h, p, bullet, code, divider, svg_item
    items = [
        h(1, "Title"),
        p("intro"),
        svg_item(open("diagram.svg").read()),
    ]
    doc.write(items, mode="rewrite")  # rewrite | append
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Iterable, Optional

LARK_OPEN = "https://open.feishu.cn"


# ─── block_type cheatsheet ─────────────────────────────────────────────
# 1   page (root, 不直接用)
# 2   text (普通段落)
# 3   heading1
# 4   heading2
# 5   heading3
# 6   heading4
# 7-11 heading5-9
# 12  bullet (无序列表项)
# 13  ordered (有序列表项)
# 14  code
# 17  quote_container
# 22  divider
# 27  image
# 43  board (原生画板)


# ─── block builders (返回单个 block dict) ──────────────────────────────

def h(level: int, text: str) -> dict:
    """heading1/2/3/4 — 用 level 1-4"""
    btype = {1: 3, 2: 4, 3: 5, 4: 6}[level]
    return {"block_type": btype, f"heading{level}": {"elements": [{"text_run": {"content": text}}], "style": {}}}


def p(text: str, bold: bool = False, italic: bool = False, link: Optional[str] = None) -> dict:
    """普通段落"""
    style: dict = {}
    if bold: style["bold"] = True
    if italic: style["italic"] = True
    if link: style["link"] = {"url": urllib.parse.quote(link, safe="")}
    el: dict = {"text_run": {"content": text}}
    if style:
        el["text_run"]["text_element_style"] = style
    return {"block_type": 2, "text": {"elements": [el], "style": {}}}


def bullet(text: str) -> dict:
    """无序列表项"""
    return {"block_type": 12, "bullet": {"elements": [{"text_run": {"content": text}}], "style": {}}}


def ordered(text: str) -> dict:
    """有序列表项"""
    return {"block_type": 13, "ordered": {"elements": [{"text_run": {"content": text}}], "style": {}}}


_CODE_LANGS = {
    "plain": 1, "ada": 2, "apache": 3, "assembly": 4, "bash": 5, "csharp": 6,
    "cpp": 7, "c": 8, "cobol": 9, "css": 10, "cuda": 11, "python": 12, "dart": 13,
    "delphi": 14, "django": 15, "dockerfile": 16, "erlang": 17, "fortran": 18,
    "foxpro": 19, "go": 20, "groovy": 21, "html": 22, "java": 23, "js": 24,
    "javascript": 24, "json": 28, "julia": 29, "kotlin": 30, "latex": 31,
    "lisp": 32, "logo": 33, "lua": 34, "matlab": 35, "ml": 26, "objc": 27,
    "openedge": 38, "pascal": 39, "perl": 40, "php": 41, "powershell": 42,
    "prolog": 43, "protobuf": 44, "python3": 12, "r": 45, "rpg": 46, "ruby": 47,
    "rust": 48, "scala": 49, "scheme": 50, "scratch": 51, "shell": 5,
    "smalltalk": 52, "sql": 53, "stata": 54, "swift": 55, "thrift": 56, "tcl": 57,
    "toml": 58, "typescript": 59, "ts": 59, "vbscript": 60, "vb": 61, "verilog": 62,
    "vhdl": 63, "vue": 64, "xml": 65, "yaml": 66,
}


def code(text: str, lang: str = "plain") -> dict:
    """代码块"""
    return {"block_type": 14, "code": {"elements": [{"text_run": {"content": text}}], "style": {"language": _CODE_LANGS.get(lang.lower(), 1)}}}


def divider() -> dict:
    return {"block_type": 22, "divider": {}}


def quote(text: str) -> dict:
    return {"block_type": 34, "quote_container": {"elements": [{"text_run": {"content": text}}]}}


# ─── 复合 item types（带 SVG / image 的）──────────────────────────────

def text_item(blocks: list[dict]) -> dict:
    """一组普通 blocks 包成 item"""
    return {"type": "blocks", "data": blocks}


def svg_item(svg_xml: str, name: str = "diagram.svg") -> dict:
    """SVG 用原生画板呈现，从 viewBox 解析尺寸"""
    return {"type": "svg", "svg": svg_xml, "name": name}


def image_item(svg_or_image_bytes: bytes, name: str = "image.svg") -> dict:
    """图片块（备用，渲染不如画板稳定）"""
    return {"type": "image", "bytes": svg_or_image_bytes, "name": name}


# ─── Lark Docx Client ─────────────────────────────────────────────────

class LarkDocxClient:
    """单文档客户端。token 自动从 env 拿，自动刷新。"""

    def __init__(self, doc_id: str, app_id: str, app_secret: str):
        self.doc_id = doc_id
        self._app_id = app_id
        self._app_secret = app_secret
        self._token: Optional[str] = None
        self._token_expires_at = 0.0

    @classmethod
    def from_env(cls, doc_id: str) -> "LarkDocxClient":
        return cls(
            doc_id=doc_id,
            app_id=os.environ["LARK_APP_ID"],
            app_secret=os.environ["LARK_APP_SECRET"],
        )

    def _ensure_token(self) -> str:
        if self._token and time.time() < self._token_expires_at - 60:
            return self._token
        req = urllib.request.Request(
            f"{LARK_OPEN}/open-apis/auth/v3/tenant_access_token/internal",
            data=json.dumps({"app_id": self._app_id, "app_secret": self._app_secret}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as resp:
            d = json.loads(resp.read())
        if int(d.get("code", -1)) != 0:
            raise RuntimeError(f"get token failed: {d}")
        self._token = d["tenant_access_token"]
        self._token_expires_at = time.time() + d.get("expire", 7200)
        return self._token

    def _api(self, method: str, path: str, body=None, params=None) -> dict:
        if params:
            qs = urllib.parse.urlencode(params)
            path = f"{path}{'&' if '?' in path else '?'}{qs}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"{LARK_OPEN}{path}", data=data, method=method,
            headers={"Authorization": f"Bearer {self._ensure_token()}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            msg = e.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"{method} {path} -> HTTP {e.code}: {msg[:500]}") from e

    # ─── 读 ────────────────────────────────────────────

    def list_root_children(self) -> list[dict]:
        """列出文档根 block 的所有 children。注意 API 响应字段是 items 不是 children。"""
        out: list = []
        page_token = ""
        while True:
            path = f"/open-apis/docx/v1/documents/{self.doc_id}/blocks/{self.doc_id}/children?page_size=500"
            if page_token:
                path += f"&page_token={page_token}"
            d = self._api("GET", path)
            data = d.get("data", {})
            items = data.get("items") or data.get("children") or []
            out.extend(items)
            if not data.get("has_more"):
                break
            page_token = data.get("page_token", "")
            if not page_token:
                break
        return out

    def raw_content(self) -> str:
        d = self._api("GET", f"/open-apis/docx/v1/documents/{self.doc_id}/raw_content?lang=0")
        return d.get("data", {}).get("content", "")

    # ─── 写 ────────────────────────────────────────────

    def delete_all_children(self) -> int:
        """清空文档（保留根 page block）。返回删除条数。"""
        children = self.list_root_children()
        n = len(children)
        if n == 0:
            return 0
        # 分批从尾部删避免 index 漂移
        chunk = 100
        deleted = 0
        while deleted < n:
            end = n - deleted
            start = max(end - chunk, 0)
            d = self._api(
                "DELETE",
                f"/open-apis/docx/v1/documents/{self.doc_id}/blocks/{self.doc_id}/children/batch_delete",
                body={"start_index": start, "end_index": end},
            )
            if int(d.get("code", -1)) != 0:
                raise RuntimeError(f"batch_delete failed: {d}")
            deleted += (end - start)
            time.sleep(0.3)
        return n

    def append_blocks(self, blocks: list[dict], chunk: int = 20) -> list[str]:
        """批量 append blocks 到文档末尾。返回新 block_id 列表。"""
        out: list[str] = []
        for start in range(0, len(blocks), chunk):
            batch = blocks[start : start + chunk]
            d = self._api(
                "POST",
                f"/open-apis/docx/v1/documents/{self.doc_id}/blocks/{self.doc_id}/children",
                body={"children": batch, "index": -1},
            )
            if int(d.get("code", -1)) != 0:
                raise RuntimeError(f"append failed: {d}")
            out.extend(c["block_id"] for c in d["data"].get("children", []))
            time.sleep(0.3)
        return out

    def append_svg_board(self, svg_xml: str) -> tuple[str, str]:
        """把 SVG 嵌入飞书原生画板（推荐做法），返回 (docx_block_id, whiteboard_token)。

        SVG 的 viewBox 自动解析作为画板节点尺寸。"""
        m = re.search(r'viewBox="0\s+0\s+(\d+)\s+(\d+)"', svg_xml)
        vw, vh = (int(m.group(1)), int(m.group(2))) if m else (1280, 720)

        # 1) 加 board block
        d = self._api(
            "POST",
            f"/open-apis/docx/v1/documents/{self.doc_id}/blocks/{self.doc_id}/children",
            body={"children": [{"block_type": 43, "board": {"align": 1}}], "index": -1},
        )
        if int(d.get("code", -1)) != 0:
            raise RuntimeError(f"create board failed: {d}")
        child = d["data"]["children"][0]
        block_id = child["block_id"]
        wb_token = child["board"]["token"]

        # 2) 往画板加 svg node
        resp = self._api(
            "POST",
            f"/open-apis/board/v1/whiteboards/{wb_token}/nodes",
            body={"nodes": [{
                "type": "svg",
                "x": 0, "y": 0, "width": vw, "height": vh,
                "svg": {"svg_code": svg_xml},
            }]},
        )
        if int(resp.get("code", -1)) != 0:
            raise RuntimeError(f"create svg node failed: {resp}")
        return block_id, wb_token

    def append_image_block(self, image_bytes: bytes, name: str = "image.svg") -> tuple[str, str]:
        """图片块（备用方案）。返回 (block_id, file_token)。"""
        d = self._api(
            "POST",
            f"/open-apis/docx/v1/documents/{self.doc_id}/blocks/{self.doc_id}/children",
            body={"children": [{"block_type": 27, "image": {"token": ""}}], "index": -1},
        )
        block_id = d["data"]["children"][0]["block_id"]

        # multipart upload
        boundary = "----larkdocx" + str(int(time.time()))
        parts: list = []
        def add(field, val):
            if isinstance(val, (int, float)): val = str(val)
            if isinstance(val, str): val = val.encode()
            parts.extend([f"--{boundary}".encode(), f'Content-Disposition: form-data; name="{field}"'.encode(), b"", val])
        add("file_name", name)
        add("parent_type", "docx_image")
        add("parent_node", block_id)
        add("size", len(image_bytes))
        add("extra", json.dumps({"drive_route_token": self.doc_id}))
        parts.extend([
            f"--{boundary}".encode(),
            f'Content-Disposition: form-data; name="file"; filename="{name}"'.encode(),
            b'Content-Type: image/svg+xml', b"", image_bytes,
        ])
        parts.append(f"--{boundary}--".encode())
        body = b"\r\n".join(parts)
        req = urllib.request.Request(
            f"{LARK_OPEN}/open-apis/drive/v1/medias/upload_all",
            data=body, method="POST",
            headers={"Authorization": f"Bearer {self._ensure_token()}",
                     "Content-Type": f"multipart/form-data; boundary={boundary}"},
        )
        with urllib.request.urlopen(req) as resp:
            up = json.loads(resp.read())
        if int(up.get("code", -1)) != 0:
            raise RuntimeError(f"upload failed: {up}")
        file_token = up["data"]["file_token"]

        # PATCH replace_image（关键步骤！否则文档里图打不开）
        patch = self._api(
            "PATCH",
            f"/open-apis/docx/v1/documents/{self.doc_id}/blocks/{block_id}",
            body={"replace_image": {"token": file_token}},
        )
        if int(patch.get("code", -1)) != 0:
            raise RuntimeError(f"PATCH replace_image failed: {patch}")
        return block_id, file_token

    # ─── 高层 API ───────────────────────────────────────

    def write(self, items: Iterable, mode: str = "append", verbose: bool = True) -> None:
        """items: text_item / svg_item / image_item 混合列表。

        mode='append' → 追加到末尾
        mode='rewrite' → 先清空再写
        """
        items = list(items)
        if mode == "rewrite":
            if verbose:
                print(f"[1/2] clearing doc {self.doc_id}...", flush=True)
            n = self.delete_all_children()
            if verbose:
                print(f"      removed {n} blocks", flush=True)
        elif mode != "append":
            raise ValueError(f"mode must be 'append' or 'rewrite', got {mode!r}")

        txt_n = sum(len(it['data']) for it in items if it['type']=='blocks')
        media_n = sum(1 for it in items if it['type'] in ('svg','image'))
        if verbose:
            print(f"[2/2] writing {len(items)} items ({txt_n} text blocks + {media_n} media)...", flush=True)

        for i, it in enumerate(items, 1):
            if it["type"] == "blocks":
                self.append_blocks(it["data"])
                if verbose:
                    print(f"  [{i}/{len(items)}] +{len(it['data'])} blocks", flush=True)
            elif it["type"] == "svg":
                block_id, wb_token = self.append_svg_board(it["svg"])
                if verbose:
                    print(f"  [{i}/{len(items)}] board {it.get('name')} (wb={wb_token[:18]}...)", flush=True)
            elif it["type"] == "image":
                block_id, file_token = self.append_image_block(it["bytes"], it.get("name", "image.svg"))
                if verbose:
                    print(f"  [{i}/{len(items)}] image {it.get('name')} (file={file_token[:18]}...)", flush=True)
            else:
                raise ValueError(f"unknown item type: {it.get('type')}")

        if verbose:
            print(f"DONE. open: https://bytedance.larkoffice.com/docx/{self.doc_id}", flush=True)

    def builder(self) -> "DocxBuilder":
        return DocxBuilder(self)


class DocxBuilder:
    """流式 API 构造文档。SVG 自动走画板。

    用法：
        doc.builder() \\
            .h1("标题") \\
            .p("正文") \\
            .h2("Section 1") \\
            .bullet("a").bullet("b") \\
            .svg(open("arch.svg").read()) \\
            .code("print('hi')", lang="python") \\
            .rewrite()   # 或 .append()
    """

    def __init__(self, client: LarkDocxClient):
        self.client = client
        self._items: list = []
        self._pending_blocks: list = []

    def _flush(self):
        if self._pending_blocks:
            self._items.append({"type": "blocks", "data": self._pending_blocks})
            self._pending_blocks = []

    def h1(self, text): self._pending_blocks.append(h(1, text)); return self
    def h2(self, text): self._pending_blocks.append(h(2, text)); return self
    def h3(self, text): self._pending_blocks.append(h(3, text)); return self
    def h4(self, text): self._pending_blocks.append(h(4, text)); return self
    def p(self, text, **kw): self._pending_blocks.append(p(text, **kw)); return self
    def bullet(self, text): self._pending_blocks.append(bullet(text)); return self
    def ordered(self, text): self._pending_blocks.append(ordered(text)); return self
    def code(self, text, lang="plain"): self._pending_blocks.append(code(text, lang=lang)); return self
    def divider(self): self._pending_blocks.append(divider()); return self
    def quote(self, text): self._pending_blocks.append(quote(text)); return self

    def svg(self, svg_xml: str, name: str = "diagram.svg"):
        """嵌入 SVG 为飞书原生画板"""
        self._flush()
        self._items.append({"type": "svg", "svg": svg_xml, "name": name})
        return self

    def image(self, image_bytes: bytes, name: str = "image"):
        """嵌入图片为 image block（备用方案）"""
        self._flush()
        self._items.append({"type": "image", "bytes": image_bytes, "name": name})
        return self

    def items(self) -> list:
        self._flush()
        return self._items

    def append(self, **kw):
        self.client.write(self.items(), mode="append", **kw)

    def rewrite(self, **kw):
        self.client.write(self.items(), mode="rewrite", **kw)


# ─── CLI 入口（方便调试，不常用）───────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: lark_docx.py <doc_id>  (prints doc metadata)", file=sys.stderr)
        sys.exit(2)
    doc = LarkDocxClient.from_env(sys.argv[1])
    print(f"doc_id: {doc.doc_id}")
    print(f"children: {len(doc.list_root_children())}")
    print(f"content preview:\n{doc.raw_content()[:500]}")
