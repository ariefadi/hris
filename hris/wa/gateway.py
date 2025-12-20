import os
import time
import random
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union
import requests
try:
    from dotenv import load_dotenv, find_dotenv
except Exception:
    load_dotenv = None
    find_dotenv = None
def _ensure_env_loaded() -> None:
    if load_dotenv is None or find_dotenv is None:
        return
    if any(k in os.environ for k in ("WHATSAPP_GATEWAY_URL", "WA_GATEWAY_URL")):
        return
    env_path = find_dotenv(usecwd=True)
    if env_path:
        load_dotenv(env_path, override=False)
def _get_settings() -> Dict[str, Any]:
    _ensure_env_loaded()
    base = os.getenv("WHATSAPP_GATEWAY_URL") or os.getenv("WA_GATEWAY_URL") or "http://localhost:3000"
    return {"base_url": base, "trend": "tgh"}
def _normalize_recipients(value: Union[str, Iterable[str], None]) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return [v for v in value if v]
def _jid(num: str) -> str:
    n = str(num or "").strip()
    if not n:
        return ""
    if n.endswith("@s.whatsapp.net"):
        return n
    return n + "@s.whatsapp.net"
def _append_trend(url: str, trend: str) -> str:
    return url + ("&trend=" + trend if "?" in url else "?trend=" + trend)
def _post_json(base: str, path: str, payload: Dict[str, Any], trend: str) -> requests.Response:
    url = _append_trend(base + path, trend)
    return requests.post(url, json=payload)
def _post_multipart(base: str, path: str, data: Dict[str, Any], files: Optional[Dict[str, Any]], trend: str) -> requests.Response:
    url = _append_trend(base + path, trend)
    return requests.post(url, data=data, files=files)
class WhatsApp:
    def __init__(self) -> None:
        self._to: List[str] = []
        self._forwarded: bool = False
        self._mode: Optional[str] = None
        self._message: Optional[str] = None
        self._link: Optional[str] = None
        self._caption: Optional[str] = None
        self._image_path: Optional[str] = None
        self._image_url: Optional[str] = None
        self._view_once: bool = False
        self._compress: bool = False
        self._file_path: Optional[str] = None
        self._delay_min_ms: int = 0
        self._delay_max_ms: int = 0
    def to(self, recipients: Union[str, Iterable[str]]) -> "WhatsApp":
        self._to.extend(_normalize_recipients(recipients))
        return self
    def forwarded(self, value: bool = True) -> "WhatsApp":
        self._forwarded = bool(value)
        return self
    def message(self, text: str) -> "WhatsApp":
        self._mode = "message"
        self._message = text
        return self
    def link(self, url: str, caption: Optional[str] = None) -> "WhatsApp":
        self._mode = "link"
        self._link = url
        self._caption = caption
        return self
    def image(self, file_path: Optional[str] = None, url: Optional[str] = None, caption: Optional[str] = None, view_once: bool = False, compress: bool = False) -> "WhatsApp":
        self._mode = "image"
        self._image_path = file_path
        self._image_url = url
        self._caption = caption
        self._view_once = bool(view_once)
        self._compress = bool(compress)
        return self
    def file(self, file_path: str, caption: Optional[str] = None) -> "WhatsApp":
        self._mode = "file"
        self._file_path = file_path
        self._caption = caption
        return self
    def delay(self, min_ms: int = 0, max_ms: int = 0) -> "WhatsApp":
        self._delay_min_ms = max(0, int(min_ms or 0))
        self._delay_max_ms = max(0, int(max_ms or 0))
        return self
    def send(self) -> bool:
        settings = _get_settings()
        base = settings["base_url"]
        trend = settings["trend"]
        recipients = self._to[:]
        ok_all = True
        for idx, raw in enumerate(recipients):
            if len(recipients) > 1 and self._delay_max_ms > 0 and idx > 0:
                ms = random.randint(self._delay_min_ms, self._delay_max_ms)
                time.sleep(ms / 1000.0)
            phone = _jid(raw)
            if self._mode == "message":
                payload = {"phone": phone, "message": self._message or "", "is_forwarded": bool(self._forwarded)}
                r = _post_json(base, "/send/message", payload, trend)
                ok_all = ok_all and r.ok
            elif self._mode == "link":
                payload = {"phone": phone, "link": self._link or "", "caption": self._caption or "", "is_forwarded": bool(self._forwarded)}
                r = _post_json(base, "/send/link", payload, trend)
                ok_all = ok_all and r.ok
            elif self._mode == "image":
                data = {
                    "phone": phone,
                    "caption": self._caption or "",
                    "view_once": "true" if self._view_once else "false",
                    "compress": "true" if self._compress else "false",
                    "is_forwarded": "true" if self._forwarded else "false",
                }
                files = None
                if self._image_path:
                    p = self._image_path
                    files = {"image": open(p, "rb")}
                if self._image_url:
                    data["image_url"] = self._image_url
                r = _post_multipart(base, "/send/image", data, files, trend)
                ok_all = ok_all and r.ok
                if files and "image" in files:
                    try:
                        files["image"].close()
                    except Exception:
                        pass
            elif self._mode == "file":
                data = {
                    "phone": phone,
                    "caption": self._caption or "",
                    "is_forwarded": "true" if self._forwarded else "false",
                }
                files = {"file": open(self._file_path or "", "rb")} if self._file_path else None
                r = _post_multipart(base, "/send/file", data, files, trend)
                ok_all = ok_all and r.ok
                if files and "file" in files:
                    try:
                        files["file"].close()
                    except Exception:
                        pass
            else:
                ok_all = False
        return bool(ok_all)
def send_whatsapp_message(to: Union[str, Sequence[str]], message: str, is_forwarded: bool = False, delay_min_ms: int = 0, delay_max_ms: int = 0) -> bool:
    return WhatsApp().to(to).forwarded(is_forwarded).message(message).delay(delay_min_ms, delay_max_ms).send()
def send_whatsapp_link(to: Union[str, Sequence[str]], url: str, caption: Optional[str] = None, is_forwarded: bool = False, delay_min_ms: int = 0, delay_max_ms: int = 0) -> bool:
    return WhatsApp().to(to).forwarded(is_forwarded).link(url, caption).delay(delay_min_ms, delay_max_ms).send()
def send_whatsapp_image(to: Union[str, Sequence[str]], image_path: Optional[str] = None, image_url: Optional[str] = None, caption: Optional[str] = None, view_once: bool = False, compress: bool = False, is_forwarded: bool = False, delay_min_ms: int = 0, delay_max_ms: int = 0) -> bool:
    return WhatsApp().to(to).forwarded(is_forwarded).image(file_path=image_path, url=image_url, caption=caption, view_once=view_once, compress=compress).delay(delay_min_ms, delay_max_ms).send()
def send_whatsapp_file(to: Union[str, Sequence[str]], file_path: str, caption: Optional[str] = None, is_forwarded: bool = False, delay_min_ms: int = 0, delay_max_ms: int = 0) -> bool:
    return WhatsApp().to(to).forwarded(is_forwarded).file(file_path, caption).delay(delay_min_ms, delay_max_ms).send()
__all__ = ["WhatsApp", "send_whatsapp_message", "send_whatsapp_link", "send_whatsapp_image", "send_whatsapp_file"]
