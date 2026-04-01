import hashlib
import json
import os
import time

import requests
from requests.exceptions import ConnectionError, ReadTimeout, SSLError, Timeout


class XianyuOpenClient:
    def __init__(
        self,
        app_key: str | None = None,
        app_secret: str | None = None,
        base_url: str | None = None,
        timeout: int = 60,
        max_retries: int = 3,
    ):
        self.app_key = app_key or os.getenv("XIANYU_OPEN_APP_KEY", "")
        self.app_secret = app_secret or os.getenv("XIANYU_OPEN_APP_SECRET", "")
        self.base_url = (base_url or os.getenv("XIANYU_OPEN_BASE_URL", "https://open.goofish.pro")).rstrip("/")
        self.timeout = timeout
        self.max_retries = max(1, int(max_retries))

        if not self.app_key:
            raise ValueError("缺少 XIANYU_OPEN_APP_KEY")
        if not self.app_secret:
            raise ValueError("缺少 XIANYU_OPEN_APP_SECRET")

    @staticmethod
    def _md5(text: str) -> str:
        return hashlib.md5(text.encode("utf-8")).hexdigest()

    def build_auth(self, body_text: str) -> tuple[dict[str, str], dict[str, str]]:
        timestamp = str(int(time.time()))
        body_md5 = self._md5(body_text)
        sign = self._md5(f"{self.app_key},{body_md5},{timestamp},{self.app_secret}")

        headers = {
            "Content-Type": "application/json",
            "AppID": self.app_key,
            "appID": self.app_key,
            "appId": self.app_key,
            "appid": self.app_key,
            "appKey": self.app_key,
            "AppKey": self.app_key,
            "timestamp": timestamp,
            "Timestamp": timestamp,
            "sign": sign,
            "Sign": sign,
        }
        query = {
            "AppID": self.app_key,
            "appID": self.app_key,
            "appId": self.app_key,
            "appid": self.app_key,
            "appKey": self.app_key,
            "timestamp": timestamp,
            "sign": sign,
        }
        return headers, query

    def post(self, path: str, payload: dict) -> dict:
        body_text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        headers, query = self.build_auth(body_text)
        url = f"{self.base_url}{path}"

        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = requests.post(
                    url,
                    params=query,
                    data=body_text.encode("utf-8"),
                    headers=headers,
                    timeout=self.timeout,
                )
                break
            except (SSLError, ConnectionError, ReadTimeout, Timeout) as e:
                last_error = e
                if attempt >= self.max_retries:
                    raise
                time.sleep(2 ** (attempt - 1))
        else:
            raise last_error  # pragma: no cover

        try:
            data = resp.json()
        except ValueError:
            resp.raise_for_status()
            raise RuntimeError(f"开放平台返回了非 JSON 响应: {resp.text[:500]}")

        if not resp.ok:
            raise RuntimeError(f"HTTP {resp.status_code}: {json.dumps(data, ensure_ascii=False)}")

        code = data.get("code")
        success = data.get("success")
        if success is False or (code not in (None, 0, "0", 200, "200")):
            raise RuntimeError(json.dumps(data, ensure_ascii=False))

        return data
