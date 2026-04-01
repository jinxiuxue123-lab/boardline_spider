import argparse
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from xianyu_open import process_callback


class CallbackHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict, status: int = 200):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        path = urlparse(self.path).path
        if path not in ("/xianyu/callback", "/callback/xianyu"):
            self._send_json({"ok": False, "msg": "not found"}, 404)
            return

        length = int(self.headers.get("Content-Length", "0") or "0")
        raw_body = self.rfile.read(length) if length > 0 else b"{}"

        try:
            payload = json.loads(raw_body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            self._send_json({"ok": False, "msg": "invalid json"}, 400)
            return

        try:
            if isinstance(payload, list):
                results = [process_callback(item) for item in payload if isinstance(item, dict)]
            else:
                results = [process_callback(payload)]
            self._send_json({"ok": True, "results": results})
        except Exception as e:
            self._send_json({"ok": False, "msg": str(e)}, 500)

    def log_message(self, format, *args):
        return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), CallbackHandler)
    print(f"闲鱼回调服务已启动: http://{args.host}:{args.port}/xianyu/callback")
    server.serve_forever()


if __name__ == "__main__":
    main()
