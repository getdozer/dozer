"""Tiny read API over the same CSVs Dozer sources.

Dozer on current main is a mover (sources -> sql -> sinks), not an API
server. This is the app-side API LangChain talks to.
"""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from bank import all_profiles, get_profile, load_bank

bank = load_bank()


def _json(handler, code, payload):
    body = json.dumps(payload).encode()
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"api {args[0]}")

    def do_GET(self):
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        if path == "/health":
            return _json(self, 200, {"ok": True})
        if path == "/customers":
            return _json(self, 200, bank["customers"])
        if path == "/transactions":
            return _json(self, 200, bank["transactions"])
        if path == "/cards":
            return _json(self, 200, bank["cards"])
        if path == "/customer_360":
            return _json(self, 200, all_profiles(bank))
        if path.startswith("/customer_360/"):
            cid = path.rsplit("/", 1)[-1]
            row = get_profile(cid, bank)
            if row is None:
                return _json(self, 404, {"error": f"unknown customer {cid}"})
            return _json(self, 200, row)
        return _json(self, 404, {"error": "not found"})


def main():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    args = p.parse_args()
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"listening on http://{args.host}:{args.port}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
