#!/usr/bin/env python3
"""Fake OpenAI-compatible streaming model, for offline demos and smoke tests.

It speaks just enough of POST /chat/completions for the tRPC-Agent-Go openai
model client: an SSE stream of chat.completion.chunk objects, a usage block,
then [DONE]. No API key and no network needed, so the whole platform loop
(IM callback -> guardrails -> model -> streaming reply) can be exercised
locally.

The reply is scripted so the output guardrail has something to catch: the
keyword 内部资料 is deliberately split across two chunks, which only a
tail-window stream checker can detect.

    python3 scripts/fake_model.py            # listens on 127.0.0.1:9009
    python3 scripts/fake_model.py --port 9   # matches base_url http://127.0.0.1:9
"""

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Chunks of the scripted reply, plus the token counts reported at the end.
CHUNKS = ["Hello ", "world, 内部", "资料 leaked.", " (done)"]
PROMPT_TOKENS = 11
COMPLETION_TOKENS = 7


def chunk_payload(delta, finish=None, usage=None):
    choice = {"index": 0, "delta": delta, "finish_reason": finish}
    payload = {
        "id": "chatcmpl-fake",
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": "fake-model",
        "choices": [choice],
    }
    if usage is not None:
        payload["usage"] = usage
    return json.dumps(payload, ensure_ascii=False)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):  # noqa: N802 - http.server naming
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        try:
            last = json.loads(body)["messages"][-1]["content"]
        except Exception:
            last = ""
        print(f"[fake-model] request: {last!r}", flush=True)

        # Chunked encoding is what the real API uses and what delimits the
        # stream: without it an HTTP/1.1 keep-alive client waits for EOF that
        # never comes and the reply never finishes.
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

        for text in CHUNKS:
            self._sse(chunk_payload({"content": text}))
            time.sleep(0.05)
        usage = {
            "prompt_tokens": PROMPT_TOKENS,
            "completion_tokens": COMPLETION_TOKENS,
            "total_tokens": PROMPT_TOKENS + COMPLETION_TOKENS,
        }
        self._sse(chunk_payload({}, finish="stop", usage=usage))
        self._write(b"data: [DONE]\n\n")
        self.wfile.write(b"0\r\n\r\n")  # terminating chunk
        self.wfile.flush()

    def _sse(self, payload):
        self._write(f"data: {payload}\n\n".encode("utf-8"))

    def _write(self, data):
        self.wfile.write(f"{len(data):x}\r\n".encode("ascii") + data + b"\r\n")
        self.wfile.flush()

    def log_message(self, fmt, *args):  # keep stdout readable
        pass


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9009)
    args = p.parse_args()
    print(f"[fake-model] listening on http://{args.host}:{args.port}", flush=True)
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
