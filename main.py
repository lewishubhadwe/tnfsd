import asyncio
import json
import os
import random
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import aiohttp
import websockets

# ================= CONFIGURATION =================
TOKENS = [
    "MTU0ODYyMDE3NjI4OTgyODg4Ng.G9dDdz.FPbbTGl5qZLN0xXDCA0Qh3buPd3iXr-PnMXAE8",
    "MTUxMzc1MjAyMzg5NDU4OTU3MQ.GynV0M.9FOEhplb8wlWE8Ye2H5wihUJxR3vpfrfBGr_xI",
]

# Empty list = monitor all channels in all servers & DMs
TARGET_CHANNEL_IDS = []

# Only trigger on messages from this username
TARGET_USERNAME = "iloves3xworkers"

# Repeat the exact text content sent by TARGET_USERNAME
REPEAT_MESSAGES = True

# Discord's :rofl: emoji
EMOJIS = ["🤣"]

# Random delay range (in seconds) before reacting/sending
MIN_DELAY = 0.1
MAX_DELAY = 0.4

# Set True to print every incoming message author & channel for debugging
DEBUG = False

GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"
API_BASE = "https://discord.com/api/v10"
# =================================================


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        pass


def start_http_server():
    port = int(os.environ.get("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"HTTP health server listening on 0.0.0.0:{port}")
    return server


def build_headers(token: str) -> dict:
    return {
        "Authorization": token,
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }


async def add_reaction(session: aiohttp.ClientSession, token: str, channel_id: str, message_id: str, emoji: str, idx: int):
    encoded_emoji = urllib.parse.quote(emoji)
    url = f"{API_BASE}/channels/{channel_id}/messages/{message_id}/reactions/{encoded_emoji}/@me"
    headers = build_headers(token)
    async with session.put(url, headers=headers) as resp:
        if resp.status in (200, 204):
            print(f"[Account {idx}] Reacted with {emoji} in channel {channel_id}")
        elif resp.status == 429:
            data = await resp.json()
            retry_after = float(data.get("retry_after", 1.0))
            print(f"[Account {idx}] Reaction rate limited (429), retrying after {retry_after}s")
            await asyncio.sleep(retry_after)
            async with session.put(url, headers=headers) as retry_resp:
                if retry_resp.status in (200, 204):
                    print(f"[Account {idx}] Reacted with {emoji} after retry in {channel_id}")
                else:
                    print(f"[Account {idx}] Reaction retry failed ({retry_resp.status}): {await retry_resp.text()}")
        else:
            body = await resp.text()
            print(f"[Account {idx}] Reaction failed ({resp.status}): {body}")


async def send_message(session: aiohttp.ClientSession, token: str, channel_id: str, content: str, idx: int):
    url = f"{API_BASE}/channels/{channel_id}/messages"
    headers = build_headers(token)
    payload = {"content": content}
    async with session.post(url, headers=headers, json=payload) as resp:
        if resp.status in (200, 201):
            print(f"[Account {idx}] Sent message in channel {channel_id}: {content}")
        elif resp.status == 429:
            data = await resp.json()
            retry_after = float(data.get("retry_after", 1.0))
            print(f"[Account {idx}] Message rate limited (429), retrying after {retry_after}s")
            await asyncio.sleep(retry_after)
            async with session.post(url, headers=headers, json=payload) as retry_resp:
                if retry_resp.status in (200, 201):
                    print(f"[Account {idx}] Sent message after retry in channel {channel_id}: {content}")
                else:
                    print(f"[Account {idx}] Message retry failed ({retry_resp.status}): {await retry_resp.text()}")
        else:
            body = await resp.text()
            print(f"[Account {idx}] Message failed ({resp.status}): {body}")


async def heartbeat_loop(ws, interval: float, seq_holder: dict):
    while True:
        await asyncio.sleep(interval)
        await ws.send(json.dumps({"op": 1, "d": seq_holder["seq"]}))


async def run_account(token: str, idx: int):
    seq_holder = {"seq": None}
    my_user_id = None

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with websockets.connect(GATEWAY_URL, max_size=None) as ws:
                    hello = json.loads(await ws.recv())
                    heartbeat_interval = hello["d"]["heartbeat_interval"] / 1000.0
                    hb_task = asyncio.create_task(heartbeat_loop(ws, heartbeat_interval, seq_holder))

                    identify = {
                        "op": 2,
                        "d": {
                            "token": token,
                            "capabilities": 16381,
                            "properties": {
                                "os": "Windows",
                                "browser": "Chrome",
                                "device": "",
                            },
                        },
                    }
                    await ws.send(json.dumps(identify))

                    try:
                        async for raw_msg in ws:
                            payload = json.loads(raw_msg)
                            if payload.get("s") is not None:
                                seq_holder["seq"] = payload["s"]

                            event_type = payload.get("t")
                            data = payload.get("d")

                            if event_type == "READY":
                                my_user_id = data["user"]["id"]
                                username = data["user"]["username"]
                                print(f"[Account {idx}] Logged in as {username} ({my_user_id})")

                            elif event_type == "MESSAGE_CREATE":
                                author = data.get("author", {})
                                author_id = author.get("id")
                                author_username = author.get("username")
                                channel_id = data.get("channel_id")
                                message_id = data.get("id")
                                content = data.get("content", "")

                                if DEBUG:
                                    print(f"[Account {idx}] Msg from {author_username} ({author_id}) in ch {channel_id}: {content}")

                                if author_id == my_user_id:
                                    continue
                                if TARGET_CHANNEL_IDS and channel_id not in TARGET_CHANNEL_IDS:
                                    continue
                                if TARGET_USERNAME and author_username != TARGET_USERNAME:
                                    continue

                                for emoji in EMOJIS:
                                    await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
                                    await add_reaction(session, token, channel_id, message_id, emoji, idx)

                                if REPEAT_MESSAGES and content:
                                    await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
                                    await send_message(session, token, channel_id, content, idx)
                    finally:
                        hb_task.cancel()

            except Exception as e:
                print(f"[Account {idx}] Disconnected ({e}), reconnecting in 3s...")
                await asyncio.sleep(3)


async def main():
    start_http_server()
    await asyncio.gather(*(run_account(token, idx) for idx, token in enumerate(TOKENS, start=1)))


if __name__ == "__main__":
    asyncio.run(main())
