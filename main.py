import asyncio
import base64
import json
import os
import random
import urllib.parse
import zlib
import aiohttp
import websockets

# ================= CONFIGURATION =================
# Option A (Recommended): Set DISCORD_TOKENS as a comma-separated env var in Render.
# Option B: Pack your tokens locally once and paste (b85_payload, seed) tuples below.
# Helper to pack locally without putting raw tokens in Git:
#   python -c "import base64, zlib; p=lambda t,s: base64.b85encode(bytes(reversed(bytes(b^((s*1315423911+2654435761)&0xFFFFFFFF).to_bytes(4,'big')[i%4] for i,b in enumerate(zlib.compress(t.encode(),9)))))).decode(); print((p(input('token: '), 101), 101))"
_ENCODED_TOKENS: list[tuple[str, int]] = []

TARGET_CHANNEL_IDS: list[str] = []
TARGET_USERNAME = "iloves3xworkers"
REPEAT_MESSAGES = True
EMOJIS = ["🤣"]

MIN_DELAY = 0.6
MAX_DELAY = 1.5
DEBUG = False

GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"
API_BASE = "https://discord.com/api/v10"
# =================================================


def _pack_token(raw_token: str, seed: int) -> str:
    compressed = zlib.compress(raw_token.encode("utf-8"), level=9)
    mask = ((seed * 1315423911 + 2654435761) & 0xFFFFFFFF).to_bytes(4, "big")
    xored = bytes(b ^ mask[i % len(mask)] for i, b in enumerate(compressed))
    reordered = bytes(reversed(xored))
    return base64.b85encode(reordered).decode("ascii")


def _unpack_token(blob: str, seed: int) -> str:
    reordered = base64.b85decode(blob.encode("ascii"))
    xored = bytes(reversed(reordered))
    mask = ((seed * 1315423911 + 2654435761) & 0xFFFFFFFF).to_bytes(4, "big")
    compressed = bytes(b ^ mask[i % len(mask)] for i, b in enumerate(xored))
    return zlib.decompress(compressed).decode("utf-8")


def get_tokens() -> list[str]:
    env_tokens = [t.strip() for t in os.getenv("DISCORD_TOKENS", "").split(",") if t.strip()]
    if env_tokens:
        return env_tokens
    return [_unpack_token(blob, seed) for blob, seed in _ENCODED_TOKENS]


def build_headers(token: str) -> dict:
    return {
        "Authorization": token,
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }


async def add_reaction(
    session: aiohttp.ClientSession,
    token: str,
    channel_id: str,
    message_id: str,
    emoji: str,
    idx: int,
):
    encoded_emoji = urllib.parse.quote(emoji)
    url = f"{API_BASE}/channels/{channel_id}/messages/{message_id}/reactions/{encoded_emoji}/@me"
    headers = build_headers(token)
    try:
        async with session.put(url, headers=headers) as resp:
            if resp.status in (200, 204):
                print(f"[Account {idx}] Reacted with {emoji} in channel {channel_id}")
            elif resp.status == 429:
                data = await resp.json()
                retry_after = float(data.get("retry_after", 1.5))
                print(f"[Account {idx}] Reaction rate limited (429), retrying after {retry_after}s")
                await asyncio.sleep(retry_after)
                async with session.put(url, headers=headers) as retry_resp:
                    if retry_resp.status in (200, 204):
                        print(f"[Account {idx}] Reacted with {emoji} after retry in {channel_id}")
                    else:
                        print(f"[Account {idx}] Reaction retry failed ({retry_resp.status}): {await retry_resp.text()}")
            else:
                print(f"[Account {idx}] Reaction failed ({resp.status}): {await resp.text()}")
    except aiohttp.ClientError as e:
        print(f"[Account {idx}] Reaction network error: {e}")


async def send_message(
    session: aiohttp.ClientSession,
    token: str,
    channel_id: str,
    content: str,
    idx: int,
):
    url = f"{API_BASE}/channels/{channel_id}/messages"
    headers = build_headers(token)
    payload = {"content": content}
    try:
        async with session.post(url, headers=headers, json=payload) as resp:
            if resp.status in (200, 201):
                print(f"[Account {idx}] Sent message in channel {channel_id}: {content}")
            elif resp.status == 429:
                data = await resp.json()
                retry_after = float(data.get("retry_after", 1.5))
                print(f"[Account {idx}] Message rate limited (429), retrying after {retry_after}s")
                await asyncio.sleep(retry_after)
                async with session.post(url, headers=headers, json=payload) as retry_resp:
                    if retry_resp.status in (200, 201):
                        print(f"[Account {idx}] Sent message after retry in channel {channel_id}: {content}")
                    else:
                        print(f"[Account {idx}] Message retry failed ({retry_resp.status}): {await retry_resp.text()}")
            else:
                print(f"[Account {idx}] Message failed ({resp.status}): {await resp.text()}")
    except aiohttp.ClientError as e:
        print(f"[Account {idx}] Message network error: {e}")


async def heartbeat_loop(ws, interval: float, seq_holder: dict):
    while True:
        await asyncio.sleep(interval)
        await ws.send(json.dumps({"op": 1, "d": seq_holder["seq"]}))


async def run_account(token: str, idx: int):
    seq_holder = {"seq": None}
    my_user_id = None

    connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
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
                print(f"[Account {idx}] Gateway disconnected ({e}), reconnecting in 3s...")
                await asyncio.sleep(3)


async def main():
    tokens = get_tokens()
    if not tokens:
        raise RuntimeError("No tokens found in DISCORD_TOKENS or _ENCODED_TOKENS.")
    await asyncio.gather(*(run_account(token, idx) for idx, token in enumerate(tokens, start=1)))


if __name__ == "__main__":
    asyncio.run(main())