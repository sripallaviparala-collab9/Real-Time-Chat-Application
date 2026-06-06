#!/usr/bin/env python3
"""
Real-Time Chat Server using Python WebSocket
=============================================
Handles multiple clients, broadcasts messages, tracks users,
and supports system events (join/leave notifications).
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Set

import websockets
from websockets.server import WebSocketServerProtocol

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("chat-server")

# ── State ─────────────────────────────────────────────────────────────────────
# Maps each connected WebSocket → username
clients: Dict[WebSocketServerProtocol, str] = {}

# ── Helpers ───────────────────────────────────────────────────────────────────

def now_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def active_users() -> list[str]:
    """Sorted list of currently connected usernames."""
    return sorted(clients.values())


async def broadcast(payload: dict, exclude: WebSocketServerProtocol | None = None) -> None:
    """Send *payload* as JSON to every connected client except *exclude*."""
    if not clients:
        return
    message = json.dumps(payload)
    recipients = [ws for ws in clients if ws is not exclude]
    if recipients:
        await asyncio.gather(*[ws.send(message) for ws in recipients], return_exceptions=True)


async def send_to(ws: WebSocketServerProtocol, payload: dict) -> None:
    """Send *payload* as JSON to a single client."""
    await ws.send(json.dumps(payload))


# ── Connection lifecycle ───────────────────────────────────────────────────────

async def register(ws: WebSocketServerProtocol, username: str) -> None:
    """Register a new client and announce their arrival."""
    clients[ws] = username
    log.info("+ %-16s  total=%d", username, len(clients))

    # Welcome the newcomer with the current user list
    await send_to(ws, {
        "type": "welcome",
        "username": username,
        "users": active_users(),
        "timestamp": now_iso(),
    })

    # Tell everyone else someone joined
    await broadcast({
        "type": "system",
        "event": "join",
        "username": username,
        "users": active_users(),
        "text": f"{username} joined the chat",
        "timestamp": now_iso(),
    }, exclude=ws)


async def unregister(ws: WebSocketServerProtocol) -> None:
    """Remove a client and announce their departure."""
    username = clients.pop(ws, "unknown")
    log.info("- %-16s  total=%d", username, len(clients))

    await broadcast({
        "type": "system",
        "event": "leave",
        "username": username,
        "users": active_users(),
        "text": f"{username} left the chat",
        "timestamp": now_iso(),
    })


# ── Message handling ───────────────────────────────────────────────────────────

async def handle_chat(ws: WebSocketServerProtocol, data: dict) -> None:
    """Broadcast a chat message from *ws* to all other clients."""
    username = clients[ws]
    text = str(data.get("text", "")).strip()
    if not text:
        return

    log.info("  [%s] %s", username, text[:80])

    await broadcast({
        "type": "message",
        "username": username,
        "text": text,
        "timestamp": now_iso(),
    })


async def handle_typing(ws: WebSocketServerProtocol, data: dict) -> None:
    """Relay typing indicator to other clients."""
    username = clients[ws]
    await broadcast({
        "type": "typing",
        "username": username,
        "active": bool(data.get("active", False)),
    }, exclude=ws)


# ── Main handler ──────────────────────────────────────────────────────────────

async def handler(ws: WebSocketServerProtocol) -> None:
    """
    Per-connection coroutine:
    1. Wait for an 'auth' message with the chosen username.
    2. Register the client.
    3. Relay messages until the connection closes.
    """
    # Step 1 — authentication / username handshake
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=10)
        auth = json.loads(raw)
        if auth.get("type") != "auth" or not auth.get("username"):
            await send_to(ws, {"type": "error", "text": "Expected {type:'auth', username:'...'}"})
            return

        username = str(auth["username"]).strip()[:20]
        if not username:
            await send_to(ws, {"type": "error", "text": "Username cannot be empty"})
            return

        # Ensure uniqueness
        if username in clients.values():
            await send_to(ws, {"type": "error", "text": f"'{username}' is already taken"})
            return

    except (asyncio.TimeoutError, json.JSONDecodeError, websockets.exceptions.ConnectionClosed):
        return

    # Step 2 — register
    await register(ws, username)

    # Step 3 — message loop
    try:
        async for raw in ws:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = data.get("type")
            if msg_type == "message":
                await handle_chat(ws, data)
            elif msg_type == "typing":
                await handle_typing(ws, data)
            else:
                log.warning("Unknown message type from %s: %s", username, msg_type)

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await unregister(ws)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    host, port = "localhost", 8765
    log.info("WebSocket chat server starting on ws://%s:%d", host, port)

    async with websockets.serve(handler, host, port):
        log.info("Server ready — waiting for connections …")
        await asyncio.Future()   # run forever


if __name__ == "__main__":
    asyncio.run(main())