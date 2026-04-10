"""Redis state management for container tracking.

Stores container metadata so the API can list/query containers
without hitting the backend every time. Reconciles with backend on startup.
"""

import json
import logging
import os
import time
from typing import Optional

logger = logging.getLogger("runtime_api.state")


def _pid_alive(pid: int) -> bool:
    """Check if a PID is alive (not a zombie). Mirrors backends.process._pid_alive."""
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError, OSError):
        return False
    try:
        with open(f"/proc/{pid}/status", "r") as f:
            for line in f:
                if line.startswith("State:"):
                    state = line.split()[1]
                    return state not in ("Z", "X", "x")
    except (FileNotFoundError, OSError):
        return False
    return True

KEY_PREFIX = "runtime:container:"
STOPPED_TTL = 86400  # 24h for stopped/failed entries
CALLBACK_PREFIX = "runtime:callback:"


async def set_container(redis, name: str, data: dict):
    """Store container metadata."""
    data["updated_at"] = time.time()
    await redis.set(f"{KEY_PREFIX}{name}", json.dumps(data))
    logger.debug(f"State set: {name} -> {data.get('status')}")


async def get_container(redis, name: str) -> Optional[dict]:
    """Get container metadata."""
    raw = await redis.get(f"{KEY_PREFIX}{name}")
    if raw:
        return json.loads(raw)
    return None


async def delete_container(redis, name: str):
    """Remove container from state."""
    await redis.delete(f"{KEY_PREFIX}{name}")


async def set_stopped(redis, name: str, status: str = "stopped", exit_code: int = None):
    """Mark container as stopped/failed with TTL."""
    data = await get_container(redis, name) or {}
    data["status"] = status
    data["stopped_at"] = time.time()
    if exit_code is not None:
        data["exit_code"] = exit_code
    await redis.set(f"{KEY_PREFIX}{name}", json.dumps(data), ex=STOPPED_TTL)


async def list_containers(redis, user_id: str = None, profile: str = None) -> list[dict]:
    """List all tracked containers, optionally filtered.

    Performs synchronous PID liveness checks on entries marked 'running' to
    eliminate the race where a dead process is still in the registry because
    the periodic reaper hasn't swept it yet.
    """
    results = []
    async for key in redis.scan_iter(f"{KEY_PREFIX}*"):
        raw = await redis.get(key)
        if not raw:
            continue
        data = json.loads(raw)
        if user_id and data.get("user_id") != user_id:
            continue
        if profile and data.get("profile") != profile:
            continue

        name = key.removeprefix(KEY_PREFIX) if isinstance(key, str) else key.decode().removeprefix(KEY_PREFIX)

        # Sweep dead processes still marked as running
        if data.get("status") == "running":
            pid = data.get("pid")
            if pid and not _pid_alive(pid):
                logger.warning(
                    f"list_containers: process {name} (PID={pid}) is dead "
                    f"but still marked 'running'; sweeping it now."
                )
                await set_stopped(redis, name, status="stopped", exit_code=0)
                # Re-fetch the updated record so callers see the corrected status
                raw = await redis.get(key)
                if raw:
                    data = json.loads(raw)

        data["name"] = name
        results.append(data)
    return results



async def count_user_containers(
    redis, user_id: str, profile: str = None
) -> int:
    """Count running containers for a user, optionally filtered by profile.

    Performs synchronous PID liveness checks on entries marked 'running' to
    eliminate the race condition where a dead process is still in the registry
    because the periodic reaper hasn't swept it yet.
    """
    count = 0
    async for key in redis.scan_iter(f"{KEY_PREFIX}*"):
        raw = await redis.get(key)
        if not raw:
            continue
        data = json.loads(raw)
        if data.get("user_id") != user_id:
            continue
        if data.get("status") != "running":
            continue
        if profile and data.get("profile") != profile:
            continue

        # Synchronously verify the PID is actually alive — fixes the race
        # where the reaper hasn't yet marked the process as exited.
        pid = data.get("pid")
        if pid and not _pid_alive(pid):
            name = key.removeprefix(KEY_PREFIX) if isinstance(key, str) else key.decode().removeprefix(KEY_PREFIX)
            logger.warning(
                f"count_user_containers: process {name} (PID={pid}) is dead "
                f"but still marked 'running'; sweeping it now."
            )
            await set_stopped(redis, name, status="stopped", exit_code=0)
            continue

        count += 1
    return count


async def store_pending_callback(redis, name: str, callback_data: dict, ttl: int = 3600):
    """Store a pending callback for retry."""
    await redis.set(
        f"{CALLBACK_PREFIX}{name}",
        json.dumps(callback_data),
        ex=ttl,
    )


async def get_pending_callback(redis, name: str) -> Optional[dict]:
    """Get a pending callback."""
    raw = await redis.get(f"{CALLBACK_PREFIX}{name}")
    if raw:
        return json.loads(raw)
    return None


async def delete_pending_callback(redis, name: str):
    """Remove a pending callback after successful delivery."""
    await redis.delete(f"{CALLBACK_PREFIX}{name}")
