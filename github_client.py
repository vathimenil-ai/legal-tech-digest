"""
github_client.py — Read and write files in the GitHub repo via the REST API.

GitHub Contents API:
  GET  /repos/{owner}/{repo}/contents/{path}  → read file (base64 encoded)
  PUT  /repos/{owner}/{repo}/contents/{path}  → create or update file
"""
import base64
import json
import logging
from typing import Any

import requests

import config

logger = logging.getLogger(__name__)

_OWNER, _REPO = config.GITHUB_REPO.split("/", 1)
_BASE = f"https://api.github.com/repos/{_OWNER}/{_REPO}/contents"
_HEADERS = {
    "Authorization": f"Bearer {config.GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}


# ── Read ───────────────────────────────────────────────────────────────────────

def read_file(path: str) -> tuple[str, str]:
    """
    Read a file from the repo.
    Returns (content_str, sha).
    Raises FileNotFoundError if the path does not exist.
    """
    url = f"{_BASE}/{path}"
    resp = requests.get(url, headers=_HEADERS, timeout=30)
    if resp.status_code == 404:
        raise FileNotFoundError(f"GitHub path not found: {path}")
    resp.raise_for_status()
    data = resp.json()
    content = base64.b64decode(data["content"]).decode("utf-8")
    return content, data["sha"]


def read_file_or_none(path: str) -> tuple[str | None, str | None]:
    """Like read_file but returns (None, None) if file does not exist."""
    try:
        return read_file(path)
    except FileNotFoundError:
        return None, None


# ── Write ──────────────────────────────────────────────────────────────────────

def write_file(path: str, content: str, commit_message: str, sha: str | None = None) -> dict[str, Any]:
    """
    Create or update a file in the repo.
    Pass sha when updating an existing file (required by the GitHub API).
    Returns the API response dict.
    """
    encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
    payload: dict[str, Any] = {
        "message": commit_message,
        "content": encoded,
    }
    if sha:
        payload["sha"] = sha

    url = f"{_BASE}/{path}"
    resp = requests.put(url, headers=_HEADERS, json=payload, timeout=30)
    resp.raise_for_status()
    logger.info("GitHub write OK: %s (%s)", path, resp.status_code)
    return resp.json()


def upsert_file(path: str, content: str, commit_message: str) -> dict[str, Any]:
    """
    Write a file whether it exists or not (handles sha lookup automatically).
    """
    _, sha = read_file_or_none(path)
    return write_file(path, content, commit_message, sha=sha)


# ── JSON helpers ───────────────────────────────────────────────────────────────

def read_json(path: str) -> tuple[Any, str]:
    """Read a JSON file. Returns (parsed_object, sha)."""
    content, sha = read_file(path)
    return json.loads(content), sha


def write_json(path: str, data: Any, commit_message: str) -> dict[str, Any]:
    """Serialise data as JSON and upsert to the repo."""
    content = json.dumps(data, indent=2, ensure_ascii=False)
    return upsert_file(path, content, commit_message)


# ── List directory ─────────────────────────────────────────────────────────────

def list_dir(path: str) -> list[dict[str, Any]]:
    """List contents of a directory in the repo. Returns raw GitHub API entries."""
    url = f"{_BASE}/{path}"
    resp = requests.get(url, headers=_HEADERS, timeout=30)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(f"Repo: {config.GITHUB_REPO}")
    entries = list_dir("")
    for e in entries[:10]:
        print(f"  {e['type']:4s}  {e['name']}")
