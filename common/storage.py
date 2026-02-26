from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "messages.db"
OUTBOX_PATH = DATA_DIR / "local_outbox.jsonl"
CONSUMER_STATE_PATH = DATA_DIR / "consumer_state.json"

SORT_OPTIONS = {
    "received_desc": ("Newest Received", "received_at DESC, id DESC"),
    "received_asc": ("Oldest Received", "received_at ASC, id ASC"),
    "published_desc": ("Newest Published", "published_at DESC, id DESC"),
    "message_id_asc": ("Message ID (A-Z)", "message_id ASC, id ASC"),
    "duplicate_first": ("Duplicates First", "is_duplicate DESC, received_at DESC, id DESC"),
}


def utc_now_iso() -> str:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def init_db() -> None:
    ensure_data_dir()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL,
                message_content TEXT NOT NULL,
                published_at TEXT NOT NULL,
                received_at TEXT NOT NULL,
                is_duplicate INTEGER NOT NULL CHECK (is_duplicate IN (0, 1)),
                transport_id TEXT,
                source TEXT,
                producer_name TEXT,
                raw_payload TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_message_id ON messages(message_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_received_at ON messages(received_at)"
        )
        conn.commit()


def create_payload(
    content: str,
    message_id: str | None = None,
    producer_name: str = "local-producer",
    user_name: str | None = None,
) -> Dict[str, Any]:
    clean_content = (content or "").strip()
    if not clean_content:
        raise ValueError("Message content is required.")

    payload: Dict[str, Any] = {
        "message_id": (message_id or str(uuid.uuid4())).strip(),
        "content": clean_content,
        "published_at": utc_now_iso(),
        "producer_name": producer_name,
    }
    if user_name:
        payload["user_name"] = user_name.strip()

    if not payload["message_id"]:
        raise ValueError("Message ID cannot be blank.")

    return payload


def append_outbox_message(payload: Dict[str, Any], source: str = "local-script") -> Dict[str, Any]:
    ensure_data_dir()
    envelope = {
        "transport_id": str(uuid.uuid4()),
        "queued_at": utc_now_iso(),
        "source": source,
        "payload": payload,
    }
    with OUTBOX_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(envelope, sort_keys=True) + "\n")
    return envelope


def iter_outbox_from_line(start_line: int = 0) -> Iterator[Tuple[int, Dict[str, Any]]]:
    if not OUTBOX_PATH.exists():
        return

    with OUTBOX_PATH.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if line_number <= start_line:
                continue
            stripped = line.strip()
            if not stripped:
                continue
            try:
                envelope = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON in outbox at line {line_number}: {exc}") from exc
            yield line_number, envelope


def _load_consumer_state_map() -> Dict[str, int]:
    ensure_data_dir()
    if not CONSUMER_STATE_PATH.exists():
        return {}

    try:
        with CONSUMER_STATE_PATH.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (json.JSONDecodeError, OSError):
        return {}

    if not isinstance(data, dict):
        return {}

    result: Dict[str, int] = {}
    for key, value in data.items():
        try:
            result[str(key)] = int(value)
        except (TypeError, ValueError):
            continue
    return result


def get_consumer_offset(consumer_name: str) -> int:
    return _load_consumer_state_map().get(consumer_name, 0)


def set_consumer_offset(consumer_name: str, last_line: int) -> None:
    state = _load_consumer_state_map()
    state[consumer_name] = max(0, int(last_line))
    ensure_data_dir()
    with CONSUMER_STATE_PATH.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)


def insert_message_from_payload(
    payload: Dict[str, Any],
    *,
    transport_id: str | None = None,
    source: str | None = None,
    consumer_name: str = "consumer-stub",
) -> Dict[str, Any]:
    init_db()

    message_id = str(payload.get("message_id", "")).strip()
    content = str(payload.get("content", "")).strip()
    published_at = str(payload.get("published_at", "")).strip() or utc_now_iso()
    producer_name = str(payload.get("producer_name", "")).strip() or "unknown-producer"
    if consumer_name:
        producer_name = producer_name

    if not message_id:
        raise ValueError("Payload missing required field: message_id")
    if not content:
        raise ValueError("Payload missing required field: content")

    received_at = utc_now_iso()
    raw_payload = json.dumps(payload, sort_keys=True)

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        # Duplicate detection happens before insert to mirror the project requirement.
        duplicate_exists = (
            cursor.execute(
                "SELECT 1 FROM messages WHERE message_id = ? LIMIT 1",
                (message_id,),
            ).fetchone()
            is not None
        )
        is_duplicate = 1 if duplicate_exists else 0

        cursor.execute(
            """
            INSERT INTO messages (
                message_id,
                message_content,
                published_at,
                received_at,
                is_duplicate,
                transport_id,
                source,
                producer_name,
                raw_payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                content,
                published_at,
                received_at,
                is_duplicate,
                transport_id,
                source,
                producer_name,
                raw_payload,
            ),
        )
        row_id = cursor.lastrowid
        conn.commit()

    return {
        "id": row_id,
        "message_id": message_id,
        "message_content": content,
        "published_at": published_at,
        "received_at": received_at,
        "is_duplicate": bool(is_duplicate),
        "transport_id": transport_id,
        "source": source,
        "consumer_name": consumer_name,
    }


def fetch_messages(
    *,
    sort_key: str = "received_desc",
    duplicate_filter: str = "all",
    search: str = "",
) -> list[Dict[str, Any]]:
    init_db()
    sort_sql = SORT_OPTIONS.get(sort_key, SORT_OPTIONS["received_desc"])[1]

    conditions: list[str] = []
    params: list[Any] = []

    if duplicate_filter == "only":
        conditions.append("is_duplicate = 1")
    elif duplicate_filter == "exclude":
        conditions.append("is_duplicate = 0")

    search_term = (search or "").strip()
    if search_term:
        like_value = f"%{search_term}%"
        conditions.append("(message_id LIKE ? OR message_content LIKE ? OR COALESCE(source, '') LIKE ?)")
        params.extend([like_value, like_value, like_value])

    sql = (
        "SELECT id, message_id, message_content, published_at, received_at, is_duplicate, "
        "transport_id, source, producer_name FROM messages"
    )
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += f" ORDER BY {sort_sql}"

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()

    messages: list[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["is_duplicate"] = bool(item["is_duplicate"])
        messages.append(item)
    return messages


def get_summary() -> Dict[str, Any]:
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        total, duplicates = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(is_duplicate), 0) FROM messages"
        ).fetchone()

    return {
        "db_path": str(DB_PATH),
        "outbox_path": str(OUTBOX_PATH),
        "outbox_count": count_outbox_lines(),
        "db_total": int(total or 0),
        "db_duplicates": int(duplicates or 0),
    }


def count_outbox_lines() -> int:
    if not OUTBOX_PATH.exists():
        return 0
    with OUTBOX_PATH.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())
