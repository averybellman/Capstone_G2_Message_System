from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common import storage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Standalone producer stub for Milestone 3. "
            "Creates a message payload and writes it to a local outbox file (no Pub/Sub yet)."
        )
    )
    parser.add_argument("content", nargs="?", help="Message content to publish to the local outbox")
    parser.add_argument(
        "--message-id",
        dest="message_id",
        help="Optional message ID. Reuse the same ID across runs to simulate duplicates.",
    )
    parser.add_argument(
        "--producer-name",
        default="producer-stub",
        help="Name of the producer component written into the payload",
    )
    parser.add_argument(
        "--user-name",
        default=None,
        help="Optional user/display name stored in the payload",
    )
    parser.add_argument(
        "--source",
        default="producer-script",
        help="Source label stored in the local outbox envelope",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    content = (args.content or "").strip()
    if not content:
        try:
            content = input("Enter message content: ").strip()
        except EOFError:
            content = ""

    if not content:
        print("Error: message content is required.", file=sys.stderr)
        return 1

    payload = storage.create_payload(
        content=content,
        message_id=args.message_id,
        producer_name=args.producer_name,
        user_name=args.user_name,
    )
    envelope = storage.append_outbox_message(payload, source=args.source)
    summary = storage.get_summary()

    print("Producer stub queued a message (local outbox only; no Pub/Sub yet).")
    print(json.dumps({"envelope": envelope, "payload": payload}, indent=2))
    print(f"Outbox file: {summary['outbox_path']}")
    print(f"Outbox messages waiting: {summary['outbox_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
