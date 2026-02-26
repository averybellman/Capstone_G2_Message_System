from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common import storage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Standalone consumer stub for Milestone 3. Reads messages from the local outbox "
            "and saves them to SQLite with duplicate detection before insert."
        )
    )
    parser.add_argument(
        "--consumer-name",
        default="consumer-stub",
        help="Consumer name used for offset tracking",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="Max number of messages to process this run (0 = all pending)",
    )
    parser.add_argument(
        "--reset-offset",
        action="store_true",
        help="Reset this consumer's outbox read offset before processing",
    )
    parser.add_argument(
        "--status-only",
        action="store_true",
        help="Show files/offset status and exit without processing",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    storage.init_db()

    if args.reset_offset:
        storage.set_consumer_offset(args.consumer_name, 0)
        print(f"Reset offset for consumer '{args.consumer_name}' to line 0.")

    current_offset = storage.get_consumer_offset(args.consumer_name)
    summary = storage.get_summary()
    print(f"Outbox: {summary['outbox_path']}")
    print(f"Database: {summary['db_path']}")
    print(f"Current offset ({args.consumer_name}): line {current_offset}")
    print(f"Outbox queued messages (all-time lines): {summary['outbox_count']}")
    print(f"DB rows before run: {summary['db_total']} (duplicates: {summary['db_duplicates']})")

    if args.status_only:
        return 0

    processed = 0
    duplicates = 0
    latest_offset = current_offset

    try:
        for line_number, envelope in storage.iter_outbox_from_line(current_offset):
            if args.batch_size and processed >= args.batch_size:
                break

            payload = envelope.get("payload") or {}
            result = storage.insert_message_from_payload(
                payload,
                transport_id=envelope.get("transport_id"),
                source=envelope.get("source"),
                consumer_name=args.consumer_name,
            )
            latest_offset = line_number
            storage.set_consumer_offset(args.consumer_name, latest_offset)

            processed += 1
            if result["is_duplicate"]:
                duplicates += 1

            status = "DUPLICATE" if result["is_duplicate"] else "NEW"
            print(
                f"Processed line {line_number}: [{status}] "
                f"message_id={result['message_id']} db_id={result['id']}"
            )
    except ValueError as exc:
        print(f"Consumer stopped due to invalid outbox message: {exc}", file=sys.stderr)
        return 1

    if processed == 0:
        print("No new messages were available for this consumer.")
    else:
        print(
            f"Processed {processed} message(s). Duplicate rows flagged this run: {duplicates}. "
            f"New offset: line {latest_offset}."
        )

    summary_after = storage.get_summary()
    print(
        f"DB rows after run: {summary_after['db_total']} "
        f"(duplicates: {summary_after['db_duplicates']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
