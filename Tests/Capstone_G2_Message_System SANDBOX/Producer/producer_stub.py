from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from google.cloud import pubsub_v1

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common import storage

# Pub/Sub Configuration
PROJECT_ID = "project-2670a393-c614-47d3-88f"
TOPIC_ID = "capstone_topic" # Ensure this matches the topic your subscription is tied to

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pub/Sub Producer for Milestone 3. "
            "Creates a message payload and publishes it to Google Pub/Sub."
        )
    )
    parser.add_argument("content", nargs="?", help="Message content to publish")
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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    content = (args.content or "").strip()
    if not content:
        try:
            content = input("Enter message content: ").strip()
        except (EOFError, KeyboardInterrupt):
            content = ""

    if not content:
        print("Error: message content is required.", file=sys.stderr)
        return 1

    # 1. Create the standardized payload using your existing logic
    payload = storage.create_payload(
        content=content,
        message_id=args.message_id,
        producer_name=args.producer_name,
        user_name=args.user_name,
    )

    # 2. Initialize the Publisher Client
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    # 3. Serialize and Publish
    # Data must be sent as bytes
    data = json.dumps(payload).encode("utf-8")
    
    print(f"Publishing message to {topic_path}...")
    
    try:
        future = publisher.publish(topic_path, data)
        # Wait for the publish result to ensure it succeeded
        pubsub_id = future.result()
        
        print("Successfully published to Google Pub/Sub.")
        print(f"Pub/Sub Message ID: {pubsub_id}")
        print(json.dumps({"payload": payload}, indent=2))
        
    except Exception as e:
        print(f"Failed to publish message: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())