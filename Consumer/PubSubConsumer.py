from __future__ import annotations

import argparse
import sys
import json
from pathlib import Path
from google.cloud import pubsub_v1

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common import storage

# Pub/Sub Configuration
PROJECT_ID = "project-2670a393-c614-47d3-88f"
SUBSCRIPTION_ID = "capstone_sub"

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pub/Sub Consumer for Milestone 3. Listens to Google Pub/Sub "
            "and saves messages to SQLite with duplicate detection."
        )
    )
    parser.add_argument(
        "--consumer-name",
        default="pubsub-consumer",
        help="Consumer name used for identification in the database",
    )
    return parser.parse_args()

def process_pubsub_message(message: pubsub_v1.subscriber.message.Message, consumer_name: str):
    """
    Callback function to process incoming Pub/Sub messages.
    """
    try:
        # Decode the message data
        data_str = message.data.decode("utf-8")
        payload = json.loads(data_str)
        
        print(f"Received message: {data_str}")

        # Insert into SQLite using your existing storage logic
        # Note: We use attributes or generated IDs since we aren't reading line numbers anymore
        result = storage.insert_message_from_payload(
            payload,
            transport_id=message.message_id,
            source="google_pubsub",
            consumer_name=consumer_name,
        )

        status = "DUPLICATE" if result["is_duplicate"] else "NEW"
        print(
            f"Processed Pub/Sub ID {message.message_id}: [{status}] "
            f"db_id={result['id']}"
        )

        # Acknowledge the message so it isn't resent
        message.ack()

    except Exception as e:
        print(f"Failed to process message: {e}", file=sys.stderr)
        # Nack tells Pub/Sub to retry the message later
        message.nack()

def main() -> int:
    args = parse_args()
    storage.init_db()

    # Initial Status
    summary = storage.get_summary()
    print(f"Database: {summary['db_path']}")
    print(f"DB rows before run: {summary['db_total']} (duplicates: {summary['db_duplicates']})")

    # Initialize Pub/Sub Subscriber
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    print(f"Listening for messages on {subscription_path}...")
    
    # Start the subscriber
    streaming_pull_future = subscriber.subscribe(
        subscription_path, 
        callback=lambda msg: process_pubsub_message(msg, args.consumer_name)
    )

    try:
        # Keep the main thread alive while the subscriber runs in the background
        # You can use a timeout or just wait for KeyboardInterrupt
        streaming_pull_future.result() 
    except KeyboardInterrupt:
        print("\nStopping consumer...")
        streaming_pull_future.cancel()
    except Exception as e:
        print(f"Streaming pull failed: {e}", file=sys.stderr)
        streaming_pull_future.cancel()
        return 1
    finally:
        subscriber.close()

    summary_after = storage.get_summary()
    print(
        f"DB rows after run: {summary_after['db_total']} "
        f"(duplicates: {summary_after['db_duplicates']})"
    )
    print("Stopped.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())