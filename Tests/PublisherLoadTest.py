import json
import uuid
import time
from concurrent import futures
from google.cloud import pubsub_v1

# --- CONFIGURATION ---
PROJECT_ID = "project-2670a393-c614-47d3-88f"
TOPIC_ID = "MyTopic"
NUM_MESSAGES = 30

# Initialize Publisher once (Global)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def get_callback(future, msg_num):
    """Callback to track when each message is successfully received by Google."""
    def callback(future):
        try:
            print(f"[{msg_num}] Published with ID: {future.result()}")
        except Exception as e:
            print(f"[{msg_num}] Failed to publish: {e}")
    return callback

def run_load_test():
    print(f"🚀 Starting load test: Sending {NUM_MESSAGES} messages...")
    start_time = time.time()
    
    publish_futures = []

    for i in range(1, NUM_MESSAGES + 1):
        # Autogenerate payload
        payload = {
            "message_id": str(uuid.uuid4()),
            "content": f"Load test message number {i}",
            "producer_name": "load-test-script",
            "user_name": "TestRunner",
            "timestamp": time.time()
        }
        
        data = json.dumps(payload).encode("utf-8")
        
        # Publish asynchronously
        future = publisher.publish(topic_path, data)
        future.add_done_callback(get_callback(future, i))
        publish_futures.append(future)

    # Wait for all messages to finish publishing
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
    
    end_time = time.time()
    print(f"\n✅ Finished! Sent {NUM_MESSAGES} messages in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    run_load_test()