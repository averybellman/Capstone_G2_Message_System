from google.cloud import pubsub_v1
import sys

project_id = "project-2670a393-c614-47d3-88f"
subscription_id = "capstone_sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    print(f"Received message: {message.data.decode('utf-8')}")
    
   
    message.ack()

print("Listening for messages... Press Ctrl+C to exit.")

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

input("Press Enter to exit...\n")

print ("close and cleanup!")
streaming_pull_future.cancel()
subscriber.close()
print("Stopped.")

