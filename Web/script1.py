"""Flask demo for the capstone cloud messaging project with Google Pub/Sub.

This version uses Google Cloud Pub/Sub for Milestone 3:
- Sender UI publishes payloads to a Pub/Sub topic
- Viewer UI reads from SQLite (populated by your separate consumer script)
"""

from __future__ import annotations

import json
import sys
import uuid
import time
from concurrent import futures
from pathlib import Path

from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for
from google.cloud import pubsub_v1

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common import storage

app = Flask(__name__)
app.secret_key = "milestone-3-demo-secret"
storage.init_db()

# Pub/Sub Configuration
PROJECT_ID = "project-2670a393-c614-47d3-88f"
TOPIC_ID = "MyTopic"

@app.context_processor
def inject_globals():
    return {
        "nav_summary": storage.get_summary(),
        "sort_options": storage.SORT_OPTIONS,
    }

ROLE_ACCESS = {
    "Publisher": ["sender"],
    "Viewer":    ["viewer"],
    "Admin":     ["sender", "viewer"],
}

@app.route("/", methods=["GET", "POST"])
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        role = request.form.get("role")
        if role in ROLE_ACCESS:
            session["display_name"] = role
            session["role"] = role
            if role == "Admin":
                landing = "admin"
            elif role == "Publisher":
                landing = "sender"
            else:
                landing = "viewer"
            flash(f"Logged in as {role}.", "success")
            return redirect(url_for(landing))
        flash("Invalid role selected.", "error")

    return render_template("home.html")

@app.route("/logout")
def logout():
    session.clear()
    flash("Logged out.", "success")
    return redirect(url_for("login"))

 # 2. Initialize Publisher and Path
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


@app.route("/sender", methods=["GET", "POST"])
def sender():
    role = session.get("role")
    if role not in ("Publisher", "Admin"):
        flash("Access denied. Publishers and Admins only.", "error")
        return redirect(url_for("login"))

    last_payload = None
    pubsub_id = None

    if request.method == "POST":
        content = (request.form.get("message_content") or "").strip()
        forced_message_id = (request.form.get("message_id") or "").strip() or None
        user_name = session.get("display_name") or "Guest"

        if not content:
            flash("Message content cannot be empty.", "error")
            return render_template("sender.html")

        try:
            # 1. Create the standardized payload
            payload = storage.create_payload(
                content=content,
                message_id=forced_message_id,
                producer_name="web-sender-ui",
                user_name=user_name,
            )

           

            # 3. Publish to Google Cloud
            data = json.dumps(payload).encode("utf-8")
            future = publisher.publish(topic_path, data)
            
            # Wait for ID to confirm it sent
            pubsub_id = future.result()
            last_payload = payload
            
            flash(f"Message published to Pub/Sub! (ID: {pubsub_id})", "success")
            
        except Exception as exc:
            flash(f"Pub/Sub Error: {str(exc)}", "error")

    return render_template(
        "sender.html",
        last_payload=last_payload,
        pubsub_id=pubsub_id,
    )

@app.route("/viewer")
def viewer():
    role = session.get("role")
    if role not in ("Viewer", "Admin"):
        flash("Access denied. Viewers and Admins only.", "error")
        return redirect(url_for("login"))

    sort_key = request.args.get("sort", "received_desc")
    duplicate_filter = request.args.get("dup", "all")
    search = (request.args.get("search") or "").strip()

    messages = storage.fetch_messages(
        sort_key=sort_key,
        duplicate_filter=duplicate_filter,
        search=search,
    )

    return render_template(
        "viewer.html",
        messages=messages,
        current_sort=sort_key,
        current_dup=duplicate_filter,
        current_search=search,
    )

@app.route("/receiver")
def receiver_alias():
    return redirect(url_for("viewer"))

@app.route("/admin")
def admin():
    role = session.get("role")
    if role != "Admin":
        flash("Access denied. Admins only.", "error")
        return redirect(url_for("login"))
    return render_template("admin.html")

NUM_LOAD_TEST_MESSAGES = 30

@app.route("/run-load-test", methods=["POST"])
def run_load_test():
    role = session.get("role")
    if role != "Admin":
        return jsonify({"error": "Access denied"}), 403

    log_lines = []
    log_lines.append(f"Starting load test: Sending {NUM_LOAD_TEST_MESSAGES} messages...")
    start_time = time.time()

    publish_futures = []
    results = {}

    def get_callback(msg_num):
        def callback(future):
            try:
                results[msg_num] = f"[{msg_num}] Published with ID: {future.result()}"
            except Exception as e:
                results[msg_num] = f"[{msg_num}] Failed to publish: {e}"
        return callback

    for i in range(1, NUM_LOAD_TEST_MESSAGES + 1):
        payload = {
            "message_id": str(uuid.uuid4()),
            "content": f"Load test message number {i}",
            "producer_name": "load-test-script",
            "user_name": "Admin",
            "timestamp": time.time(),
        }
        data = json.dumps(payload).encode("utf-8")
        future = publisher.publish(topic_path, data)
        future.add_done_callback(get_callback(i))
        publish_futures.append(future)

    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

    for i in range(1, NUM_LOAD_TEST_MESSAGES + 1):
        log_lines.append(results.get(i, f"[{i}] No result"))

    elapsed = time.time() - start_time
    log_lines.append(f"\nFinished! Sent {NUM_LOAD_TEST_MESSAGES} messages in {elapsed:.2f} seconds.")

    return jsonify({"output": "\n".join(log_lines)})

if __name__ == "__main__":
    app.run(debug=True)