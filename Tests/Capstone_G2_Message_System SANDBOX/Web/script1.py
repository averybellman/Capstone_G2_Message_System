"""Flask demo for the capstone cloud messaging project with Google Pub/Sub.

This version uses Google Cloud Pub/Sub for Milestone 3:
- Sender UI publishes payloads to a Pub/Sub topic
- Viewer UI reads from SQLite (populated by your separate consumer script)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from flask import Flask, flash, redirect, render_template, request, session, url_for
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

@app.route("/", methods=["GET", "POST"])
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        display_name = (request.form.get("display_name") or "").strip()
        if not display_name:
            flash("Enter a display name to continue (demo login only).", "error")
        else:
            session["display_name"] = display_name
            flash(f"Logged in as {display_name}.", "success")
            return redirect(url_for("sender"))

    return render_template("home.html")

@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    flash("Logged out.", "success")
    return redirect(url_for("login"))

 # 2. Initialize Publisher and Path
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


@app.route("/sender", methods=["GET", "POST"])
def sender():
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

@app.route("/about")
def about():
    return render_template("about.html")

if __name__ == "__main__":
    app.run(debug=True)