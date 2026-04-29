"""Flask demo shell for the capstone cloud messaging project.

This version is intentionally local-only for Milestone 3:
- Sender UI writes payloads to a local outbox file (simulating a producer)
- Consumer is a separate script that reads the outbox and writes to SQLite
- Viewer UI reads from SQLite and supports sorting/filtering/highlighting duplicates
"""

from __future__ import annotations

import sys
from pathlib import Path

from flask import Flask, flash, redirect, render_template, request, session, url_for

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common import storage

app = Flask(__name__)
app.secret_key = "milestone-3-demo-secret"
storage.init_db()


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


@app.route("/sender", methods=["GET", "POST"])
def sender():
    last_envelope = None
    last_payload = None

    if request.method == "POST":
        content = request.form.get("message_content") or ""
        forced_message_id = (request.form.get("message_id") or "").strip() or None
        user_name = session.get("display_name") or "Guest"

        try:
            payload = storage.create_payload(
                content=content,
                message_id=forced_message_id,
                producer_name="web-sender-ui",
                user_name=user_name,
            )
            envelope = storage.append_outbox_message(payload, source="web-sender")
        except ValueError as exc:
            flash(str(exc), "error")
        else:
            last_payload = payload
            last_envelope = envelope
            flash(
                "Message added to the local outbox. Run the consumer stub to move it into the database.",
                "success",
            )

    return render_template(
        "sender.html",
        last_payload=last_payload,
        last_envelope=last_envelope,
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
