# Capstone_G2_Message_System

Milestone 3 demo shell for IS Capstone Project 1 (Cloud Messaging Provider).

## What this project is about
This project teaches the publish-subscribe messaging model using Google Pub/Sub (later milestone).
Your final system will:
- Publish messages from a producer UI to a Pub/Sub topic
- Read messages from a subscription using a consumer component
- Detect duplicates before DB insert using a unique `message_id` in the payload
- Save messages to a database
- Display messages in a separate viewer UI with sorting/filtering and duplicate highlighting

## Current Milestone 3 shell (local demo)
This repo now includes a local-only demo shell so your team can show progress before Pub/Sub integration:
- `Web/script1.py` Flask app with separate Login, Sender, and Receiver/Sorter pages
- `Producer/producer_stub.py` standalone producer stub (writes to local outbox file)
- `Consumer/consumer_stub.py` standalone consumer stub (reads local outbox and writes to SQLite)
- `common/storage.py` shared local storage helpers (SQLite + outbox simulation)

This simulates the flow:
`Sender UI / Producer Stub -> local outbox file -> Consumer Stub -> SQLite DB -> Viewer UI`

## Local setup (macOS/Linux example)
1. Create a virtual environment and install Flask:
   - `python3 -m venv .venv`
   - `source .venv/bin/activate`
   - `pip install flask`
2. Run the web app:
   - `python3 Web/script1.py`
3. In a second terminal, queue messages (producer stub):
   - `python3 Producer/producer_stub.py "hello world"`
   - `python3 Producer/producer_stub.py "duplicate test" --message-id DEMO-001`
   - `python3 Producer/producer_stub.py "duplicate test again" --message-id DEMO-001`
4. Consume and save to DB:
   - `python3 Consumer/consumer_stub.py`
5. Open the viewer page and filter duplicates.

## Google Cloud setup note (later)
To set up your local dev environment for Google Cloud authentication, follow:
https://docs.cloud.google.com/docs/authentication/set-up-adc-local-dev-environment

After `gcloud` is installed and authenticated, you can run:
- `gcloud auth application-default login`

(Windows example from original notes)
- `setx GOOGLE_APPLICATION_CREDENTIALS C:\Users\<your-user>\AppData\Roaming\gcloud\application_default_credentials.json`
