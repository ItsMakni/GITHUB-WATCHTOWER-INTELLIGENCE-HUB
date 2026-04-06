import os
import time
import json
import requests
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from confluent_kafka import Producer

# --- 1. CONFIGURATION (Environment Variables) ---
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_KEY = os.getenv("KAFKA_KEY")
KAFKA_SECRET = os.getenv("KAFKA_SECRET")
PORT = int(os.getenv("PORT", 8080))

# Kafka Producer Configuration
conf = {
    'bootstrap.servers': KAFKA_SERVER,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': KAFKA_KEY,
    'sasl.password': KAFKA_SECRET,
    'client.id': 'watchtower-ingester'
}

producer = Producer(conf)

# --- 2. HEALTH CHECK SERVER ---
# Cloud Run requires a web server to listen on $PORT to mark the instance as "Healthy"
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK - Watchtower Ingestor is Running")

    def log_message(self, format, *args):
        return # Silent logs for health checks to keep Cloud Run logs clean

def run_health_server():
    server = HTTPServer(('0.0.0.0', PORT), HealthCheckHandler)
    print(f"[*] Health check server started on port {PORT}")
    server.serve_forever()

# --- 3. GITHUB POLLING LOGIC ---

def fetch_events():
    last_etag = None
    iteration = 0 # Track loops
    print("[*] Starting GitHub Event Polling...")
    
    while True:
        iteration += 1
        if iteration % 10 == 0:
            print(f"[#] Heartbeat: Poller is alive. Total loops: {iteration}")

        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        if last_etag:
            headers["If-None-Match"] = last_etag
            
        try:
            # We poll the global events firehose
            response = requests.get("https://api.github.com/events", headers=headers, timeout=10)
            
            if response.status_code == 200:
                events = response.json()
                last_etag = response.headers.get("ETag")
                
                push_events = [e for e in events if e["type"] == "PushEvent"]
                print(f"[+] Found {len(push_events)} PushEvents. Streaming to Kafka...")
                
                for event in push_events:
                    # We use the Repo ID as the Kafka Key for partitioning strategy
                    repo_id = str(event["repo"]["id"])
                    producer.produce(
                        "github-raw-events", 
                        key=repo_id, 
                        value=json.dumps(event)
                    )
                
                producer.flush() # Ensure messages are sent
                
            elif response.status_code == 304:
                # 304 means no new events since last ETag; perfectly normal
                pass
                
            elif response.status_code == 403:
                print(f"[!] Rate limit hit or secondary limit. Sleeping longer...")
                time.sleep(30)
            else:
                print(f"[!] Unexpected Error: {response.status_code} - {response.text}")

        except Exception as e:
            print(f"[!] Connection Error: {e}")
            
        # Poll every 2 seconds (Standard for GitHub events)
        time.sleep(2)

# --- 4. MAIN EXECUTION ---
if __name__ == "__main__":
    # Start the Health Check server in a background thread
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    
    # Start the main polling logic in the foreground
    fetch_events()