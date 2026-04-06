import os
import json
import requests
from confluent_kafka import Producer

# --- 1. CONFIGURATION (Environment Variables) ---
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_KEY = os.getenv("KAFKA_KEY")
KAFKA_SECRET = os.getenv("KAFKA_SECRET")

# Kafka Producer Configuration
conf = {
    'bootstrap.servers': KAFKA_SERVER,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': KAFKA_KEY,
    'sasl.password': KAFKA_SECRET,
    'client.id': 'watchtower-trend-job',
    'linger.ms': 100,  
    'retry.backoff.ms': 500
}

producer = Producer(conf)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f"[!] Message delivery failed: {err}")
    else:
        pass

def run_ingest_job():
    print("[*] Starting Tech Trend Ingest Job...")
    
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "Watchtower-Trend-Pulse-Bot"
    }
    
    try:
        response = requests.get("https://api.github.com/events", headers=headers, timeout=15)
        
        if response.status_code == 200:
            events = response.json()
            
            push_events = [e for e in events if e["type"] == "PushEvent"]
            print(f"[+] Received {len(events)} total events. Found {len(push_events)} PushEvents.")

            count = 0
            for event in push_events:
                repo_name = event.get("repo", {}).get("name")
                # The 'head' is the latest commit SHA in this push
                commit_sha = event.get("payload", {}).get("head")
                
                if repo_name and commit_sha:
                    payload = {
                        "name": repo_name,
                        "head": commit_sha,
                        "pushed_at": event.get("created_at")
                    }
                    
                    producer.produce(
                        "github-raw-events", 
                        value=json.dumps(payload),
                        callback=delivery_report
                    )
                    count += 1
            
            producer.flush(timeout=10)
            print(f"[*] Job Finished. {count} signals sent to Kafka.")
            
        elif response.status_code == 403:
            print("[!] Rate Limit Hit. Check your GitHub Token status.")
        else:
            print(f"[!] Unexpected API Response: {response.status_code}")

    except Exception as e:
        print(f"[!] Critical Job Error: {e}")

if __name__ == "__main__":
    run_ingest_job()