# popindau_siem_fraud_backend/sse_producer.py

import json
import requests
import urllib3
from sseclient import SSEClient
from confluent_kafka import Producer

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURARE API ---
API_KEY = "726b8811029a43be71d7c997f21a983ff24b524c5d94cfdfa60f0efbeeaa4322"
STREAM_URL = "https://95.217.75.14:8443/stream"
FLAG_URL = "https://95.217.75.14:8443/api/flag"
headers = {"X-API-Key": API_KEY}

# --- CONFIGURARE KAFKA ---
KAFKA_SERVER = 'localhost:9092'
KAFKA_TOPIC = 'transactions'

producer_conf = {
    'bootstrap.servers': KAFKA_SERVER,
    'client.id': 'sse-producer'
}

producer = Producer(producer_conf)


def delivery_report(err, msg):
    """Callback after each message delivery."""
    if err is not None:
        print(f"❌ Eroare la trimiterea mesajului: {err}")
    else:
        print(f"✅ Tranzactie publicata in Kafka: {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


def run_sse_to_kafka_producer():
    """Citeste din SSE Stream si publica mesajele in Kafka."""
    print("--- Conectare la SSE Stream si publicare in Kafka... ---")

    try:
        response = requests.get(STREAM_URL, headers=headers, stream=True, verify=False)
        client = SSEClient(response)

        for event in client.events():
            if not event.data:
                continue

            try:
                transaction = json.loads(event.data)

                # Trimite tranzactia catre Kafka
                producer.produce(
                    topic=KAFKA_TOPIC,
                    value=json.dumps(transaction).encode('utf-8'),
                    callback=delivery_report
                )

                producer.poll(0)  # run delivery callbacks
                trans_num = transaction.get('trans_num', 'N/A')
                print(f"📤 Publicat tranzactia {trans_num} in Kafka.")

            except json.JSONDecodeError:
                print(f"⚠️ Eroare la decodare JSON: {event.data}")

    except Exception as e:
        print(f"\n⛔ Eroare majora in SSE Producer: {e}")

    finally:
        print("🧹 Golesc bufferul Kafka...")
        producer.flush()
        print("✅ Producer oprit corect.")


if __name__ == "__main__":
    run_sse_to_kafka_producer()
