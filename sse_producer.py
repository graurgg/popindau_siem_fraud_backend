# popindau_siem_fraud_backend/sse_producer.py

# Install: pip install sseclient-py requests kafka-python
import json
import requests
import urllib3
from sseclient import SSEClient
from kafka import KafkaProducer

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURARE API ---
API_KEY = "726b8811029a43be71d7c997f21a983ff24b524c5d94cfdfa60f0efbeeaa4322"
STREAM_URL = "https://95.217.75.14:8443/stream"
FLAG_URL = "https://95.217.75.14:8443/api/flag"
headers = {"X-API-Key": API_KEY}

# --- CONFIGURARE KAFKA ---
KAFKA_SERVER = 'localhost:9092'
KAFKA_TOPIC = 'transactions'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_block_ms=30000 
)

def run_sse_to_kafka_producer():
    """Citeste din SSE Stream si publica mesajele in Kafka."""
    if not producer.bootstrap_connected():
        print("Eroare: Nu ma pot conecta la Kafka Broker.")
        return

    print("--- Conectare la SSE Stream si publicare in Kafka... ---")
    try:
        response = requests.get(STREAM_URL, headers=headers, stream=True, verify=False)
        client = SSEClient(response)

        for event in client.events():
            if event.data:
                try:
                    transaction = json.loads(event.data)
                    
                    # Trimite tranzactia bruta catre topic-ul 'transactions'
                    producer.send(KAFKA_TOPIC, value=transaction)
                    producer.flush() # Asigura ca mesajul e trimis imediat
                    
                    trans_num = transaction.get('trans_num', 'N/A')
                    print(f"✅ Publicat tranzactia {trans_num} in Kafka.")
                    
                except json.JSONDecodeError:
                    print(f"Eroare la decodare JSON: {event.data}")
            
    except Exception as e:
        print(f"\n⛔ Eroare majora in SSE Producer: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    run_sse_to_kafka_producer()