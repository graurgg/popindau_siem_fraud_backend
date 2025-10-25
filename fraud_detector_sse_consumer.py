# popindau_siem_fraud_backend/fraud_detector_sse_consumer.py

import json
import requests
import urllib3
from confluent_kafka import Consumer, KafkaError, KafkaException

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURARE API ---
API_KEY = "726b8811029a43be71d7c997f21a983ff24b524c5d94cfdfa60f0efbeeaa4322"
FLAG_URL = "https://95.217.75.14:8443/api/flag"
headers = {"X-API-Key": API_KEY}

# --- CONFIGURARE KAFKA ---
KAFKA_SERVER = 'localhost:9092'
KAFKA_TOPIC = 'transactions'
CONSUMER_GROUP = 'fraud-detector-main'

# --- CONFIGURARE LOCAL API ---
LOCAL_API_URL = 'http://localhost:8000/api/v1/processed-transaction'


def flag_transaction(trans_num, flag_value):
    """Trimite un flag catre API pentru tranzactia suspecta."""
    payload = {"trans_num": trans_num, "flag_value": flag_value}
    try:
        response = requests.post(FLAG_URL, headers=headers, json=payload, verify=False, timeout=5)
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"success": False, "reason": f"API Request Failed: {e}"}


def send_to_local_api(data):
    """Trimite rezultatul procesarii catre backend-ul local."""
    try:
        requests.post(LOCAL_API_URL, json=data, verify=False, timeout=2)
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Nu pot trimite datele procesate catre API local: {e}")


def run_detector_and_flag():
    consumer_conf = {
        'bootstrap.servers': KAFKA_SERVER,
        'group.id': CONSUMER_GROUP,
        'auto.offset.reset': 'latest'
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])

    print(f"--- 🧠 Detectorul de Frauda asculta topicul '{KAFKA_TOPIC}' ---")

    try:
        while True:
            msg = consumer.poll(1.0)  # asteapta 1 secunda
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            try:
                transaction = json.loads(msg.value().decode('utf-8'))
            except json.JSONDecodeError:
                print(f"⚠️ Mesaj invalid JSON: {msg.value()}")
                continue

            trans_num = transaction.get('trans_num', 'N/A')
            amount = float(transaction.get('amt', 0))

            # --- Logica simpla de detectare ---
            is_fraud = 1 if amount > 100 else 0
            flag_status = "FRAUDA" if is_fraud else "Legitima"

            result = flag_transaction(trans_num, is_fraud)
            send_to_local_api({
                "trans_num": trans_num,
                "amount": amount,
                "fraud_flag": is_fraud
            })

            print(f"[TRZ {trans_num}] → {flag_status} (Amt: ${amount}) | API: {result.get('success', False)}")

    except KeyboardInterrupt:
        print("\n🛑 Consumator oprit manual.")
    except Exception as e:
        print(f"⛔ Eroare consumator: {e}")
    finally:
        consumer.close()
        print("✅ Consumer inchis corect.")


if __name__ == "__main__":
    run_detector_and_flag()
