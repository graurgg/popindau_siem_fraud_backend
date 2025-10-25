# popindau_siem_fraud_backend/fraud_detector_sse_consumer.py

# Install: pip install requests kafka-python
import json
import requests
import urllib3
from kafka import KafkaConsumer
from fastapi import FastAPI

app = FastAPI()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

transactions = []

# --- CONFIGURARE API ---
API_KEY = "YOUR_API_KEY" # Schimba aici
FLAG_URL = "https://95.217.75.14:8443/api/flag"
headers = {"X-API-Key": API_KEY}

# --- CONFIGURARE KAFKA ---
KAFKA_SERVER = 'localhost:9092'
KAFKA_TOPIC = 'transactions'
CONSUMER_GROUP = 'fraud-detector-main' 

# Functie pentru a trimite flag-ul (copiata din codul tau initial)
def flag_transaction(trans_num, flag_value):
    """Flag a transaction as fraud (1) or legitimate (0)"""
    payload = {"trans_num": trans_num, "flag_value": flag_value}
    try:
        response = requests.post(FLAG_URL, headers=headers, json=payload, verify=False, timeout=5)
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"success": False, "reason": f"API Request Failed: {e}"}

# Conectare la Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    group_id=CONSUMER_GROUP,
    auto_offset_reset='latest', # Citeste doar mesaje noi
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def run_detector_and_flag():
    print(f"--- 🧠 Detectorul de Frauda citeste din Kafka topic '{KAFKA_TOPIC}' ---")
    
    for message in consumer:
        transactions.append(message.value)
        transaction = message.value
        
        # Extrage campurile necesare
        trans_num = transaction.get('trans_num')
        amount = float(transaction.get('amt', 0))
        category = transaction.get('category')
        
        # -----------------------------------------------------------------
        # LOGICA TA DE DETECTARE A FRAUDEI AICI
        # Foloseste toate campurile disponibile in dictionarul 'transaction'
        # -----------------------------------------------------------------
        
        # EXEMPLU DE LOGICA SIMPLA: FLAG pe tranzactii de peste 5000 USD
        is_fraud = 0
        if amount > 100:
            is_fraud = 1
        
        # Trimite flag-ul
        result = flag_transaction(trans_num, is_fraud)
        
        flag_status = "FRAUDA" if is_fraud == 1 else "Legitima"
        
        print(f"[TRZ {trans_num}] Decizie: {flag_status} (Amt: ${amount}) | API Status: {result.get('success', False)}")
        print("-" * 50)
    
@app.get("/transactions")
def get_transactions():
    """Returneaza toate tranzactiile procesate."""
    return transactions
    
if __name__ == "__main__":
    run_detector_and_flag()