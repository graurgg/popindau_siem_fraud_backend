# popindau_siem_fraud_backend/fraud_detector_sse_consumer.py

import json
import requests
import urllib3
from confluent_kafka import Consumer, KafkaError, KafkaException
from fraud_detection_model import FraudDetectionModel  # Import your model class
import os

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
    print("Current working directory:", os.getcwd())

    # Initialize the ML model (do this once at startup)
    try:
        fraud_model = FraudDetectionModel(
            model_path='best_fraud_model.h5',  # Update with your actual model path
            scaler_path='scaler.pkl',  # Update with your actual scaler path
            label_encoders_path='label_encoders.pkl'  # Update with your actual encoders path
        )
        print("✅ ML Model incarcat cu succes pentru detectie frauda", flush=True)
    except Exception as e:
        print(f"⛔ Eroare la incarcarea modelului ML: {e}", flush=True)
        print("🔄 Folosesc logica simpla de rezerva...", flush=True)
        fraud_model = None

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

            # --- ML MODEL FRAUD DETECTION ---
            if fraud_model:
                try:
                    # Prepare transaction data for ML model
                    ml_transaction_data = {
                        # Original features
                        'amt': amount,
                        'lat': transaction.get('lat', 0),
                        'long': transaction.get('long', 0),
                        'merch_lat': transaction.get('merch_lat', 0),
                        'merch_long': transaction.get('merch_long', 0),
                        'unix_time': transaction.get('unix_time', 0),
                        
                        # Date/time features
                        'trans_date': transaction.get('trans_date', '2024-01-01'),
                        'trans_time': transaction.get('trans_time', '00:00:00'),
                        'dob': transaction.get('dob', '1980-01-01'),
                        
                        # Categorical features
                        'gender': transaction.get('gender', 'M'),
                        'state': transaction.get('state', 'NY'),
                        'job': transaction.get('job', 'Unknown'),
                        'category': transaction.get('category', 'other')
                    }
                    
                    # Use ML model for prediction
                    ml_result = fraud_model.predict(ml_transaction_data)
                    is_fraud = 1 if ml_result['is_fraud'] else 0
                    fraud_probability = ml_result['fraud_probability']
                    confidence = ml_result['confidence']
                    
                    flag_status = f"FRAUDA ({fraud_probability:.1%})" if is_fraud else f"Legitima ({fraud_probability:.1%})"

                    # Log ML prediction details
                    print(f"🔍 [ML] Tranzactie {trans_num}: {flag_status} | Expected: {transaction.get('is_fraud')} | Probabilitate: {fraud_probability:.1%} | Confidenta: {confidence:.1%}", flush=True)

                except Exception as e:
                    print(f"⚠️ Eroare la predictia ML pentru {trans_num}: {e}", flush=True)
                    # Fallback to simple logic if ML fails
                    is_fraud = 1 if amount > 100 else 0
                    flag_status = "FRAUDA (fallback)" if is_fraud else "Legitima (fallback)"
                    fraud_probability = 0.99 if is_fraud else 0.01
                    confidence = 0.99 if is_fraud else 0.99
            else:
                # Fallback to simple logic if model not loaded
                is_fraud = 1 if amount > 100 else 0
                flag_status = "FRAUDA (simple)" if is_fraud else "Legitima (simple)"
                fraud_probability = 0.99 if is_fraud else 0.01
                confidence = 0.99 if is_fraud else 0.99

            # Flag transaction in database
            result = flag_transaction(trans_num, is_fraud)
            
            # Send to local API with enhanced ML data
            send_to_local_api({
                "trans_num": trans_num,
                "amount": amount,
                "fraud_flag": is_fraud,
                "fraud_probability": fraud_probability,
                "confidence": confidence,
                "detection_method": "ml_model" if fraud_model and 'ml_result' in locals() else "fallback"
            })

            # Enhanced logging with ML insights
            if is_fraud:
                print(f"🚨 [TRZ {trans_num}] → {flag_status} | Amt: ${amount:.2f} | Confidence: {confidence:.1%} | API: {result.get('success', False)}", flush=True)
            else:
                print(f"✅ [TRZ {trans_num}] → {flag_status} | Amt: ${amount:.2f} | Confidence: {confidence:.1%} | API: {result.get('success', False)}", flush=True)

    except KeyboardInterrupt:
        print("\n🛑 Consumator oprit manual.")
    except Exception as e:
        print(f"⛔ Eroare consumator: {e}")
    finally:
        consumer.close()
        print("✅ Consumer inchis corect.")


if __name__ == "__main__":
    run_detector_and_flag()
