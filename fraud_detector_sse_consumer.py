#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys

# Force unbuffered output
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)

import json
import time
from confluent_kafka import Consumer, KafkaError, KafkaException
import tensorflow as tf
import pandas as pd
import requests
import urllib3
import numpy as np
import joblib
from datetime import datetime

from pymongo import MongoClient
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

# --- Kafka Configuration ---
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "transactions"
CONSUMER_GROUP = "fraud-detector-group"

# Model paths - updated to include both models
MODEL_PATH_KERAS = "./models/final_model.keras"
MODEL_PATH_H5 = "./models/final_model.h5"  # Add your H5 model path
SCALER_PATH = "./models/scaler.pkl"
LABEL_ENCODERS_PATH = "./models/label_encoders.pkl"

# Global variables for models and preprocessing objects
fraud_model_keras = None
fraud_model_h5 = None
scaler = None
label_encoders = None

# Ensemble configuration
ENSEMBLE_WEIGHT_KERAS = 0.8  # Weight for Keras model prediction
ENSEMBLE_WEIGHT_H5 = 0.2     # Weight for H5 model prediction
FRAUD_THRESHOLD = 0.9        # Combined threshold for fraud detection

def store_transaction_in_mongodb(transaction_data, fraud_result):
    """Store transaction and fraud detection result in MongoDB with authentication"""
    try:
        from pymongo import MongoClient
        from pymongo.errors import OperationFailure
        
        MONGODB_URL = os.getenv("MONGODB_URL")
        
        if not MONGODB_URL:
            print("❌ MONGODB_URL not found in environment variables")
            return False
        
        client = MongoClient(
            MONGODB_URL,
            serverSelectionTimeoutMS=5000,
            authSource='admin',
            username='admin',
            password='password'
        )
        
        db_name = "fraud_detection"
        db = client[db_name]
        
        try:
            admin_db = client.admin
            admin_db.command('ping')
            print("✅ MongoDB authentication successful")
        except OperationFailure as e:
            print(f"❌ MongoDB authentication failed: {e}")
            client.close()
            return False
        
        trans_num = transaction_data.get('trans_num')
        if not trans_num:
            print("❌ No trans_num found in transaction data")
            client.close()
            return False
        
        # Enhanced document with ensemble results
        doc = {
            'transaction_id': transaction_data.get('transaction_id'),
            'trans_num': trans_num,
            'amount': float(transaction_data.get('amt', 0)),
            'category': transaction_data.get('category', 'unknown'),
            'merchant': transaction_data.get('merchant', 'unknown'),
            'state': transaction_data.get('state', 'unknown'),
            'city_pop': int(transaction_data.get('city_pop', 0)),
            'timestamp': datetime.now(),
            'fraud_detection': {
                'is_fraud': bool(fraud_result['fraud_flag']),
                'fraud_probability': fraud_result['fraud_probability'],
                'keras_probability': fraud_result.get('keras_probability', 0),
                'h5_probability': fraud_result.get('h5_probability', 0),
                'confidence': fraud_result['confidence'],
                'model_version': fraud_result['model_version'],
                'ensemble_used': fraud_result.get('ensemble_used', False),
                'processed_at': datetime.now()
            },
            'raw_data': transaction_data,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        collection = db.transactions
        
        result = collection.update_one(
            {'trans_num': trans_num},
            {'$set': doc},
            upsert=True
        )
        
        print(f"💾 MongoDB update result: matched={result.matched_count}, modified={result.modified_count}, upserted_id={result.upserted_id}")
        
        if fraud_result['fraud_flag']:
            fraud_flag_doc = {
                'trans_num': trans_num,
                'flag_value': 1,
                'fraud_probability': fraud_result['fraud_probability'],
                'keras_probability': fraud_result.get('keras_probability', 0),
                'h5_probability': fraud_result.get('h5_probability', 0),
                'confidence': fraud_result['confidence'],
                'model_version': fraud_result['model_version'],
                'ensemble_used': fraud_result.get('ensemble_used', False),
                'flagged_at': datetime.now()
            }
            db.fraud_flags.insert_one(fraud_flag_doc)
            print(f"🚩 Fraud flag stored for transaction: {trans_num}")
        
        client.close()
        print(f"💾 Transaction stored in MongoDB: {trans_num}")
        return True
        
    except OperationFailure as e:
        print(f"❌ MongoDB operation failed (auth/permissions): {e}")
        return False
    except Exception as e:
        print(f"❌ Error storing in MongoDB: {e}")
        import traceback
        traceback.print_exc()
        return False

def load_fraud_detection_components():
    """Load both trained models, scaler, and label encoders"""
    global fraud_model_keras, fraud_model_h5, scaler, label_encoders
    
    try:
        # Load Keras model
        fraud_model_keras = tf.keras.models.load_model(MODEL_PATH_KERAS)
        print("✅ Keras fraud detection model loaded successfully")
        
        # Load H5 model
        fraud_model_h5 = tf.keras.models.load_model(MODEL_PATH_H5)
        print("✅ H5 fraud detection model loaded successfully")
        
        # Load preprocessing components
        scaler = joblib.load(SCALER_PATH)
        label_encoders = joblib.load(LABEL_ENCODERS_PATH)
        print("✅ Preprocessing components loaded successfully")
        
        return True
    except Exception as e:
        print(f"❌ Error loading fraud detection components: {e}")
        return False

def preprocess_transaction_for_model(transaction):
    """Preprocess a single transaction to match the model's expected input format"""
    try:
        # Create a copy of the transaction
        tx_data = transaction.copy()
        
        # Convert to DataFrame for easier processing
        df = pd.DataFrame([tx_data])
        
        # Drop the same columns as in training
        columns_to_drop = [
            'ssn', 'cc_num', 'first', 'last', 'street', 'city', 'zip',
            'acct_num', 'trans_num', 'transaction_id', 'merchant'
        ]
        df = df.drop(columns=columns_to_drop, errors='ignore')
        
        # Convert date/time features (same as training)
        df['trans_date'] = pd.to_datetime(df['trans_date'])
        df['trans_time'] = pd.to_timedelta(df['trans_time'])
        df['dob'] = pd.to_datetime(df['dob'])
        
        # Extract features from datetime (same as training)
        df['trans_hour'] = df['trans_time'].dt.total_seconds() // 3600
        df['trans_day_of_week'] = df['trans_date'].dt.dayofweek
        df['trans_month'] = df['trans_date'].dt.month
        df['age'] = (df['trans_date'] - df['dob']).dt.days // 365
        
        # Calculate distance between home and merchant location
        df['distance_from_home'] = np.sqrt(
            (df['lat'].astype(float) - df['merch_lat'].astype(float)) ** 2 +
            (df['long'].astype(float) - df['merch_long'].astype(float)) ** 2
        )
        
        # Drop original datetime columns
        df = df.drop(columns=['trans_date', 'trans_time', 'dob', 'unix_time'])
        
        # Encode categorical variables using the saved label encoders
        categorical_columns = ['gender', 'state', 'job', 'category']
        
        for col in categorical_columns:
            if col in df.columns and col in label_encoders:
                try:
                    df[col] = label_encoders[col].transform(df[col].astype(str))
                except ValueError:
                    df[col] = 0
        
        # Ensure all columns are numeric
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Fill any NaN values with 0
        df = df.fillna(0)
        
        # Apply the same scaling used during training
        scaled_features = scaler.transform(df)
        
        return scaled_features
        
    except Exception as e:
        print(f"❌ Error preprocessing transaction: {e}")
        return None

def predict_with_single_model(model, features, model_name=""):
    """Make prediction with a single model"""
    try:
        prediction = model.predict(features, verbose=0)
        probability = float(prediction[0][0])
        print(f"   📊 {model_name} model probability: {probability:.4f}")
        return probability
    except Exception as e:
        print(f"❌ Error predicting with {model_name} model: {e}")
        return 0.0

def ensemble_predict(keras_prob, h5_prob):
    """Combine predictions from both models using weighted average"""
    combined_prob = (keras_prob * ENSEMBLE_WEIGHT_KERAS + 
                    h5_prob * ENSEMBLE_WEIGHT_H5)
    return combined_prob

def predict_fraud_ensemble(transaction):
    """Use both models to predict if a transaction is fraudulent"""
    global fraud_model_keras, fraud_model_h5
    
    if fraud_model_keras is None or fraud_model_h5 is None:
        print("❌ One or both models not loaded")
        return 0, 0.0, 0.0, 0.0
    
    try:
        # Preprocess the transaction
        features = preprocess_transaction_for_model(transaction)
        
        if features is None:
            print("⚠️ Could not preprocess transaction, using fallback")
            return 0, 0.0, 0.0, 0.0
        
        print("🤖 Running ensemble prediction...")
        
        # Get predictions from both models
        keras_probability = predict_with_single_model(fraud_model_keras, features, "Keras")
        h5_probability = predict_with_single_model(fraud_model_h5, features, "H5")
        
        # Combine predictions using ensemble
        combined_probability = ensemble_predict(keras_probability, h5_probability)
        
        # Use threshold for final decision
        is_fraud = 1 if combined_probability > FRAUD_THRESHOLD else 0
        
        print(f"   🎯 Combined probability: {combined_probability:.4f} (Threshold: {FRAUD_THRESHOLD})")
        print(f"   🚩 Final decision: {'FRAUD' if is_fraud else 'LEGITIMATE'}")
        
        return is_fraud, combined_probability, keras_probability, h5_probability
        
    except Exception as e:
        print(f"❌ Ensemble prediction error: {e}")
        return 0, 0.0, 0.0, 0.0

def fallback_predict_fraud(transaction):
    """Fallback prediction if ensemble fails - use either available model"""
    global fraud_model_keras, fraud_model_h5
    
    try:
        features = preprocess_transaction_for_model(transaction)
        if features is None:
            return 0, 0.0, 0.0, 0.0
        
        # Try Keras model first
        if fraud_model_keras is not None:
            probability = predict_with_single_model(fraud_model_keras, features, "Keras (fallback)")
            is_fraud = 1 if probability > FRAUD_THRESHOLD else 0
            return is_fraud, probability, probability, 0.0
        
        # Try H5 model if Keras not available
        elif fraud_model_h5 is not None:
            probability = predict_with_single_model(fraud_model_h5, features, "H5 (fallback)")
            is_fraud = 1 if probability > FRAUD_THRESHOLD else 0
            return is_fraud, probability, 0.0, probability
        
        else:
            return 0, 0.0, 0.0, 0.0
            
    except Exception as e:
        print(f"❌ Fallback prediction error: {e}")
        return 0, 0.0, 0.0, 0.0

def flag_transaction(trans_num, is_fraud):
    """Send fraud flag to API"""
    try:
        flag_url = "https://95.217.75.14:8443/api/flag"
        headers = {
            "X-API-Key": "726b8811029a43be71d7c997f21a983ff24b524c5d94cfdfa60f0efbeeaa4322",
            "Content-Type": "application/json"
        }
        
        payload = {
            "trans_num": trans_num,
            "flag_value": is_fraud
        }
        
        print(f"🚩 Sending flag {is_fraud} for transaction {trans_num} to API...")
        
        response = requests.post(flag_url, json=payload, headers=headers, verify=False, timeout=10)
        
        if response.status_code == 200:
            print(f"✅ Flag {is_fraud} sent successfully for transaction {trans_num}")
            return {"success": True, "status_code": response.status_code}
        else:
            print(f"❌ Failed to send flag. Status: {response.status_code}, Response: {response.text}")
            return {"success": False, "status_code": response.status_code}
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error flagging transaction: {e}")
        return {"success": False, "error": str(e)}
    except Exception as e:
        print(f"❌ Error flagging transaction: {e}")
        return {"success": False, "error": str(e)}

def send_to_local_api(data):
    """Send data to local API"""
    try:
        print(f"📤 Sending to API: {data}")
        return {"success": True}
    except Exception as e:
        print(f"❌ Error sending to API: {e}")
        return {"success": False}

def run_detector_and_flag():
    # Load the fraud detection components
    if not load_fraud_detection_components():
        print("❌ Cannot start detector without model components")
        return

    consumer_conf = {
        'bootstrap.servers': KAFKA_SERVER,
        'group.id': CONSUMER_GROUP,
        'auto.offset.reset': 'latest'
    }

    print(f"🔧 Configuratie Kafka: {consumer_conf}")
    
    try:
        consumer = Consumer(consumer_conf)
        consumer.subscribe([KAFKA_TOPIC])
        print(f"✅ Consumer creat si subscris la topicul '{KAFKA_TOPIC}'")
    except Exception as e:
        print(f"❌ Eroare la crearea consumer-ului: {e}")
        return

    print(f"--- 🧠🤖 Detectorul de Frauda cu ENSEMBLE AI Models asculta topicul '{KAFKA_TOPIC}' ---")
    print(f"⚖️  Ensemble weights: Keras={ENSEMBLE_WEIGHT_KERAS}, H5={ENSEMBLE_WEIGHT_H5}")
    print(f"🎯 Fraud threshold: {FRAUD_THRESHOLD}")
    print("⏳ Astept mesaje Kafka...")

    message_count = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                if message_count == 0:
                    print("⏳ Nu s-au primit mesaje inca... (poll timeout)")
                continue
            
            message_count += 1
            print(f"📨 Mesaj #{message_count} primit!")
            
            if msg.error():
                print(f"❌ Eroare in mesaj: {msg.error()}")
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            try:
                transaction = json.loads(msg.value().decode('utf-8'))
                print(f"🔍 Tranzactie parsata: {transaction}")
            except json.JSONDecodeError as e:
                print(f"⚠️ Mesaj invalid JSON: {msg.value()} - Error: {e}")
                continue
            except Exception as e:
                print(f"⚠️ Eroare la parsarea JSON: {e}")
                continue

            trans_num = transaction.get('trans_num', 'N/A')
            amount = float(transaction.get('amt', 0))

            print(f"🎯 Procesare tranzactie {trans_num} cu suma ${amount}")

            # --- ENSEMBLE AI MODEL PREDICTION ---
            is_fraud, fraud_probability, keras_prob, h5_prob = predict_fraud_ensemble(transaction)
            
            # If ensemble failed, try fallback
            if fraud_probability == 0.0 and (fraud_model_keras is not None or fraud_model_h5 is not None):
                print("🔄 Attempting fallback prediction...")
                is_fraud, fraud_probability, keras_prob, h5_prob = fallback_predict_fraud(transaction)
            
            flag_status = "FRAUDA" if is_fraud else "Legitima"
            confidence_level = "HIGH" if fraud_probability > 0.8 else "MEDIUM" if fraud_probability > 0.5 else "LOW"

            print(f"🤖 Rezultat ensemble: {flag_status} (probabilitate combinata: {fraud_probability:.4f})")

            result = flag_transaction(trans_num, is_fraud)
            
            # Enhanced data for API and MongoDB
            enhanced_data = {
                "trans_num": trans_num,
                "amount": amount,
                "fraud_flag": is_fraud,
                "fraud_probability": fraud_probability,
                "keras_probability": keras_prob,
                "h5_probability": h5_prob,
                "confidence": confidence_level,
                "model_version": "ensemble_v1",
                "ensemble_used": True
            }
            
            send_to_local_api(enhanced_data)

            mongodb_result = store_transaction_in_mongodb(transaction, enhanced_data)

            if mongodb_result:
                print(f"💾 Stored in MongoDB: {trans_num}")
            else:
                print(f"⚠️  Failed to store in MongoDB: {trans_num}")

            print(f"📊 [TRZ {trans_num}] → {flag_status} (Amt: ${amount:.2f})")
            print(f"   📈 Probabilities - Combined: {fraud_probability:.4f}, Keras: {keras_prob:.4f}, H5: {h5_prob:.4f}")
            print(f"   🎯 Confidence: {confidence_level} | API Success: {result.get('success', False)}")
            print("-" * 80)

    except KeyboardInterrupt:
        print("\n🛑 Consumator oprit manual.")
    except Exception as e:
        print(f"⛔ Eroare consumator: {e}")
        import traceback
        traceback.print_exc()
    finally:
        consumer.close()
        print(f"✅ Consumer inchis corect. Total mesaje procesate: {message_count}")

def test_ensemble_detection():
    """Test the ensemble fraud detection without Kafka"""
    print("🧪🤖 Testare sistem ENSEMBLE de detectie frauda...")
    
    if not load_fraud_detection_components():
        print("❌ Componente incarcate cu esec")
        return
    
    # Test transaction
    test_transaction = {
        "transaction_id": "181e5ed4-0f35-4785-896e-11c9c487a491",
        "ssn": "670-97-4056", 
        "cc_num": "4745171339596292335",
        "first": "Robert",
        "last": "Hudson",
        "gender": "M",
        "street": "825 Roberts Grove Apt. 260",
        "city": "Hampton",
        "state": "GA",
        "zip": "30228",
        "lat": "33.4124",
        "long": "-84.2947",
        "city_pop": "38569",
        "job": "Health and safety inspector",
        "dob": "1980-01-01",
        "acct_num": "720983479468",
        "trans_num": "616ceb5d75c429c47be5b271af8af2ba",
        "trans_date": "2025-10-22",
        "trans_time": "09:54:29",
        "unix_time": "1761116069",
        "category": "grocery_net",
        "amt": "75.18",
        "merchant": "fraud_Wiegand-Lowe",
        "merch_lat": "33.928527",
        "merch_long": "-83.908964"
    }
    
    print("🔍 Testare preprocesare...")
    features = preprocess_transaction_for_model(test_transaction)
    if features is not None:
        print(f"✅ Preprocesare reusita. Shape: {features.shape}")
    else:
        print("❌ Preprocesare esuata")
        return
    
    global ENSEMBLE_WEIGHT_KERAS, ENSEMBLE_WEIGHT_H5
    print("🤖 Testare predictie ensemble...")
    is_fraud, combined_prob, keras_prob, h5_prob = predict_fraud_ensemble(test_transaction)
    print(f"✅ Rezultat ensemble:")
    print(f"   - Fraud: {is_fraud}")
    print(f"   - Combined Probability: {combined_prob:.4f}")
    print(f"   - Keras Model Probability: {keras_prob:.4f}")
    print(f"   - H5 Model Probability: {h5_prob:.4f}")
    print(f"   - Ensemble Weights: Keras({ENSEMBLE_WEIGHT_KERAS}), H5({ENSEMBLE_WEIGHT_H5})")

def adjust_ensemble_weights_based_on_performance():
    """Function to dynamically adjust ensemble weights based on model performance"""
    # This could be enhanced to track model performance over time
    # and adjust weights accordingly
    global ENSEMBLE_WEIGHT_KERAS, ENSEMBLE_WEIGHT_H5
    
    # For now, using fixed weights as defined above
    # In production, you could load performance metrics from MongoDB
    # and adjust weights dynamically
    pass

if __name__ == "__main__":
    # Test the ensemble system first
    test_ensemble_detection()
    
    # Then run the Kafka consumer
    print("\n" + "="*60)
    print("Pornire consumator Kafka cu Ensemble Models...")
    print("="*60)
    run_detector_and_flag()