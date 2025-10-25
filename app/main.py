import json
import requests
import urllib3
import time
import threading
import collections
import uvicorn
from sseclient import SSEClient
from fastapi import FastAPI
from typing import List, Dict, Any

# --- Configuration -----------------------------------------------------------

# !!! REPLACE WITH YOUR ACTUAL API KEY !!!
API_KEY = "YOUR_API_KEY"

STREAM_URL = "https.95.217.75.14:8443/stream"
FLAG_URL = "https://95.217.75.14:8443/api/flag"

# Disable insecure request warnings (since verify=False is used)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Global Data Store -------------------------------------------------------

# We need a thread-safe way to store transactions for the web server to access.
# A deque (double-ended queue) with a max length is perfect for this.
# It will automatically discard old transactions, preventing memory leaks.
MAX_TRANSACTIONS_STORED = 100
transactions_store = collections.deque(maxlen=MAX_TRANSACTIONS_STORED)
data_lock = threading.Lock()

# --- Mock AI Model -----------------------------------------------------------

def mock_ai_fraud_detector(transaction: dict) -> int:
    """
    Simulates an AI model processing a transaction to detect fraud.
    
    Args:
        transaction: The transaction data as a dictionary.
    
    Returns:
        int: 0 for legitimate, 1 for fraud.
    """
    try:
        amount = float(transaction.get('amt', 0))
        category = transaction.get('category')
        
        # Rule 1: Any transaction over $1500 is suspicious
        if amount > 1500:
            print(f"[AI_MODEL] Flagged (Amount > 1500): ${amount}")
            return 1
            
        # Rule 2: 'gambling' transactions over $500 are suspicious
        if category == 'gambling' and amount > 500:
            print(f"[AI_MODEL] Flagged (Gambling > 500): ${amount}")
            return 1
            
        # If no rules match, it's legitimate
        return 0
        
    except Exception as e:
        print(f"[AI_MODEL_ERROR] Could not process transaction: {e}")
        # Default to legitimate if an error occurs
        return 0

# --- API Interaction (from your example) -------------------------------------

def flag_transaction(trans_num: str, flag_value: int) -> dict:
    """
    Flag a transaction as fraud (1) or legitimate (0)
    
    Args:
        trans_num: Transaction number from the stream
        flag_value: 0 for legitimate, 1 for fraud
    
    Returns:
        Response from the flag endpoint as a dictionary
    """
    headers = {"X-API-Key": API_KEY}
    payload = {
        "trans_num": trans_num,
        "flag_value": flag_value
    }
    try:
        response = requests.post(FLAG_URL, headers=headers, json=payload, verify=False, timeout=5)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"[FLAG_ERROR] Failed to flag transaction {trans_num}: {e}")
        return {"success": False, "reason": str(e)}

# --- Background SSE Worker ---------------------------------------------------

def sse_worker():
    """
    Runs in a separate thread.
    Connects to the SSE stream, processes transactions, and stores them.
    """
    headers = {"X-API-Key": API_KEY}
    
    # Loop indefinitely to handle automatic reconnections
    while True:
        try:
            print("[SSE_WORKER] Connecting to stream...")
            response = requests.get(STREAM_URL, headers=headers, stream=True, verify=False)
            client = SSEClient(response)
            
            print("[SSE_WORKER] Connection successful. Waiting for events...")

            for event in client.events():
                if not event.data:
                    continue
                    
                try:
                    # 1. Load the transaction
                    transaction = json.loads(event.data)
                    trans_num = transaction.get('trans_num')
                    if not trans_num:
                        continue
                        
                    print(f"[SSE_WORKER] Received transaction: {trans_num}")

                    # 2. Send to Mock AI Model
                    is_fraud_prediction = mock_ai_fraud_detector(transaction)
                    
                    # 3. Add our prediction to the transaction object
                    transaction['is_fraud_prediction'] = is_fraud_prediction

                    # 4. Flag the transaction back to the source API
                    flag_result = flag_transaction(trans_num, is_fraud_prediction)
                    print(f"[SSE_WORKER] Flag response for {trans_num}: {flag_result}")
                    
                    # 5. Add to our thread-safe global store for the frontend
                    with data_lock:
                        transactions_store.append(transaction)
                        
                    print("-" * 80)
                
                except json.JSONDecodeError:
                    print(f"[SSE_WORKER_ERROR] Failed to decode event data: {event.data}")
                except Exception as e:
                    print(f"[SSE_WORKER_ERROR] Error processing transaction: {e}")

        except requests.exceptions.ConnectionError as e:
            print(f"[SSE_WORKER_ERROR] Connection failed: {e}. Retrying in 5 seconds...")
        except Exception as e:
            print(f"[SSE_WORKER_ERROR] An unexpected error occurred: {e}. Retrying in 5 seconds...")
        
        # Wait 5 seconds before attempting to reconnect
        time.sleep(5)


# --- FastAPI Web Server ------------------------------------------------------

app = FastAPI(
    title="Fraud Detection API",
    description="Processes a live transaction stream and provides an endpoint to view recent transactions.",
    version="1.0.0"
)

@app.on_event("startup")
def on_startup():
    """
    FastAPI startup event handler.
    Starts the background SSE worker in a daemon thread.
    """
    print("[FASTAPI] Application startup...")
    print("[FASTAPI] Starting background SSE worker thread...")
    worker_thread = threading.Thread(target=sse_worker, daemon=True)
    worker_thread.start()

@app.get('/api/transactions', response_model=List[Dict[str, Any]])
def get_transactions():
    """
    GET endpoint for the frontend.
    Returns the list of most recent transactions processed by the backend.
    """
    with data_lock:
        # Create a thread-safe copy of the transactions
        current_transactions = list(transactions_store)
    
    # Return newest-first by reversing the list
    # FastAPI automatically serializes this to JSON
    return current_transactions[::-1]