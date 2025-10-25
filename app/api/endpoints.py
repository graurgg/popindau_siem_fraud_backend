from fastapi import APIRouter, HTTPException
from datetime import datetime
import uuid

router = APIRouter()

# Simple in-memory storage for demo
transactions_store = []

@router.get("/test")
async def test_endpoint():
    return {"message": "API is working!", "timestamp": datetime.now()}

@router.post("/transactions")
async def create_transaction(transaction_data: dict):
    try:
        # Generate IDs if not provided
        transaction_id = transaction_data.get('transaction_id') or str(uuid.uuid4())
        
        # Simple fraud detection logic
        amount = transaction_data.get('amount', 0)
        location = transaction_data.get('location', '').lower()
        
        risk_score = 0.0
        if amount > 1000:
            risk_score += 0.3
        if 'international' in location:
            risk_score += 0.2
            
        import random
        risk_score += random.uniform(0, 0.3)
        
        is_fraud = risk_score > 0.6
        confidence = min(abs(risk_score - 0.5) * 2, 0.99)
        
        response = {
            "transaction_id": transaction_id,
            "is_fraud": is_fraud,
            "confidence": confidence,
            "model_version": "1.0.0-demo",
            "timestamp": datetime.now(),
            "risk_score": risk_score
        }
        
        # Store transaction
        transaction_data['id'] = str(uuid.uuid4())
        transaction_data['is_fraud'] = is_fraud
        transaction_data['confidence'] = confidence
        transaction_data['processed_at'] = datetime.now()
        transactions_store.append(transaction_data)
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/transactions")
async def get_transactions(limit: int = 50):
    try:
        # Return latest transactions
        return transactions_store[-limit:][::-1]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")