# endpoints.py
from fastapi import APIRouter, HTTPException, Depends
from datetime import datetime
from typing import List, Dict
import uuid
from app.database import get_async_database
from app.models import TransactionInDB, FraudDetectionResult
from bson import ObjectId
import json

router = APIRouter()

@router.get("/test")
async def test_endpoint():
    return {"message": "API is working!", "timestamp": datetime.now(), "database": "MongoDB"}

@router.post("/transactions")
async def create_transaction(transaction_data: dict, db=Depends(get_async_database)):
    try:
        # Generate IDs if not provided
        transaction_id = transaction_data.get('transaction_id') or str(uuid.uuid4())
        trans_num = transaction_data.get('trans_num') or str(uuid.uuid4())
        
        # Check if transaction already exists
        existing_tx = await db.transactions.find_one({"trans_num": trans_num})
        if existing_tx:
            return {
                "transaction_id": existing_tx.get("transaction_id"),
                "is_fraud": existing_tx.get("fraud_detection", {}).get("is_fraud", False),
                "confidence": existing_tx.get("fraud_detection", {}).get("confidence", 0.0),
                "model_version": existing_tx.get("fraud_detection", {}).get("model_version", "1.0.0"),
                "timestamp": datetime.now(),
                "status": "existing"
            }
        
        # Simple fraud detection logic (fallback)
        amount = float(transaction_data.get('amt', 0))
        location = transaction_data.get('state', '').lower()
        
        risk_score = 0.0
        if amount > 1000:
            risk_score += 0.3
        if 'international' in location:
            risk_score += 0.2
            
        import random
        risk_score += random.uniform(0, 0.3)
        
        is_fraud = risk_score > 0.6
        confidence = min(abs(risk_score - 0.5) * 2, 0.99)
        
        # Create transaction document
        transaction_doc = {
            "transaction_id": transaction_id,
            "trans_num": trans_num,
            "amount": amount,
            "category": transaction_data.get('category', 'unknown'),
            "merchant": transaction_data.get('merchant', 'unknown'),
            "state": transaction_data.get('state', 'unknown'),
            "city_pop": int(transaction_data.get('city_pop', 0)),
            "timestamp": datetime.now(),
            "fraud_detection": {
                "is_fraud": is_fraud,
                "fraud_probability": risk_score,
                "confidence": "HIGH" if confidence > 0.8 else "MEDIUM" if confidence > 0.5 else "LOW",
                "model_version": "1.0.0-fallback",
                "processed_at": datetime.now()
            },
            "raw_data": transaction_data,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        
        # Insert into MongoDB
        result = await db.transactions.insert_one(transaction_doc)
        
        response = {
            "transaction_id": transaction_id,
            "trans_num": trans_num,
            "is_fraud": is_fraud,
            "confidence": confidence,
            "model_version": "1.0.0-fallback",
            "timestamp": datetime.now(),
            "risk_score": risk_score,
            "mongodb_id": str(result.inserted_id)
        }
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")



@router.post("/processed-transaction")
async def receive_processed_transaction(data: dict, db=Depends(get_async_database)):
    """
    Acest endpoint este apelat de fraud_detector_sse_consumer.py 
    pentru a stoca tranzactiile procesate (cu is_fraud, confidence, etc.).
    """
    try:
        trans_num = data.get('trans_num')
        if not trans_num:
            raise HTTPException(status_code=400, detail="trans_num is required")
        
        # Update existing transaction or create new one
        fraud_detection_data = {
            "is_fraud": data.get('fraud_flag', False),
            "fraud_probability": data.get('fraud_probability', 0.0),
            "confidence": data.get('confidence', 'LOW'),
            "model_version": data.get('model_version', 'ai_v1'),
            "processed_at": datetime.now()
        }
        
        # Try to update existing transaction
        result = await db.transactions.update_one(
            {"trans_num": trans_num},
            {
                "$set": {
                    "fraud_detection": fraud_detection_data,
                    "updated_at": datetime.now()
                },
                "$setOnInsert": {
                    "transaction_id": data.get('transaction_id', str(uuid.uuid4())),
                    "amount": float(data.get('amount', 0)),
                    "category": data.get('category', 'unknown'),
                    "merchant": data.get('merchant', 'unknown'),
                    "state": data.get('state', 'unknown'),
                    "city_pop": int(data.get('city_pop', 0)),
                    "timestamp": datetime.now(),
                    "raw_data": data,
                    "created_at": datetime.now()
                }
            },
            upsert=True
        )
        
        # Also store fraud flag separately if it's fraud
        if data.get('fraud_flag'):
            fraud_flag_doc = {
                "trans_num": trans_num,
                "flag_value": 1,
                "flagged_at": datetime.now(),
                "fraud_probability": data.get('fraud_probability', 0.0),
                "model_version": data.get('model_version', 'ai_v1')
            }
            await db.fraud_flags.insert_one(fraud_flag_doc)
        
        return {
            "status": "success", 
            "message": "Transaction stored/updated",
            "upserted_id": str(result.upserted_id) if result.upserted_id else None,
            "matched_count": result.matched_count,
            "modified_count": result.modified_count
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Storage error: {str(e)}")

@router.get("/transactions")
async def get_transactions(
    limit: int = 50, 
    skip: int = 0,
    fraud_only: bool = False,
    db=Depends(get_async_database)
):
    try:
        # Build query
        query = {}
        if fraud_only:
            query["fraud_detection.is_fraud"] = True
        
        # Get transactions from MongoDB
        cursor = db.transactions.find(query).sort("timestamp", -1).skip(skip).limit(limit)
        transactions = []
        
        async for doc in cursor:
            # Convert ObjectId to string
            doc["_id"] = str(doc["_id"])
            transactions.append(doc)
        
        return transactions
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/transactions/{trans_num}")
async def get_transaction(trans_num: str, db=Depends(get_async_database)):
    try:
        transaction = await db.transactions.find_one({"trans_num": trans_num})
        if not transaction:
            raise HTTPException(status_code=404, detail="Transaction not found")
        
        transaction["_id"] = str(transaction["_id"])
        return transaction
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/stats/transactions")
async def get_transaction_stats(db=Depends(get_async_database)):
    try:
        # Get basic statistics
        total_transactions = await db.transactions.count_documents({})
        fraud_transactions = await db.transactions.count_documents({"fraud_detection.is_fraud": True})
        
        # Get transactions by hour (last 24 hours)
        twenty_four_hours_ago = datetime.now().timestamp() - 86400
        
        pipeline = [
            {
                "$match": {
                    "timestamp": {"$gte": datetime.fromtimestamp(twenty_four_hours_ago)}
                }
            },
            {
                "$group": {
                    "_id": {"$hour": "$timestamp"},
                    "count": {"$sum": 1},
                    "fraud_count": {
                        "$sum": {"$cond": [{"$eq": ["$fraud_detection.is_fraud", True]}, 1, 0]}
                    },
                    "total_amount": {"$sum": "$amount"}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        
        hourly_stats = []
        async for doc in db.transactions.aggregate(pipeline):
            hourly_stats.append(doc)
        
        return {
            "total_transactions": total_transactions,
            "fraud_transactions": fraud_transactions,
            "fraud_percentage": (fraud_transactions / total_transactions * 100) if total_transactions > 0 else 0,
            "hourly_stats": hourly_stats,
            "timestamp": datetime.now()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")