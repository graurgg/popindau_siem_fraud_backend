# main.py
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from fastapi.responses import StreamingResponse

from app.database import get_async_database, test_async_connection, close_async_connection

from datetime import datetime
import os
import asyncio
import json
import time

# Load environment variables
load_dotenv()

# Create FastAPI app instance
app = FastAPI(
    title="Popindau Fraud Detection API",
    description="Real-time fraud detection system with MongoDB storage",
    version="1.1.0"  # Updated version
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB event handlers
# MongoDB event handlers
@app.on_event("startup")
async def startup_db_client():
    """Initialize database connection on startup"""
    from app.database import get_async_database, test_async_connection
    db = get_async_database()
    if db is not None:
        # Test connection first
        connection_ok = await test_async_connection()
        if connection_ok:
            # Create indexes
            try:
                await db.transactions.create_index("trans_num", unique=True)
                await db.transactions.create_index([("timestamp", -1)])
                await db.transactions.create_index([("fraud_detection.is_fraud", 1)])
                await db.transactions.create_index([("amount", 1)])
                await db.transactions.create_index([("category", 1)])
                print("✅ MongoDB indexes created/verified")
            except Exception as e:
                print(f"⚠️ Warning: Could not create indexes: {e}")
        else:
            print("❌ MongoDB connection test failed during startup")
    else:
        print("❌ Could not initialize MongoDB client during startup")

@app.on_event("shutdown")
async def shutdown_db_client():
    """Close database connection on shutdown"""
    from app.database import close_async_connection
    await close_async_connection()  # Now properly awaited
    print("✅ MongoDB connection closed")

@app.get("/sse/transactions")
async def sse_transactions(
    request: Request, 
    recent: bool = True,  # Send recent transactions on connect
    stats_interval: int = 30  # Send stats every N seconds
):
    async def event_generator():
        try:
            db = get_async_database()
            if db is None:
                yield f"data: {json.dumps({'type': 'error', 'message': 'Database connection failed'})}\n\n"
                return
            
            # Send initial connection message
            yield f"data: {json.dumps({'type': 'connected', 'message': 'SSE connection established'})}\n\n"
            
            # Send recent transactions if requested
            if recent:
                recent_transactions = await get_recent_transactions(db, limit=20)
                for tx in recent_transactions:
                    yield f"data: {json.dumps({'type': 'recent_transaction', 'data': tx})}\n\n"
            
            # Set up real-time monitoring
            last_timestamp = datetime.utcnow()
            last_stats_sent = datetime.utcnow()
            
            while True:
                if await request.is_disconnected():
                    break
                
                current_time = datetime.utcnow()
                
                # Check for new transactions
                new_transactions = await get_new_transactions(db, last_timestamp)
                for tx in new_transactions:
                    yield f"data: {json.dumps({'type': 'new_transaction', 'data': tx})}\n\n"
                    # Update last timestamp
                    if 'timestamp' in tx:
                        last_timestamp = datetime.fromisoformat(tx['timestamp'].replace('Z', '+00:00'))
                
                # Send statistics periodically
                if (current_time - last_stats_sent).total_seconds() >= stats_interval:
                    stats = await get_live_stats(db)
                    yield f"data: {json.dumps({'type': 'stats_update', 'data': stats})}\n\n"
                    last_stats_sent = current_time
                
                # Send heartbeat if no activity
                if len(new_transactions) == 0:
                    yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': current_time.isoformat()})}\n\n"
                
                await asyncio.sleep(1)  # Check every second
                
        except asyncio.CancelledError:
            print("SSE connection closed by client")
        except Exception as e:
            print(f"SSE error: {e}")
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
        }
    )

async def get_recent_transactions(db, limit: int = 20):
    """Get recent transactions for initial load"""
    try:
        cursor = db.transactions.find().sort("timestamp", -1).limit(limit)
        transactions = []
        async for doc in cursor:
            # Convert for JSON serialization
            doc = convert_doc_for_json(doc)
            transactions.append(doc)
        return transactions
    except Exception as e:
        print(f"Error getting recent transactions: {e}")
        return []

async def get_new_transactions(db, since: datetime):
    """Get transactions newer than the given timestamp"""
    try:
        query = {"timestamp": {"$gt": since}}
        cursor = db.transactions.find(query).sort("timestamp", 1)
        transactions = []
        async for doc in cursor:
            transactions.append(convert_doc_for_json(doc))
        return transactions
    except Exception as e:
        print(f"Error getting new transactions: {e}")
        return []

async def get_live_stats(db):
    """Get current statistics"""
    try:
        total = await db.transactions.count_documents({})
        fraud_count = await db.transactions.count_documents({"fraud_detection.is_fraud": True})
        
        # Last hour transactions
        one_hour_ago = datetime.utcnow().timestamp() - 3600
        hourly_count = await db.transactions.count_documents({
            "timestamp": {"$gte": datetime.fromtimestamp(one_hour_ago)}
        })
        
        return {
            "total_transactions": total,
            "fraud_transactions": fraud_count,
            "fraud_percentage": (fraud_count / total * 100) if total > 0 else 0,
            "transactions_last_hour": hourly_count,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        print(f"Error getting stats: {e}")
        return {}

def convert_doc_for_json(doc):
    """Convert MongoDB document for JSON serialization"""
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    
    # Convert datetime fields to ISO format
    datetime_fields = ["timestamp", "created_at", "updated_at"]
    for field in datetime_fields:
        if field in doc and doc[field]:
            doc[field] = doc[field].isoformat()
    
    # Convert fraud detection timestamps
    if "fraud_detection" in doc and doc["fraud_detection"]:
        if "processed_at" in doc["fraud_detection"] and doc["fraud_detection"]["processed_at"]:
            doc["fraud_detection"]["processed_at"] = doc["fraud_detection"]["processed_at"].isoformat()
    
    return doc

@app.get("/")
async def root():
    return {
        "message": "Popindau Fraud Detection API with MongoDB", 
        "status": "running",
        "version": "1.1.0",
        "database": "MongoDB"
    }

@app.get("/health")
async def health_check():
    from app.database import get_async_database
    from datetime import datetime
    try:
        db = get_async_database()
        if db is not None:
            # Test database connection
            await db.command("ping")
            db_status = "connected"
        else:
            db_status = "not connected"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return {
        "status": "healthy", 
        "timestamp": datetime.utcnow().isoformat(),
        "database": db_status
    }

@app.get("/info")
async def api_info():
    return {
        "name": "Popindau Fraud Detection",
        "version": "1.1.0",
        "database": "MongoDB",
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "api": "/api/v1"
        }
    }

# Import and include API routes
try:
    from app.api.endpoints import router as api_router
    app.include_router(api_router, prefix="/api/v1")
    print("✅ API routes loaded successfully")
except ImportError as e:
    print(f"⚠ Warning: Could not load API routes: {e}")