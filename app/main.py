# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os
import asyncio

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