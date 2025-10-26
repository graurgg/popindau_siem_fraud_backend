# app/database.py
import os
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
from pymongo.errors import OperationFailure, ServerSelectionTimeoutError
from dotenv import load_dotenv

load_dotenv()

# MongoDB configuration
MONGODB_URL = os.getenv("MONGODB_URL")
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", "fraud_detection")

# Async client for FastAPI
async_client = None
database = None

# Sync client for scripts
sync_client = None
sync_database = None

def get_async_database():
    """Get async database connection for FastAPI"""
    global async_client, database
    if async_client is None:
        try:
            async_client = AsyncIOMotorClient(
                MONGODB_URL, 
                serverSelectionTimeoutMS=5000,
                authSource="fraud_detection"
            )
            database = async_client[MONGODB_DB_NAME]
            print(f"✅ Async MongoDB client initialized for: {MONGODB_DB_NAME}")
            
        except Exception as e:
            print(f"❌ Error creating async MongoDB connection: {e}")
            database = None
            
    return database

async def test_async_connection():
    """Test the async connection (called during startup)"""
    global database
    if database is not None:
        try:
            await database.command('ping')
            print("✅ Async MongoDB connection test successful!")
            return True
        except OperationFailure as e:
            print(f"❌ MongoDB authentication failed: {e}")
            return False
        except Exception as e:
            print(f"❌ MongoDB connection error: {e}")
            return False
    return False

def get_sync_database():
    """Get sync database connection for scripts"""
    global sync_client, sync_database
    if sync_client is None:
        try:
            sync_client = MongoClient(
                MONGODB_URL,
                serverSelectionTimeoutMS=5000,
                authSource="fraud_detection"
            )
            sync_database = sync_client[MONGODB_DB_NAME]
            
            # Test connection
            sync_client.admin.command('ping')
            print(f"✅ Sync MongoDB connected: {MONGODB_DB_NAME}")
            
        except OperationFailure as e:
            print(f"❌ MongoDB authentication failed: {e}")
            sync_database = None
        except ServerSelectionTimeoutError as e:
            print(f"❌ MongoDB server not available: {e}")
            sync_database = None
        except Exception as e:
            print(f"❌ MongoDB connection error: {e}")
            sync_database = None
            
    return sync_database

async def close_async_connection():
    """Close async database connection - properly async"""
    global async_client
    if async_client:
        async_client.close()
        async_client = None
        database = None
        print("✅ Async MongoDB connection closed")

def close_sync_connection():
    """Close sync database connection"""
    global sync_client
    if sync_client:
        sync_client.close()
        sync_client = None
        sync_database = None
        print("✅ Sync MongoDB connection closed")