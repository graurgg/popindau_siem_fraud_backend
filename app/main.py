from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Create FastAPI app instance
app = FastAPI(
    title="Popindau Fraud Detection API",
    description="Real-time fraud detection system for transaction monitoring",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {
        "message": "Popindau Fraud Detection API", 
        "status": "running",
        "version": "1.0.0"
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": "2024-01-01T00:00:00Z"}

@app.get("/info")
async def api_info():
    return {
        "name": "Popindau Fraud Detection",
        "version": "1.0.0",
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