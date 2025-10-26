# app/models.py
from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field

class TransactionBase(BaseModel):
    transaction_id: str
    trans_num: str
    amount: float
    category: str
    merchant: str
    state: str
    city_pop: int
    timestamp: datetime

class FraudDetectionResult(BaseModel):
    is_fraud: bool
    fraud_probability: float
    confidence: str
    model_version: str
    processed_at: datetime

class TransactionInDB(TransactionBase):
    id: Optional[str] = Field(None, alias="_id")
    fraud_detection: Optional[FraudDetectionResult] = None
    raw_data: Optional[Dict[str, Any]] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

class FraudFlag(BaseModel):
    trans_num: str
    flag_value: int  # 0 or 1
    flagged_at: datetime = Field(default_factory=datetime.utcnow)
    api_response: Optional[Dict[str, Any]] = None