"""
Prediction model
"""


class Prediction:
    """Fraud prediction data model"""
    
    def __init__(self, transaction_id, is_fraud, confidence_score):
        self.transaction_id = transaction_id
        self.is_fraud = is_fraud
        self.confidence_score = confidence_score
