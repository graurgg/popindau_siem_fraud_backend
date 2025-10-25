"""
Machine Learning service for fraud detection
"""


class MLService:
    """Service for fraud detection using machine learning"""
    
    def __init__(self, model_path=None):
        self.model_path = model_path
        self.model = None
    
    def load_model(self):
        """Load the fraud detection model"""
        pass
    
    def predict(self, transaction_data):
        """Predict if a transaction is fraudulent"""
        pass
