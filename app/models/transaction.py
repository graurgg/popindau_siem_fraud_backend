"""
Transaction model
"""


class Transaction:
    """Transaction data model"""
    
    def __init__(self, transaction_id, amount, timestamp, user_id):
        self.transaction_id = transaction_id
        self.amount = amount
        self.timestamp = timestamp
        self.user_id = user_id
