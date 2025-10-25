import tensorflow as tf
import numpy as np
import pandas as pd
import joblib

class FraudDetectionModel:
    def __init__(self, model_path, scaler_path, label_encoders_path):
        self.model = tf.keras.models.load_model(model_path)
        self.scaler = joblib.load(scaler_path)
        self.label_encoders = joblib.load(label_encoders_path)
        
    def preprocess_input(self, input_data):
        """Preprocess input data to match training format"""
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame([input_data])
        
        # Convert date/time features
        df['trans_date'] = pd.to_datetime(df['trans_date'])
        df['trans_time'] = pd.to_timedelta(df['trans_time'])
        df['dob'] = pd.to_datetime(df['dob'])
        
        # Extract features from datetime (same as training)
        df['trans_hour'] = df['trans_time'].dt.total_seconds() // 3600
        df['trans_day_of_week'] = df['trans_date'].dt.dayofweek
        df['trans_month'] = df['trans_date'].dt.month
        df['age'] = (df['trans_date'] - df['dob']).dt.days // 365
        
        # Calculate distance between home and merchant location
        df['distance_from_home'] = np.sqrt(
            (df['lat'] - df['merch_lat']) ** 2 +
            (df['long'] - df['merch_long']) ** 2
        )
        
        # Drop original datetime columns
        df = df.drop(columns=['trans_date', 'trans_time', 'dob', 'unix_time'])
        
        # Encode categorical variables using saved label encoders
        categorical_columns = ['gender', 'state', 'job', 'category']
        for col in categorical_columns:
            if col in df.columns:
                le = self.label_encoders[col]
                # Handle unseen categories by using most frequent class
                df[col] = df[col].apply(lambda x: x if x in le.classes_ else le.classes_[0])
                df[col] = le.transform(df[col])
        
        # Select final features in correct order
        final_features = [
            'amt', 'lat', 'long', 'merch_lat', 'merch_long',
            'trans_hour', 'trans_day_of_week', 'trans_month', 'age',
            'distance_from_home', 'gender', 'state', 'job', 'category'
        ]
        
        # Ensure all features are present
        for feature in final_features:
            if feature not in df.columns:
                raise ValueError(f"Missing required feature: {feature}")
        
        return df[final_features]
    
    def predict(self, input_data):
        """Make fraud prediction"""
        # Preprocess input
        processed_data = self.preprocess_input(input_data)
        
        # Scale features
        scaled_data = self.scaler.transform(processed_data)
        
        # Make prediction
        prediction_prob = self.model.predict(scaled_data, verbose=0)[0][0]
        
        # Convert to binary prediction and confidence
        is_fraud = prediction_prob > 0.5
        confidence = prediction_prob if is_fraud else 1 - prediction_prob
        
        return {
            'is_fraud': bool(is_fraud),
            'fraud_probability': float(prediction_prob),
            'confidence': float(confidence),
            'threshold_used': 0.5
        }