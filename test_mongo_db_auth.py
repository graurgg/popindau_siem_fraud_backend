# test_mongodb_auth.py
from pymongo import MongoClient
from pymongo.errors import OperationFailure, ServerSelectionTimeoutError

def test_mongodb_connection():
    print("🔐 Testing MongoDB authentication...")
    
    # Test with application user
    app_connection_string = "mongodb://admin:password@localhost:27017/fraud_detection?authSource=admin"
    
    try:
        print("Testing application user connection...")
        client = MongoClient(app_connection_string, serverSelectionTimeoutMS=5000)
        db = client.fraud_detection
        
        # Test authentication
        db.command('ping')
        print("✅ Application user authentication successful!")
        
        # Test write operation
        test_doc = {
            'test': 'connection_test',
            'timestamp': 'now',
            'message': 'This is a test document'
        }
        
        result = db.transactions.insert_one(test_doc)
        print(f"✅ Write operation successful! Document ID: {result.inserted_id}")
        
        # Clean up test document
        db.transactions.delete_one({'_id': result.inserted_id})
        print("✅ Cleanup successful")
        
        client.close()
        return True
        
    except OperationFailure as e:
        print(f"❌ Authentication failed: {e}")
        return False
    except ServerSelectionTimeoutError as e:
        print(f"❌ Server not available: {e}")
        return False
    except Exception as e:
        print(f"❌ Other error: {e}")
        return False

def test_admin_connection():
    print("\nTesting admin connection...")
    admin_connection_string = "mongodb://admin:password@localhost:27017/admin"
    
    try:
        client = MongoClient(admin_connection_string, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("✅ Admin authentication successful!")
        
        # List databases
        dbs = client.list_database_names()
        print(f"📊 Available databases: {dbs}")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ Admin connection failed: {e}")
        return False

if __name__ == "__main__":
    print("🧪 MongoDB Authentication Test")
    print("=" * 40)
    
    success1 = test_mongodb_connection()
    success2 = test_admin_connection()
    
    if success1 and success2:
        print("\n🎉 All MongoDB tests passed!")
    else:
        print("\n❌ Some tests failed. Check the MongoDB setup.")