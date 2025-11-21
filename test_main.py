from fastapi.testclient import TestClient
from main import app
import json

def test_create_user():
    """Simple test for the create_user endpoint"""
    client = TestClient(app)
    
    # Test data
    test_data = {
        "user_id": 1,
        "username": "testuser"
    }
    
    # Make POST request
    response = client.post("/create_user/", json=test_data)
    
    # Check status code
    assert response.status_code == 200, f"Expected status 200, got {response.status_code}"
    
    # Check response data
    result = response.json()
    assert result["msg"] == "we got data succesfully", "Message doesn't match"
    assert result["user_id"] == 1, "User ID doesn't match"
    assert result["username"] == "testuser", "Username doesn't match"
    
    print("✅ All tests passed!")
    print(f"Response: {json.dumps(result, indent=2)}")

if __name__ == "__main__":
    try:
        test_create_user()
    except AssertionError as e:
        print(f"❌ Test failed: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

