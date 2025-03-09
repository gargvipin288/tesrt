import os
from lambda_handler import lambda_handler
from dotenv import load_dotenv
from mock_data import setup_mock_aws
from moto import mock_aws

# Load environment variables from .env file
load_dotenv()

@mock_aws
def main():
    # Set up mock AWS services
    setup_mock_aws()
    
    # Create a test event (empty event since the Lambda doesn't seem to use event parameters)
    test_event = {}
    test_context = None
    
    try:
        # Call the lambda handler
        result = lambda_handler(test_event, test_context)
        print("Lambda execution result:", result)
    except Exception as e:
        print(f"Error executing lambda: {e}")

if __name__ == "__main__":
    main() 
