"""
Test script for GAM Action Pack

This script demonstrates how to use the GAM Action Pack functions.
For testing purposes only.
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import json
import google.auth

# Import the actions
from actions import get_network, get_report, run_report

# Load environment variables for testing
load_dotenv(Path(__file__).absolute().parent / "devdata" / ".env")

# Sample network code
NETWORK_CODE = os.getenv("GAM_NETWORK_CODE", "32252587")

def main():
    """Main test function"""
    print("GAM Action Pack Test")
    print("====================")
    
    # For testing locally, we create a direct credentials object instead of mocking OAuth2Secret
    try:
        # Try to load from application default credentials
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/admanager"]
        )
        
        # If credentials need refreshing
        if credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
    except Exception as e:
        print(f"Warning: Could not load default credentials: {e}")
        # If that fails, create a basic Credentials object for testing
        credentials = Credentials(
            token="YOUR_ACCESS_TOKEN",  # Replace with real token for testing
            scopes=["https://www.googleapis.com/auth/admanager"]
        )
    
    # For Sema4 AI OAuth2Secret simulation, we can also create simple passthrough objects
    class DirectCredentialsWrapper:
        """A simple wrapper that directly returns a Credentials object"""
        def __init__(self, credentials):
            self.credentials = credentials
        
        def to_credentials(self):
            """Return the wrapped credentials object"""
            return self.credentials
    
    # Create a wrapper around the credentials
    token_wrapper = DirectCredentialsWrapper(credentials)
    
    try:
        # Get network information using the credentials directly
        print("\nTesting get_network with direct credentials...")
        network_response = get_network(token=credentials, network_code=NETWORK_CODE)
        print(f"Network: {json.dumps(network_response.result, indent=2)}")
        
        # Test with the wrapper object
        print("\nTesting get_network with credentials wrapper...")
        wrapper_response = get_network(token=token_wrapper, network_code=NETWORK_CODE)
        print(f"Wrapper Network: {json.dumps(wrapper_response.result, indent=2)}")
        
        # You can uncomment and modify the following test cases as needed
        
        # # Get report information (you need a valid report ID)
        # print("\nTesting get_report()...")
        # report_id = "12345678"  # Replace with a real report ID
        # report_response = get_report(token=credentials, network_code=NETWORK_CODE, report_id=report_id)
        # print(f"Report: {json.dumps(report_response.result, indent=2)}")
        
        # # Run a report (you need a valid report ID)
        # print("\nTesting run_report()...")
        # report_id = "12345678"  # Replace with a real report ID
        # run_response = run_report(token=credentials, network_code=NETWORK_CODE, report_id=report_id)
        # print(f"Run report: {json.dumps(run_response.result, indent=2)}")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 