# GAM Action Pack

A Sema4 AI action pack for interacting with the Google Ad Manager API.

## Setup

1. Create a Google Ad Manager API project in the Google Cloud Console
2. Set up OAuth2 authentication for your application
3. Ensure your OAuth2 token has the scope: `https://www.googleapis.com/auth/admanager`

## Authentication

This action pack uses the OAuth2 authentication flow to access the Google Ad Manager API. The OAuth2Secret provided by Sema4 AI is converted to Google OAuth2 Credentials before being used with the Google Ad Manager clients.

The action pack is designed to work directly with Sema4 AI's OAuth2Secret objects with minimal property access to avoid potential runtime errors in the authentication process. It uses a series of fallback mechanisms to ensure compatibility:

1. **Direct Credentials Conversion**: Attempts to use the token object's `to_credentials()` method if available
2. **Safe Attribute Access**: Uses `__dict__` to safely access token attributes without triggering property methods
3. **Direct Passthrough**: Passes the token object directly to the Google client if it's already a Credentials object
4. **Minimal Placeholder**: Creates a minimal credentials object with placeholder values as a last resort

For local development:
- Use `gcloud auth application-default login --scopes="https://www.googleapis.com/auth/admanager"` to set up local credentials
- Or authenticate as a Service Account by setting the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to your service account key file

For production:
- The OAuth2 token is provided by the Sema4 AI platform through the function parameter
- The action pack has been optimized to work with the Sema4 AI token handling system

## Available Actions

### get_network(token, network_code)
Retrieves information about your Google Ad Manager network.

- **token**: OAuth2 token for Google Ad Manager API
- **network_code**: Your GAM network code

### get_report(token, network_code, report_id)
Gets information about a specific report by its ID.

- **token**: OAuth2 token for Google Ad Manager API
- **network_code**: Your GAM network code
- **report_id**: The ID of the report to get information about

### run_report(token, network_code, report_id)
Runs a report and returns the results.

- **token**: OAuth2 token for Google Ad Manager API
- **network_code**: Your GAM network code
- **report_id**: The ID of the report to run

## Example Usage

```python
# Get network information (using Sema4 AI OAuth2 integration)
network_info = get_network(token=my_oauth2_token, network_code="32252587")
print(f"Network: {network_info.result['display_name']}")

# Get report details
report_info = get_report(token=my_oauth2_token, network_code="32252587", report_id="12345678")
print(f"Report status: {report_info.result['status']}")

# Run a report
report_results = run_report(token=my_oauth2_token, network_code="32252587", report_id="12345678")
print(f"Report job ID: {report_results.result['report_job_id']}")
```

## Testing With Direct Credentials

For testing locally without the Sema4 AI platform, you can pass credentials directly:

```python
from google.oauth2.credentials import Credentials

# Create a credentials object directly
credentials = Credentials(
    token="your_access_token_here",
    scopes=["https://www.googleapis.com/auth/admanager"]
)

# Use it with the actions
network_info = get_network(token=credentials, network_code="32252587")
``` 