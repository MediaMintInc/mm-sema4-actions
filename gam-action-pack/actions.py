"""
Google Ad Manager (GAM) Action Pack

This action pack provides functions to interact with the Google Ad Manager API,
allowing users to get network information and work with reports.
"""
from google.ads import admanager_v1
from google.oauth2.credentials import Credentials
from typing import Literal, Dict, Any
from pathlib import Path
from dotenv import load_dotenv
import os
from sema4ai.actions import action, OAuth2Secret, Response


def _convert_oauth2_token_to_credentials(token: OAuth2Secret) -> Credentials:
    """
    Helper function to convert OAuth2Secret to google.oauth2.credentials.Credentials
    
    Args:
        token: OAuth2Secret token from Sema4 AI
    
    Returns:
        Google OAuth2 Credentials
    """
    # Check if token is already a Credentials object
    if isinstance(token, Credentials):
        return token
    
    # Create a new credentials object using the token's access_token property
    # We directly use the property as intended in the _OAuth2SecretInActionContext class
    try:
        # This will trigger the property access correctly
        token_value = token.access_token
        
        # Create and return credentials with the token
        return Credentials(
            token=token_value,
            scopes=["https://www.googleapis.com/auth/admanager"]
        )
    except Exception as e:
        # If anything goes wrong, provide a diagnostic error
        raise ValueError(
            f"Could not get access_token from OAuth2Secret. Error: {str(e)}"
        ) from e


@action(is_consequential=False)
def get_network(
    token: OAuth2Secret[
        Literal["google"],
        list[Literal["https://www.googleapis.com/auth/admanager"]],
    ],
    network_code: str,
) -> Response[dict]:
    """
    Get information about the GAM network.
    
    Args:
        token: OAuth2 token for Google Ad Manager API
        network_code: The network code for your GAM account
    
    Returns:
        Network information
    """
    # Convert OAuth2Secret to Credentials object
    credentials = _convert_oauth2_token_to_credentials(token)
    
    client = admanager_v1.NetworkServiceClient(credentials=credentials)
    
    request = admanager_v1.GetNetworkRequest(
        name=f"networks/{network_code}",
    )
    
    response = client.get_network(request=request)
    
    # Convert the response to a JSON-serializable dictionary
    result = {
        "name": response.name,
        "display_name": response.display_name,
        "network_code": response.network_code,
        "property_code": response.property_code if hasattr(response, 'property_code') else None,
        "currency_code": response.currency_code,
        "network_id": response.network_id if hasattr(response, 'network_id') else None
    }
    
    # Handle time_zone specially
    if hasattr(response, 'time_zone'):
        if isinstance(response.time_zone, str):
            result["time_zone"] = response.time_zone
        elif hasattr(response.time_zone, 'id'):
            result["time_zone"] = response.time_zone.id
        else:
            result["time_zone"] = str(response.time_zone)
    
    # Handle effective_root_ad_unit specially
    if hasattr(response, 'effective_root_ad_unit'):
        if isinstance(response.effective_root_ad_unit, str):
            result["effective_root_ad_unit"] = response.effective_root_ad_unit
        else:
            result["effective_root_ad_unit"] = str(response.effective_root_ad_unit)
    
    # Add second_currencies if present
    if hasattr(response, 'second_currencies') and response.second_currencies:
        result["second_currencies"] = list(response.second_currencies)
    
    return Response(result=result)

@action(is_consequential=False)
def get_report(
    token: OAuth2Secret[
        Literal["google"],
        list[Literal["https://www.googleapis.com/auth/admanager"]],
    ],
    network_code: str,
    report_id: str,
) -> Response[dict]:
    """
    Get information about a specific report.
    
    Args:
        token: OAuth2 token for Google Ad Manager API
        network_code: The network code for your GAM account
        report_id: The ID of the report to get information about
    
    Returns:
        Report information
    """
    # Convert OAuth2Secret to Credentials object
    credentials = _convert_oauth2_token_to_credentials(token)
    
    client = admanager_v1.ReportServiceClient(credentials=credentials)
    
    report_name = f"networks/{network_code}#reports/reports/{report_id}"
    
    request = admanager_v1.GetReportRequest(
        name=report_name,
    )
    response = client.get_report(request=request)
    
    # Return the raw response as a string
    result = {
        "report": str(response)
    }
    
    return Response(result=result)

@action(is_consequential=True)
def run_report(
    token: OAuth2Secret[
        Literal["google"],
        list[Literal["https://www.googleapis.com/auth/admanager"]],
    ],
    network_code: str,
    report_id: str,
) -> Response[dict]:
    """
    Run a specific report and retrieve the results.
    
    Args:
        token: OAuth2 token for Google Ad Manager API
        network_code: The network code for your GAM account
        report_id: The ID of the report to run
    
    Returns:
        Report results
    """
    # Convert OAuth2Secret to Credentials object
    credentials = _convert_oauth2_token_to_credentials(token)
    
    client = admanager_v1.ReportServiceClient(credentials=credentials)
    
    report_name = f"networks/{network_code}/reports/{report_id}"
    
    request = admanager_v1.RunReportRequest(
        name=report_name,
    )
    
    operation = client.run_report(request=request)
    response = operation.result()
    
    # Return the raw response as a string
    result = {
        "report": str(response)
    }
    
    return Response(result=result)

@action(is_consequential=False)
def list_reports(
    token: OAuth2Secret[
        Literal["google"],
        list[Literal["https://www.googleapis.com/auth/admanager"]],
    ],
    network_code: str,
) -> Response[dict]:
    """
    List all reports available in the GAM network.
    
    Args:
        token: OAuth2 token for Google Ad Manager API
        network_code: The network code for your GAM account
    
    Returns:
        List of available reports
    """
    # Convert OAuth2Secret to Credentials object
    credentials = _convert_oauth2_token_to_credentials(token)
    
    client = admanager_v1.ReportServiceClient(credentials=credentials)
    
    parent = f"networks/{network_code}"
    
    request = admanager_v1.ListReportsRequest(
        parent=parent,
    )
    
    # Make the request and get the paginated results
    page_result = client.list_reports(request=request)
    
    # Return the raw response directly
    reports = []
    for report in page_result:
        # Convert each report object to a string representation
        reports.append(str(report))
    
    result = {
        "reports": reports
    }
    
    return Response(result=result)

@action(is_consequential=False)
def fetch_report_result_rows(
    token: OAuth2Secret[
        Literal["google"],
        list[Literal["https://www.googleapis.com/auth/admanager"]],
    ],
    network_code: str,
    report_id: str,
    report_job_id: str,
) -> Response[dict]:
    """
    Fetch the result rows from a report job that has been run.
    
    Args:
        token: OAuth2 token for Google Ad Manager API
        network_code: The network code for your GAM account
        report_id: The ID of the report to fetch results for
        report_job_id: The ID of the report job to fetch results for
    
    Returns:
        Report result rows
    """
    # Convert OAuth2Secret to Credentials object
    credentials = _convert_oauth2_token_to_credentials(token)
    
    client = admanager_v1.ReportServiceClient(credentials=credentials)
    
    # Construct the report job name
    report_job_name = f"networks/{network_code}/reports/{report_id}/results/{report_job_id}"
    
    # Create the request without using parent parameter
    request = admanager_v1.FetchReportResultRowsRequest(name=report_job_name)
    
    # Make the request with the report job name as a parameter to the method
    page_result = client.fetch_report_result_rows(request=request)
    
    # Return the raw response directly
    rows = []
    for row in page_result:
        # Convert each row object to a string representation
        rows.append(str(row))
    
    result = {
        "rows": rows
    }
    
    return Response(result=result) 