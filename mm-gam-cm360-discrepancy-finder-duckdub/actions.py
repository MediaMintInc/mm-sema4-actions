import errno
from pydantic import ConfigDict
from sema4ai.actions import action, chat, Response, Request
import duckdb
import os
from pathlib import Path
import re
import json
import pandas as pd
import logging
from typing import Any, Union

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DUCKDB_DIR = r"data"
os.makedirs(DUCKDB_DIR, exist_ok=True)

# Define a type alias for clarity
SerializableResult = Union[list[dict[str, Any]], dict[str, str]]

# --- Helper Function for Safe Cleaning ---
# It's often cleaner to put the cleaning logic in a helper
def _clean_value(value: Any, target_dtype: str) -> Any:
    if pd.isna(value) or value == '':
        return None # Represent missing values consistently as None

    value_str = str(value).strip()
    if value_str == '':
        return None

    if target_dtype in ["int", "int64", "float", "float64"]:
        # Remove common non-numeric chars (commas, currency symbols like $, £, €, etc.)
        # Keep decimal points and negative signs
        cleaned_str = re.sub(r"[^0-9\.\-]", "", value_str)
        if cleaned_str == '' or cleaned_str == '-': # Handle cases like only '$' or empty after cleaning
             return None
        # Use pd.to_numeric for robust conversion attempt
        numeric_val = pd.to_numeric(cleaned_str, errors='coerce')
        if pd.isna(numeric_val):
             return None # Failed conversion

        # Handle final type conversion
        if target_dtype in ["int", "int64"]:
            # Check if it's effectively an integer before converting
            if numeric_val == round(numeric_val):
                 # Use pandas nullable integer type
                return pd.NA if pd.isna(numeric_val) else int(numeric_val)
            else:
                logger.warning(f"Value '{value}' cleaned to '{numeric_val}' cannot be safely converted to int, returning as float or None.")
                # Decide: return float or None? Returning float might be safer.
                return float(numeric_val) if not pd.isna(numeric_val) else None
        else: # float, float64
             # Use standard float or None
            return float(numeric_val) if not pd.isna(numeric_val) else None

    elif target_dtype in ["str", "string"]:
        return value_str # Already stripped

    else:
        # Attempt direct conversion for other types (e.g., datetime if specified)
        # This part remains risky without more specific handlers
        try:
            # Note: This simple astype might not be robust for all types (like dates)
            # You might need specific pd.to_datetime etc. handlers here based on dtypes
            return pd.Series([value_str]).astype(target_dtype).iloc[0]
        except Exception as e:
            logger.warning(f"Could not convert value '{value_str}' to type '{target_dtype}': {e}. Returning original string.")
            return value_str # Return the original string if conversion fails


@action
def clean_csv_data(csv_path: str, dtypes: dict) -> Response[SerializableResult]:
    """
    Load and clean CSV data using specified data types for columns.

    Cleaning includes:
        - Stripping leading/trailing whitespace
        - Removing commas, currency symbols, and other non-numeric characters from numeric fields
        - Converting to specified data types

    Parameters:
        csv_path (str): File path to the input CSV file.
        dtypes (dict): Dictionary mapping column names to desired data types.

    Returns:
        Response containing the cleaned data as a list of dictionaries
        or an error dictionary.
    """
    try:
        file_path = Path(csv_path)
        if not file_path.is_file():
             logger.error(f"CSV file not found: {csv_path}")
             return Response(result={"error": f"CSV file not found: {csv_path}"})

        # Load the CSV with all fields initially as string to allow cleaning
        # keep_default_na=False and na_values=[''] helps control NA interpretation initially
        raw_dataframe: pd.DataFrame = pd.read_csv(
            file_path, dtype=str, keep_default_na=False, na_values=['']
        )
        logger.debug(f"Raw DataFrame head (first 5 rows):\n{raw_dataframe.head().to_string()}")
        logger.debug(f"Raw DataFrame dtypes:\n{raw_dataframe.dtypes}")

        # Create a new DataFrame for cleaned data to avoid modifying while iterating (safer)
        cleaned_data = {}

        for column_name in raw_dataframe.columns:
            target_dtype = dtypes.get(column_name)
            logger.debug(f"Processing column: {column_name} (Target type: {target_dtype})")

            if target_dtype:
                # Apply cleaning and conversion using the helper function
                cleaned_series = raw_dataframe[column_name].apply(
                    lambda x: _clean_value(x, target_dtype)
                )

                # Attempt final dtype conversion using pandas more robust methods
                if target_dtype in ["int", "int64"]:
                    # Use pandas nullable integer type
                    cleaned_data[column_name] = pd.to_numeric(cleaned_series, errors="coerce").astype("Int64")
                elif target_dtype in ["float", "float64"]:
                     # Use standard float (which supports NaN)
                    cleaned_data[column_name] = pd.to_numeric(cleaned_series, errors="coerce").astype("float64")
                elif target_dtype in ["str", "string"]:
                     # Use pandas nullable string type
                    cleaned_data[column_name] = cleaned_series.astype("string")
                else:
                    # For other types, attempt conversion, warn on failure
                    try:
                        # Example: Add specific handling for dates if needed
                        if target_dtype in ['datetime64[ns]', 'datetime']:
                             cleaned_data[column_name] = pd.to_datetime(cleaned_series, errors='coerce')
                        else:
                            cleaned_data[column_name] = cleaned_series.astype(target_dtype)
                    except Exception as e:
                        logger.warning(f"Could not apply final conversion for column '{column_name}' to type '{target_dtype}': {e}. Leaving as processed objects.")
                        cleaned_data[column_name] = cleaned_series # Keep the series as is (likely object dtype)
            else:
                # If no dtype specified, just strip whitespace and keep as string/object
                logger.debug(f"No dtype specified for column '{column_name}'. Stripping whitespace only.")
                cleaned_data[column_name] = raw_dataframe[column_name].str.strip().astype("string") # Default to string

        # Create the final cleaned DataFrame
        cleaned_dataframe = pd.DataFrame(cleaned_data)

        logger.debug(f"Cleaned DataFrame head (first 5 rows):\n{cleaned_dataframe.head().to_string()}")
        logger.debug(f"Cleaned DataFrame dtypes:\n{cleaned_dataframe.dtypes}")

        # *** THE KEY CHANGE IS HERE ***
        # Convert DataFrame to a list of dictionaries before returning
        result_list = cleaned_dataframe.to_dict(orient='records')

        # Handle potential NaT/NA values which might not be JSON serializable directly
        # Convert pd.NA to None, NaT to None or ISO format string
        serializable_result = []
        for row in result_list:
            processed_row = {}
            for key, value in row.items():
                if pd.isna(value):
                    processed_row[key] = None # Convert all NA types (NaN, NaT, pd.NA) to None for JSON
                # Optional: Convert timestamps to strings if needed by downstream systems
                # elif isinstance(value, pd.Timestamp):
                #    processed_row[key] = value.isoformat()
                else:
                    processed_row[key] = value
            serializable_result.append(processed_row)

        # Remove the model_config=model_config as it's not needed here
        # and arbitrary_types_allowed was for Pydantic *models*, not generic Responses.
        return Response(result=serializable_result)

    except FileNotFoundError: # Catch specific error if Path check somehow missed it (permissions etc.)
         logger.error(f"CSV file not found at path: {csv_path}")
         return Response(result={"error": f"CSV file not found: {csv_path}"})
    except pd.errors.EmptyDataError:
        logger.warning(f"CSV file is empty: {csv_path}")
        return Response(result=[]) # Return empty list for empty CSV
    except Exception as e:
        logger.exception(f"An unexpected error occurred while cleaning CSV data for {csv_path}: {e}")
        # Consider returning more specific error info if possible
        return Response(result={"error": f"Error cleaning CSV: {str(e)}"})

@action
def upload_reports(request: Request, gam_csv: str, cm360_csv: str, dtypes: dict) -> Response[str]:
    """
    Uploads GAM and CM360 reports into DuckDB with thread-specific tables.

    Args:
        request: Request object with headers.
        gam_csv: GAM report filepath.
        cm360_csv: CM360 report filepath.
        dtypes: Dictionary mapping column names to desired data types. Eg. {column_name_1: int, column_name_2: str, column_name_3: float, ...}

    Returns:
        Success message including table names and row counts.
    """
    logger.debug("Starting action upload_reports")
    thread_id = request.headers.get("X-INVOKED_FOR_THREAD_ID", "57fc9d4c-5a16-4aaa-b044-792ad2c355d3")
    if not thread_id:
        return Response(result="Thread ID is missing from request headers.")
    thread_id = _remove_special_chars(thread_id)

    db_path = os.path.join(DUCKDB_DIR, f"{thread_id}.duckdb")
    gam_basename, gam_tempfile = _access_file(gam_csv)
    cm360_basename, cm360_tempfile = _access_file(cm360_csv)

    gam_table = f"GAM_{thread_id}"
    cm360_table = f"CM360_{thread_id}"

    # Read all columns as strings first
    gam_response = clean_csv_data(gam_tempfile, dtypes)
    cm360_response = clean_csv_data(cm360_tempfile, dtypes)

    if gam_response.error:
        return Response(result=f'Failed to parse GAM data, {gam_response.error}')
    
    if cm360_response.error:
        return Response(result=f'Failed to parse CM360 data, {cm360_response.error}')

    gam_df = pd.DataFrame(gam_response.result)
    cm360_df = pd.DataFrame(cm360_response.result)    

    print(json.dumps(json.loads(gam_df.to_json(orient="split")), indent=2))
    print(json.dumps(json.loads(cm360_df.to_json(orient="split")), indent=2))

    con = duckdb.connect(database=db_path, read_only=False)

    con.execute(f"DROP TABLE IF EXISTS {gam_table}")
    con.execute(f"DROP TABLE IF EXISTS {cm360_table}")

    gam_df.to_sql(gam_table, con)
    cm360_df.to_sql(cm360_table, con)

    # con.register(gam_table, gam_df)
    # con.register(cm360_table, cm360_df)

    gam_count = con.execute(f"SELECT COUNT(*) FROM {gam_table}").fetchone()[0]
    cm360_count = con.execute(f"SELECT COUNT(*) FROM {cm360_table}").fetchone()[0]

    return Response(result=(
        f"Successfully loaded:\n"
        f"- {gam_count} rows from '{gam_basename}' into table '{gam_table}'\n"
        f"- {cm360_count} rows from '{cm360_basename}' into table '{cm360_table}'\n"
        f"- GAM column names: {list(gam_df.columns)}\n"
        f"- CM360 column names: {list(cm360_df.columns)}\n"
        f"Database path: {db_path}"
    ))


@action
def run_query_on_duckdb(request: Request, sql_query: str) -> Response[str]:
    """
    Run LLM-generated SQL query on the user's thread-specific DuckDB.

    Args:
        request: Request object with thread ID.
        sql_query: SQL query to run.

    Returns:
        Formatted SQL result or error message.
    """
    thread_id = request.headers.get("X-INVOKED_FOR_THREAD_ID", "57fc9d4c-5a16-4aaa-b044-792ad2c355d3")
    if not thread_id:
        return Response(result="Thread ID is missing from request headers.")
    thread_id = _remove_special_chars(thread_id)

    db_path = os.path.join(DUCKDB_DIR, f"{thread_id}.duckdb")
    con = duckdb.connect(database=db_path)

    try:
        result = con.execute(sql_query).fetchall()
        column_names = [col[0] for col in con.description]

        if not result:
            return Response(result="No discrepancies found (0 rows returned).")

        output = "Discrepancy Query Results:\n"
        output += "=" * 50 + "\n"
        output += " \\| ".join(column_names) + "\n"
        output += "-" * 50 + "\n"

        for row in result:
            output += " \\| ".join(str(value) for value in row) + "\n"

        return Response(result=output)
    except Exception as e:
        return Response(result=f"Error executing query: {str(e)}")


def _access_file(filename: str):
    filepath = Path(filename)
    orig_basename = filepath.name

    try:
        temp_file: Path = chat.get_file(orig_basename)
        temp_file_dir = temp_file.parent
        new_temp_file_name = f"{temp_file.stem}{filepath.suffix}"
        new_temp_file_path = temp_file_dir / new_temp_file_name
        temp_file = temp_file.rename(new_temp_file_path)
    except Exception as err:
        print(f"Error getting file from chat - using filename as is: {err}")
        temp_file = filepath
    return orig_basename, temp_file

def _remove_special_chars(text: str) -> str:
    return re.sub(r'[^\w\s]', '', text)