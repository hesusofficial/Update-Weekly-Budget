import os
import json
from typing import List, Any, Tuple
from decimal import Decimal
from datetime import date, datetime, timedelta, timezone

import snowflake.connector
from google.oauth2 import service_account
from googleapiclient.discovery import build


# ---------- DATE RANGE / QUERY GENERATION ----------

def compute_date_range() -> Tuple[date, date]:
    """
    Rolling weekly window:
      START = today (date of run)
      END   = START + 75 days
    """
    start_date = date.today()
    end_date = start_date + timedelta(days=75)
    print(f"DATE RANGE  →  {start_date}  →  {end_date} (75-day rolling window)")
    return start_date, end_date


def generate_query(start_date: date, end_date: date) -> str:
    """Generate the Snowflake query with dynamic date range."""
    return f"""
        SELECT
          wiba.WORK_ITEM_ID,
          TRIM(wiba.WORK_TITLE) AS WORK_TITLE,
          wiba.WORK_TYPE,
          TRIM(wid.CLIENT) AS CLIENT,
          CONCAT_WS(' ', TRIM(wiba.WORK_TITLE), TRIM(wid.CLIENT)) AS WORK_CLIENT,
          wiba.USER_ID,
          wiba.USER_NAME,
          wiba.ROLE_NAME,
          wiba.TASK_TYPE,
          wid.SECONDARY_STATUS,
          ROUND(wid.BUDGETED_MINUTES / 60.0, 1) AS TOTAL_HOURS,
          wid.BUDGET_REMAINING_HOURS,
          ROUND(wiba.BUDGETED_MINUTES / 60.0, 1) AS BUDGETED_HOURS,
          ROUND(wiba.ACTUAL_MINUTES / 60.0, 1) AS ACTUAL_HOURS,
          ROUND((wiba.BUDGETED_MINUTES / 60.0) - (wiba.ACTUAL_MINUTES / 60.0), 1) AS REMAINING,
          TO_DATE(wid.START_DATETIME) AS START_DATE,
          TO_DATE(wid.DUE_DATETIME) AS DUE_DATE

        FROM WORK_ITEM_BUDGET_VS_ACTUAL wiba
        JOIN WORK_ITEM_DETAILS wid ON wiba.WORK_ITEM_ID = wid.WORK_ITEM_ID

        WHERE wiba.BUDGETED_MINUTES >= 0
          AND wid.START_DATETIME BETWEEN '{start_date}' AND '{end_date}'

        ORDER BY wiba.WORK_ITEM_ID ASC;
    """


# ---------- SNOWFLAKE ----------

def _normalize_snowflake_account(raw: str) -> str:
    """Clean up SNOWFLAKE_ACCOUNT if a full URL or whitespace was pasted."""
    raw = raw.strip()
    if raw.startswith("https://"):
        raw = raw[len("https://"):]
    elif raw.startswith("http://"):
        raw = raw[len("http://"):]
    if ".snowflakecomputing.com" in raw:
        raw = raw.split(".snowflakecomputing.com")[0]
    return raw


def get_snowflake_connection():
    """Create a Snowflake connection from environment variables."""
    raw_account = os.environ["SNOWFLAKE_ACCOUNT"]
    account = _normalize_snowflake_account(raw_account)

    print(f"\nConnecting to Snowflake account: {repr(account)}\n")

    conn = snowflake.connector.connect(
        account=account,
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )
    return conn


def fetch_budget_data(start_date: date, end_date: date) -> Tuple[List[str], List[List[Any]]]:
    """Run the Snowflake query and return headers + normalized rows."""
    query = generate_query(start_date, end_date)
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        headers = [col[0] for col in cursor.description]

        def normalize(value):
            if value is None:
                return ""
            if isinstance(value, Decimal):
                return float(value)
            if hasattr(value, "isoformat"):
                return value.isoformat()
            return value

        data_rows = [[normalize(v) for v in row] for row in rows]
        return headers, data_rows

    finally:
        cursor.close()
        conn.close()


# ---------- GOOGLE SHEETS CORE SERVICE ----------

def get_sheets_service():
    """Build a Google Sheets API service using service account JSON in an env var."""
    raw_json = os.environ["GOOGLE_CREDENTIALS_JSON"]
    info = json.loads(raw_json)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)

    service = build("sheets", "v4", credentials=creds)
    return service


# ---------- WRITE MAIN DATA (BudgetData TAB) ----------

def write_to_sheet(headers: List[str], rows: List[List[Any]], tab_name: str = "BudgetData"):
    """Clear the tab and write new data starting at A1."""
    spreadsheet_id = os.environ["GOOGLE_SHEET_ID"]
    service = get_sheets_service()
    sheet_range = f"{tab_name}!A1"

    # Clear existing data on the tab
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=tab_name,
        body={}
    ).execute()

    # Prepare body: header row + data rows
    values = [headers] + rows
    body = {"values": values}

    # Write data
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=sheet_range,
        valueInputOption="RAW",
        body=body,
    ).execute()


# ---------- LOGGING (Log TAB) ----------

def ensure_log_sheet(service, spreadsheet_id: str, tab_name: str = "Log"):
    """
    Ensure a 'Log' sheet exists with a header row.
    If it doesn't exist, create it and add headers.
    """
    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get("sheets", [])

    for s in sheets:
        title = s.get("properties", {}).get("title")
        if title == tab_name:
            return  # already exists

    # Create the Log sheet
    body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": tab_name
                    }
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()

    # Write header row
    headers = [["Run Timestamp (UTC)", "Start Date", "End Date", "Row Count"]]
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A1",
        valueInputOption="RAW",
        body={"values": headers}
    ).execute()


def log_run(start_date: date, end_date: date, row_count: int, tab_name: str = "Log"):
    """Append a log row with timestamp, date range, and row count."""
    spreadsheet_id = os.environ["GOOGLE_SHEET_ID"]
    service = get_sheets_service()

    # Make sure Log sheet exists
    ensure_log_sheet(service, spreadsheet_id, tab_name)

    # Timestamp in UTC
    timestamp_utc = datetime.now(timezone.utc).isoformat()

    values = [[
        timestamp_utc,
        start_date.isoformat(),
        end_date.isoformat(),
        row_count,
    ]]

    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A2:D2",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": values}
    ).execute()


# ---------- MAIN ----------

def main():
    # 1. Compute rolling window
    start_date, end_date = compute_date_range()

    # 2. Fetch data from Snowflake
    print("\nFetching data from Snowflake...")
    headers, rows = fetch_budget_data(start_date, end_date)
    row_count = len(rows)
    print(f"Fetched {row_count} rows.")

    # 3. Write to BudgetData tab
    print("Writing data to Google Sheet (BudgetData tab)...")
    write_to_sheet(headers, rows, tab_name="BudgetData")
    print("Finished writing BudgetData.")

    # 4. Log the run
    print("Logging run in Log tab...")
    log_run(start_date, end_date, row_count, tab_name="Log")
    print("Log entry added.")
    print("\nDone.\n")


if __name__ == "__main__":
    main()
