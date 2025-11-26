import os
import json
from typing import List, Any, Tuple
from decimal import Decimal
from datetime import date, datetime, timedelta, timezone

import snowflake.connector
from google.oauth2 import service_account
from googleapiclient.discovery import build


# ========================================================================
# 1. DATE WINDOWS
# ========================================================================

def compute_budget_date_range() -> Tuple[date, date]:
    """
    Rolling window centered on today:
      START = today - 75 days
      END   = today + 75 days
    """
    today = date.today()
    start_date = today - timedelta(days=75)
    end_date = today + timedelta(days=75)
    print(f"📅 BudgetData RANGE  →  {start_date}  →  {end_date} (±75 days)")
    return start_date, end_date


def compute_time_entries_range() -> Tuple[date, date]:
    """
    Previous week Monday–Sunday window, regardless of run day.

    Example:
      If today is Tue 2025-11-25:
        this_monday = 2025-11-24
        prev_monday = 2025-11-17
        prev_sunday = 2025-11-23
    """
    today = date.today()
    this_monday = today - timedelta(days=today.weekday())  # Monday of current week
    prev_monday = this_monday - timedelta(days=7)
    prev_sunday = prev_monday + timedelta(days=6)
    print(f"🕒 TimeEntries RANGE →  {prev_monday}  →  {prev_sunday} (prev Mon–Sun)")
    return prev_monday, prev_sunday


def generate_budget_query(start_date: date, end_date: date) -> str:
    """Generate the Snowflake query for BudgetData with dynamic date range."""
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


def generate_time_entries_query(start_date: date, end_date: date) -> str:
    """Generate the Snowflake query for TimeEntriesData (previous week Mon–Sun)."""
    return f"""
        SELECT *
        FROM user_time_entry_detail
        WHERE reporting_date BETWEEN '{start_date}' AND '{end_date}';
    """


# ========================================================================
# 2. SNOWFLAKE
# ========================================================================

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


def _normalize_value(value: Any) -> Any:
    """Normalize values for JSON/Sheets."""
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def run_query(sql: str) -> Tuple[List[str], List[List[Any]]]:
    """Run a SQL query in Snowflake and return headers + normalized rows."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        headers = [col[0] for col in cursor.description]
        data_rows = [[_normalize_value(v) for v in row] for row in rows]
        return headers, data_rows
    finally:
        cursor.close()
        conn.close()


# ========================================================================
# 3. GOOGLE SHEETS
# ========================================================================

def get_sheets_service():
    """Build a Google Sheets API service using service account JSON in an env var."""
    raw_json = os.environ["GOOGLE_CREDENTIALS_JSON"]
    info = json.loads(raw_json)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)

    service = build("sheets", "v4", credentials=creds)
    return service


def write_to_sheet(headers: List[str], rows: List[List[Any]], tab_name: str):
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


# ========================================================================
# 4. LOGGING (Log TAB)
# ========================================================================

def ensure_log_sheet(service, spreadsheet_id: str, tab_name: str = "Log"):
    """
    Ensure a 'Log' sheet exists and always has the latest header row.
    """
    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get("sheets", [])

    exists = any(
        s.get("properties", {}).get("title") == tab_name
        for s in sheets
    )

    if not exists:
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

    # Always update header row to the latest structure
    headers = [[
        "Run Timestamp (UTC)",
        "Budget Start Date",
        "Budget End Date",
        "Budget Row Count",
        "TimeEntries Start Date",
        "TimeEntries End Date",
        "TimeEntries Row Count",
    ]]
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A1",
        valueInputOption="RAW",
        body={"values": headers}
    ).execute()


def log_run(
    budget_start: date,
    budget_end: date,
    budget_rows: int,
    te_start: date,
    te_end: date,
    te_rows: int,
    tab_name: str = "Log",
):
    """Append a log row with timestamp + date ranges + row counts."""
    spreadsheet_id = os.environ["GOOGLE_SHEET_ID"]
    service = get_sheets_service()

    # Make sure Log sheet exists and header is correct
    ensure_log_sheet(service, spreadsheet_id, tab_name)

    # Timestamp in UTC
    timestamp_utc = datetime.now(timezone.utc).isoformat()

    values = [[
        timestamp_utc,
        budget_start.isoformat(),
        budget_end.isoformat(),
        budget_rows,
        te_start.isoformat(),
        te_end.isoformat(),
        te_rows,
    ]]

    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A2:G2",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": values}
    ).execute()


# ========================================================================
# 5. MAIN
# ========================================================================

def main():
    # --- Compute date ranges for both queries ---
    budget_start, budget_end = compute_budget_date_range()
    te_start, te_end = compute_time_entries_range()

    # --- BudgetData query ---
    print("\n🔹 Fetching BudgetData from Snowflake...")
    budget_sql = generate_budget_query(budget_start, budget_end)
    budget_headers, budget_rows = run_query(budget_sql)
    print(f"BudgetData: {len(budget_rows)} rows.")
    print("Writing BudgetData tab...")
    write_to_sheet(budget_headers, budget_rows, tab_name="BudgetData")

    # --- TimeEntriesData query ---
    print("\n🔹 Fetching TimeEntriesData from Snowflake...")
    te_sql = generate_time_entries_query(te_start, te_end)
    te_headers, te_rows = run_query(te_sql)
    print(f"TimeEntriesData: {len(te_rows)} rows.")
    print("Writing TimeEntriesData tab...")
    write_to_sheet(te_headers, te_rows, tab_name="TimeEntriesData")

    # --- Log both ranges + counts ---
    print("\n📝 Logging run in Log tab...")
    log_run(
        budget_start=budget_start,
        budget_end=budget_end,
        budget_rows=len(budget_rows),
        te_start=te_start,
        te_end=te_end,
        te_rows=len(te_rows),
        tab_name="Log",
    )
    print("✅ Log entry added.\n")


if __name__ == "__main__":
    main()
