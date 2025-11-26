import os
import json
from typing import List, Any

import snowflake.connector
from google.oauth2 import service_account
from googleapiclient.discovery import build


SNOWFLAKE_QUERY = """
SELECT
  wiba.WORK_ITEM_ID,
  TRIM(wiba.WORK_TITLE) AS WORK_TITLE,
  wiba.WORK_TYPE,
  TRIM(wid.CLIENT) AS CLIENT,

  -- CLEAN, CONTROLLED SPACING
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

  -- REMAINING HOURS (Budget - Actual)
  ROUND((wiba.BUDGETED_MINUTES / 60.0) - (wiba.ACTUAL_MINUTES / 60.0), 1) AS REMAINING,

  TO_DATE(wid.START_DATETIME) AS START_DATE,
  TO_DATE(wid.DUE_DATETIME) AS DUE_DATE

FROM WORK_ITEM_BUDGET_VS_ACTUAL wiba
JOIN WORK_ITEM_DETAILS wid
    ON wiba.WORK_ITEM_ID = wid.WORK_ITEM_ID

WHERE wiba.BUDGETED_MINUTES >= 0
  AND wid.START_DATETIME BETWEEN '2025-10-01' AND '2025-12-15'

ORDER BY wiba.WORK_ITEM_ID ASC;
"""


def get_snowflake_connection():
    """Create a Snowflake connection from environment variables."""
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )
    return conn


def fetch_budget_data() -> (List[str], List[List[Any]]):
    """Run the Snowflake query and return headers + rows."""
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(SNOWFLAKE_QUERY)
        rows = cursor.fetchall()

        # Column names from cursor.description
        headers = [col[0] for col in cursor.description]

        # Convert all values to basic Python types / strings for Sheets
        def normalize(value):
            if value is None:
                return ""
            if hasattr(value, "isoformat"):
                # dates / datetimes
                return value.isoformat()
            return value

        data_rows = [[normalize(v) for v in row] for row in rows]
        return headers, data_rows
    finally:
        cursor.close()
        conn.close()


def get_sheets_service():
    """Build a Google Sheets API service using service account JSON in an env var."""
    raw_json = os.environ["GOOGLE_CREDENTIALS_JSON"]
    info = json.loads(raw_json)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)

    service = build("sheets", "v4", credentials=creds)
    return service


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


def main():
    print("Fetching data from Snowflake...")
    headers, rows = fetch_budget_data()
    print(f"Fetched {len(rows)} rows.")

    print("Writing data to Google Sheet...")
    write_to_sheet(headers, rows, tab_name="BudgetData")
    print("Done.")


if __name__ == "__main__":
    main()

