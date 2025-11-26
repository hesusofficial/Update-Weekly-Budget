import os
import json
from typing import List, Any
from decimal import Decimal
from datetime import date, timedelta

import snowflake.connector
from google.oauth2 import service_account
from googleapiclient.discovery import build


def generate_query():
    """
    Creates rolling weekly date window:
    👉 START = Today (Tuesday run)
    👉 END   = START + 75 days
    """

    start_date = date.today()                 # (moves automatically each run)
    end_date   = start_date + timedelta(days=75)

    print(f"DATE RANGE --> {start_date}  →  {end_date} (75-day rolling window)")

    return f"""
        SELECT
          wiba.WORK_ITEM_ID,
          TRIM(wiba.WORK_TITLE) AS WORK_TITLE,
          wiba.WORK_TYPE,
          TRIM(wid.CLIENT) AS CLIENT,
          CONCAT_WS(' ', TRIM(wiba.WORK_TITLE), TRIM(wid.CLIENT)) AS WORK_CLIENT,
          wiba.USER_ID, wiba.USER_NAME, wiba.ROLE_NAME, wiba.TASK_TYPE,
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


def _normalize_snowflake_account(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("https://"): raw = raw[8:]
    if raw.startswith("http://"): raw = raw[7:]
    if ".snowflakecomputing.com" in raw: raw = raw.split(".snowflakecomputing.com")[0]
    return raw


def get_snowflake_connection():
    account = _normalize_snowflake_account(os.environ["SNOWFLAKE_ACCOUNT"])
    print(f"\nConnecting → {account}\n")

    return snowflake.connector.connect(
        account=account,
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )


def fetch_budget_data():
    query = generate_query()
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        headers = [col[0] for col in cursor.description]

        def normalize(value):
            if value is None: return ""
            if isinstance(value, Decimal): return float(value)
            if hasattr(value, "isoformat"): return value.isoformat()
            return value

        return headers, [[normalize(v) for v in row] for row in rows]

    finally:
        cursor.close()
        conn.close()


def get_sheets_service():
    info = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
    creds = service_account.Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds)


def write_to_sheet(headers, rows, tab_name="BudgetData"):
    spreadsheet_id = os.environ["GOOGLE_SHEET_ID"]
    service = get_sheets_service()

    # Clear sheet
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id, range=tab_name, body={}
    ).execute()

    # Insert
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A1",
        valueInputOption="RAW",
        body={"values": [headers] + rows},
    ).execute()


def main():
    print("\nFetching Weekly Rolling Snowflake Data…")
    headers, rows = fetch_budget_data()
    print(f"→ {len(rows)} rows retrieved")

    print("\nUpdating Google Sheet…")
    write_to_sheet(headers, rows)
    print("\n✔ Sheet Updated Successfully\n")


if __name__ == "__main__":
    main()
