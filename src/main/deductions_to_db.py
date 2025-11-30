import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import asyncio
import requests
from time import sleep
from datetime import datetime, timedelta
from typing import Literal, Optional
from utils.logger import setup_logger
from utils.utils import load_api_tokens
from psycopg2.extras import execute_values
from utils.my_db_functions import create_connection_w_env

logger = setup_logger("deductions.log")

# def to_iso(d):
#     return datetime.strptime(d, "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00Z")

def to_iso(d):
    if isinstance(d, datetime):
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    return datetime.strptime(d, "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00Z")

def parse_dt(dt):
    if dt is None:
        return None
    return datetime.fromisoformat(dt.replace("Z", ""))

def get_wb_measurements(token,
                        date_from,
                        date_to,
                        tab: Optional[Literal["penalty", "measurement"]] = None,
                        limit=1000):
    """Fetch all warehouse-measurements or penalty reports with pagination."""

    if tab not in ("penalty", "measurement"):
        raise ValueError(f"Параметр tab должен быть одним из двух - 'penalty' или 'measurement', передано {tab}")
    
    url = "https://seller-analytics-api.wildberries.ru/api/v1/analytics/warehouse-measurements"
    headers = {"Authorization": token}
    params = {
        "dateFrom": to_iso(date_from),
        "dateTo": to_iso(date_to),
        "tab": tab,
        "limit": limit,
        "offset": 0
    }

    all_reports = []

    while True:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()["data"]

        reports = data.get("reports", [])
        all_reports.extend(reports)
        logger.info(f'Retrieved {len(reports)} rows')

        if len(reports) < limit:
            break
        
        sleep(12)
        params["offset"] += limit

    return all_reports


def insert_records(table_name, records, column_mapping, conn):
    """
    Generic insert function for PostgreSQL with rollback on error.
    
    :param table_name: str, target table
    :param records: list of dicts
    :param column_mapping: dict, {source_key: db_column_name}
    :param conn: psycopg2 connection
    """
    if not records:
        return

    db_columns = list(column_mapping.values())
    values = []

    for rec in records:
        row = []
        for key in column_mapping.keys():
            val = rec.get(key)
            if isinstance(val, str) and 'T' in val and '-' in val and 'Z' in val:
                val = parse_dt(val)
            row.append(val)
        values.append(row)

    query = f"""
        INSERT INTO {table_name} ({", ".join(db_columns)})
        VALUES %s
    """

    try:
        with conn.cursor() as cur:
            execute_values(cur, query, values)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


async def process_measurements_client(client: str, token: str, conn, date_from, date_to):
    try:
        # 1. Penalties (удержания)
        mode_penalty = "penalty"
        penalties = await asyncio.to_thread(
            get_wb_measurements, token, date_from, date_to, mode_penalty
        )

        if not penalties:
            logger.info(f'No penalty data for client {client} between {date_from} and {date_to}')
        else:
            penalties_cols = {
                "nmId": "nm_id",
                "subject": "subject",
                "dimId": "dim_id",
                "prcOver": "prc_over",
                "volume": "volume",
                "width": "width",
                "length": "length",
                "height": "height",
                "volumeSup": "volume_sup",
                "widthSup": "width_sup",
                "lengthSup": "length_sup",
                "heightSup": "height_sup",
                "photoUrls": "photo_urls",
                "dtBonus": "dt_bonus",
                "isValid": "is_valid",
                "isValidDt": "is_valid_dt",
                "reversalAmount": "reversal_amount",
                "penaltyAmount": "penalty_amount"
            }

            await asyncio.to_thread(
                insert_records,
                'deductions_warehouse_penalties',
                penalties,
                penalties_cols,
                conn
            )

        sleep(12)

        # 2. Measurements (замеры ВБ)
        mode_measurements = "measurement"

        measures = await asyncio.to_thread(
            get_wb_measurements, token, date_from, date_to, mode_measurements
        )

        if not measures:
            logger.info(f'No measurement data for client {client} between {date_from} and {date_to}')
        else:
            measures_cols = {
                "nmId": "nm_id",
                "subject": "subject",
                "dimId": "dim_id",
                "prcOver": "prc_over",
                "volume": "volume",
                "width": "width",
                "length": "length",
                "height": "height",
                "volumeSup": "volume_sup",
                "widthSup": "width_sup",
                "lengthSup": "length_sup",
                "heightSup": "height_sup",
                "photoUrls": "photo_urls",
                "dt": "dt",
                "dateStart": "date_start",
                "dateEnd": "date_end"
            }

            await asyncio.to_thread(
                insert_records,
                'deductions_measurements',
                measures,
                measures_cols,
                conn
            )

    except Exception as e:
        logger.error(f'Encountered an unexpected error while uploading data for client {client}: {e}')
        raise


async def main():
    tokens = load_api_tokens()
    conn = create_connection_w_env()

    now = datetime.now()
    yesterday = now - timedelta(days=1)
    date_from = yesterday.replace(hour=23, minute=55, second=0, microsecond=0)
    date_to = now

    tasks = []
    for client, token in tokens.items():
        tasks.append(
            asyncio.create_task(
                process_measurements_client(client, token, conn, date_from, date_to)
            )
        )

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())