import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from psycopg2.extras import execute_values

from utils.logger import setup_logger
from utils.my_general import open_json
from utils.my_gspread import connect_to_remote_sheet, clean_float_number
from utils.my_db_functions import create_connection_w_env

# ---- LOGS ----
logger = setup_logger("expenses_gs_to_db.log")


def insert_data_to_db(df):
    conn = create_connection_w_env()

    cols = list(df.columns)

    insert_query = f"""
    INSERT INTO expenses ({', '.join(cols)})
    VALUES %s
    ON CONFLICT (start_date, end_date, total_expenses)
    DO UPDATE SET
    {', '.join([f"{col} = EXCLUDED.{col}" for col in cols if col not in ('start_date', 'end_date', 'total_expenses')])},
    created_at = NOW()
    """

    data_tuples = [tuple(x) for x in df.to_numpy()]
    
    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, data_tuples)
        conn.commit()
        logger.info(f"Inserted/updated {len(df)} rows successfully.")
    except Exception as e:
        conn.rollback()
        logger.exception(f"Failed to insert/update data. Transaction rolled back: {e}")
        raise
    finally:
        conn.close()


def define_main_month(row):
    dates = pd.date_range(row['start_date'], row['end_date'])
    month_counts = dates.month.value_counts()
    return month_counts.idxmax()

def load_gs_data():
    sh = connect_to_remote_sheet('Отчет_по_расходам_2025', 'расходы неделя')

    # берем данные до первой пустой строки
    data = sh.get_all_values()
    first_col = [i[0] for i in data]
    cut_data = data[:first_col.index('')]

    df = pd.DataFrame(cut_data).T
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)

    # обработка дат
    df_dates = df[df["Период"].str.match(r"^\d{2}\.\d{2}-\d{2}\.\d{2}$", na=False)].copy()
    df_dates[["date_from", "date_to"]] = df_dates["Период"].str.split("-", expand=True)
    year = 2025
    df_dates["start_dt"] = pd.to_datetime(
        df_dates["date_from"] + f".{year}",
        format="%d.%m.%Y")
    df_dates["end_year"] = df_dates.apply(
        lambda row: year + 1 
        if int(row["date_to"].split(".")[1]) < int(row["date_from"].split(".")[1]) 
        else year,
        axis=1)
    df_dates["end_dt"] = pd.to_datetime(
        df_dates["date_to"] + "." + df_dates["end_year"].astype(str),
        format="%d.%m.%Y")
    df_dates = df_dates.drop(columns=["end_year", 'Период', 'date_from', 'date_to'])

    # чистим форматирование
    df_final = df_dates.applymap(clean_float_number)
    rename_dct = open_json('../../data/expenses_rename.json')
    df_final.rename(columns = rename_dct, inplace=True)
    df_final['month'] = df_final.apply(define_main_month, axis=1)
    
    return df_final

if __name__ == "__main__":
    try:
        data = load_gs_data()
    except Exception as e:
        logger.exception(f"Failed to load data from gs: {e}")
        raise
    insert_data_to_db(data)