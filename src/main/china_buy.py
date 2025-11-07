# ---- IMPORTS ----

# making it work for cron
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# libraries
from datetime import datetime
import pandas as pd
import gspread
import logging
import os

# my packages
from utils.env_loader import *
from utils.my_db_functions import create_connection_w_env, fetch_db_data_into_dict, list_to_sql_select
from utils.my_gspread import column_number_to_letter, add_data_to_range, clean_number, connect_to_local_sheet
from utils.my_general import open_json


# ---- SET UP ----

CREDS_PATH=os.getenv('CREDS_PATH')
PRO_CREDS_PATH=os.getenv('PRO_CREDS_PATH')

UNIT_TABLE=os.getenv('UNIT_TABLE')

PURCHASE_TABLE=os.getenv('PURCHASE_TABLE')

CHINA_TABLE=os.getenv('CHINA_TABLE')
CHINA_ORDERS=os.getenv('CHINA_ORDERS')
CHINA_COUNT=os.getenv('CHINA_COUNT')

ITEMS_FIXED_PRICE=os.getenv('ITEMS_FIXED_PRICE')

DB_PURCHASE_PRICE=os.getenv('DB_PURCHASE_PRICE')
DB_ANALYSIS=os.getenv('DB_ANALYSIS')


# ---- LOGS ----

LOGS_PATH = os.getenv("LOGS_PATH")

os.makedirs(LOGS_PATH, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOGS_PATH}/china_buy.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def load_db_data(wilds):

    if not isinstance(wilds, list):
        wilds = list(wilds)

    conn = create_connection_w_env()
    wilds_sql = list_to_sql_select(wilds, extra_quotes = True)

    query = f'''
    with week as (
        select
            date,
            local_vendor_code,
            sum(orders_count) as orders_per_day
    from {DB_ANALYSIS}
    where
        date BETWEEN CURRENT_DATE - INTERVAL '7 days' AND CURRENT_DATE - INTERVAL '1 day'
    group by
        date,
        local_vendor_code),
    two_weeks as (
        select
            date,
            local_vendor_code,
            sum(orders_count) as orders_per_day
    from {DB_ANALYSIS}
    where date BETWEEN CURRENT_DATE - INTERVAL '14 days' AND CURRENT_DATE - INTERVAL '1 day'
    group by
        date,
        local_vendor_code),
    month as (
        select
            date,
            local_vendor_code,
            sum(orders_count) as orders_per_day
        from {DB_ANALYSIS}
    where
        date BETWEEN CURRENT_DATE - INTERVAL '30 days' AND CURRENT_DATE - INTERVAL '1 day'
    group by
        date,
        local_vendor_code),
    warehouse_rem as (
        select 
            o.local_vendor_code,
            max(o.subject_name) as subject_name,
            max(p.name) as name,
            sum(o.total_quantity) as fbo,
            avg(o.stock_fbs) as fbs
        from {DB_ANALYSIS} o
        left join products p
        on o.local_vendor_code = p.id
        where o.date = CURRENT_DATE - INTERVAL '1 day'
        group by o.local_vendor_code
    )
    select
        w.local_vendor_code,
        wr.subject_name,
        wr.name,
        wr.fbo,
        wr.fbs,
        ROUND(avg(w.orders_per_day), 2) as avg_orders_week,
        ROUND(avg(tw.orders_per_day), 2) as avg_orders_two_weeks,
        ROUND(avg(m.orders_per_day), 2) as avg_orders_month
    from week w
    join two_weeks tw
    on w.local_vendor_code = tw.local_vendor_code
    join month m 
    on w.local_vendor_code = m.local_vendor_code
    join warehouse_rem wr 
    on w.local_vendor_code = wr.local_vendor_code
    where w.local_vendor_code in ({wilds_sql})
    group by w.local_vendor_code, wr.subject_name, wr.name, wr.fbo, wr.fbs
    order by local_vendor_code
    '''
    res = fetch_db_data_into_dict(query, conn)

    return res


def load_unique_wilds_from_orders(orders_sh = None, table = None, client = None):
    '''
    Returns a list of unique wilds from 'wild' column from CHINA_ORDERS sheet, CHINA_COUNT table
    '''
    if orders_sh is None:
        if table is not None:
            orders_sh = table.worksheet(CHINA_ORDERS)
        elif client is not None:
            sheet = client.open(CHINA_COUNT)
            orders_sh = sheet.worksheet(CHINA_ORDERS)
        else:
            client = gspread.service_account(filename=CREDS_PATH)
            sheet = client.open(CHINA_COUNT)
            orders_sh = sheet.worksheet(CHINA_ORDERS)

    first_col_values = orders_sh.col_values(1)
    header_row_num = first_col_values.index('Фото') + 1
    headers = orders_sh.row_values(header_row_num)
    wilds = sorted(list(set(orders_sh.col_values(headers.index('wild') + 1)))[header_row_num:])
    return wilds


def load_sopost_prices(client = None, wilds = None):
    '''
    Arguments:
        wilds [list]: if given, returns the result for given wilds

    Result:
        {wild1 : purchase_price_from_sopost, wild2 : ...}
    '''
    if client is None:
        client = gspread.service_account(filename=CREDS_PATH)
    sopost = client.open(UNIT_TABLE).worksheet('Сопост')
    sopost_headers = sopost.row_values(1)
    sopost_wilds = sopost.col_values(sopost_headers.index('wild') + 1)[1:]
    sopost_prices = sopost.col_values(sopost_headers.index('Стоимость в закупке (руб.)') + 1)[1:]
    sopost_dct = {sopost_wilds[i]:clean_number(sopost_prices[i]) for i in range(len(sopost_wilds))}
    if wilds:
        sopost_dct = { k:v for k, v in sopost_dct.items() if k in wilds}
    return sopost_dct


def load_unique_wilds_from_china(sh):
    '''
    Returns:
        a list of dicts with unique wilds and their names if they have flag 'K' or 'KK' in the column 'Страна' in the given sheet 
    '''
    headers_num = sh.col_values(1).index('Фото') + 1
    headers = sh.row_values(headers_num)
    wilds = sh.col_values(headers.index('wild') + 1)[headers_num:]
    names = sh.col_values(headers.index('Модель') + 1)[headers_num:]
    country = sh.col_values(headers.index('Страна') + 1)[headers_num:]
    # dct = [{'wild': w, 'name': n} for w, n, c in zip(wilds, names, country) if str(c).capitalize() == 'К']
    dct = {w : n for w, n, c in zip(wilds, names, country) if str(c).upper() in ['К', 'КК']}
    return dct


def load_avg_purch_price(wilds = None):
    if wilds:
        wilds_sql = list_to_sql_select(wilds, extra_quotes=True)
        wilds_sql_row = f"AND local_vendor_code IN ({wilds_sql})"
    else:
        wilds_sql_row = "AND local_vendor_code LIKE 'wild%'"
    query = f'''
    SELECT 
        local_vendor_code,
        ROUND(SUM(amount_with_vat) / NULLIF(SUM(quantity), 0), 2) AS weighted_avg_price_per_item
    FROM public.{DB_PURCHASE_PRICE}
    WHERE 
        is_valid = TRUE
        {wilds_sql_row}
        AND supplier_name != 'РВБ ООО'
        AND supply_date >= CURRENT_DATE - INTERVAL '3 months'
    GROUP BY local_vendor_code
    HAVING SUM(quantity) > 0;
    '''
    return fetch_db_data_into_dict(query)


def load_last_purch_price(wilds = None):
    '''
    Arguments:
        wilds: if given, returns data only for given wilds

    Result:
        {'local_vendor_code': 'wild1234',  'price_per_item': 150, ...}
    '''

    if wilds:
        wilds_sql = list_to_sql_select(wilds, extra_quotes=True)
        wilds_sql_row = f"AND local_vendor_code IN ({wilds_sql})"
    else:
        wilds_sql_row = "AND local_vendor_code LIKE 'wild%'"

    query = f'''
    SELECT DISTINCT ON (local_vendor_code)
        local_vendor_code,
        ROUND(amount_with_vat/quantity, 2) as price_per_item
    FROM {DB_PURCHASE_PRICE}
    WHERE is_valid = True
        {wilds_sql_row}
        AND supplier_name != 'РВБ ООО'
    ORDER BY local_vendor_code, supply_date DESC
    '''

    db_data = fetch_db_data_into_dict(query)
    
    # fixed_price_items = open_json(ITEMS_FIXED_PRICE)

    # for item in db_data:
    #     wild = item['local_vendor_code']
    #     if wild in fixed_price_items:
    #         item['price_per_item'] = fixed_price_items[wild]

    return db_data


if __name__ == "__main__":
    
    # 1. connect to client
    try:
        client = gspread.service_account(filename=CREDS_PATH)
        table = client.open(CHINA_TABLE) # prod

        logging.info(f"Connected to the table {CHINA_TABLE}")
    except Exception as e:
        logging.error(f"Failed to connect to the table '{CHINA_TABLE}:\n{e}")

    # 2. выгружаем закупочные цены в CHINA_ORDERS
    try:
        orders_sh = table.worksheet(CHINA_ORDERS)
        header_row_num = list(orders_sh.col_values(1)).index('Фото') + 1
        headers = orders_sh.row_values(header_row_num)

        wilds_raw = orders_sh.col_values(headers.index('wild') + 1)[header_row_num:]
        names_raw = orders_sh.col_values(headers.index('Модель') + 1)[header_row_num:]

        orders_sh_wilds_lst = [[w, n] for w, n in zip(wilds_raw, names_raw)] # 23.10: вкл пустые строки для выгрузки в гугл

        orders_sh_wilds = {i[0]:i[1] for i in orders_sh_wilds_lst if i[0]} # 23.10: убираем пустые строки для запроса в бд
        orders_ids = list(orders_sh_wilds.keys())
        purch_price = load_last_purch_price(orders_ids)

        purch_dict = {d['local_vendor_code']: d['price_per_item'] for d in purch_price}

        list_of_ordered_ids = [i[0] for i in orders_sh_wilds_lst]
        result = [
            [purch_dict.get(wild, 0)]
            for wild in list_of_ordered_ids
        ]

        price_col_letter = column_number_to_letter(headers.index('Последняя цена рынок'))
        orders_sh.update(result, range_name = f'{price_col_letter}{header_row_num + 1}:{price_col_letter}{orders_sh.row_count}')
        logging.info("Successfully added purchase price to the sheet 'Заказы'")
        
    except Exception as e:
        logging.error(f"Failed to upload purchase price. Error:\n{e}")
        raise

    # 3. выгружаем данные в CHINA_COUNT
    try:
        logging.info(f"Started processing sheet {CHINA_COUNT}")

        # sh = connect_to_local_sheet(os.getenv("LOCAL_TEST_TABLE"), CHINA_COUNT) # test

        sh = table.worksheet(CHINA_COUNT) # prod
        first_col_values = sh.col_values(1)
        header_row_num = first_col_values.index('Фото') + 1
        headers = sh.row_values(header_row_num) # нужны только для расчёта range


        # ---- new part: get wilds from three tables ----

        # Добавляем данные из Расчёта закупки
        pro_client = gspread.service_account(filename=PRO_CREDS_PATH)
        purch_table = pro_client.open(PURCHASE_TABLE)
        market_res = load_unique_wilds_from_china(purch_table.worksheet('Рынок_сервис'))
        xiamoi_res = load_unique_wilds_from_china(purch_table.worksheet('Ксиоми_сервис'))
        logging.info('Retrieved wilds from three gs tables')
        
        wilds_w_names = {**orders_sh_wilds, **market_res, **xiamoi_res} # {wild : name}
        wilds = list(wilds_w_names.keys()) # just wilds names

        # ---- end of the new part: get wilds from three tables ----


        db_data = load_db_data(wilds)


        # ---- new part: merge db data with absent wilds ----

        db_wilds = [i['local_vendor_code'] for i in db_data]
        df = pd.DataFrame(db_data)
        absent_wilds = list(set(wilds) - set(db_wilds))
        absent_df = pd.DataFrame(absent_wilds, columns = ['local_vendor_code'])
        full_df = df.merge(absent_df, on = 'local_vendor_code', how = 'outer')
        full_df['name'] = full_df['local_vendor_code'].map(wilds_w_names)
        full_df.fillna(0, inplace = True)

        full_df['sort_key'] = full_df['local_vendor_code'].str.extract(r'(\d+)').fillna(0).astype(int)
        full_df.sort_values('sort_key', inplace=True)
        full_df.drop('sort_key', axis=1, inplace=True)

        data = full_df.to_dict('records')

        # ---- end of the new part: get wilds from three tables ----


        # ---- 6.11 - move cells of the final order ----
        # достаем текущие данные {wild : итоговый заказ}
        fin_order_wilds = sh.col_values(headers.index('Артикул') + 1)
        fin_order_data = sh.col_values(headers.index('Итоговый заказ') + 1)
        fin_order_dct = {w : o for w, o in zip(fin_order_wilds, fin_order_data)}


        metrics = {
            'local_vendor_code': {
                'metric_ru':'Артикул'
            },
            'subject_name': {
                'metric_ru':'Категория'
            },
            'name': {
                'metric_ru':'Наименование внутреннее'
            },
            'fbo': {
                'metric_ru':'Остаток ВБ'
            },
            # 'fbs': {
            #     'metric_ru':'Склад'
            # },
            'avg_orders_month': {
                'metric_ru':'Ср. заказы в день за мес'
            },
            'avg_orders_two_weeks':{
                'metric_ru':'Ср. заказы в день за 14 дней'
            },
            'avg_orders_week':{
                'metric_ru':'Ср. заказы в день за 7 дней'
            }
        }

        for m in metrics:
            col_num = headers.index(metrics[m]['metric_ru'])
            col_letter = column_number_to_letter(col_num)
            metrics[m]['col_num'] = col_num
            metrics[m]['col_letter'] = col_letter

        # выгружаем по столбцам в CHINA_TABLE
        for metric_name, metric_data in metrics.items():
            try:
                output_data = [[i[metric_name]] for i in data]
                end_row = header_row_num + len(output_data)
                output_range = f'{metric_data["col_letter"]}{header_row_num + 1}:{metric_data["col_letter"]}{end_row}'
                add_data_to_range(sh, output_data, output_range, clean_range=True)
                logging.info(f"Successfully added {metric_data['metric_ru']} to the range {output_range}, sheet {CHINA_ORDERS}")
            except Exception as e:
                logging.error(f"Failed to add {metric_data['metric_ru']} to the range {output_range}, sheet {CHINA_ORDERS}:\n{e}")

        # --- 6.11 fin order insert ---
        output_wilds = [i['local_vendor_code'] for i in data]
        fin_order_output = [[fin_order_dct.get(i, '')] for i in output_wilds]
        fin_order_col_letter = column_number_to_letter(headers.index('Итоговый заказ'))
        sh.update(values = fin_order_output, range_name=f"{fin_order_col_letter}{header_row_num + 1}:{fin_order_col_letter}{end_row}")
        logging.info(f"Successfully added final order to the sheet {CHINA_ORDERS}")

        sh.update([[f'Актуализировано на {datetime.now().strftime("%d.%m.%Y %H:%M")}']], range_name = 'A2')
    
    except Exception as e:
        logging.error(f"Failed to upload data to the sheet {CHINA_COUNT}:\n{e}")
        raise