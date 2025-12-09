import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import os
import asyncio
import requests
import pandas as pd
from datetime import datetime, timedelta

from utils.logger import setup_logger
from utils.utils import load_api_tokens
from utils.my_db_functions import fetch_db_data_into_dict
from utils.my_gspread import connect_to_local_sheet, connect_to_remote_sheet, clean_number, column_number_to_letter

logger = setup_logger("remains_report_update.log")

def get_wb_remains(api_token, date):
    '''
    Arguments:
        date: in format 'YYYY-MM-DD'
    Result:
        Full json from WB API method supplier/stocks
    '''
    url = 'https://statistics-api.wildberries.ru/api/v1/supplier/stocks'
    headers = {'Authorization': api_token}
    params = {'dateFrom': date}
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    result = response.json()
    logger.info(f'gathered {len(result)} recordings')
    return result


def load_data_from_unit(clip_remains = True):
    '''
    Arguments:
        clip_remains: if True, sets negative remains to 0

    Result:
        Data from Unit with columns 'wild', 'Название', 'Предмет', 'Стоимость в закупке', 'Остаток склада факт' for unique wilds
    '''
    # остатки и себестоимость из юнитки
    unit_sh = connect_to_remote_sheet('UNIT 2.0 (tested)', 'MAIN (tested)')

    # Get headers and data columns
    unit_headers = unit_sh.row_values(1)
    wild_col = unit_sh.col_values(unit_headers.index('wild') + 1)[1:]
    name_col = unit_sh.col_values(unit_headers.index('Название') + 1)[1:]
    cat_col = unit_sh.col_values(unit_headers.index('Предмет') + 1)[1:]
    purch_price_col = unit_sh.col_values(unit_headers.index('Стоимость в закупке (руб.)') + 1)[1:]
    remains = unit_sh.col_values(unit_headers.index('Остаток ФАКТ СКЛАД') + 1)[1:]

    # Build list of rows
    data = []
    for w, n, c, p, r in zip(wild_col, name_col, cat_col, purch_price_col, remains):
        data.append({
            'item': w,
            'name': n,
            'category': c,
            'purchase_price': float(clean_number(p)) if p else 0.0,
            'remains': int(r) if r else 0
        })

    # Create DataFrame
    unit_df = pd.DataFrame(data)

    unit_df_clean = unit_df.drop_duplicates()

    if clip_remains:
        unit_df_clean['remains'] = unit_df_clean['remains'].clip(lower=0)
    
    return unit_df_clean

def load_current_balances():
    data = fetch_db_data_into_dict('''
    select
        cb.product_id,
        sum(cb.physical_quantity) as "full_quantity"
    from current_balances cb
    where cb.product_id like 'wild%'
    group by cb.product_id
    ''')
    return {i['product_id'] : i['full_quantity'] for i in data}


async def fetch_client(client_name: str, api_token: str, date: str) -> list:
    """
    Fetch WB remains for a single client asynchronously.
    Adds 'client' field to each item.
    """
    logger.info(f'Processing {client_name}')
    client_data = await asyncio.to_thread(get_wb_remains, api_token, date)
    for item in client_data:
        item['client'] = client_name
    return client_data


async def get_wb_remains_for_clients(tokens: dict, date: str) -> list:
    """
    Fetch WB remains for multiple clients concurrently.
    
    Arguments:
        tokens: dict of {client_name: api_token}
        date: str in 'YYYY-MM-DD' format

    Returns:
        List of all records with 'client' field added.
    """
    full_data = []

    # Create tasks for all clients
    tasks = [fetch_client(client_name, token, date) for client_name, token in tokens.items()]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Combine results and handle errors
    for client_result in results:
        if isinstance(client_result, Exception):
            logger.error(f"Error fetching client data: {client_result}")
        else:
            full_data.extend(client_result)

    return full_data


if __name__ == "__main__":

    try:

        # 1. load data from api
        tokens = load_api_tokens()
        date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        full_data = asyncio.run(get_wb_remains_for_clients(tokens, date))
        print(f"Total records: {len(full_data)}")

        wb_data = pd.DataFrame(full_data)
        short_df = wb_data.drop(columns = ['lastChangeDate', 'warehouseName', 'nmId', 'barcode', 'brand', 'techSize', 'Price', 'Discount', 'isSupply', 'isRealization', 'SCCode', 'quantityFull'])
        short_df.rename(columns={'item_base' : 'wild',
                                'quantity': 'Остатки на складах WB',
                                'inWayToClient': 'Едут к клиенту',
                                'inWayFromClient': 'Возвращаются на склад'}, inplace=True)
        short_df['wild'] = short_df['supplierArticle'].str.replace(r'(\d+)([dD].*)?$', r'\1', regex=True) # process wild1234d, wild1234d1
        short_df['wild'] = short_df['wild'].str.replace(r'-d$', '', case=False, regex=True) # process wild1234-d
        
        final_df = short_df.drop(columns=['supplierArticle', 'category', 'subject']) \
        .pivot_table(columns='client', index='wild', aggfunc='sum') \
        .swaplevel(axis=1) \
        .sort_index(axis=1).fillna(0)

        # 2. load data from unit
        unit_data = load_data_from_unit()
        unit_data = unit_data.drop_duplicates('item')
        unit_data = unit_data.rename(columns = {'item': 'wild',
                                            'name': 'Название',
                                            'category': 'Категория', 
                                            'purchase_price': 'Себестоимость',
                                            'remains': 'Остаток факт склад'})


        # 3. map
        info_dict = unit_data.set_index('wild')[['Название', 'Категория', 'Себестоимость', 'Остаток факт склад']].to_dict('index')

        for col in ['Название', 'Категория', 'Себестоимость', 'Остаток факт склад']:
            final_df[col] = final_df.index.map(lambda x: info_dict.get(x, {}).get(col))

        # 4. reorder
        cols_to_front = [
            ('Название', ''),
            ('Категория', ''),
            ('Себестоимость', ''),
            ('Остаток факт склад', '')
        ]

        remaining_cols = [col for col in final_df.columns if col not in cols_to_front]
        final_df = final_df[cols_to_front + remaining_cols]

        # 5. upload to gs

        # add logic with current_balances
        current_balances = load_current_balances()
        final_df[('Остаток факт склад', '')] = final_df.index.map(lambda x: current_balances.get(x, 0))


        final_df = final_df.fillna(0)

        final_df_reset = final_df.reset_index()
        level0 = final_df_reset.columns.get_level_values(0).tolist()  # e.g., 'Вектор', 'Даниелян', ...
        level1 = final_df_reset.columns.get_level_values(1).tolist()  # e.g., 'Едут к клиенту', ...
        header_row_1 = level0
        header_row_2 = level1
        data_rows = final_df_reset.values.tolist()
        values = [header_row_1, header_row_2] + data_rows

        sh = connect_to_remote_sheet('Стоимость остатков', 'Таблица')
        letter_range_end = column_number_to_letter(len(final_df.columns))
        output_range = f"A3:{letter_range_end}{len(final_df) + 4}"

        sh.update(values, range_name=output_range)
        sh.update([[f'Актуализировано на {datetime.now().strftime("%d.%m.%Y %H:%M")}']], range_name = 'B1')

        logger.info('Successfully updated the gs table')
    
    except Exception as e:
        logger.error(str(e))