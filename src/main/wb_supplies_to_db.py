# ---- IMPORTS ----

# making it work for cron
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import requests
from time import sleep
from psycopg2.extras import execute_values

from utils.utils import load_api_tokens
from utils.logger import setup_logger
from utils.my_db_functions import create_connection_w_env

# ---- LOGS ----
logger = setup_logger("wb_supplies_to_db.log")


def get_supplies_paginated(token, limit=1000):
    '''
    Отдает номера поставок и заказов по одному клиенту.
    В БД не хранится, т.к. все отдаваемые данные есть в другом методе
    '''
    base_url = "https://supplies-api.wildberries.ru/api/v1/supplies"

    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }

    payload = {
        "dates": [
            {
                "type": "createDate"
            }
        ]
    }

    offset = 0
    all_items = []

    while True:
        params = {
            "limit": limit,
            "offset": offset
        }

        response = requests.post(base_url, headers=headers, params=params, json=payload)
        response.raise_for_status()

        batch = response.json()

        if not batch:
            break

        all_items.extend(batch)

        if len(batch) < limit:
            break  # no more pages

        offset += limit

    return all_items


def get_supply_by_id(ID: int, token: str, is_preorder: bool = False) -> dict:
    """
    Fetches a single supply by ID.
    """
    url = f"https://supplies-api.wildberries.ru/api/v1/supplies/{ID}"
    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }
    params = {
        "isPreorderID": is_preorder
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()

    data['ID'] = ID
    return data


def get_supplies_by_ids(IDs: list, token: str, is_preorder: bool = False) -> list:
    """
    Fetches multiple supplies by a list of IDs.
    Returns a list of dictionaries, each with 'supply_id' added.
    """
    results = []
    count = 0
    ids_num = len(IDs)
    for supply_id in IDs:
        print(f'{count}/{ids_num} processed')
        count += 1
        try:
            supply_data = get_supply_by_id(supply_id, token, is_preorder)

            if supply_id != IDs[-1]:
                sleep(2)

            results.append(supply_data)
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch supply {supply_id}: {e}")
            results.append({"supply_id": supply_id, "error": str(e)})
    return results


def get_supply_goods(ID: int, token: str, limit: int = 1000, is_preorder: bool = False) -> list:
    """
    Fetch all goods for a single supply ID, handling pagination (offset).
    Returns a list of dictionaries, each with 'ID' added.
    """
    url = f"https://supplies-api.wildberries.ru/api/v1/supplies/{ID}/goods"
    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }

    all_goods = []
    offset = 0

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "isPreorderID": is_preorder
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        goods = response.json()
        
        for item in goods:
            item['ID'] = ID
        
        all_goods.extend(goods)
        
        if len(goods) < limit:
            break
        
        offset += limit

    return all_goods


def get_multiple_supplies_goods(IDs: list, token: str, limit: int = 1000, is_preorder: bool = False) -> list:
    """
    Fetch goods for multiple supply IDs.
    Returns a combined list of dictionaries with 'ID' added.
    """
    all_results = []
    count = 0
    ids_num = len(IDs)
    for ID in IDs:
        print(f'{count}/{ids_num} processed')
        count +=1
        try:
            goods = get_supply_goods(ID, token, limit, is_preorder)
            all_results.extend(goods)

            if ID != IDs[-1]:
                sleep(2)
                
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch goods for supply {ID}: {e}")
            all_results.append({"ID": ID, "error": str(e)})
    return all_results

def insert_wb_supplies_to_db(records, conn):
    """
    Insert a list of supply dicts into wb_supplies table.
    Unmapped fields are ignored.
    ON CONFLICT (id, updated_date) DO NOTHING.
    """

    # camelCase → snake_case mapping
    rename_map = {
        "ID": "id",
        "phone": "phone",
        "statusID": "status_id",
        "boxTypeID": "box_type_id",
        "createDate": "create_date",
        "supplyDate": "supply_date",
        "factDate": "fact_date",
        "updatedDate": "updated_date",
        "warehouseID": "warehouse_id",
        "warehouseName": "warehouse_name",
        "actualWarehouseID": "actual_warehouse_id",
        "actualWarehouseName": "actual_warehouse_name",
        "transitWarehouseID": "transit_warehouse_id",
        "transitWarehouseName": "transit_warehouse_name",
        "acceptanceCost": "acceptance_cost",
        "paidAcceptanceCoefficient": "paid_acceptance_coefficient",
        "rejectReason": "reject_reason",
        "supplierAssignName": "supplier_assign_name",
        "storageCoef": "storage_coef",
        "deliveryCoef": "delivery_coef",
        "quantity": "quantity",
        "readyForSaleQuantity": "ready_for_sale_quantity",
        "acceptedQuantity": "accepted_quantity",
        "unloadingQuantity": "unloading_quantity",
        "depersonalizedQuantity": "depersonalized_quantity",
    }

    # Normalize each record
    normalized = []
    for item in records:
        row = {rename_map[k]: v for k, v in item.items() if k in rename_map}
        normalized.append(row)

    if not normalized:
        return

    # All columns we will insert
    columns = list(normalized[0].keys())
    col_names_sql = ", ".join(columns)

    # Build values list
    values = [[row.get(col) for col in columns] for row in normalized]

    query = f"""
        INSERT INTO wb_supplies ({col_names_sql})
        VALUES %s
        ON CONFLICT (id, updated_date) DO NOTHING;
    """

    with conn.cursor() as cur:
        execute_values(cur, query, values)
    conn.commit()


def insert_wb_supplies_goods(records, conn):
    """
    Insert a list of goods dicts into wb_supplies_goods.
    Column names are mapped using rename_map.
    ON CONFLICT DO NOTHING.
    """

    # camelCase → snake_case mapping
    rename_map = {
        "ID": "id",
        "barcode": "barcode",
        "vendorCode": "vendor_code",
        "nmID": "nm_id",
        "needKiz": "need_kiz",
        "tnved": "tnved",
        "techSize": "tech_size",
        "color": "color",
        "supplierBoxAmount": "supplier_box_amount",
        "quantity": "quantity",
        "readyForSaleQuantity": "ready_for_sale_quantity",
        "unloadingQuantity": "unloading_quantity",
        "acceptedQuantity": "accepted_quantity"
    }

    normalized = []
    for item in records:
        row = {rename_map[k]: v for k, v in item.items() if k in rename_map}
        normalized.append(row)

    if not normalized:
        return

    columns = list(normalized[0].keys())
    col_names_sql = ", ".join(columns)

    values = [[row.get(col) for col in columns] for row in normalized]

    query = f"""
        INSERT INTO wb_supplies_goods ({col_names_sql})
        VALUES %s
        ON CONFLICT DO NOTHING;
    """

    with conn.cursor() as cur:
        execute_values(cur, query, values)
    conn.commit()


if __name__ == "__main__":
    tokens = load_api_tokens()

    for client, token in tokens.items():

        # получаем номера поставок
        supplies = get_supplies_paginated(token)
        supplies_ids = [i['supplyID'] for i in supplies]
        # supplies_ids = supplies_ids[:3] # test
        try:

            # получаем информацию о поставке
            supplies_info = get_supplies_by_ids(IDs=supplies_ids, token = token)

            conn = create_connection_w_env()
            insert_wb_supplies_to_db(records = supplies_info, conn = conn)
            logger.info(f'Added {client} client data to wb_supplies')

            # получаем информацию о товарах в поставке
            supplies_goods = get_multiple_supplies_goods(IDs = supplies_ids, token = token)

            insert_wb_supplies_goods(records=supplies_goods, conn = conn)
            logger.info(f'Added {client} client data to wb_supplies_goods')
        
        except Exception as e:
            logger.error(f'Error while uploading data to the wb_supplies db table: {e}')
            raise