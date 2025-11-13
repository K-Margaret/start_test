import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime
import pandas as pd

from utils.logger import setup_logger
from utils.my_pandas import datetime_to_str
from utils.my_db_functions import get_df_from_db
from utils.my_gspread import init_client

logger = setup_logger("db_data_to_purch_gs.log")

ORDERS_RENAME = {
    "document_number": "Номер документа",
    "document_created_at": "Дата создания документа",
    "supply_date": "Дата поставки",
    "local_vendor_code": "Внутренний код поставщика",
    "product_name": "Наименование товара",
    "event_status": "Статус события",
    "quantity": "Количество",
    "amount_with_vat": "Сумма с НДС",
    "amount_without_vat": "Сумма без НДС",
    "supplier_name": "Название поставщика",
    "supplier_code": "Код поставщика",
    "update_document_datetime": "Дата обновления документа",
    "author_of_the_change": "Автор изменения",
    "our_organizations_name": "Название нашей организации",
    "warehouse_id": "ID склада",
    "is_valid": "Корректность документа",
    "in_acceptance": "В приёмке",
    "created_at": "Дата создания записи",
    "is_printed_barcode": "Штрихкод напечатан",
    "acceptance_completed": "Приёмка завершена",
    "expected_receipt_date": "Ожидаемая дата поступления",
    "actual_quantity": "Фактическое количество",
    "currency": "Валюта",
    "unit_price": "Цена за единицу",
    "last_purchase_price": "Цена последней закупки",
    "last_purchase_supplier": "Поставщик последней закупки",
    "payment_indicator": "Признак/способ оплаты",
    "payment_document_number": "Номер платёжного документа",
    "shipment_date": "Дата отгрузки",
    "receipt_transaction_number": "Номер приходной операции",
    "cancelled_due_to": "Причина аннулирования",
    "comment": "Комментарий"
}

SUPPLY_RENAME = {
    "document_number": "Номер документа",
    "document_created_at": "Дата создания документа",
    "supply_date": "Дата поставки",
    "local_vendor_code": "Внутренний код поставщика",
    "product_name": "Наименование товара",
    "event_status": "Статус события",
    "quantity": "Количество",
    "amount_with_vat": "Сумма с НДС",
    "amount_without_vat": "Сумма без НДС",
    "supplier_name": "Название поставщика",
    "supplier_code": "Код поставщика",
    "update_document_datetime": "Дата обновления документа",
    "author_of_the_change": "Автор изменения",
    "our_organizations_name": "Название нашей организации",
    "is_valid": "Корректность документа"
}


def load_orders_data(months):
    """
    Загружает данные из БД ordered_goods_from_buyers за последние `months` месяцев.
    Загружает только конкретные колонки для стабильности.
    """
    columns = [
        "id", "guid", "document_number",
        "document_created_at::date AS document_created_at",
        "supply_date::date AS supply_date",
        "local_vendor_code", "product_name", "event_status", "quantity",
        "amount_with_vat", "amount_without_vat", "supplier_name", "supplier_code",
        "update_document_datetime::date AS update_document_datetime",
        "author_of_the_change", "our_organizations_name",
        "warehouse_id", "is_valid", "in_acceptance",
        "created_at::date AS created_at",
        "is_printed_barcode", "acceptance_completed",
        "expected_receipt_date::date AS expected_receipt_date",
        "actual_quantity", "currency",
        "unit_price", "last_purchase_price", "last_purchase_supplier", "payment_indicator",
        "payment_document_number", "shipment_date::date AS shipment_date",
        "receipt_transaction_number", "cancelled_due_to", "comment"
    ]
    cols_str = ", ".join(columns)
    query = f'''
    SELECT {cols_str}
    FROM ordered_goods_from_buyers
    WHERE is_valid = TRUE
      AND update_document_datetime >= '2025-05-01' --NOW() - INTERVAL '{months} months';
    '''
    df = get_df_from_db(query)
    return df


def load_supply_data(months):
    """
    Загружает данные из БД supply_to_sellers_warehouse за последние `months` месяцев.
    Загружает только конкретные колонки для стабильности.
    """
    columns = [
        "id", "guid", "document_number", "document_created_at", "supply_date",
        "local_vendor_code", "product_name", "event_status", "quantity",
        "amount_with_vat", "amount_without_vat", "supplier_name", "supplier_code",
        "update_document_datetime", "author_of_the_change", "our_organizations_name",
        "is_valid"
    ]
    cols_str = ", ".join(columns)
    query = f'''
    SELECT {cols_str}
    FROM public.supply_to_sellers_warehouse
    WHERE is_valid = TRUE
      AND update_document_datetime >= '2025-05-01' -- NOW() - INTERVAL '{months} months';
    '''
    df = get_df_from_db(query)
    return df


if __name__ == "__main__":
    
    try:
        months = None
        client = init_client()
        gs_table = client.open('Тест Расчет закупки')

        orders_db = load_orders_data(months = months)

        datetime_cols = ['document_created_at', 'supply_date', 'update_document_datetime',
                 'created_at', 'expected_receipt_date', 'shipment_date']

        for col in datetime_cols:
            if col in orders_db.columns:
                orders_db[col] = pd.to_datetime(orders_db[col], errors='coerce')
                orders_db[col] = orders_db[col].dt.strftime('%d.%m.%Y')
                orders_db[col] = orders_db[col].fillna('')

        orders_renamed = orders_db.rename(columns=ORDERS_RENAME)

        orders_output = [orders_renamed.columns.tolist()] + orders_renamed.values.tolist()
        orders_sh = gs_table.worksheet('Заказы_поставщиков_1С')
        orders_sh.update(values = orders_output, range_name = 'A2')
        orders_sh.update(
            values=[[f'Обновлено {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}']],
            range_name='A1'
        )
    except Exception as e:
        raise e

    try:
        supply_db = load_supply_data(months = months)
        supply_renamed = datetime_to_str(supply_db.rename(columns=SUPPLY_RENAME))
        supply_output = [supply_renamed.columns.tolist()] + supply_renamed.values.tolist()
        supply_sh = gs_table.worksheet('Приходы_1С')
        supply_sh.update(values = supply_output, range_name = 'A2')
        supply_sh.update(
            values=[[f"Обновлено {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"]],
            range_name='A1'
        )
    except Exception as e:
        logger.error(f'Ошибка при обновлении листа "Приходы_1С": {e}')