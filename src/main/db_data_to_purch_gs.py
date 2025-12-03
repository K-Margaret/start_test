import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime
import pandas as pd

# from utils.my_gspread import connect_to_local_sheet

from utils.logger import setup_logger
from utils.my_db_functions import get_df_from_db
from utils.my_gspread import init_client
from utils.env_loader import *

logger = setup_logger("db_data_to_purch_gs.log")

LOCAL_TABLE=os.getenv('LOCAL_TABLE')

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

def load_wb_supplies():
    query = '''
    SELECT DISTINCT ON (wsg.id, wsg.vendor_code)
        wsg.id                                   AS "Номер поставки",
        ws.supply_date                           AS "Плановая дата поставки",
        ws.fact_date                             AS "Фактическая дата поставки",
        ws.status_id                             AS "Статус",
        wsg.quantity                              AS "Добавлено в поставку",
        wsg.unloading_quantity                    AS "Раскладывается",
        wsg.accepted_quantity                     AS "Принято, шт",
        wsg.ready_for_sale_quantity               AS "Поступило в продажу",
        wsg.vendor_code                          AS "Артикул продавца",
        wsg.nm_id                                AS "Артикул WB",
        wsg.supplier_box_amount                  AS "Указано в упаковке, шт"
    FROM wb_supplies_goods wsg
    LEFT JOIN wb_supplies ws 
        ON wsg.id = ws.id
    WHERE ws.create_date >= NOW() - INTERVAL '2 months'
    ORDER BY wsg.id, wsg.vendor_code, ws.updated_date DESC, wsg.created_at DESC;
    '''
    return get_df_from_db(query)


if __name__ == "__main__":
    
    try:
        months = None
        client = init_client()
        gs_table = client.open(LOCAL_TABLE)

        # orders_db = load_orders_data(months = months)

        # datetime_cols = ['document_created_at', 'supply_date', 'update_document_datetime',
        #          'created_at', 'expected_receipt_date', 'shipment_date']

        # for col in datetime_cols:
        #     if col in orders_db.columns:
        #         orders_db[col] = orders_db[col].replace(['0', '00.00.0000', 0, ''], pd.NA)
        #         orders_db[col] = pd.to_datetime(orders_db[col], errors='coerce')
        #         orders_db[col] = orders_db[col].dt.strftime('%d.%m.%Y')
        #         orders_db[col] = orders_db[col].fillna('')

        # orders_renamed = orders_db.rename(columns=ORDERS_RENAME)

        # orders_output = [orders_renamed.columns.tolist()] + orders_renamed.values.tolist()
        # orders_sh = gs_table.worksheet('Заказы_поставщиков_1С')
        # orders_sh.update(values = orders_output, range_name = 'A2')
        # orders_sh.update(
        #     values=[[f"Обновлено {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"]],
        #     range_name='A1'
        # )
        # logger.info('Данные успешно добавлены на лист "Заказы_поставщиков_1С"')
    except Exception as e:
        logger.error(f'Ошибка при обновлении листа "Заказы_поставщиков_1С": {e}')

    # try:
    #     supply_db = load_supply_data(months = months)

    #     datetime_cols = ['document_created_at', 'supply_date', 'update_document_datetime']

    #     for col in datetime_cols:
    #         if col in supply_db.columns:
    #             supply_db[col] = supply_db[col].replace(['0', '00.00.0000', 0, ''], pd.NA)
    #             supply_db[col] = pd.to_datetime(supply_db[col], errors='coerce')
    #             supply_db[col] = supply_db[col].dt.strftime('%d.%m.%Y')
    #             supply_db[col] = supply_db[col].fillna('')

    #     supply_renamed = supply_db.rename(columns=SUPPLY_RENAME)
    #     supply_output = [supply_renamed.columns.tolist()] + supply_renamed.values.tolist()
    #     supply_sh = gs_table.worksheet('Приходы_1С')
    #     supply_sh.update(values = supply_output, range_name = 'A2')
    #     supply_sh.update(
    #         values=[[f"Обновлено {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"]],
    #         range_name='A1'
    #     )
    #     logger.info('Данные успешно добавлены на лист "Приходы_1С"')

    # except Exception as e:
    #     logger.error(f'Ошибка при обновлении листа "Приходы_1С": {e}')
    
    try:
        wb_supplies = load_wb_supplies()
        wb_supplies['Статус'] = wb_supplies['Статус'].map({
            1: "Не запланировано",
            2: "Запланировано",
            3: "Отгрузка разрешена",
            4: "Идёт приёмка",
            5: "Принято",
            6: "Отгружено на воротах",
        })

        for col in ["Плановая дата поставки", "Фактическая дата поставки"]:
            if col in wb_supplies.columns:
                wb_supplies[col] = wb_supplies[col].replace(['0', '00.00.0000', 0, ''], pd.NA)
                wb_supplies[col] = pd.to_datetime(wb_supplies[col], errors='coerce')
                wb_supplies[col] = wb_supplies[col].dt.strftime('%d.%m.%Y')
                wb_supplies[col] = wb_supplies[col].fillna('')

        wb_output = [wb_supplies.columns.tolist()] + wb_supplies.values.tolist()
        wb_supplies_sh = gs_table.worksheet('БД_поставки')
        wb_supplies_sh.update(values = wb_output, range_name = 'A2')
        wb_supplies_sh.update(
            values=[[f"Обновлено {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"]],
            range_name='A1'
        )
        logger.info('Данные успешно добавлены на лист "БД_поставки"')

    except Exception as e:
        logger.error(f'Failed to upload data to БД_поставки: {e}')