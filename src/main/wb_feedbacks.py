import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import requests
import time
import logging
import json
from psycopg2.extras import execute_values

from utils.utils import load_api_tokens
from utils.my_db_functions import create_connection_w_env

# ---- LOGS ----

LOGS_PATH = os.getenv("LOGS_PATH")

os.makedirs(LOGS_PATH, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOGS_PATH}/wb_feedbacks.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def get_wb_feedbacks(api_token: str, nm_id: int | None = None, is_answered: bool = True, date_from: int = 0, date_to: int = 0) -> dict:
    """
    Получает все отзывы с Wildberries, автоматически обрабатывая постраничную загрузку.

    Аргументы:
        api_token (str): API-ключ Wildberries.
        nm_id (int | None): Артикул WB (если None — выгружаются все отзывы).
        is_answered (bool): Фильтр по обработанным отзывам (по умолчанию True).
        date_from (int): Дата начала периода в формате Unix timestamp (по умолчанию 0 — без фильтрации).
        date_to (int): Дата конца периода в формате Unix timestamp (по умолчанию 0 — без фильтрации).

    Возвращает:
        dict: Объединённый JSON с ключом "data" и всеми отзывами.
    """

    url = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"
    headers = {"Authorization": api_token}
    take = 5000
    skip = 0

    all_feedbacks = []

    while True:
        params = {
            "isAnswered": is_answered,
            "take": take,
            "skip": skip,
            "order": "dateAsc"
        }
        if nm_id:
            params["nmId"] = nm_id
        if date_from > 0:
            params["dateFrom"] = date_from
        if date_to > 0:
            params["dateTo"] = date_to

        res = requests.get(url=url, headers=headers, params=params)
        if res.status_code != 200:
            logging.error(f"Ошибка {res.status_code}: {res.text}")
            break

        js = res.json()
        feedbacks = js.get("data", {}).get("feedbacks", [])
        if not feedbacks:
            break

        if skip == 0:
            logging.info(f'Найдена информация о {js['data']['countArchive']} отзывах')

        all_feedbacks.extend(feedbacks)
        skip += take

        # Прекращаем цикл, если данных меньше лимита (всё выгружено)
        if len(feedbacks) < take:
            break

        logging.info(f"Получена информация о {len(feedbacks)} отзывах. Продолжаем...")

        # Ограничение API — не более 3 запросов в секунду
        time.sleep(0.35)

    return all_feedbacks


def get_wb_feedbacks_batch(
    api_token: str,
    nm_id: int | None = None,
    is_answered: bool = True,
    date_from: int = 0,
    date_to: int = 0,
    take: int = 5000,
    skip: int = 0
) -> list:
    """
    Получает один "батч" отзывов с Wildberries (с контролем skip и take).

    Аргументы:
        api_token (str): API-ключ Wildberries.
        nm_id (int | None): Артикул WB (если None — выгружаются все отзывы).
        is_answered (bool): Фильтр по обработанным отзывам.
        date_from (int): Дата начала периода в Unix timestamp.
        date_to (int): Дата конца периода в Unix timestamp.
        take (int): Сколько отзывов забрать за один вызов (по умолчанию 5000).
        skip (int): Сколько отзывов пропустить перед выборкой.

    Возвращает:
        list: Список отзывов (максимум `take`).
    """

    url = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"
    headers = {"Authorization": api_token}
    
    params = {
        "isAnswered": is_answered,
        "take": take,
        "skip": skip,
        "order": "dateAsc"
    }
    if nm_id:
        params["nmId"] = nm_id
    if date_from > 0:
        params["dateFrom"] = date_from
    if date_to > 0:
        params["dateTo"] = date_to

    res = requests.get(url=url, headers=headers, params=params)
    if res.status_code != 200:
        logging.error(f"Ошибка {res.status_code}: {res.text}")
        return []

    js = res.json()
    feedbacks = js.get("data", {}).get("feedbacks", [])

    if skip == 0 and 'data' in js and 'countArchive' in js['data']:
        logging.info(f"Всего отзывов для этой выборки: {js['data']['countArchive']}")

    # Ограничение API — не более 3 запросов в секунду
    time.sleep(0.35)

    return feedbacks


def insert_feedbacks_into_db(connection, feedbacks: list):
    """
    Вставляет батч отзывов в таблицу wb_feedbacks PostgreSQL.

    Аргументы:
        connection: psycopg2 connection.
        feedbacks (list): Список отзывов из get_wb_feedbacks_batch.
    """
    if not feedbacks:
        return

    insert_query = """
    INSERT INTO public.wb_feedbacks (
        id, nmid, productvaluation, createddate, "text", pros, cons,
        bables, answer_text, photolinks, video, username,
        isablereturnproductorders, isablesupplierfeedbackvaluation,
        isablesupplierproductvaluation, wasviewed, parentfeedbackid,
        childfeedbackid, matchingsize, lastordercreatedat, lastordershkid,
        returnproductordersdate, supplierfeedbackvaluation, supplierproductvaluation
    ) VALUES %s
    ON CONFLICT (id) DO NOTHING
    """

    values = []
    for f in feedbacks:
        # Flatten nested fields
        nmId = f.get("productDetails", {}).get("nmId")
        answer_text = (f.get("answer") or {}).get("text")
        bables = json.dumps(f.get("bables") or [])
        photoLinks = json.dumps(f.get("photoLinks") or [])
        video = json.dumps(f.get("video") or {})

        values.append((
            f.get("id"),
            nmId,
            f.get("productValuation"),
            f.get("createdDate"),
            f.get("text"),
            f.get("pros"),
            f.get("cons"),
            bables,
            answer_text,
            photoLinks,
            video,
            f.get("userName"),
            f.get("isAbleReturnProductOrders"),
            f.get("isAbleSupplierFeedbackValuation"),
            f.get("isAbleSupplierProductValuation"),
            f.get("wasViewed"),
            f.get("parentFeedbackId"),
            f.get("childFeedbackId"),
            f.get("matchingSize"),
            f.get("lastOrderCreatedAt"),
            f.get("lastOrderShkId"),
            f.get("returnProductOrdersDate"),
            f.get("supplierFeedbackValuation"),
            f.get("supplierProductValuation")
        ))

    # Use execute_values for fast bulk insert
    from psycopg2.extras import execute_values
    try:
        with connection.cursor() as cur:
            execute_values(cur, insert_query, values)
        connection.commit()
        logging.info(f"Вставлено {len(values)} отзывов в базу.")
    except Exception as e:
        logging.error(f"Ошибка при вставке в базу: {e}")
        connection.rollback()


if __name__ == "__main__":

    tokens = load_api_tokens()
    conn = create_connection_w_env()
    take = 5000  # batch size

    for client, token in tokens.items():
        logging.info(f"Начинаем обработку отзывов для клиента: {client}")
        skip = 0

        try:
            while True:
                batch = get_wb_feedbacks_batch(api_token=token, skip=skip, take=take)
                logging.info(f"Клиент {client}: получено {len(batch)} отзывов с offset {skip}")

                if not batch:
                    logging.warning(f"Клиент {client}: нет новых отзывов для обработки, пропуск {skip}")
                    break

                insert_feedbacks_into_db(conn, batch)
                logging.info(f"Клиент {client}: вставлено {len(batch)} отзывов в базу данных")

                batch_size = len(batch)
                skip += batch_size

                if batch_size < take:
                    logging.info(f"Клиент {client}: достигнут конец отзывов (последний batch размером {batch_size})")
                    break

        except Exception as e:
            logging.error(f"Ошибка при обработке клиента {client} на offset {skip}: {e}")