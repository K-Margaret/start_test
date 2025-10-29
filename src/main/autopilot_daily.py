# ---- IMPORTS ----

# making it work for cron
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# libraries
import json
import time
import random
import logging
import pandas as pd
import numpy as np
from gspread.exceptions import APIError

# my packages
from utils.env_loader import *
from utils import my_pandas, my_gspread
from utils import my_db_functions as db

from autopilot_hourly import parse_data_from_WB


# tasks:
# 1. change structure to server-acceptable 
# 2. add logging



# ---- SET UP ----
AUTOPILOT_TABLE_NAME = os.getenv("AUTOPILOT_TABLE_NAME")
AUTOPILOT_SHEET_NAME = os.getenv("AUTOPILOT_SHEET_NAME")

NEW_ITEMS_TABLE_NAME = os.getenv("NEW_ITEMS_TABLE_NAME")
NEW_ITEMS_ARTICLES_SHEET_NAME = os.getenv("NEW_ITEMS_ARTICLES_SHEET_NAME")

METRICS_RU = {
    "orders_sum_rub": "Сумма заказов",
    "orders_count": "Кол-во заказов",
    "adv_spend": "Сумма затрат",
    "price_with_disc": "Цены",
    "spp": "скидка WB",
    "total_quantity": "Остатки",
    "profit_by_cond_orders": "Прибыль c заказов по ИУ",
    "views": "Показы",
    "clicks": "Клики",
    "ctr": "ctr",
    "to_cart_convers": "Конверсия в корзину",
    "to_orders_convers": "Конверсия в заказ",
    "add_to_cart_count": "Добавления в корзину",
    "open_card_count": "Переходы в карточку товара",
    "cpc": "cpc",
    "rating": "Рейтинг"
}


# ---- LOGS ----
LOGS_PATH = os.getenv("LOGS_PATH")

os.makedirs(LOGS_PATH, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{LOGS_PATH}/autopilot_daily.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)



def load_data(rename = True):

    # берём метрики (рус и англ) из файла
    # with open('autopilot_curr_metrics_cut.json', 'r', encoding='utf-8') as f:
    #     metrics_dict = json.load(f) 

    percent_metrics = ['ctr', 'to_cart_convers', 'to_orders_convers']
    metric_selects = [
        f"ROUND({col}/100, 5) AS {col}" if col in percent_metrics else col
        for col in METRICS_RU.keys()
    ]

    # select_avg = ', '.join([f'ROUND(avg({col}), 2) as avg_{col}' for col in metrics_dict.keys()])

    select_avg = ', '.join([
        f"ROUND(avg({col})/100, 5) as avg_{col}" if col in percent_metrics else f"ROUND(avg({col}), 2) as avg_{col}"
        for col in METRICS_RU.keys()
    ])

    query_curr = f'''
    SELECT
        -- все метрики
        {', '.join(['date', 'article_id', 'subject_name', 'account', 'local_vendor_code', 'promo_title'] + metric_selects)},
        (profit_by_cond_orders - adv_spend) AS ЧП_РК,
        
        -- ДРР
        CASE 
            WHEN orders_sum_rub = 0 THEN 1
            ELSE ROUND((adv_spend / orders_sum_rub), 2)
        END AS "ДРР",

        -- cpo
        CASE 
            WHEN orders_count = 0 THEN adv_spend 
            ELSE ROUND((adv_spend / orders_count), 2)
        END AS "cpo",

        -- Акции
        CASE WHEN promo_title != '' THEN 1 ELSE 0 END AS "Акции"

    FROM orders_articles_analyze
    WHERE date >= CURRENT_DATE - INTERVAL '6 days'
    '''

    query_hist = f'''
    SELECT *
    FROM
        (
            SELECT
                article_id,
                {select_avg},
                (ROUND(avg(profit_by_cond_orders), 2) - ROUND(avg(adv_spend),2)) AS "ЧП-РК за 7 дней",
                -- ДРР (ROAS)
                CASE 
                    WHEN SUM(orders_sum_rub) = 0 THEN 1
                    ELSE ROUND((SUM(adv_spend) / SUM(orders_sum_rub)), 2)
                END AS "ДРР факт за 7 дней",
                -- CPO
                CASE 
                    WHEN SUM(orders_count) = 0 THEN SUM(adv_spend)
                    ELSE ROUND((SUM(adv_spend) / SUM(orders_count)), 2)
                END AS "Ср. \ncpo"
            FROM orders_articles_analyze
            WHERE date >= CURRENT_DATE - INTERVAL '2 weeks' + INTERVAL '1 day'
            AND date < CURRENT_DATE - INTERVAL '1 week' + INTERVAL '1 day'
            GROUP BY article_id
        )
        AS week_metrics
    JOIN (
            SELECT
                article_id,
                ROUND(avg(price_with_disc), 2) as month_avg_price_with_disc,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY price_with_disc) as month_median_price_with_disc
            FROM orders_articles_analyze
            WHERE date > CURRENT_DATE - INTERVAL '1 month'
            GROUP BY article_id 
        )
        AS month_metrics
    ON week_metrics.article_id = month_metrics.article_id
    '''
    
    curr, hist = db.get_df_from_db([query_curr, query_hist], decimal_to_num = False)
    hist = hist.loc[:, ~hist.columns.duplicated(keep='first')]

    hist = my_pandas.process_decimal(hist)
    curr = my_pandas.process_decimal(curr)

    curr = curr.rename(columns = {'ЧП_РК':'ЧП-РК'})
    
    if rename:
        curr = curr.rename(columns = METRICS_RU)

        hist_metrics_names = {
            'avg_orders_sum_rub': 'ср. заказы за прошлые 7 дней',
            'avg_orders_count': 'ср.  зак 7 д',
            'avg_adv_spend': 'ср. затраты за прошлые 7 дней',
            'avg_price_with_disc': 'Ср.цена за 7 дней',
            'avg_spp': 'Ср. \nскидка WB',
            'avg_total_quantity': 'Остатки ФБО ср.за 7 дней',
            'avg_profit_by_cond_orders': 'Ср. прибыль c заказов по ИУ за 7 дней',
            'avg_views': 'Ср. показы за 7 дней',
            'avg_clicks': 'Ср. клики за 7 дней',
            'avg_ctr': 'ctr за 7 дней',
            'avg_to_cart_convers': 'Конверсия в корзину за 7 дней',
            'avg_to_orders_convers': 'Конверсия в заказ за 7 дней',
            'avg_add_to_cart_count': 'Ср. добавления в корзину за 7 дней',
            'avg_open_card_count': 'Ср. переходы в карточку товара за 7 дней',
            'avg_cpc': 'Ср. \ncpc',
            'avg_rating': 'Ср. \nрейтинг',
            'month_avg_price_with_disc':'Ср.цена за 30 дней',
            'month_median_price_with_disc': 'Медианная цена 30 дней'
        }
        hist = hist.rename(columns = hist_metrics_names)

    return curr, hist


def push_data(df, headers, col_num, articles_sorted, values_first_row, sh_len, pivot):
    cols = list(df.columns)
    absent_metrics = set(cols) - set(headers)

    if absent_metrics:
        logging.warning(f'\nСледующие метрики отсутствуют в таблице:\n{absent_metrics}\n')
    
    present_metrics = list(set(cols) - set(absent_metrics))
    
    for metric in present_metrics:
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                if pivot:
                    temp_df = df.pivot(columns='date', index='article_id', values=metric)
                    if metric == 'spp' or metric == "скидка WB":
                        temp_df = temp_df.reindex(articles_sorted).fillna('')
                    else:
                        temp_df = temp_df.reindex(articles_sorted).fillna(0)
                else:
                    temp_df = df[['article_id', metric]].set_index('article_id')
                    if metric == 'spp' or metric == "скидка WB":
                        temp_df = temp_df.reindex(articles_sorted).fillna('')
                    else:
                        temp_df = temp_df.reindex(articles_sorted).fillna(0)
                    temp_df = temp_df[[metric]]

                metric_range = my_gspread.define_range(metric, headers, col_num, values_first_row, sh_len)
                my_gspread.add_data_to_range(sh, temp_df, metric_range)
                logging.info(f'Данные по "{metric}" успешно добавлены в диапазон {metric_range}')
                break
                
            except APIError as e:
                if e.response.status_code == 429:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logging.error(f'Достигнуто максимальное количество попыток для метрики "{metric}"')
                        break
                    wait_time = random.uniform(1, 5) * retry_count
                    logging.error(f'Превышен лимит запросов. Попытка {retry_count}/{max_retries} через {wait_time:.1f} сек...')
                    time.sleep(wait_time)
                else:
                    logging.error(f'Ошибка API при загрузке "{metric}":\n{e}')
                    break
                    
            except Exception as e:
                logging.error(f'Неизвестная ошибка при загрузке "{metric}":\n{e}')
                break


def process_adv_status(unit_sh, autopilot_adv_status, unit_skus = None):
    '''
    Склеивает статус "реклама" из Автопилота со статусом "Товар удалён" из UNIT
    '''
    if not unit_skus:
        unit_skus = my_gspread.get_skus_unit(unit_sh)
    
    # get deleted status
    adv_col_num = my_gspread.find_gscol_num_by_name('Реклама', unit_sh)
    adv_prev_values = unit_sh.col_values(adv_col_num)[1:]
    del_status = {unit_skus[i]:adv_prev_values[i] for i in range(len(adv_prev_values)) if adv_prev_values[i] == 'ТОВАР \nУДАЛЕН '}

    # check that skus w active status are not advertised
    adv_skus = {key:autopilot_adv_status[key] for key in autopilot_adv_status if autopilot_adv_status[key] == 'реклама'}
    errors = set(adv_skus.keys()).intersection(set(del_status.keys()))

    if errors:
        raise ValueError(f'Skus marked as deleted have active adv status: {errors}')

    # add deleted status to autopilot
    for sku in unit_skus:       # используем юнитку, пч не все скю есть в автопилоте 
        if sku in del_status:
            autopilot_adv_status[sku] = del_status[sku]

    # dict to list
    output_data = {sku:autopilot_adv_status.get(sku, '') for sku in unit_skus}
    return output_data
    

def update_adv_status_in_unit(unit_sh, adv_dict):
    '''
    Принимает adv_dict вида {unit_sku : 'реклама', unit_sku : ''}.
    ! Предполагается, что adv_dict уже отсортирован, как в UNIT !

    Преобразует adv_dict в лист вида [['реклама'],['']...] и отправляет в gs
    '''
    output_data = [[adv_dict[key]] for key in adv_dict]
    output_range = my_gspread.define_range('Реклама', unit_sh.row_values(1), number_of_columns=1, values_first_row=2, sh_len = unit_sh.row_count)
    my_gspread.add_data_to_range(unit_sh, output_data, output_range, False)
    logging.info(f'Статус рекламы успешно добавлен в диапазон {output_range}')


def load_and_update_feedbacks_unit(unit_sh, unit_skus):
    feedback_data = parse_data_from_WB(articles=unit_skus, return_keys=['feedbacks'])
    output_data = [value for key, value in feedback_data.items()]
    ouput_range = my_gspread.define_range('Кол-во отзывов ВБ', unit_sh.row_values(1), 1, 2, unit_sh.row_count)
    my_gspread.add_data_to_range(unit_sh, output_data, ouput_range, True)


# new function to avoid 503 error
def push_data_static_range(df, headers, col_num, articles_sorted, values_first_row, sh_len, pivot):
    """
    Pushes data using STATIC column ranges defined in METRIC_TO_COL.
    Only uses sheet length (sh_len) and first row of values.
    All other logic (pivoting, retries) remains unchanged.
    """

    METRIC_TO_COL = {
        # Основные метрики
        "Сумма заказов": "AW",
        "Кол-во заказов": "BH",
        "Сумма затрат": "BP",
        "Цены": "CB",
        "скидка WB": "CU",
        "Остатки": "DL",
        "Прибыль c заказов по ИУ": "DU",
        "Показы": "ET",
        "Клики": "FC",
        "ctr": "FK",
        "Конверсия в корзину": "FS",
        "Конверсия в заказ": "GA",
        "Добавления в корзину": "GI",
        "Переходы в карточку товара": "GQ",
        "cpc": "HG",
        "Рейтинг": "HO",
        "cpo": "GY",
        "Акции": "DD",
        "ЧП-РК": "EC",
        "ДРР": "EL",

        # Исторические (средние) метрики
        "ср. заказы за прошлые 7 дней": "AV",
        "ср.  зак 7 д": "BG",
        "ср. затраты за прошлые 7 дней": "BO",
        "Ср.цена за 7 дней": "CA",
        "Ср. \nскидка WB": "CT",
        "Остатки ФБО ср.за 7 дней": "DK",
        "Ср. прибыль c заказов по ИУ за 7 дней": "DT",
        "Ср. показы за 7 дней": "ES",
        "Ср. клики за 7 дней": "FB",
        "ctr за 7 дней": "FJ",
        "Конверсия в корзину за 7 дней": "FR",
        "Конверсия в заказ за 7 дней": "FZ",
        "Ср. добавления в корзину за 7 дней": "GH",
        "Ср. переходы в карточку товара за 7 дней": "GP",
        "Ср. \ncpc": "HF",
        "Ср. \nрейтинг": "HN",
        "Ср.цена за 30 дней": "BZ",
        "Медианная цена 30 дней": "BY",
        'ЧП-РК за 7 дней': "EB",
        'Ср. \ncpo': "GX",
        'ДРР факт за 7 дней' : "EK"
    }

    cols = list(df.columns)
    absent_metrics = set(cols) - set(METRIC_TO_COL.keys())
    
    if absent_metrics:
        logging.warning(f'\nСледующие метрики не имеют статического диапазона и будут пропущены:\n{absent_metrics}\n')
    
    present_metrics = list(set(cols) - set(absent_metrics))
    
    for metric in present_metrics:
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                # Data preparation (unchanged)
                if pivot:
                    temp_df = df.pivot(columns='date', index='article_id', values=metric)
                    if metric == 'spp' or metric == "скидка WB":
                        temp_df = temp_df.reindex(articles_sorted).fillna('')
                    else:
                        temp_df = temp_df.reindex(articles_sorted).fillna(0)
                else:
                    temp_df = df[['article_id', metric]].set_index('article_id')
                    if metric == 'spp' or metric == "скидка WB":
                        temp_df = temp_df.reindex(articles_sorted).fillna('')
                    else:
                        temp_df = temp_df.reindex(articles_sorted).fillna(0)
                    temp_df = temp_df[[metric]]

                # === STATIC RANGE LOGIC REPLACES define_range() ===
                range_start = METRIC_TO_COL[metric]  # Start column from dict
                range_end = my_gspread.calculate_range_end(range_start, col_num)  # Expand by col_num columns

                # Format range: StartColRow:EndColRow
                metric_range = f'{range_start}{values_first_row}:{range_end}{sh_len + 1}'

                # Push data (assumes my_gspread.add_data_to_range is available)
                my_gspread.add_data_to_range(sh, temp_df, metric_range)
                logging.info(f'Данные по "{metric}" успешно добавлены в диапазон {metric_range}')
                break
                
            except APIError as e:
                if e.response.status_code == 429:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logging.warning(f'Достигнуто максимальное количество попыток для метрики "{metric}"')
                        break
                    wait_time = random.uniform(1, 5) * retry_count
                    logging.warning(f'Превышен лимит запросов. Попытка {retry_count}/{max_retries} через {wait_time:.1f} сек...')
                    time.sleep(wait_time)
                else:
                    logging.error(f'Ошибка API при загрузке "{metric}":\n{e}')
                    break
                    
            except Exception as e:
                logging.error(f'Неизвестная ошибка при загрузке "{metric}":\n{e}')
                break


if __name__ == "__main__":

    # ----- 1. загрузка данных из бд -----
    curr_data, hist_data = load_data()

    # ----- 2. берём данные из гугл таблицы -----

    # sh = my_gspread.connect_to_local_sheet(os.getenv("LOCAL_TEST_TABLE"), AUTOPILOT_SHEET_NAME)
    sh = my_gspread.connect_to_remote_sheet(AUTOPILOT_TABLE_NAME, AUTOPILOT_SHEET_NAME)
    
    # сколько нужно выделить колонок под каждую метрику (по кол-ву дней)
    col_num = 6

    # начало диапазона
    values_first_row = 4

    # окончание диапазона
    sh_len = sh.row_count
    
    # заголовки для подсчёта номера колонки
    curr_headers = None #sh.row_values(2)
    hist_headers = None #sh.row_values(3)

    # отсортированный список артикулов, чтобы замэтчить данные
    # articles_raw = sh.col_values(1)[3:]
    # articles_sorted = [int(n) for n in articles_raw]
    
    sos_page = my_gspread.connect_to_remote_sheet(NEW_ITEMS_TABLE_NAME, NEW_ITEMS_ARTICLES_SHEET_NAME)
    articles_sorted = [int(i) for i in sos_page.col_values(1)]


    # ----- 3. обработка данных -----

    push_data_static_range(curr_data, curr_headers, col_num, articles_sorted, values_first_row, sh_len, pivot = True)
    logging.info('Данные за последнюю неделю успешно добавлены.\n')

    push_data_static_range(hist_data, hist_headers, 1, articles_sorted, values_first_row, sh_len, pivot = False)
    logging.info('Более ранние данные успешно добавлены.\n')



    # ----- 4. юнит -----


    # 4.1. обновление статуса рекламы

    logging.info('Updating the adv_status in Unit')

    # take yesterday's adv spend data {sku: 'реклама', sku1: ''}
    df_cut_adv_status = curr_data[curr_data['date'] == max(curr_data['date'])][['date', 'article_id', 'Сумма затрат']]

    # convert to dict
    autopilot_adv_status = df_cut_adv_status[['article_id', 'Сумма затрат']].set_index('article_id').to_dict()['Сумма затрат']

    # adv aspend --> adv status
    autopilot_adv_status = {int(key): 'реклама' if value > 0 else '' for key, value in autopilot_adv_status.items()}

    # connect to unit
    unit_sh = my_gspread.connect_to_remote_sheet('UNIT 2.0 (tested)', 'MAIN (tested)')
    # unit_sh = my_gspread.connect_to_local_sheet('https://docs.google.com/spreadsheets/d/1Cpxi7HbND5JuDz18FzDcm6Kdx5Ks8THf80cWt4hwFtc/edit?gid=1686563401#gid=1686563401',
    #                                             'MAIN (tested)')
    
    unit_skus = my_gspread.get_skus_unit(unit_sh)
    
    # добавляем удалённые товары
    new_adv_status_sorted = process_adv_status(unit_sh, autopilot_adv_status, unit_skus)
    
    # отправляем данные в gs
    update_adv_status_in_unit(unit_sh, new_adv_status_sorted)


    # 4.2. обновление отзывов

    logging.info('Updating the feedbacks in Unit')

    load_and_update_feedbacks_unit(unit_sh, unit_skus)

    logging.info('Выполнение скрипта завершено')