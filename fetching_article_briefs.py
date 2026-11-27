# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Скрипт для забирания кратких данных ("брифов") статей из "Популярного"
# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# - предусмотрены разные режимы запуска, на Кэггл юзаем только Nпоследних
# - нет :) //Сперва отправляемся в БД и забираем дату посл. записанной статьи
# - Коннект к спортсу и парсим аккуратно, батчами по 20
# - Если не получилось спарсить, выходим
# - Заносим новые данные в БД

import time
import math
import random
import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, date, timezone, timedelta
import re
import sys
import psycopg2
from psycopg2.extras import execute_values
import traceback

# вариант с запуском на Kaggle
from kaggle_secrets import UserSecretsClient
user_secrets = UserSecretsClient()
DB_HOST = user_secrets.get_secret("DB_HOST")
DB_NAME = user_secrets.get_secret("DB_NAME")
DB_PASSWORD = user_secrets.get_secret("DB_PASSWORD")
DB_USER = user_secrets.get_secret("DB_USER")

TG_CHAT_ID = user_secrets.get_secret("tg_chat_id")
TG_TOKEN = user_secrets.get_secret("tg_token")

#|%%%%%%%%%%%%%%%%%%%%%%%%| ПАРАМЕТРЫ БД |%%%%%%%%%%%%%%%%%%%%%%%%|

# вариант с запуском на Kaggle
DB_PARAMS = {
    "host": DB_HOST,
    "database": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "port": 5432,
    "sslmode": "require"
}
TABLE_NAME_ARTICLES = 'articles_briefs'
TABLE_NAME_BLOGS = 'blogs'
TABLE_NAME_AUTHORS = 'authors'
TABLE_NAME_CHANNELS = 'channels'
#|%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%|


# |%%%%%%%%%%%%%%%%%%%%%% ПАРАМЕТРЫ ПАРСЕРА %%%%%%%%%%%%%%%%%%%%%%|
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

QUERY_BASE_URL = 'https://www.sports.ru/core/post/list/?args='

# параметры для football / best
ARGS_FOOTBALL_ARTICLES = {
    "filter": {
        "type": "section-name",
        "name": "football",
        "sub-name": "",
        "article_type": "best"
    },
    "feeding": {
        "type": "time",
        # Для самого первого запроса можно поставить текущее время (в секундах)
        "last_published":int(time.time())
    }
}

# стартовые параметры GET-запроса для получения статей по всем блогам / best
ARGS_ALL_ARTICLES = {
"filter": {
    "type":"homepage",
    "article_type":"best"  # best - популярные статьи, all - все статьи
  },
 "feeding": {
     "type":"time",
     "last_published":int((datetime.now() + timedelta(hours=3)).timestamp())        # костыль, чтобы УТЦ+3 вставлять
   }
}
# |%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%|


# |%%%%%%%%%%%%%%%%%% РЕЖИМ ПАРСИНГА ПО УМОЛЧАНИЮ %%%%%%%%%%%%%%%%%%%|
PARSE_MODE = 'till_last_saved'
# PARSE_MODE = 'n_last_articles'
# PARSE_MODE = 'till_certain_date'

# для режима "парсить до определённой даты" мы юзаем ту же переменную last_saved_ts, в которую кладём таймстемп даты из PARSE_UNTIL
PARSE_UNTIL = '2026-10-15 00:00'

# для режима "скачать последние N статей" ('N_last_articles') нужно вручную задать след. константу
# не забываем, что реальное кол-во забранных статей будет кратно десяти: задали 3 - получили 10, задали 11 - держите 20
ARTICLES_TO_FETCH = 50
# |%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%|


# # считываем значения id "каналов" (видов спорта) НАЧАЛЬНЫЙ ВАРИАНТ
# CHANNELS_ID_FILENAME = 'channels_id.json'
# CHANNELS_ID_FILEPATH= '/content/drive/MyDrive/' + CHANNELS_ID_FILENAME

# try:
#     with open(CHANNELS_ID_FILEPATH, 'r', encoding='utf-8') as f:
#         CHANNELS = json.load(f)
#     print('Id каналов спорта успешно считаны в словарь CHANNELS, всего каналов', len(CHANNELS))
# except Exception as e:
#     print('КРИТИЧНО: Не удалось считать файл с данными по id видов спорта, выходим из скрипта', e)
#     raise SystemExit(1)
#     # если не считали - у нас есть дефолтное!
# # А ВОТ ЭТО ПРЯМ ПЛОХО РЕШЕНИЕ. ПРЕДСТАВЬ, ТЫ ЗАКИНУЛ НЕСКОЛЬКО НОВЫХ ВИДОВ ДЕНЬ НАЗАД, ЗАПИСАЛ ИХ К БРИФАМ.
# # На след. день вдруг файл не считался, мы берём дефолтный с 4 только видами, и дальше виды нам попадаются в рандомном порядке,
# # и их айди будут уже НЕ ТЕМИ ЖЕ, ЧТО МЫ ЗАПИСАЛИ ВЧЕРА!! ТО БИШЬ ТУТ НАДО, ПО ХОРОШЕМУ, ОСТАНАВЛИВАТЬ ПАРСЕР, если не считали файл
#     CHANNELS = {
#         'football': 1,
#         'hockey': 2,
#         'basketball': 3,
#         'figure-skating': 4
#     }

# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- #
#   -----------  D B   F U N C T I O N S  --------------   #
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- #

# ----- забираем из БД названия каналов для дикта CHANNELS -----

def GetChannelsFromDB(db_params, table_name):
    '''
    забираем из БД названия каналов, немного трансформируем их (ключи и значения местами меняем)
    возвращаем в том виде, который используется в скрипте
    '''

    all_channels = None

    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        query = f"SELECT * FROM {table_name}"

        cursor.execute(query)
        channels_data = cursor.fetchall()
        conn.commit()

    except Exception as e:
        print(f"не удалось получить данные по каналам из таблицы {table_name}. Произошла ошибка: {e}")
        if conn:
            conn.rollback()
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

    # сразу переводим каналы в такой дикт, какой юзается здесь в скрипте
    try:
        if channels_data and len(channels_data) > 0:
            all_channels = {chan:id for (id, chan) in channels_data}
            print('Успешно считали из БД и сохранили локально инфу о каналах CHANNELS')
        else:
            print('КРИТИЧНО: таблица с каналами, вероятно, пустая, без этой информации нельзя продолжать скрипт')
            sys.exit()
    except Exception as e:
        print('КРИТИЧНО: Не получилось забрать информацию по каналам из БД, останавливаю скрипт')
        sys.exit()

    return all_channels


# ----- отдаём дату последнего сохр. брифа в виде timestamp -----

def GetLastSavedDate(db_params, table_name) -> int:
    '''
    отдаём дату последнего сохр. брифа статьи в виде timestamp (интовый)
    '''

    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        sql_query = f'''
        SELECT published_at FROM {table_name}
        ORDER BY published_at DESC
        LIMIT 1
        '''
        cursor.execute(sql_query)
        raw_data = cursor.fetchall()
    except Exception as e:
        print(f'Не удалось подключиться или выполнить запрос в БД {table_name}. Ошибка {e}')
        return None
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

    return int(raw_data[0][0].timestamp())


# ----- добавлям новое значение в табличку каналов -----

def update_channels_table(db_params, table_name, new_channels):
    '''
    new_channels - лист из тьюплов (айди_канала, имя канала)
    '''
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        query = f"""
            INSERT INTO {table_name} (
                channel_id, channel_name
            )
            VALUES %s
            ON CONFLICT (channel_id)
            DO NOTHING
            RETURNING channel_id;
        """
        execute_values(cursor, query, new_channels)
        inserted = [row for row in cursor]
        conn.commit()
        print(f"Вставленные новые записи: {inserted}")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()
            
    return inserted


# ----- выдаём айди канала по названию канала из урла и обновляем список, если канал раньше не встречался -----

def get_channel_id_by_name(url):
    '''
    То же самое, что и пред. версия функции, но "более взрослая" - если у нас уже есть БД,
    логичнее хранить инфу "айди - название канала" ТАМ.

    если канал не нашли в урле -> -1, если не нашли канал среди имеющихся - апдейтим и БД, и локальный словарь глобально
    '''

    # обязательно, будем изменять их туть
    global CHANNELS

    # достаём то, что между слэшами перед словом blogs
    # пример актуального на 11.10.26 урла: 'https://www.sports.ru/football/blogs/3522667.html'
    #    забыли в регулярке учесть дефис, из-за этого не узнали ice-skating )) fixed
    id_matching = re.search(r"/([\w-]+)/blogs", url)
    if id_matching:
        sports_name = id_matching.group(1)
    else:
        print('ИСПРАВИТЬ: Структура урла для статей блогов поменялась, не найден вид спорта в урле', url)
        return -1

    channel_id = CHANNELS.get(sports_name)

    if not channel_id:
        print(f'Найден новый канал спорта: {sports_name}. Записываем его в базу данных!')
        channel_id = max(CHANNELS.values())+1
        try:
            # обновляем таблицу
            ee = update_channels_table(db_params=DB_PARAMS, table_name=TABLE_NAME_CHANNELS, new_channels=[(channel_id, sports_name),])
            # обновляем и "локальный глобальный" дикт, ну вот так
            CHANNELS.update({sports_name: channel_id})
            print('Данные по каналам обновлены')
        except Exception as e:
            print('Не удалось записать новый канал в таблицу каналов в БД, для статьи будет использовано значение "-1". Ошибка', e)
            return -1

    return channel_id


# ------- записываем данные по статьям -------

def SavingBriefsArt_DB(fetched_briefs, db_params, table_name):
    '''
    записываем в БД, что там наскребли из общего канала блогов в очередной раз
    '''
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        print(f"Успешное подключение к БД для записи брифов статей в таблицу {table_name}")
        query = f"""
            INSERT INTO {table_name} (
                channel_id, title, url, blog_id, authors_id, published_at, article_id
            )
            VALUES %s
            ON CONFLICT (article_id)
            DO NOTHING
            RETURNING article_id;
        """
        execute_values(cursor, query, fetched_briefs)
        inserted_ids = [row for row in cursor]
        real_inserted = len(inserted_ids)
        conn.commit()
        print(f"Всего пришло записей: {len(fetched_briefs)}")
        print(f"Вставлено новых записей: {real_inserted}")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

    return True


# ------- записываем данные по блогам -------

def SavingBlogsData_DB(fetched_blog_briefs, db_params, table_name):
    '''
    записываем в БД данные по блогам (да, отдельная табличка для блогов есть)
    СМОТРЮ ХОРОШЕНЬКО, И ДУМАЮ, ЧТО ПОСЛ. ДВЕ ФУНКЦИИ Ж ЛЕГКО ОБЪЕДИНИТЬ...
    '''
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        print(f"Успешно подключились к ДБ для записи инфы по блогам в таблицу {table_name}")
        query = f"""
            INSERT INTO {table_name} (
                blog_slug, blog_id, blog_url, blog_name
            )
            VALUES %s
            ON CONFLICT (blog_id)
            DO NOTHING
            RETURNING blog_id;
        """
        execute_values(cursor, query, fetched_blog_briefs)
        # DONE. ПОФИКСИТЬ! ВЫДАЁТ ДЛИНУ ПОСЛ. БАТЧА
        # real_inserted = cursor.rowcount
        inserted_ids = [row for row in cursor]
        real_inserted = len(inserted_ids)

        conn.commit()
        print(f"Всего пришло записей: {len(fetched_blog_briefs)}")
        print(f"Вставлено новых записей: {real_inserted}")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

    return True


# ------- записываем данные по авторам -------

def SavingAuthorsData_DB(fetched_authors, db_params, table_name):
    '''
    записываем в БД инфу про авторов (пока просто имя :)
    наконец-то это сделал, пару недель собирался )
    '''
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        print(f"Успешное подключение к БД для записи инфы про авторов в таблицу {table_name}")
        query = f"""
            INSERT INTO {table_name} (
                authors_id, name
            )
            VALUES %s
            ON CONFLICT (authors_id)
            DO NOTHING
            RETURNING authors_id;
        """
        execute_values(cursor, query, fetched_authors)
        inserted_ids = [row for row in cursor]
        real_inserted = len(inserted_ids)
        conn.commit()
        print(f"Всего пришло записей по авторам: {len(fetched_authors)}")
        print(f"Вставлено новых записей: {real_inserted}")
    except Exception as e:
        print(f"Произошла ошибка!")
        err_text = traceback.format_exc()   # полная трассировка как текст
        print(f"Ошибка: {e}\nПодробнее:\n{err_text}")
        if conn:
            conn.rollback()
        return False
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

    return True

# ВТОРОЙ ПОЛНОТЕКСТ-ПАРСЕР: для блогов. "Бесконечная" лента, изначально подгружается 15 статей.
# КСТАТИ тоже ВАЖНЫЙ МОМЕНТ!
# Таймстемпы из джсона - они по нулю UTC, то есть на 3 часа меньше реальной даты публикации!

def OLD_get_channel_id_by_name(url):
    '''
    Эта штука нам нужна для того, чтобы вместо темы блога\канала сайта, например, "футбол" записывать числа 1, 2...
    Чот наверное лишние усилия, ну да ладно, сделал и жаль удалять ))
    крч,
    берём урл статьи для блога на спортсе,
    возвращаем число, соотв. виду спорта (сами определяем этот номер в глоб. словаре CHANNELS)

    если канал не нашли в урле -> -1, если не нашли канал среди имеющихся - апдейтим словарь, записываем в файлик
    '''
    global CHANNELS
    # достаём то, что между слэшами перед словом blogs
    # пример актуального на 11.10.26 урла: 'https://www.sports.ru/football/blogs/3522667.html'
    #    забыли в регулярке учесть дефис, из-за этого не узнали ice-skating )) fixed
    id_matching = re.search(r"/([\w-]+)/blogs", url)
    if id_matching:
        sports_name = id_matching.group(1)
    else:
        print('ИСПРАВИТЬ: Структура урла для статей блогов поменялась, не найден вид спорта в урле', url)
        # sports_name = 'unknown'
        return -1
        
    # если не в списке самых важных - записываем в "другое"
    #   хм... а сделаем-ка для интереса самозапись каналов спортса! то есть если не нашли,
    #     записываем в Чэннелс и даём номер...

    channel_id = CHANNELS.get(sports_name)
    if not channel_id:
        print(f'Найден новый канал спорта: {sports_name}. Записываем его в файл начальных данных {CHANNELS_ID_FILENAME}')
        channel_id = max(CHANNELS.values())+1
        CHANNELS.update({sports_name: channel_id})
        try:
            with open(CHANNELS_ID_FILEPATH, 'w', encoding='utf-8') as f:
                json.dump(CHANNELS, f, ensure_ascii=False)
            print('Данные по каналам спорта обновлены')
        except Exception as e:
            print('Не удалось записать файл, при след. запуске будут использованы значения channels id по умолчанию.', e)

    return channel_id


def EndlessFeedDownloading(n_articles=10, last_saved_ts=None) -> list:
    '''
    n_articles - сколько брифов мы хотим собрать
    last_saved_ts - дата-время, до которого будем собирать статьи (задействовано в двух режимах запуска парсера)
    '''

    all_articles = []
    iteration = 0
    # в одной "порции" бесконечной ленты блогостатей - 10 айтемов
    #  => для "не круглых" значений articles_to_fetch реально забранное кол-во статей будет кратно 10
    # апд НЕАКТУАЛЬНО в тек. версии
    # pages_count = math.ceil(articles_to_fetch / 10)

    print(f'{"="*15} Запускаем парсер для сбора брифов статей из блогов {"="*15}')
    print(f'Режим парсинга: {PARSE_MODE}')
    print('Отправляем GET-параметры через базовый урл для сбора брифов статей из блогов')
    # Открываем ОДНУ сессию на все запросы
    with requests.Session() as session:
        session.headers.update(headers)
        # были ли проблемы с подгружением статей?
        is_connection_troubles = False
        # кол-во попыток коннекта
        attempts = 0
        # for iteration in range(pages_count):
        # ок, переписал с цикла for - так просто удобнее реализовать разные режимы скрейпинга (посл. N статей, до посл. сохранённого и т.п...)
        # крч чтоб помнить: одна итерация цикла - ОДНА ПОРЦИЯ подгружаемых статей, это 10 штук
        while True:
            # аххах, из-за того, что объявляю урл вне цикла что происходит? правильно, в каждой итерации мы скачиваем одни и те же первые брифы...
            # fixed
            url = QUERY_BASE_URL + json.dumps(ARGS_ALL_ARTICLES)

            try:
                # делаем запрос ЧЕРЕЗ СЕССИЮ, она использует уже открытое соединение
                response = session.get(url, timeout=7)

                if response.status_code != 200:
                    print(f"Ошибка запроса: {response.status_code}")
                    is_connection_troubles = True
                    attempts += 1
                    if attempts > 3:
                        print('ВАЖНО: проблемы со связью, не удалось подключиться больше 3 раз')
                        break
                    else:
                        time.sleep(random.uniform(2, 5))
                        continue

                # на этот раз всё просто - у нас в ответе чистейшей воды джейсончик без всякой хтмльщины!
                data = response.json()
                posts = data.get('documents')
                print('Посты:', posts)
                if not posts:
                    print("Статьи не удалось подгрузить...")
                    is_connection_troubles = True
                    continue

                all_articles.extend(posts)

                # несколько условий по выходу... здесь цикл while очень помогает в условиях, когда мы явно не прокидываем в процедуру PARSE_MODE
                if len(all_articles) >= n_articles and not last_saved_ts:
                    print(f'\nНабрали нужное кол-во статей ({len(all_articles)}), выходим...')
                    break

                # условие значит, что мы "перескочили" через посл. сохранённую статью
                if last_saved_ts and last_saved_ts > data.get('last_published'):
                    print('\nБолее "старые" статьи у нас уже есть, выходим...')
                    break

                if not data.get('has_more_documents'):
                    print('\nСтатей больше нет, спарсили буквально всё! Выходим...')
                    break

                # вывод, чтобы было видно, что мы не зависли :)
                if iteration == 0:
                    print('Всего загружено статей: ', end='')
                else:
                    print(len(all_articles), end='... ')

                # если скрипт запускается даже раз в неделю, это условие не должно выполняться, но для цикла while стоп необходим...
                if iteration > 1000:
                    print('ВАЖНО: Достигли макс. значения глубины парсинга за один запуск: 1000 брифов.')
                    break

                ARGS_ALL_ARTICLES['feeding']['last_published'] = data.get('last_published')

            except requests.exceptions.RequestException as e:
                print(f"\nСетевая ошибка: {e}")
                # из-за этого break мы не добирали статей... обнаружил баг только когда на первом же урле таймаут случился и выкинуло из парсера с НУЛЁМ брифов, хотя реквестовал 60 в режиме N-последних
                # break

            iteration += 1
            time.sleep(random.uniform(3, 6))

    return all_articles, is_connection_troubles


def picking_briefs_data(all_articles):
    # --- ЦИКЛ ПЕРВИЧНОЙ ОБРАБОТКИ СТАТЕЙ ---
    # (ДАННЫЕ ДЛЯ БД, ПО ЭТИМ ДАННЫМ ОТДЕЛЬНЫМ СКРИПТОМ БУДЕМ ЗАБИРАТЬ ПОЛНЫЕ СТАТЬИ - СХЕМА, КАК С НОВОСТНЫМ ХАБОМ... )
    articles_data_to_save = []
    blogs_data_to_save = []
    authors_data_to_save = set() # тут просто айди-имя для авторов
    unique_blog_id  = set() # простой способ скипнуть повторы блог-Id...
    if all_articles:
        # print(f'{"-"*7} Подключение завершено, выбираем необходимые для ДБ данные {"-"*7}')
        try:
            for post in all_articles:
                # Пример извлечения данных (названия ключей сверьте в Network -> Preview)
                article_title = post.get('title')
                article_url = post.get('desktop_url')   # отсюда мы выбираем название спорта, а сам урл не вносим в БД... апд. по совету ИИ всё-таки оставим урл ))
                article_id = post.get('id')
                article_date = post.get('ptime').get('full')  # у спортса есть 'ptime' - когда впервые статья опубл., и есть 'published' - когда снова статью вывесили на главную
                blog_id = post.get('blog').get('id')
                blog_slug = post.get('blog').get('webname')
                blog_url = post.get('blog').get('url')
                blog_name = post.get('blog').get('name')
                authors_ids = []
                for aut in post.get('authors', []):
                    author_id = aut['id']
                    if author_id:
                        authors_ids.append(author_id)
                        authors_data_to_save.add(tuple([author_id, aut.get('nick')]))
                channel_id = get_channel_id_by_name(article_url)

                # сразу формируем список из тьюплов, проще вносить в нашу БД

            # -------------- ФОРМИРУЕМ ДАННЫЕ ДЛЯ ВСТАВКИ В ТАБЛ. БРИФОВ СТАТЕЙ ---------------
                articles_data_to_save.append(
                    (channel_id, article_title, article_url, blog_id, json.dumps(authors_ids), article_date, article_id)
                    )
            # -------------- ФОРМИРУЕМ ДАННЫЕ ДЛЯ ВСТАВКИ В ТАБЛ. БЛОГОВ ---------------
                # добавляем только если есть, чо
                if blog_id:
                    blogs_data_to_save.append(
                        (blog_slug, blog_id, blog_url, blog_name)
                        )


            print(f'{"="*20} Данные по статьям и блогам успешно выбраны {"="*20}')
        except Exception as e:
            print('\n! Вероятно, не удалось собрать данные из json, проверьте ключи title, desktop_url, id, authors (+вложенные id для авторов)')
            err_text = traceback.format_exc()   # полная трассировка как текст
            print(f"Ошибка: {e}\nПодробнее:\n{err_text}")
    else:
        print('ВАЖНО: Список статей пуст, выходим')

    return (articles_data_to_save, blogs_data_to_save, authors_data_to_save)


def main(parse_mode = PARSE_MODE, parse_until = PARSE_UNTIL, articles_to_fetch = ARTICLES_TO_FETCH):
    '''
    Режимы работы скрипта: 'till_last_saved', 'n_last_articles', 'till_certain_date'
    ---
    till_last_saved
    до последнего сохранённого брифа
    ---
    parse_until
    для режима "парсить до определённой даты" мы юзаем ту же переменную last_saved_ts, в которую кладём таймстемп даты из PARSE_UNTIL
    пример parse_until = '2026-10-15 00:00'
    ---
    n_last_articles
    для режима "скачать последние N статей" нужно вручную задать articles_to_fetch
    не забываем, что реальное кол-во забранных статей будет кратно десяти: задали 3 - получили 10, задали 11 - держите 20
    '''

    # режим работы влияет на то, какую дату будем брать как последнюю сохранённую
    if parse_mode == 'till_last_saved':
        last_saved_ts = GetLastSavedDate(DB_PARAMS, TABLE_NAME_ARTICLES)
    elif parse_mode == 'till_certain_date':
        last_saved_ts = int(datetime.strptime(parse_until, "%Y-%m-%d %H:%M").timestamp())
    else:
        last_saved_ts = None

    # v.22ноября - забираем каналы ИЗ БД и перекидываем их в глобальный дикт для удобства
    global CHANNELS
    CHANNELS = GetChannelsFromDB(DB_PARAMS, TABLE_NAME_CHANNELS)
    
    arts, connect_troubles = EndlessFeedDownloading(articles_to_fetch, last_saved_ts)

    # если не набрали статей, сразу выходим
    if len(arts) == 0:
        print('ВАЖНО: Cтатьи получить не удалось, выходим.')
        return arts, connect_troubles
    else:
        data_to_save = picking_briefs_data(arts)
        # articles_data_to_save
        SavingBriefsArt_DB(data_to_save[0], DB_PARAMS, TABLE_NAME_ARTICLES)
        # blogs_data_to_save
        SavingBlogsData_DB(data_to_save[1], DB_PARAMS, TABLE_NAME_BLOGS)
        # authors_data_to_save, БД спокойно кушает множества при вставке, можно не отправлять списком
        SavingAuthorsData_DB(data_to_save[2], DB_PARAMS, TABLE_NAME_AUTHORS)

    return data_to_save, connect_troubles


if __name__ == '__main__':
    # print('Запуск мэйна...')
    main()