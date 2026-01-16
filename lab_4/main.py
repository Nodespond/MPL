import pandas as pd
import requests, re, os
from typing import Dict, List, Optional
import openpyxl
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Integer, Float, Date, BigInteger
from dotenv import load_dotenv

# Шаг 1 - получение файлов со страницы

# url = "https://spimex.com//files/trades/result/upload/reports/oil_xls/oil_xls_20251210162000.xls?r=8982&amp;p=L3VwbG9hZC9yZXBvcnRzL3BkZi9vaWwvb2lsXzIwMjUxMjEwMTYyMDAwLnBkZg.."
# response = requests.get(url)
# print(response.headers)
#
# print("Заголовки ответа:")
# for key, value in response.headers.items():
#     if 'content' in key.lower():
#         print(f"  {key}: {value}")
#
# # Способ 1: Сохраняем как есть (бинарный файл)
# with open('spimex_file_original.xls', 'wb') as f:
#     f.write(response.content)
# print("✓ Файл сохранен как 'spimex_file_original.xls'")

# Парсинг Excel файла

def simple_extract_data(file_path):
    """
    Максимально упрощенный вариант - только извлечение данных
    """
    # Читаем файл
    df = pd.read_excel(file_path, header=None, dtype=str)

    # Буфер для данных
    data_rows = []
    data_column_value = ""

    # Флаг, что мы внутри таблицы с данными
    inside_data_table = False

    i = 0
    # Просто идем по строкам
    while i < len(df):
        cell_value = str(df.iloc[i, 1]) if pd.notna(df.iloc[i, 1]) else ""

        # Нашли начало таблицы
        if 'Код\nИнструмента' in cell_value.replace(' ', ''):
            inside_data_table = True
            i+=2
            # Пропускаем следующую строку (вторая строка заголовка)
            continue

        if "Дата торгов:" in cell_value:
            data_column_value = cell_value.split(": ")[1]
            i+=1
            continue


        # Нашли конец таблицы
        elif 'Итого:' in cell_value and inside_data_table:
            inside_data_table = False
            i+=1
            continue

        # Если мы внутри таблицы с данными - сохраняем строку
        elif inside_data_table:
            data_rows.append(df.iloc[i].tolist())
            i+=1
        else:
            i+=1


    data_array = data_column_value.split(".")
    data_column_value = data_array[2] + "-" + data_array[1] + "-" + data_array[0]
    # Создаем DataFrame
    result_df = pd.DataFrame(data_rows, columns=df.columns)
    result_df['Дата'] = data_column_value
    print(f"Извлечено строк: {len(result_df)}")
    return result_df


# Использование
data = simple_extract_data("spimex_file_original.xls")
data.to_csv("simple_extracted.csv", index=False)

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
print(data.head(20))


# преобразуем полученные данные
data = data.iloc[:, 1:]
new_column_names = [
    'КодИнструмента',
    'НаименованиеИнструмента',
    'БазисПоставки',
    'ОбъемДоговоровЕИ',
    'ОбъемДоговоровРуб',
    'ИзмРынРуб',
    'ИзмРынПроц',
    'МинЦена',
    'СреднЦена',
    'МаксЦена',
    'РынЦена',
    'ЛучшПредложение',
    'ЛучшСпрос',
    'КоличествоДоговоров',
    'Дата'
]

data.columns = new_column_names
data['Товар'] = data['НаименованиеИнструмента'].apply(
        lambda x: x.split(',')[0] if ',' in x else x
    )
data = data.replace('-', None)
print(data.head(20))
data.to_csv("Parsed_data.csv", index=False)

# Теперь загрузим данные в PostgreSQL

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

DB_URL = (f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
          f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

def load_via_sqlalchemy(df, db_url, table_name='trade_data'):

    engine = create_engine(db_url)

    dtype_mapping = {
        'КодИнструмента': String(50),
        'НаименованиеИнструмента': String(1000),
        'БазисПоставки': String(500),
        'ОбъемДоговоровЕИ': Integer(),
        'ОбъемДоговоровРуб': BigInteger(),
        'ИзмРынРуб': Float(),
        'ИзмРынПроц': Float(),
        'МинЦена': Float(),
        'СреднЦена': Float(),
        'МаксЦена': Float(),
        'РынЦена': Float(),
        'ЛучшПредложение': Integer(),
        'ЛучшСпрос': Integer(),
        'КоличествоДоговоров': Integer(),
        'Дата': Date(),
        'Товар': String(200)
    }

    try:
        # Загружаем данные
        df.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False,
            dtype=dtype_mapping,
            method='multi'
        )

        print(f"Успешно загружено {len(df)} записей в {table_name}")
        return True

    except Exception as e:
        print(f"Ошибка при загрузке: {e}")
        return False


load_via_sqlalchemy(data, DB_URL,"trade_data" )