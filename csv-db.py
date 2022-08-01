import psycopg2
from psycopg2 import OperationalError
import requests
import copy


def download_csv():
    for month in range(1, 13):
        if month < 10:
            month = '0' + str(month)
        else:
            str(month)
        url = f'https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2021-{month}.parquet'
        filename = url.split('/')[-1]  # получаем последний элемент списка
        r = requests.get(url, allow_redirects=True)
        open(filename, 'wb').write(r.content)
        print(url)


def create_connection_db():
    connection = None
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="postgres")
        print("Подключение к базе PostgreSQL успешно")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


def create_tables():
    connection = create_connection_db()
    connection.autocommit = True
    # Создайте курсор для выполнения операций с базой данных
    cur = connection.cursor()
    try:
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE IF NOT EXISTS public.samplecsvfile_1 (
                                "1" int4 NULL,
                                "Eldon Base for stackable storage shelf, platinum" varchar(128) NULL,
                                "Muhammed MacIntyre" varchar(32) NULL,
                                "3" int4 NULL,
                                "-213.25" float4 NULL,
                                "38.94" float4 NULL,
                                "35" float4 NULL,
                                nunavut varchar(32) NULL,
                                "Storage & Organization" varchar(32) NULL,
                                "0.8" float4 NULL
                                );'''
        # Выполнение команды: это создает новую таблицу
        cur.execute(create_table_query)
        print("Таблица успешно создана в PostgreSQL")

    except OperationalError as e:
        print(f"The error '{e}' occurred")


def copy_csv():
    connection = create_connection_db()
    connection.autocommit = True
    cur = connection.cursor()
    try:
        f = open(r'/home/stern/PythonTEMP/SampleCSVFile_11kb.csv', 'r')
        f.readline()
        cur.copy_from(f, 'samplecsvfile_1', sep=',')
        f.close()
        print("Данные из CSV-файла успешно загружены в PostgreSQL")
    except OperationalError as e:
        print(f"The error '{e}' occurred")


# download_csv()
create_connection_db()
create_tables()
copy_csv()
