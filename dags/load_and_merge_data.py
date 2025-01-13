import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.configuration import conf
from airflow.models import Variable


PATH = Variable.get('my_path')                                                                                      # Получаем путь из переменной Airflow
conf.set('core', 'template_searchpath', PATH)

def load_and_merge_data(table_name, encoding='windows-1251', delimiter=',', schema='rd', subset=None):
    '''
    Функция считывает данные из CSV и базы данных, объединяет их, удаляет дубликаты и загружает обратно в базу данных.

    :param table_name: Имя таблицы, из которой считываются и в которую загружаются данные.
    :param encoding: Кодировка CSV файла (по умолчанию 'windows-1251').
    :param delimiter: Разделитель в CSV файле (по умолчанию ',').
    :param schema: Схема базы данных (по умолчанию 'rd').
    :param subset: Список столбцов, по которым будут удаляться дубликаты (по умолчанию None).
    '''
    postgres_hook = PostgresHook('dwh_db')                                                                          # Создаем подключение к базе данных
    engine = postgres_hook.get_sqlalchemy_engine()
    csv_file_path = f"{PATH}data/loan_holiday_info/{table_name}.csv"                                                # Формируем путь к CSV файлу
    
    primary_keys = {
        "deal_info": ['deal_num', 'effective_from_date'],
        "product": ['product_rk', 'product_name', 'effective_from_date']
    }
    df_from_csv = pd.read_csv(csv_file_path, delimiter=delimiter, encoding=encoding)        # Читаем данные из CSV файла
    df_from_csv.drop_duplicates(subset=subset, keep='first', inplace=True)                  # Удаляем дубликаты в данных из файла
    pk_columns = primary_keys[table_name]                                                   # Подтягиваем наименование столбцов, по которым ищем дубликаты

    with engine.begin() as conn:
        delete_duplicates_query = f"""                                                      
            WITH RowNumbers AS (
                SELECT ctid, ROW_NUMBER() OVER part as rn
                  FROM {schema}.{table_name}
                WINDOW part AS (PARTITION BY {', '.join(pk_columns)})
            )
            DELETE FROM {schema}.{table_name}
            WHERE ctid IN (
                SELECT ctid
                  FROM RowNumbers
                 WHERE rn > 1)
            RETURNING *;
            """
        
        drop_unique_constraint_query = f"""
            ALTER TABLE {schema}.{table_name}
            DROP CONSTRAINT IF EXISTS unique_constraint_{table_name};
            """
        
        # Добавляем ограничение уникальности для дальнейшего использования синтаксиса INSERT INTO...ON CONFLICT
        add_unique_constraint_query = f"""
            ALTER TABLE {schema}.{table_name}
            ADD CONSTRAINT unique_constraint_{table_name} UNIQUE ({', '.join(pk_columns)});
            """
        
        conn.execute(drop_unique_constraint_query)   # Удаляем ограничение уникальности в базе, если оно есть
        conn.execute(delete_duplicates_query)        # Удаляем дубликаты в базе
        conn.execute(add_unique_constraint_query)    # Создаем ограничение уникальности

        for _, row in df_from_csv.iterrows():
            insert_query = f"""
            INSERT INTO {schema}.{table_name} ({', '.join([col for col in row.index])})
            VALUES ({', '.join(['%s'] * len(row))})
            ON CONFLICT ({', '.join(pk_columns)})
            DO UPDATE SET {', '.join([f"{col} = EXCLUDED.{col}" for col in row.index if col not in pk_columns])}
            RETURNING *;
            """
            conn.execute(insert_query, tuple(row))   # Итерируемся по каждой строчке и проверяем её наличие в базе, если есть, данные обновляются, если нет - вставляем строку
    
default_args = {
    "owner": "avbershits",
    "start_date": datetime.now(),
    "retries": 2
}

with DAG(
    "load_and_merge_data",
    default_args=default_args,
    description="Merge данных в RD",
    catchup=False,
    template_searchpath=[PATH],
    schedule="0 0 1 * *"    # Настраиваем расписания выполнения DAG-а каждый месяц 1 числа в полночь, т.к. у нас медленноменяющиеся данные.
) as dag:

    start = EmptyOperator(task_id="start")

    deal_info_data = PythonOperator(
        task_id='deal_info_data',
        python_callable=load_and_merge_data,
        op_kwargs={'table_name': 'deal_info', 'subset': ['deal_num', 'effective_from_date']}
    )

    product_info_data = PythonOperator(
        task_id='product_info_data',
        python_callable=load_and_merge_data,
        op_kwargs={'table_name': 'product', 'subset': ['product_rk', 'product_name', 'effective_from_date']}
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> [deal_info_data, product_info_data]
        >> end
    )
