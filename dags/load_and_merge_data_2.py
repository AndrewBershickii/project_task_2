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

def merge_data(table_name, encoding='utf-8', delimiter=',', schema='dm', subset=None):
    '''
    Функция считывает данные из CSV и загружает их в базу данных, обновляя их, если данные по столбцам, указанным в параметре subset существуют.

    :param table_name: Имя таблицы, в которую загружаются данные.
    :param encoding: Кодировка CSV файла (по умолчанию 'utf-8').
    :param delimiter: Разделитель в CSV файле (по умолчанию ',').
    :param schema: Схема базы данных (по умолчанию 'dm').
    :param subset: Список столбцов, по которым будут удаляться дубликаты (по умолчанию None).
    '''
    postgres_hook = PostgresHook('dwh_db')                                                  # Создаем подключение к базе данных
    engine = postgres_hook.get_sqlalchemy_engine()
    csv_file_path = f"{PATH}data/dict_currency/{table_name}.csv"                            # Формируем путь к CSV файлу
    df_from_csv = pd.read_csv(csv_file_path, delimiter=delimiter, encoding=encoding)        # Читаем данные из файла
    df_from_csv.drop_duplicates(subset=subset, keep='first', inplace=True)                  # Удаляем дубликаты в данных, если есть

    with engine.begin() as conn:

        drop_unique_constraint_query = f"""
            ALTER TABLE {schema}.{table_name}
            DROP CONSTRAINT IF EXISTS unique_constraint_{table_name};
            """
        
        # Добавляем ограничение уникальности для дальнейшего использования синтаксиса INSERT INTO...ON CONFLICT
        add_unique_constraint_query = f"""
            ALTER TABLE {schema}.{table_name}
            ADD CONSTRAINT unique_constraint_{table_name} UNIQUE (currency_cd);
            """
        
        conn.execute(drop_unique_constraint_query)   # Удаляем ограничение уникальности в базе, если оно есть
        conn.execute(add_unique_constraint_query)    # Создаем ограничение уникальности

        for _, row in df_from_csv.iterrows():
            insert_query = f"""
            INSERT INTO {schema}.{table_name} ({', '.join([col for col in row.index])})
            VALUES ({', '.join(['%s'] * len(row))})
            ON CONFLICT (currency_cd)
            DO UPDATE SET {', '.join([f"{col} = EXCLUDED.{col}" for col in row.index if col != 'currency_cd'])}
            RETURNING *;
            """
            conn.execute(insert_query, tuple(row))
   
default_args = {
    "owner": "avbershits",
    "start_date": datetime.now(),
    "retries": 2
}

with DAG(
    "load_and_merge_data_2",
    default_args=default_args,
    description="Merge данных в RD",
    catchup=False,
    template_searchpath=[PATH],
    schedule="0 0 1 * *"    # Настраиваем расписания выполнения DAG-а каждый месяц 1 числа в полночь, т.к. у нас медленноменяющиеся данные.
) as dag:

    start = EmptyOperator(task_id="start")

    currency_merge = PythonOperator(
        task_id='currency_merge',
        python_callable=merge_data,
        op_kwargs={'table_name': 'dict_currency', 'subset': 'currency_cd'}
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> currency_merge
        >> end
    )