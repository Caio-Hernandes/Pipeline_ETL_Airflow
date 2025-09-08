from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import pandas as pd

DEFAULT_ARGS = {
    'owner': 'caio',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


DATA_ROOT = '/opt/airflow/data'  # Base para salvar no FileSystem Local
INCLUDE_CSV = "/opt/airflow/include/transacoes.csv"

with DAG(
    dag_id='banvic_pipeline',
    default_args=DEFAULT_ARGS,
    schedule_interval='35 4 * * *',
    start_date=datetime(2025, 1, 1), 
    catchup=False,
    tags=['banvic'],
) as dag:

    def extract_sql(ds, **kwargs):
        
        hook = PostgresHook(postgres_conn_id='banvic_postgres')
        conn = hook.get_conn()
        cur = conn.cursor()
        
        tables = ['agencias','clientes','colaboradores','colaborador_agencia','contas','propostas_credito']
        
        
        target_base = os.path.join(DATA_ROOT, ds, 'sql')
        os.makedirs(target_base, exist_ok=True)

        for table in tables:
            cur.execute(f"SELECT * FROM {table};")
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=cols)
            
            file_path = os.path.join(target_base, f"{table}.csv")
            df.to_csv(file_path, index=False)
            print(f"Arquivo salvo: {file_path}")

        cur.close()
        conn.close()

    def extract_csv(ds, **kwargs):
        
        
        target_dir = os.path.join(DATA_ROOT, ds, 'csv')
        os.makedirs(target_dir, exist_ok=True)
        
        
        df = pd.read_csv(INCLUDE_CSV)
        
        file_path = os.path.join(target_dir, 'transacoes.csv')
        df.to_csv(file_path, index=False)
        print(f"Arquivo salvo: {file_path}")

    def load_dw(ds, **kwargs):
        hook = PostgresHook(postgres_conn_id='dw_postgres')
        conn = hook.get_conn()
        cur = conn.cursor()

        staging_schema = 'staging'
        mart_schema = 'mart'
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {staging_schema};")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {mart_schema};")

        base_dir = os.path.join(DATA_ROOT, ds)
        print(f"Carregando dados de: {base_dir}")

        for sub in ['sql', 'csv']:
            folder = os.path.join(base_dir, sub)
            if not os.path.isdir(folder):
                print(f"Pasta não encontrada: {folder}")
                continue
                
            
            for fname in os.listdir(folder):
                if not fname.lower().endswith('.csv'):
                    continue
                    
                path = os.path.join(folder, fname)
                table = f"{sub}_{os.path.splitext(fname)[0]}"
                
            
                # Lê cabeçalho e cria staging com as colunas text
                with open(path, 'r', encoding='utf-8') as f:
                    header = f.readline().strip()
                cols = [c.strip().strip('"') for c in header.split(',')]
                cols_ddl = ', '.join([f'"{c}" TEXT' for c in cols])
                cur.execute(f"CREATE TABLE IF NOT EXISTS {staging_schema}.{table} ({cols_ddl});")
                cur.execute(f"TRUNCATE {staging_schema}.{table};")

                cols_quoted = ', '.join([f'"{c}"' for c in cols])
                with open(path, 'r', encoding='utf-8') as f:
                    cur.copy_expert(f"COPY {staging_schema}.{table} ({cols_quoted}) FROM STDIN WITH CSV HEADER DELIMITER ','", f)

                # Passar para mart e garantir independência
                cur.execute(f"CREATE TABLE IF NOT EXISTS {mart_schema}.{table} (LIKE {staging_schema}.{table} INCLUDING ALL);")
                cur.execute(f"TRUNCATE {mart_schema}.{table};")
                cur.execute(f"INSERT INTO {mart_schema}.{table} SELECT * FROM {staging_schema}.{table};")

        conn.commit()
        cur.close()
        conn.close()

    t1 = PythonOperator(
        task_id='extract_sql',
        python_callable=extract_sql,
        op_kwargs={'ds': '{{ ds }}'}
    )

    t2 = PythonOperator(
        task_id='extract_csv',
        python_callable=extract_csv,
        op_kwargs={'ds': '{{ ds }}'}
    )

    t3 = PythonOperator(
        task_id='load_dw',
        python_callable=load_dw,
        op_kwargs={'ds': '{{ ds }}'}
    )

    [t1, t2] >> t3 # faz o 1, o 2, se der green, faz o 3.