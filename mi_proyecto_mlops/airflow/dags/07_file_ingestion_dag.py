from __future__ import annotations

import pendulum
import pandas as pd
import re
from pathlib import Path
from datetime import datetime
import shutil
import numpy as np

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ... (Constantes y MESES no cambian) ...
POSTGRES_CONN_ID = "postgres_sipsa"; TARGET_SCHEMA = "sipsa_data"; LANDING_ZONE = Path("/opt/airflow/data/landing_zone"); ARCHIVE_ZONE = Path("/opt/airflow/data/archive"); LOG_TABLE = "file_ingestion_log"; TARGET_TABLE = "mayoristas_prices"; MESES = {'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12}

@task
def setup_database_task():
    # ... (Sin cambios) ...
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"
    create_log_table_sql = f"""CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{LOG_TABLE} (id SERIAL PRIMARY KEY, file_name VARCHAR(255) UNIQUE NOT NULL, status VARCHAR(50), processed_at TIMESTAMP, finished_at TIMESTAMP, row_count INTEGER, error_message TEXT);"""
    create_target_table_sql = f"""CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} (id SERIAL PRIMARY KEY, product_name VARCHAR(255), city_name VARCHAR(255), report_date DATE, price NUMERIC(10, 2), variation FLOAT, source_file VARCHAR(255), UNIQUE (product_name, city_name, report_date));"""
    hook.run(create_schema_sql); hook.run(create_log_table_sql); hook.run(create_target_table_sql)

@task
def scan_landing_zone_task():
    # ... (Sin cambios) ...
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    processed_files_df = hook.get_pandas_df(f"SELECT file_name FROM {TARGET_SCHEMA}.{LOG_TABLE}")
    processed_files = processed_files_df['file_name'].tolist() if not processed_files_df.empty else []
    all_files = [f.name for f in LANDING_ZONE.glob("*.xls")]
    new_files = [f for f in all_files if f not in processed_files]
    return new_files

@task
def process_file_task(file_name: str):
    """
    Procesa un archivo, valida duplicados y carga los registros nuevos fila por fila
    para un control de errores granular.
    """
    file_path = LANDING_ZONE / file_name
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(f"INSERT INTO {TARGET_SCHEMA}.{LOG_TABLE} (file_name, status, processed_at) VALUES ('{file_name}', 'processing', NOW()) ON CONFLICT (file_name) DO UPDATE SET status = 'processing', processed_at = NOW(), error_message = NULL")
    
    try:
        # 1. Lectura del Excel y extracción de la fecha
        df_raw = pd.read_excel(file_path, header=[0, 1, 2], engine='xlrd')
        report_date_str = df_raw.columns[0][1]
        
        # 2. Simplificación del DataFrame
        df = df_raw['Boletín diario de precios mayoristas'][report_date_str]
        df.rename(columns={'Precio $/Kg': 'product_name'}, inplace=True)
        df.dropna(subset=['product_name'], inplace=True)
        df['product_name'] = df['product_name'].str.rstrip(' *')

        # 3. Transformación Vectorizada (de "ancho" a "largo")
        df_melted = df.melt(id_vars='product_name', var_name='city_raw', value_name='value')
        df_melted['metric'] = np.where(df_melted['city_raw'].str.endswith('.1'), 'variation', 'price')
        df_melted['city_name'] = df_melted['city_raw'].str.replace(r'\.1$', '', regex=True)
        df_final = df_melted.pivot_table(index=['product_name', 'city_name'], columns='metric', values='value', aggfunc='first').reset_index()

        # 4. Limpieza final y adición de metadatos
        match = re.search(r'(\d{1,2}) de (\w+) de (\d{4})', report_date_str)
        if match:
            day, month_name, year = match.groups()
            month = MESES.get(month_name.lower())
            if month:
                df_final['report_date'] = datetime(int(year), month, int(day)).date()
        
        df_final['source_file'] = file_name
        df_final['price'] = pd.to_numeric(df_final['price'], errors='coerce')
        df_final['variation'] = pd.to_numeric(df_final['variation'], errors='coerce')
        df_final.dropna(subset=['price'], inplace=True)

        if 'report_date' not in df_final.columns or df_final.empty:
            raise ValueError("No se pudo extraer una fecha válida o el archivo está vacío después de la transformación.")

        # 5. Validación de Duplicados (igual que antes)
        report_date = df_final['report_date'].iloc[0]
        existing_records_df = hook.get_pandas_df(
            f"SELECT product_name, city_name FROM {TARGET_SCHEMA}.{TARGET_TABLE} WHERE report_date = '{report_date}'"
        )
        
        if not existing_records_df.empty:
            df_merged = df_final.merge(existing_records_df, on=['product_name', 'city_name'], how='left', indicator=True)
            df_to_insert = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])
            skipped_rows = len(df_final) - len(df_to_insert)
            print(f"[{file_name}] Se omitieron {skipped_rows} registros duplicados ya existentes.")
        else:
            df_to_insert = df_final

        # 6. Carga a la Base de Datos (fila por fila)
        if df_to_insert.empty:
            print(f"[{file_name}] No hay filas nuevas para insertar.")
            success_count = 0
            failed_rows_info = []
        else:
            conn = hook.get_conn()
            cursor = conn.cursor()
            success_count = 0
            failed_rows_info = []
            
            cols = ['product_name', 'city_name', 'report_date', 'price', 'variation', 'source_file']
            df_to_insert = df_to_insert[cols]

            for row in df_to_insert.itertuples(index=False, name=None):
                try:
                    # Convertimos valores NaN de pandas a None, que es el NULL de SQL
                    row_with_none = [None if pd.isna(val) else val for val in row]
                    insert_sql = f"""
                        INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE} (product_name, city_name, report_date, price, variation, source_file)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_sql, row_with_none)
                    conn.commit()
                    success_count += 1
                except Exception as row_e:
                    conn.rollback()
                    failed_rows_info.append({'row_preview': row[:2], 'error': str(row_e).replace("'", "''")})
            
            cursor.close()
            conn.close()

        print(f"[{file_name}] Proceso de carga finalizado. Éxitos: {success_count}, Fallos: {len(failed_rows_info)}.")

        # 7. Registro de Éxito / Errores y Archivo
        final_status = 'success'
        log_message = f"Se cargaron {success_count} filas con éxito."
        
        if failed_rows_info:
            final_status = 'completed_with_errors'
            error_summary = f"Fallaron {len(failed_rows_info)} filas. Primer error: {failed_rows_info[0]['error']}"
            log_message += f" {error_summary}"
            hook.run(f"UPDATE {TARGET_SCHEMA}.{LOG_TABLE} SET error_message = '{log_message}' WHERE file_name = '{file_name}'")

        hook.run(f"UPDATE {TARGET_SCHEMA}.{LOG_TABLE} SET status = '{final_status}', row_count = {success_count}, finished_at = NOW() WHERE file_name = '{file_name}'")
        archive_path = ARCHIVE_ZONE / file_name
        shutil.move(file_path, archive_path)
        print(f"[{file_name}] Proceso completado. Estado final: {final_status}.")

    except Exception as e:
        error_message = str(e).replace("'", "''")
        print(f"¡ERROR CRÍTICO! [{file_name}] Falló el procesamiento del archivo: {error_message}")
        hook.run(f"UPDATE {TARGET_SCHEMA}.{LOG_TABLE} SET status = 'failed', error_message = 'Error crítico: {error_message}', finished_at = NOW() WHERE file_name = '{file_name}'")

# ... (El resto del DAG no cambia)
with DAG(dag_id="file_ingestion_from_excel", start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), schedule="0 */12 * * *", catchup=False, tags=["etl", "files", "excel"], max_active_runs=1) as dag:
    setup_db = setup_database_task(); new_files = scan_landing_zone_task(); setup_db >> new_files; process_file_task.expand(file_name=new_files)