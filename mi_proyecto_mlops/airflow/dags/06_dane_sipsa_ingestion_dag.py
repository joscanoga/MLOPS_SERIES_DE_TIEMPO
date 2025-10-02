from __future__ import annotations

import pendulum
import pandas as pd
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.variable import Variable
from zeep import Client
from zeep.helpers import serialize_object

# --- Constantes de Configuración ---
POSTGRES_CONN_ID = "postgres_sipsa"
TARGET_SCHEMA = "sipsa_data"  # <-- NUEVO: Definimos el esquema
TARGET_TABLE = "sipsa_prices"
STAGING_TABLE = "sipsa_prices_staging"
WSDL_URL_VAR = "sipsa_wsdl_url"

@task
def extract_sipsa_data():
    # ... (Esta tarea no cambia, la dejamos como estaba)
    wsdl_url = Variable.get(WSDL_URL_VAR)
    print(f"--- [E] Iniciando Tarea de Extracción ---")
    print(f"Conectando al WSDL: {wsdl_url}...")
    client = Client(wsdl_url)
    print("Invocando el método 'promediosSipsaCiudad'...")
    response = client.service.promediosSipsaCiudad()
    if not response:
        print("La API no devolvió datos. Finalizando.")
        return None
    print(f"Llamada a la API exitosa. Se recibieron {len(response)} registros.")
    data = serialize_object(response)
    df = pd.DataFrame(data)
    print(f"DataFrame crudo creado con {len(df)} filas y {len(df.columns)} columnas.")
    df.rename(columns={'codProducto': 'product_code', 'producto': 'product_name', 'ciudad': 'city_name', 'fechaCaptura': 'capture_date', 'precioPromedio': 'average_price'}, inplace=True)
    df['capture_date'] = pd.to_datetime(df['capture_date']).dt.date
    df['average_price'] = pd.to_numeric(df['average_price'])
    final_cols = ['product_code', 'product_name', 'city_name', 'capture_date', 'average_price']
    df = df[final_cols]
    print(f"DataFrame transformado. Columnas: {df.columns.tolist()}, Filas: {len(df)}")
    print("--- [E] Tarea de Extracción Completada ---")
    return df.to_json()

@task
def load_data_to_staging(data_json: str | None):
    """Tarea 2: Cargar a Staging dentro del nuevo esquema."""
    if data_json is None:
        print("No hay datos para cargar. Omitiendo tarea.")
        return

    print(f"--- [L] Iniciando Carga a Staging en el esquema '{TARGET_SCHEMA}' ---")
    df = pd.read_json(data_json)
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    print(f"Cargando {len(df)} registros en la tabla '{TARGET_SCHEMA}.{STAGING_TABLE}'...")
    # Añadimos el parámetro 'schema' a la función to_sql
    df.to_sql(STAGING_TABLE, hook.get_uri(), schema=TARGET_SCHEMA, if_exists='replace', index=False, method='multi')
    print(f"--- [L] Carga a Staging Completada ---")

@task
def merge_data_to_production():
    """Tarea 3: Fusionar (Merge/Upsert) en el nuevo esquema."""
    print(f"--- [T] Iniciando Fusión a Producción en el esquema '{TARGET_SCHEMA}' ---")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # Primero, nos aseguramos de que el esquema exista
    create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"
    
    # SQL para crear la tabla final DENTRO del nuevo esquema
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} (
            id SERIAL PRIMARY KEY,
            product_code INTEGER,
            product_name VARCHAR(255),
            city_name VARCHAR(255),
            capture_date DATE,
            average_price NUMERIC(10, 2),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE (product_code, city_name, capture_date)
        );
    """
    
    count_sql = f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{STAGING_TABLE};"
    record_count = hook.get_val(count_sql)
    
    if record_count == 0:
        print("La tabla de staging está vacía. No hay nada que fusionar.")
        return
        
    print(f"Se fusionarán {record_count} registros.")
    
    # La magia del "UPSERT", ahora especificando el esquema en todas las tablas
    merge_sql = f"""
        INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE} (product_code, product_name, city_name, capture_date, average_price)
        SELECT product_code, product_name, city_name, capture_date, average_price
        FROM {TARGET_SCHEMA}.{STAGING_TABLE}
        ON CONFLICT (product_code, city_name, capture_date)
        DO UPDATE SET
            average_price = EXCLUDED.average_price,
            product_name = EXCLUDED.product_name,
            created_at = NOW();
    """
    
    print("Asegurando que el esquema de destino exista...")
    hook.run(create_schema_sql)
    print("Asegurando que la tabla de producción exista...")
    hook.run(create_table_sql)
    print("Ejecutando la fusión de datos (UPSERT)...")
    hook.run(merge_sql)
    print(f"--- [T] Fusión a Producción Completada ---")

with DAG(
    dag_id="dane_sipsa_prices_ingestion",
    # ... (el resto del DAG no cambia) ...
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"), schedule="0 15 * * *", catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=int(Variable.get("sipsa_retry_delay_minutes", default_var=60)))},
    tags=["data", "sipsa", "prices"],
) as dag:
    extracted_data = extract_sipsa_data()
    staging_load = load_data_to_staging(extracted_data)
    merge_production = merge_data_to_production()
    staging_load >> merge_production