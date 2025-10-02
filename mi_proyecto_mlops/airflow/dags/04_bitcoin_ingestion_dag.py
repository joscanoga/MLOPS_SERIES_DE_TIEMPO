from __future__ import annotations

import pendulum
import pandas as pd
from binance.client import Client

from airflow.models.dag import DAG
from airflow.decorators import task

# La ruta DENTRO del contenedor donde guardaremos los datos
OUTPUT_DATA_PATH = "/opt/airflow/data/btc_usdt_hourly_data.csv"


@task
def get_btc_data_task():
    """
    Se conecta a la API de Binance para descargar datos históricos
    de BTC/USDT y los guarda en un archivo CSV.
    """
    # Usamos una API pública, no se necesitan claves
    client = Client()
    
    # Descargamos los datos: 1 hora de intervalo, desde el 1 de enero de 2020
    klines = client.get_historical_klines(
        "BTCUSDT", Client.KLINE_INTERVAL_1HOUR, "1 Jan, 2020"
    )
    
    # Convertimos a un DataFrame de Pandas
    columns = [
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ]
    df = pd.DataFrame(klines, columns=columns)
    
    # Nos quedamos con las columnas que nos interesan
    df = df[['open_time', 'open', 'high', 'low', 'close', 'volume']]
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    
    # Guardamos los datos en la carpeta compartida
    print(f"Descargados {len(df)} registros. Guardando en {OUTPUT_DATA_PATH}...")
    df.to_csv(OUTPUT_DATA_PATH, index=False)
    print("¡Datos guardados con éxito!")


with DAG(
    dag_id="bitcoin_data_ingestion",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily", # Lo programamos para que se ejecute diariamente
    catchup=False,
    tags=["data", "bitcoin"],
) as dag:
    get_btc_data_task()
