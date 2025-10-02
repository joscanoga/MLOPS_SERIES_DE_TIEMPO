from __future__ import annotations

import pendulum
import pandas as pd
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
import talib
import xgboost as xgb
import optuna
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

from airflow.models.dag import DAG
from airflow.decorators import task

# Rutas a los datos dentro del contenedor de Airflow
INPUT_DATA_PATH = "/opt/airflow/data/btc_usdt_hourly_data.csv"

@task
def feature_engineering_task(raw_data_path: str):
    """Carga los datos y calcula los indicadores técnicos con TA-Lib."""
    df = pd.read_csv(raw_data_path, parse_dates=['open_time'])
    df.set_index('open_time', inplace=True)
    
    # Calcular indicadores
    df['SMA_10'] = talib.SMA(df['close'], timeperiod=10)
    df['SMA_30'] = talib.SMA(df['close'], timeperiod=30)
    df['RSI'] = talib.RSI(df['close'], timeperiod=14)
    
    # Crear la variable objetivo (1 si el precio sube en la siguiente hora, 0 si no)
    df['target'] = (df['close'].shift(-1) > df['close']).astype(int)
    
    df.dropna(inplace=True)
    
    print(f"Datos procesados. {len(df)} filas listas para entrenar.")
    return df.to_json()

@task
def hyperparameter_tuning_task(processed_data_json: str):
    """Realiza la optimización de hiperparámetros para un modelo XGBoost utilizando Optuna.
        Esta tarea utiliza la biblioteca Optuna para buscar de manera eficiente la mejor
        combinación de hiperparámetros para un modelo `XGBClassifier`. El proceso se
        integra con MLflow para registrar cada prueba (trial) de optimización,
        permitiendo un seguimiento detallado de los experimentos.
        Proceso detallado:
        1.  **Carga de Datos**: Se recibe una cadena JSON, que se convierte en un DataFrame de pandas.
        2.  **Preparación**: Se definen las características (features) y la variable objetivo (target),
            y los datos se dividen en conjuntos de entrenamiento y prueba. La división se
            realiza sin barajar (`shuffle=False`) para respetar la naturaleza de serie temporal
            de los datos.
        3.  **Función Objetivo**: Se define una función `objective` que Optuna debe maximizar.
            Para cada `trial`, esta función:
            - Sugiere un conjunto de hiperparámetros (ej. `n_estimators`, `max_depth`).
            - Entrena un modelo XGBoost con dichos parámetros.
            - Evalúa el `accuracy` del modelo en el conjunto de prueba.
            - Devuelve el `accuracy` como el valor a maximizar.
        4.  **Integración con MLflow**: Se configura la conexión con el servidor de MLflow y se
            habilita el autologging de Optuna (`mlflow.optuna.autolog()`). Esto hace que cada
            `trial` de Optuna se registre automáticamente como una ejecución anidada en MLflow.
        5.  **Optimización**: Se crea un estudio (`study`) de Optuna con el objetivo de maximizar
            el `accuracy` y se ejecuta el proceso de optimización durante un número
            predeterminado de `trials`.
        6.  **Resultado**: La función imprime el mejor `accuracy` y los mejores parámetros
            encontrados, y finalmente devuelve estos últimos como un diccionario.
        Args:
            processed_data_json (str): Una cadena en formato JSON que representa el DataFrame
                                       con los datos procesados y listos para el entrenamiento.
        Returns:
            dict: Un diccionario que contiene los mejores hiperparámetros encontrados por Optuna.
                  Ejemplo: {'n_estimators': 500, 'max_depth': 5, ...}.
        """
    """Usa Optuna para encontrar los mejores hiperparámetros para XGBoost."""
    df = pd.read_json(processed_data_json)
    
    features = ['SMA_10', 'SMA_30', 'RSI', 'open', 'high', 'low', 'close', 'volume']
    target = 'target'
    
    X = df[features]
    y = df[target]
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
    
    def objective(trial):
        params = {
            'objective': 'binary:logistic',
            'eval_metric': 'logloss',
            'n_estimators': trial.suggest_int('n_estimators', 100, 1000),
            'max_depth': trial.suggest_int('max_depth', 3, 10),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
            'subsample': trial.suggest_float('subsample', 0.6, 1.0),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
        }
        
        model = xgb.XGBClassifier(**params)
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        accuracy = accuracy_score(y_test, preds)
        
        return accuracy

    mlflow.set_tracking_uri("http://mlflow_server:5000")
    mlflow.set_experiment("bitcoin_trading_optimization")
    
    # Usamos el autologging de Optuna para registrar cada prueba en MLflow
    mlflow.autolog()
    
    study = optuna.create_study(direction="maximize")
    with mlflow.start_run(run_name="optuna_study"):
        study.optimize(objective, n_trials=15) # Hacemos 15 pruebas para encontrar los mejores params
    
    print(f"Mejor accuracy encontrado: {study.best_value}")
    print(f"Mejores parámetros: {study.best_params}")
    
    return study.best_params

@task
def train_final_model_task(processed_data_json: str, best_params: dict):
    """Entrena el modelo final con los mejores hiperparámetros."""
    df = pd.read_json(processed_data_json)
    
    features = ['SMA_10', 'SMA_30', 'RSI', 'open', 'high', 'low', 'close', 'volume']
    target = 'target'
    
    X = df[features]
    y = df[target]
    
    mlflow.set_tracking_uri("http://mlflow_server:5000")
    mlflow.set_experiment("bitcoin_trading_optimization")
    
    with mlflow.start_run(run_name="final_model"):
        final_model = xgb.XGBClassifier(**best_params)
        final_model.fit(X, y)
        
        accuracy = accuracy_score(y, final_model.predict(X))
        
        print(f"Accuracy del modelo final en todo el dataset: {accuracy}")
        
        mlflow.log_params(best_params)
        mlflow.log_metric("final_accuracy", accuracy)
        mlflow.xgboost.log_model(final_model, "final_xgboost_model", signature=False)
        # Etiquetamos esta ejecución como la "candidata a producción"
        mlflow.set_tag("model_type", "production_candidate")


with DAG(
    dag_id="bitcoin_training_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ml", "bitcoin", "optuna"],
) as dag:
    # Definimos el flujo: feat eng -> tuning -> final model
    processed_data = feature_engineering_task(raw_data_path=INPUT_DATA_PATH)
    best_hyperparams = hyperparameter_tuning_task(processed_data)
    train_final_model_task(processed_data, best_hyperparams)