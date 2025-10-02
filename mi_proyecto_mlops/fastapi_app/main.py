import mlflow
import pandas as pd
import talib
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Annotated

# --- 1. Definición de los Contratos de Datos ---
# Un punto de datos histórico individual
class HistoricalDataPoint(BaseModel):
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float

# La entrada de la API: una lista de puntos de datos históricos
class PredictionRequest(BaseModel):
    data: Annotated[List[HistoricalDataPoint], Field(min_length=31)]

# --- 2. Creación de la Aplicación FastAPI ---
app = FastAPI(title="Bitcoin Trend Predictor API", version="2.0")

# --- 3. Carga del Modelo (Solo al iniciar) ---
model = None

@app.on_event("startup")
def load_model():
    """
    Carga el modelo etiquetado como "production_candidate" desde MLflow.
    """
    global model
    mlflow.set_tracking_uri("http://mlflow_server:5000")
    client = mlflow.tracking.MlflowClient()
    
    try:
        experiment = client.get_experiment_by_name("bitcoin_trading_optimization")
        # Buscamos la última ejecución que tenga nuestra etiqueta especial
        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            filter_string="tags.model_type = 'production_candidate'",
            order_by=["start_time DESC"],
            max_results=1
        )
        if not runs:
            print("!!! No model tagged as 'production_candidate' found. !!!")
            return
        
        latest_run_id = runs[0].info.run_id
        model_uri = f"runs:/{latest_run_id}/final_xgboost_model"
        
        print(f"--- Loading model from URI: {model_uri} ---")
        model = mlflow.pyfunc.load_model(model_uri)
        print("--- Bitcoin model loaded successfully! ---")

    except Exception as e:
        print(f"!!! Error loading model: {e} !!!")

# --- 4. Lógica de Feature Engineering (Debe ser IDÉNTICA a la del entrenamiento) ---
def feature_engineering(data: List[HistoricalDataPoint]):
    """Toma datos históricos y calcula los indicadores técnicos."""
    df = pd.DataFrame([vars(s) for s in data])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df.set_index('open_time', inplace=True)
    
    df['SMA_10'] = talib.SMA(df['close'], timeperiod=10)
    df['SMA_30'] = talib.SMA(df['close'], timeperiod=30)
    df['RSI'] = talib.RSI(df['close'], timeperiod=14)
    
    return df.dropna()
    
# --- 5. Definición del Endpoint de Predicción ---
@app.post("/predict_btc_trend")
def predict(request: PredictionRequest):
    """
    Recibe los últimos ~30 puntos de datos, calcula las características
    y predice la tendencia de la siguiente hora.
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model is not loaded")

    # 1. Aplicar la misma ingeniería de características que en el entrenamiento
    processed_df = feature_engineering(request.data)
    
    if processed_df.empty:
        raise HTTPException(status_code=400, detail="Not enough data to compute features")
        
    # 2. Usar la última fila de datos procesados para la predicción
    latest_features = processed_df.iloc[[-1]]
    
    # 3. Realizar la predicción
    prediction = model.predict(latest_features)
    
    # 4. Devolver el resultado (0 = Baja, 1 = Sube)
    return {
        "prediction": int(prediction[0]),
        "prediction_meaning": "Trend Up" if int(prediction[0]) == 1 else "Trend Down"
    }