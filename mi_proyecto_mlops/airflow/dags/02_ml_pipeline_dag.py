from __future__ import annotations

import pendulum
import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.datasets import load_wine

from airflow.models.dag import DAG
from airflow.decorators import task

with DAG(
    dag_id="ml_pipeline_dag_with_validation",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ml", "mlflow", "validation"],
) as dag:
    
    @task
    def validate_data():
        """
        Tarea de validación de datos. Carga los datos y comprueba su calidad.
        Si algo está mal, los 'asserts' fallarán y detendrán el pipeline.
        """
        print("--- Cargando y validando datos ---")
        wine_data = load_wine(as_frame=True)
        df = wine_data.frame
        
        # --- NUESTRAS REGLAS DE VALIDACIÓN ---
        # 1. El dataframe no debe estar vacío
        assert not df.empty, "El DataFrame está vacío."
        
        # 2. Deben existir columnas clave
        required_columns = ["alcohol", "malic_acid", "ash", "magnesium", "target"]
        for col in required_columns:
            assert col in df.columns, f"Falta la columna requerida: {col}"
            
        # 3. No debe haber valores nulos en columnas importantes
        assert df[required_columns].isnull().sum().sum() == 0, "Se encontraron valores nulos."
        
        # 4. Los valores deben estar en un rango razonable
        assert df['alcohol'].min() > 10, "Valor de alcohol anómalamente bajo."
        assert df['magnesium'].max() < 200, "Valor de magnesio anómalamente alto."
        
        print("--- Validación de datos superada con éxito ---")
        return df.to_json() # Pasamos los datos validados a la siguiente tarea

    @task
    def train_wine_model(validated_data_json: str):
        """
        Esta tarea ahora recibe los datos ya validados.
        """
        df = pd.read_json(validated_data_json)
        
        mlflow.set_tracking_uri("http://mlflow_server:5000")
        mlflow.set_experiment("wine_classification")

        X = df.drop("target", axis=1)
        y = df["target"]
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.3, random_state=42
        )

        with mlflow.start_run():
            model = LogisticRegression(max_iter=3000) # Aumenté max_iter
            model.fit(X_train, y_train)

            predictions = model.predict(X_test)
            accuracy = accuracy_score(y_test, predictions)

            print(f"Accuracy: {accuracy}")
            mlflow.log_param("model_type", "LogisticRegression")
            mlflow.log_metric("accuracy", accuracy)
            mlflow.sklearn.log_model(model, "wine_model")
            
    # --- Definimos el flujo: validación PRIMERO, entrenamiento DESPUÉS ---
    validated_data = validate_data()
    train_wine_model(validated_data)