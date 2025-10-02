# Paso 1: Usar la imagen oficial de MLflow como nuestra base.
FROM ghcr.io/mlflow/mlflow:latest

# Paso 2: Instalar la librería (el "traductor") para PostgreSQL.
# Usamos psycopg2-binary que es más fácil de instalar.
RUN pip install psycopg2-binary