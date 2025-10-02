# Proyecto de Pipeline MLOps End-to-End

![Python](https://img.shields.io/badge/Python-3.9-blue.svg)
![Docker](https://img.shields.io/badge/Docker-20.10-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.5-brightgreen.svg)
![MLflow](https://img.shields.io/badge/MLflow-2.3-brightgreen.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.95-green.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue.svg)

## Descripción

Este proyecto implementa una plataforma y varios pipelines de MLOps de extremo a extremo utilizando un stack tecnológico moderno y open-source. La plataforma está completamente contenerizada con Docker y orquestada con Apache Airflow.

El objetivo es demostrar las mejores prácticas de ingeniería de datos y MLOps a través de varios casos de uso realistas, incluyendo:

-   **Ingesta de datos desde APIs**: Conexión a servicios web (REST y SOAP).
-   **Procesamiento de archivos**: Detección y procesamiento automático de archivos (`.xls`) depositados en un directorio.
-   **Entrenamiento y optimización de modelos**: Un pipeline completo de Machine Learning para series de tiempo, incluyendo ingeniería de características, optimización de hiperparámetros y registro de experimentos.
-   **Despliegue de modelos**: Exposición de un modelo entrenado a través de una API REST para predicciones en tiempo real.

## Arquitectura y Tecnologías

La plataforma se compone de varios servicios que interactúan entre sí, orquestados por Docker Compose.

```mermaid
graph TD
    subgraph "Fuentes de Datos"
        DANE_API[API SOAP DANE]
        Binance_API[API REST Binance]
        FileSystem["FileSystem <br> (landing_zone)"]
    end

    subgraph "Plataforma MLOps"
        Airflow[Apache Airflow]
        MLflow[MLflow Tracking]
        PostgreSQL[(PostgreSQL DB)]
        FastAPI[API de Predicción]
    end

    subgraph "Usuario Final"
        User[Usuario / Cliente API]
    end

    DANE_API -- "Ingesta de datos" --> Airflow
    Binance_API -- "Ingesta de datos" --> Airflow
    FileSystem -- "Monitoreado por" --> Airflow

    Airflow -- "Almacena metadatos y datos" --> PostgreSQL
    Airflow -- "Registra experimentos y modelos" --> MLflow
    MLflow -- "Usa como backend" --> PostgreSQL

    FastAPI -- "Carga modelo desde" --> MLflow
    User -- "Solicita predicción" --> FastAPI
```

-   **Orquestación**: Apache Airflow (construido desde una imagen de Miniconda para manejar dependencias complejas).
-   **Seguimiento de Experimentos**: MLflow.
-   **Base de Datos**: PostgreSQL (utilizada por Airflow, MLflow y para almacenar los datos de los pipelines).
-   **Despliegue de Modelos**: FastAPI.
-   **Contenerización**: Docker & Docker Compose.
-   **Librerías Clave**: Pandas, Zeep (para SOAP), `python-binance`, `TA-Lib`, Scikit-learn, XGBoost, Optuna.

## Estructura del Proyecto

```
/mi_proyecto_mlops/
├── airflow/
│   ├── dags/             # Contiene el código de todos los pipelines de Airflow.
│   ├── Dockerfile        # Define la imagen personalizada de Airflow (base Miniconda).
│   └── requirements.txt  # Dependencias de Python para el entorno de Airflow.
├── data/
│   ├── landing_zone/     # Carpeta para depositar archivos .xls a ser procesados.
│   └── archive/          # Los archivos procesados con éxito se mueven aquí.
├── fastapi_app/
│   ├── Dockerfile        # Define la imagen de la API (base Miniconda).
│   ├── main.py           # El código de la API de predicción.
│   └── requirements.txt  # Dependencias de Python para la API.
├── mlflow_custom/
│   └── Dockerfile        # Añade el driver de PostgreSQL a la imagen de MLflow.
├── postgres_init/
│   └── init-databases.sh # Script para crear bases de datos adicionales al inicio.
├── .env                  # Archivo de configuración con variables de entorno.
└── docker-compose.yml    # El archivo principal que define y orquesta todos los servicios.
```

## Guía de Instalación y Puesta en Marcha

Sigue estos pasos para levantar toda la plataforma.

### 1. Prerrequisitos

-   Tener **Docker** y **Docker Compose** instalados y en funcionamiento.
-   Clonar este repositorio en tu máquina local.

### 2. Archivo de Configuración

Crea un archivo llamado `.env` en la raíz del proyecto y copia el siguiente contenido. Contiene las credenciales y configuraciones para los servicios.

````env
# Variables para la Base de Datos PostgreSQL
POSTGRAMS_USER=admin
POSTGRAMS_PASSWORD=admin
POSTGRAMS_DB_MLFLOW=mlflow_db
POSTGRAMS_DB_AIRFLOW=airflow_db

# UID/GID de Airflow para permisos (usar 50000 para Linux/Mac)
AIRFLOW_UID=50000

# Clave secreta para la comunicación interna de Airflow
AIRFLOW__WEBSERVER__SECRET_KEY=gN-_gN4DD_yNAn2vN_yNAn-_gN4D-yNAgN4DD_yNAnE=

# Habilitar el test de conexiones en la UI de Airflow
AIRFLOW__CORE__TEST_CONNECTION=Enabled
````