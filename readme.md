# MLOps para Series de Tiempo

Este proyecto proporciona una estructura base para aplicar prácticas de MLOps en el desarrollo de modelos de Machine Learning para series de tiempo. Incluye un servidor de seguimiento de experimentos con MLflow y un entorno de desarrollo interactivo con Jupyter Lab habilitado para GPU.

## Estructura del Proyecto

El repositorio está organizado en las siguientes carpetas principales:

```
.
├── mlflow/      # Configuración y almacenamiento de datos para el servidor de MLflow.
├── notebooks/   # Contiene los notebooks de Jupyter para análisis, experimentación y modelado.
└── ...          # Otros archivos de configuración (ej. docker-compose.yml).
```

### 1. Seguimiento con MLflow (`mlflow/`)

Esta carpeta está dedicada a la gestión de MLflow. Se utiliza para:
*   **Registrar experimentos**: Guarda los parámetros, métricas y artefactos (como modelos entrenados) de cada ejecución.
*   **Comparar modelos**: Facilita la visualización y comparación del rendimiento entre diferentes modelos o configuraciones.
*   **Gestionar el ciclo de vida del modelo**: Permite versionar y pasar modelos a través de diferentes etapas (staging, producción, etc.).

Para iniciar el servidor de MLflow, puedes usar un comando como `docker-compose up mlflow-server`.

### 2. Jupyter Lab con GPU (`notebooks/`)

Esta carpeta contiene el entorno de desarrollo principal.
*   **Entorno interactivo**: Permite desarrollar, probar y visualizar código de forma rápida en un entorno basado en notebooks.
*   **Soporte para GPU**: El entorno está configurado para utilizar la potencia de las GPUs (si están disponibles en la máquina anfitriona), lo cual es fundamental para acelerar el entrenamiento de modelos complejos de Deep Learning.

Para iniciar el entorno de Jupyter, puedes usar un comando como `docker-compose up jupyter-lab`.

## Cómo Empezar

### Prerrequisitos

*   Docker y Docker Compose.
*   NVIDIA Container Toolkit si deseas utilizar la aceleración por GPU en los contenedores de Docker.

### Ejecución

1.  Clona este repositorio en tu máquina local.
2.  Navega a la raíz del proyecto.
3.  Levanta los servicios utilizando Docker Compose:
    ```bash
    docker-compose up -d
    ```
4.  **Accede a la interfaz de MLflow** abriendo tu navegador en `http://localhost:5000`.
5.  **Accede a Jupyter Lab** abriendo tu navegador en `http://localhost:8888`. La primera vez, necesitarás un token de acceso que se mostrará en los logs del contenedor.
