# Entorno de Desarrollo con JupyterLab y Soporte para GPU

Este directorio contiene la configuración necesaria para crear un entorno de desarrollo para ciencia de datos y machine learning utilizando JupyterLab, Docker y Docker Compose, con soporte para GPUs NVIDIA.

## Descripción General

El objetivo de este proyecto es proporcionar un entorno de desarrollo reproducible y aislado que aproveche la aceleración por hardware de las GPUs NVIDIA para el entrenamiento de modelos de inteligencia artificial.

El entorno incluye:
- JupyterLab como IDE interactivo.
- Python 3.10.
- Librerías populares de ciencia de datos como TensorFlow, PyTorch, Pandas, y Scikit-learn.
- Soporte para CUDA y cuDNN a través de las imágenes base de NVIDIA.

## Prerrequisitos

Antes de comenzar, asegúrate de tener instalado lo siguiente en tu sistema:

1.  **Docker y Docker Compose**: Para construir y gestionar los contenedores. [Instrucciones de instalación de Docker](https://docs.docker.com/get-docker/).
2.  **NVIDIA GPU Drivers**: Drivers actualizados para tu tarjeta gráfica NVIDIA.
3.  **NVIDIA Container Toolkit**: Permite a Docker interactuar con las GPUs. [Instrucciones de instalación](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html).

## Cómo Usar

Sigue estos pasos para levantar el entorno de JupyterLab:

### 1. Construir e Iniciar el Contenedor

Abre una terminal en este directorio (`notebooks`) y ejecuta el siguiente comando:

```bash
docker compose up --build
```

-   `docker compose up`: Inicia los servicios definidos en el archivo `docker-compose.yml`.
-   `--build`: Fuerza la reconstrucción de la imagen de Docker. Deberías usar esta opción la primera vez que inicies el entorno o si has realizado cambios en el `Dockerfile` o en `requirements.txt`.

Para ejecutar el contenedor en segundo plano (detached mode), puedes usar:

```bash
docker compose up -d
```

### 2. Acceder a JupyterLab

Una vez que el contenedor esté en funcionamiento, abre tu navegador web y navega a la siguiente URL:

[http://localhost:8888](http://localhost:8888)

Verás la interfaz de JupyterLab, donde podrás crear, editar y ejecutar tus notebooks.

### 3. Detener el Contenedor

Para detener el entorno, puedes presionar `Ctrl + C` en la terminal donde se está ejecutando `docker compose up`.

Si el contenedor se está ejecutando en segundo plano, o desde una nueva terminal en el mismo directorio, ejecuta:

```bash
docker compose down
```

Este comando detendrá y eliminará el contenedor, pero tus archivos de notebook no se perderán gracias al volumen que hemos configurado.

## Estructura de Archivos

-   **`Dockerfile`**: Contiene las instrucciones para construir la imagen de Docker. Define la imagen base de NVIDIA, instala Python y las dependencias del sistema, y copia los archivos necesarios.
-   **`docker-compose.yml`**: Orquesta la ejecución del contenedor. Define el servicio de JupyterLab, mapea los puertos, configura el acceso a la GPU y gestiona los volúmenes para la persistencia de datos.
-   **`requirements.txt`**: Lista todas las librerías de Python que se instalarán en el entorno. Esto facilita la gestión de dependencias.
-   **`README.md`**: Este archivo, con toda la documentación que necesitas.

## Persistencia de Datos

El archivo `docker-compose.yml` está configurado para montar el directorio  (`./notebooks`) en la ruta `/app` dentro del contenedor. Esto significa que cualquier archivo o notebook que crees o modifiques dentro de JupyterLab se guardará directamente en esta carpeta en tu máquina local.
