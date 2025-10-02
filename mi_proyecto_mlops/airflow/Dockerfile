# Usamos la imagen moderna de Airflow con Python 3.10
FROM apache/airflow:2.8.1-python3.10

# Cambiamos al usuario root
USER root

# Instalamos un conjunto más completo de herramientas para compilar
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    build-essential \
    make \
    gcc \
    libtool \
    autoconf \
    pkg-config

# Instalamos TA-Lib desde el código fuente
RUN wget 'http://prdownloads.sourceforge.net/project/ta-lib/ta-lib/0.4.0/ta-lib-0.4.0-src.tar.gz' -O ta-lib-0.4.0-src.tar.gz && \
    tar -xzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    cd .. && \
    rm -rf ta-lib-0.4.0-src.tar.gz ta-lib/ && \
    ldconfig

# Creamos y damos permisos a las carpetas necesarias
RUN mkdir -p /mlruns /opt/airflow/data && \
    chown -R airflow /mlruns /opt/airflow/data

# Volvemos a ser el usuario airflow
USER airflow

# Copiamos nuestro archivo de requerimientos
COPY --chown=airflow:root requirements.txt .

# --- LA CORRECCIÓN FINAL ---
# Forzamos la desinstalación de cualquier versión preexistente para evitar conflictos
# y LUEGO instalamos las librerías de nuestro archivo.
RUN pip uninstall -y mlflow optuna && \
    pip install --no-cache-dir -r requirements.txt