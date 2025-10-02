# Usamos una imagen base de Miniconda para consistencia y fiabilidad
FROM continuumio/miniconda3

# Instalamos TA-Lib y Python 3.10 usando el gestor de paquetes Conda
RUN conda install -c conda-forge -y python=3.10 ta-lib

# Establecemos el directorio de trabajo
WORKDIR /app

# Copiamos el archivo de requerimientos
COPY requirements.txt .

# Instalamos el resto de las dependencias con pip
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el código de la aplicación
COPY main.py .

# Comando para iniciar el servidor de la API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]