# Utiliza una imagen oficial de NVIDIA con CUDA como imagen base
# Esta imagen contiene los drivers y librerías necesarios para el soporte de GPU
FROM nvidia/cuda:12.1.1-cudnn8-devel-ubuntu22.04

# Establece el directorio de trabajo en el contenedor
WORKDIR /app

# Evita que apt-get pida confirmación interactiva
ENV DEBIAN_FRONTEND=noninteractive

# Instala Python, pip y otras dependencias del sistema
RUN apt-get update && apt-get install -y \
    python3.10 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Copia el archivo de requerimientos al contenedor
COPY requirements.txt ./

# Instala las librerías de Python desde el archivo de requerimientos
# Se usa --no-cache-dir para reducir el tamaño de la imagen
RUN pip install --no-cache-dir -r requirements.txt

# Copia el contenido del directorio local de notebooks al contenedor en /app
COPY . .

# Expone el puerto 8888 para que sea accesible desde la máquina anfitriona
EXPOSE 8888

# Define el comando para ejecutar Jupyter Lab
# --ip=0.0.0.0 para permitir conexiones desde fuera del contenedor
# --allow-root para ejecutar Jupyter como usuario root (común en Docker)
# --no-browser para no abrir un navegador automáticamente
# --NotebookApp.token='' para deshabilitar la autenticación por token por conveniencia (cuidado en producción)
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]
