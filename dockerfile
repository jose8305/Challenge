# Usa una imagen base oficial de Python
FROM python:3.9

# Instala las dependencias del sistema, incluyendo libodbc y otros paquetes necesarios
RUN apt-get update && apt-get install -y \
    unixodbc \
    unixodbc-dev \
    libodbc1 \
    build-essential \
    curl \
    gnupg2

# Descarga y registra la clave del repositorio de Microsoft
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

# Añade el repositorio de Microsoft para instalar el driver ODBC
RUN curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list \
    > /etc/apt/sources.list.d/mssql-release.list

# Instala el driver ODBC para SQL Server
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Limpia archivos innecesarios
RUN apt-get clean

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia los archivos locales al directorio de trabajo en el contenedor
COPY . /app

# Instala las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Expon el puerto que usa FastAPI (8000 es el estándar)
EXPOSE 8000

# Define el comando para ejecutar la API con uvicorn
CMD ["uvicorn", "apiTest:app", "--host", "0.0.0.0", "--port", "8000"]
