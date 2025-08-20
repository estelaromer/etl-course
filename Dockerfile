# Imagen base de Python
FROM python:3.11-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar requirements y instalarlos
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto de la aplicación
COPY app/ .

# Comando por defecto al ejecutar el contenedor
CMD ["python", "main.py"]
