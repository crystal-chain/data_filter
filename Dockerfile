# syntax=docker/dockerfile:1
FROM python:3.9-slim-buster
WORKDIR /app
RUN mkdir -p /tmp/shared && chmod -R 777 /tmp/shared

RUN useradd -m -r data-filter && \
    chown data-filter /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
# RUN pip install  -r requirements.txt

# RUN apt update && apt upgrade -y
COPY . . 
USER data-filter
EXPOSE 5000

# La commande par défaut lance l'application Flask.
CMD [ "python3", "-m", "flask", "run", "--host=0.0.0.0" ]
