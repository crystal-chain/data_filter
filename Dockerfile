# syntax=docker/dockerfile:1
FROM python:3.9-slim-buster
WORKDIR /app
RUN useradd -m -r data-filter && \
    chown data-filter /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN apt update && apt upgrade -y
COPY . . 
USER data-filter
EXPOSE 5000

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]