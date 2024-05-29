#!/usr/bin/env bash
git pull
aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 173367659232.dkr.ecr.eu-west-1.amazonaws.com
docker build -t crystalchain/data_filter .
docker tag crystalchain/data_filter:latest 173367659232.dkr.ecr.eu-west-1.amazonaws.com/crystalchain/data_filter:latest
docker push 173367659232.dkr.ecr.eu-west-1.amazonaws.com/crystalchain/data_filter:latest
docker compose down && docker compose pull && docker compose up -d