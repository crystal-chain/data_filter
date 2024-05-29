#!/usr/bin/env bash
docker build -t data_filter:latest .
docker run -P --name data_filter data_filter:latest