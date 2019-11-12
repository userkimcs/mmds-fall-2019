#!/bin/bash

export PYTHONPATH=/src
python3 scraper.py \
--kafka_host=127.0.0.1 \
--pg_host=10.255.255.10 \
--pg_user=postgres \
--pg_relation=data