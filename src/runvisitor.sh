#!/bin/bash

export PYTHONPATH=/src
python3 visitor.py \
--kafka_host=127.0.0.1 \
--redis_host=127.0.0.1