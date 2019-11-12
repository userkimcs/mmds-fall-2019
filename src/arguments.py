import argparse
import sys


def create_arguments(name=sys.argv[0], args=sys.argv[1:]):
    parser = argparse.ArgumentParser()

    parser.add_argument("--kafka_host", required=True, help="Kafka host. Example: 127.0.0.1:9092")
    parser.add_argument("--kafka_default_group", required=False, help="Default consumer group", default="default")
    parser.add_argument("--pg_relation", required=False, default='data')

    if 'downstream' in name:
        parser.add_argument("--pg_host", required=True)
        parser.add_argument("--pg_port", required=False, default=5432)
        parser.add_argument("--pg_user", required=True)
        parser.add_argument("--pg_password", required=False, default=None)
        parser.add_argument("--pg_db", required=False, default="data")
    elif 'visitor' in name:
        parser.add_argument("--redis_host", required=True, help="Redis server")
        parser.add_argument("--redis_port", required=False, help="Redis port", default=6379)
        parser.add_argument("--redis_db", required=False, help="Redis database number", default=1)
        parser.add_argument("--redis_password", required=False, help="Redis authentication password", default=None)

    cfg = parser.parse_args(args)
    return cfg


configs = create_arguments()
