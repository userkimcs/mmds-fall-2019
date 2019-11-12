import kafka
import time
import logger
from arguments import configs
from helpers import DatabaseService
from datetime import datetime


def insert(value):
    return value


if __name__ == "__main__":
    # connect pg
    pg = DatabaseService(
        host=configs.pg_host,
        user=configs.pg_user,
        password=configs.pg_password,
        port=configs.pg_port,
        database=configs.pg_db
    )

    consumer = kafka.KafkaConsumer(
        "pipeline",
        bootstrap_servers=configs.kafka_host,
        auto_offset_reset='earliest',
        group_id=configs.kafka_default_group
    )

    for message in consumer:
        try:
            # insert
            logger.log.info("Insert item")
            pg.query("INSERT INTO kw_stream(time, keyword) VALUES({}, {})"
                     .format(datetime.now(), message.value.decode("utf-8")))
            time.sleep(0.01)
        except:
            logger.log.info("Something went wrong.")
