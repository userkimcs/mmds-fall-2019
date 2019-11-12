import kafka
import json
import time
import logger
from arguments import configs
from helpers import DatabaseService, DatabaseModel

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
        "postgres",
        bootstrap_servers=configs.kafka_host,
        auto_offset_reset='earliest',
        group_id=configs.kafka_default_group,
        value_serializer=lambda x: json.dumps(x, indent=4, sort_keys=True, default=str).encode('utf-8')
    )

    for message in consumer:
        # insert
        try:
            logger.log.info("Insert item")
            model = DatabaseModel()
            model.data = message.value
            # insert
            pg.insert_one(model)
            time.sleep(0.01)
        except:
            logger.log.info("Some thing went wrong")
