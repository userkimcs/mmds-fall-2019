import kafka
import time
import redis

from arguments import configs
from helpers import get_absolute_links, normalize_url, to_sha1

if __name__ == "__main__":

    producer = kafka.KafkaProducer(bootstrap_servers=configs.kafka_host)

    exist_urls = redis.StrictRedis(host=configs.redis_host, port=configs.redis_port,
                                   db=configs.redis_db, password=configs.redis_password)

    # load homepages
    homepages = open("pages.txt").readlines()

    while True:

        for homepage in homepages:
            homepage = homepage.strip()
            # send encoded home page to redis anh add to kafka consumer
            all_urls = get_absolute_links(homepage)
            for url in all_urls:
                url = normalize_url(url)
                encoded_url = to_sha1(url)
                if not exist_urls.exists(encoded_url):
                    # add new url to redis and add original to kafka producer
                    exist_urls.set(encoded_url, 0)
                    producer.send("links", url.encode())

        time.sleep(10)
