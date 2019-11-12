import kafka
from newspaper import Article

import logger
from arguments import configs
from helpers import get_keywords
import nltk
import json


def download_nltk():
    nltk.download('words')
    nltk.download('maxent_ne_chunker')
    nltk.download('averaged_perceptron_tagger')
    nltk.download('punkt')


if __name__ == "__main__":

    download_nltk()

    consumer = kafka.KafkaConsumer(
        "links",
        bootstrap_servers=configs.kafka_host,
        group_id=configs.kafka_default_group,
        auto_offset_reset='earliest'
    )

    pg_producer = kafka.KafkaProducer(
        bootstrap_servers=configs.kafka_host,
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )

    stream_producer = kafka.KafkaProducer(bootstrap_servers=configs.kafka_host)

    for message in consumer:
        url = message.value.decode("utf-8")

        article = Article(url)
        article.download()
        article.parse()
        # add article object (full text, title, date, authors)
        article_dict = dict()
        article_dict['url'] = url
        article_dict['text'] = article.text
        article_dict['title'] = article.title
        article_dict['authors'] = article.authors

        pg_producer.send("postgres", article_dict)

        keywords = get_keywords(article.text)
        keywords.extend(get_keywords(article.title))

        for keyword in keywords:
            logger.log.info("Send keyword {}".format(keyword))
            stream_producer.send("pipeline", keyword.encode())
