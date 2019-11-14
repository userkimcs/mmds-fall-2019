import time
import redis
from helpers import get_absolute_links, normalize_url, to_sha1, DatabaseService
from datetime import datetime
from newspaper import Article
from helpers import get_keywords
import json
import nltk


def download_nltk():
    nltk.download('words')
    nltk.download('maxent_ne_chunker')
    nltk.download('averaged_perceptron_tagger')
    nltk.download('punkt')


def get_content(input_url):
    try:
        article = Article(input_url)
        article.download()
        article.parse()
        # add article object (full text, title, date, authors)
        article_dict = dict()
        article_dict['url'] = input_url
        article_dict['text'] = article.text
        article_dict['title'] = article.title
        article_dict['authors'] = article.authors
        keywords = get_keywords(article.text)
        keywords.extend(get_keywords(article.title))

        return article_dict, keywords
    except:
        return None, None

if __name__ == "__main__":
    download_nltk()
    pg = DatabaseService(
        host="10.255.255.6",
        user="postgres",
        password='',
        port=5432,
        database="data"
    )

    exist_urls = redis.Redis(host='10.255.255.10')

    # load homepages
    homepages = open("pages.txt").readlines()

    while True:

        for homepage in homepages:
            homepage = homepage.strip()
            print(homepage)
            # send encoded home page to redis anh add to kafka consumer
            all_urls = get_absolute_links(homepage)
            for url in all_urls:
                url = normalize_url(url)
                encoded_url = to_sha1(url)
                if not exist_urls.exists(encoded_url):
                    # add new url to redis and add original to kafka producer
                    exist_urls.set(encoded_url, 0)
                    # get content
                    data, keywords = get_content(url)
                    
                    if data is None:
                        continue
                        
                    # insert
                    pg.query("INSERT INTO articles(data) VALUES('{}')".format(json.dumps(data)))
                    print("INSERT articles")
                    # insert
                    time.sleep(0.01)

                    for keyword in keywords:
                        pg.query("INSERT INTO kw_stream VALUES('{}', '{}')".format(datetime.now(), keyword))
                        print("Insert {}".format(keyword))

        time.sleep(10)
