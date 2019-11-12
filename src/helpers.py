import hashlib
from bs4 import BeautifulSoup
import urllib.parse
import requests
import re
import urllib.parse
import sqlalchemy
from nltk import ne_chunk, pos_tag, word_tokenize, Tree
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSON
import logger
from arguments import configs


class DatabaseService:
    Base = declarative_base()
    connection = None

    def __init__(self, host, user, password, port, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = database
        self.engine = None
        self.connect()

    def connect(self):
        # connection string
        connection_string = 'postgresql+psycopg2://__USERNAME__:__PASSWORD__@__HOST__:__PORT__/__DATABASE__' \
            .replace('__USERNAME__', self.user) \
            .replace('__PASSWORD__', self.password) \
            .replace('__HOST__', self.host) \
            .replace('__PORT__', str(self.port)) \
            .replace('__DATABASE__', self.dbname)
        # create engine
        self.engine = sqlalchemy.create_engine(connection_string, echo=False, convert_unicode=True)
        # create database if not exists
        try:
            self.create_database()
        except Exception as ex:
            logger.log.exception(str(ex))

        SessionMaker = sessionmaker(bind=self.engine)
        self.connection = SessionMaker()
        print("Engine created!!!")

    def create_database(self):
        self.Base.metadata.create_all(bind=self.engine)

    def query(self, query):
        rs = self.connection.execute(query)
        return rs

    def insert_one(self, new_document):
        try:
            self.connection.add(new_document)
            self.connection.commit()
            logger.log.info("Insert new document")
        except Exception as ex:
            logger.log.exception(str(ex))
            self.connection.rollback()


class DatabaseModel(DatabaseService.Base):
    __tablename__ = configs.pg_relation

    id = sqlalchemy.Column(sqlalchemy.BigInteger,
                           sqlalchemy.Sequence('prop_seq', start=1, increment=1),
                           primary_key=True)

    data = sqlalchemy.Column(JSON)
    created_time = sqlalchemy.Column(sqlalchemy.DateTime, default=func.now())


def to_sha1(text):
    sha = hashlib.sha1(text.encode('utf-8'))
    return sha.hexdigest()


def get_absolute_links(url):
    """
    Get all absolute link from given url
    :param url:
    :return:
    """
    rq = requests.get(url)
    soup = BeautifulSoup(rq.text)
    all_links = list()
    for atag in soup.find_all('a'):
        link = urllib.parse.urljoin(url, atag.get('href'))
        all_links.append(link)

    return all_links


def normalize_url(url):
    """
    Normalize url
    1. Remove default port: web:80.com => web.com
    2. Add slash: web.com => web.com/
    3. Remove fragment: web.com/content.html#link => web.com/content.html
    4. Remove default name: web.com/index.html => web.com
    5. Decode: web.com/%7Ekim => web.com/~kim
    6. Encode space: web.com/tan kim => web.com/tan%20kim
    7. Remove www.
    To lower: WEB.COM => web.com
    :return: normalized url as string
    """
    url = url.lower()
    # return urllib.urlunparse(urlparse(normurl))
    # 1. Remove default port
    url = re.sub(r'\:\d{1,6}\/', '', url)

    # 3. Remove fragment
    url = re.sub(r'#.+', '', url)

    # 4. Remove default name
    url = re.sub(r'\/index\.html', '', url)

    # 5. Decode: %xx
    url = urllib.parse.unquote(url)

    # 6. Encode spaces
    if ' ' in url.__str__():
        # normurl = urllib.parse.quote(normurl, safe= "_.-~/:@")
        url = re.sub(r'\s', '%20', url)

    # 2. Add slash
    #if normurl[-1] != '/':
    #    normurl += '/'

    # if "www\." in normurl:
    #     normurl = re.sub(r'www\.', '', normurl)

    return url


def get_keywords(text):
    chunked = ne_chunk(pos_tag(word_tokenize(text)))
    continuous_chunk = []
    current_chunk = []
    for i in chunked:
        if type(i) == Tree:
            current_chunk.append(" ".join([token for token, pos in i.leaves()]))
        elif current_chunk:
            named_entity = " ".join(current_chunk)
            if named_entity not in continuous_chunk:
                continuous_chunk.append(named_entity)
                current_chunk = []
        else:
            continue

    return continuous_chunk
