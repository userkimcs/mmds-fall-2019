FROM redis
COPY redis.conf /usr/local/etc/redis/redis.conf

WORKDIR /my-redis

CMD [ "redis-server", "/usr/local/etc/redis/redis.conf" ]
