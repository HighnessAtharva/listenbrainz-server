import time
from typing import Optional

import redis

from listenbrainz.listenstore import RedisListenStore

_redis: Optional[RedisListenStore] = None


def init_redis_connection(logger):
    """Create a connection to the Redis server."""

    global _redis
    while True:
        try:
            _redis = RedisListenStore(logger)
            _redis.check_connection()
            break
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Connection to redis failed: {str(e)}")
            logger.error("Sleeping 2 seconds and trying again...")
            time.sleep(2)

    return _redis
