# Uncomment for Challenge #7
import datetime
import random
from redis.client import Redis

from redisolar.dao.base import RateLimiterDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema
# Uncomment for Challenge #7
from redisolar.dao.base import RateLimitExceededException


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""
    def __init__(self,
                 window_size_ms: float,
                 max_hits: int,
                 redis_client: Redis,
                 key_schema: KeySchema = None,
                 **kwargs):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        p = self.redis.pipeline()
        sliding_window_rate_limiter_key = self.key_schema.sliding_window_rate_limiter_key(
            name=name, 
            window_size_ms=self.window_size_ms, 
            max_hits=self.max_hits
        )
        ts_ms = int(datetime.datetime.now().timestamp()*1000)
        rand_val = random.randint(1, 1000)
        p.zadd(sliding_window_rate_limiter_key, {f'{ts_ms}-{rand_val}': ts_ms})
        p.zremrangebyscore(sliding_window_rate_limiter_key, 0, ts_ms - self.window_size_ms)
        p.zcard(sliding_window_rate_limiter_key)
        *_,  requests = p.execute()
        if requests > self.max_hits:
            raise RateLimitExceededException
