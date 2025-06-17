import time
from typing import Union, List, Dict
import redis


def limit_and_tokenize(
    token_ids: list[int],
    user_id: str,
    redis_client: redis.Redis,
    config_key_template: str = "ratelimit:config:{user_id}",
    usage_key_template: str = "ratelimit:{user_id}",
    default_tokens_per_minute: int = 1000,
) -> Union[List[int], Dict[str, Union[str, float]]]:
    """Tokenize text and block the request if the token limit was reached.

    :param token_ids: A list of tokens.
    :param user_id: The ID of the user.
    :param redis_client: A redis Client used for storing the current number of the user's used tokens.
    :param config_key_template: The template of the redis key containing user information (like a special token limit).
    :param usage_key_template: The template of the redis key containing the number of the user's used tokens.
    :param default_tokens_per_minute: The default token limit per user.
    :return: A list of tokens if the limit wasn't reached, else a dict containing information about the limit.
    """
    now = int(time.time())
    token_count = len(token_ids)

    # Look up user-specific token limit
    limit_str = redis_client.hget(config_key_template.format(user_id=user_id), "limit")
    try:
        tokens_per_minute = int(limit_str) if limit_str else default_tokens_per_minute
    except (ValueError, TypeError):
        tokens_per_minute = default_tokens_per_minute

    # Get token usage state
    pipe = redis_client.pipeline()
    pipe.hget(usage_key_template.format(user_id=user_id), "tokens")
    pipe.hget(usage_key_template.format(user_id=user_id), "last_refill")
    tokens_str, last_refill_str = pipe.execute()

    tokens_used = float(tokens_str) if tokens_str else 0.0
    last_refill = float(last_refill_str) if last_refill_str else now

    # Refill logic
    elapsed = now - last_refill
    refill = (elapsed / 60.0) * tokens_per_minute
    tokens_used = max(0, tokens_used - refill)
    last_refill = now

    if tokens_used + token_count > tokens_per_minute:
        tokens_over = (tokens_used + token_count) - tokens_per_minute
        retry_after = (tokens_over / tokens_per_minute) * 60  # in seconds

        return {
            "error": "Rate limit exceeded",
            "retry_after": round(retry_after, 2),
            "tokens_requested": token_count,
            "tokens_available": max(0, tokens_per_minute - tokens_used),
            "user_token_limit": tokens_per_minute,
        }

    # Store updated usage
    tokens_used += token_count
    pipe = redis_client.pipeline()
    pipe.hset(
        usage_key_template.format(user_id=user_id),
        mapping={"tokens": tokens_used, "last_refill": last_refill},
    )
    pipe.expire(usage_key_template.format(user_id=user_id), 120)
    pipe.execute()

    return token_ids
