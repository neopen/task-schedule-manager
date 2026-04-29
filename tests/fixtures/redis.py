"""
@FileName: redis.py
@Description: Redis 相关 fixtures
@Author: HiPeng
@Time: 2026/4/29
"""

import os
import pytest
import redis.asyncio as redis

# Redis 配置
REDIS_URL = "redis://localhost:6379/11"
TEST_REDIS_URL = "redis://localhost:6379/12"


@pytest.fixture(scope="session")
async def redis_client():
    """Redis 客户端 fixture (session scope)"""
    client = redis.from_url(TEST_REDIS_URL, decode_responses=True, max_connections=10,
                           socket_connect_timeout=5, socket_timeout=5)
    try:
        await client.ping()
        yield client
    finally:
        await client.close()


@pytest.fixture(autouse=False)  # 改为非自动使用
async def clean_redis(redis_client, request):
    """手动清理 Redis - 只在需要时使用"""
    yield
    # 使用 try/except 避免事件循环关闭错误
    try:
        await redis_client.flushdb()
    except (RuntimeError, ConnectionError) as e:
        # 事件循环已关闭或连接错误，忽略
        pass


@pytest.fixture(scope="function")
def distributed_redis_url():
    """分布式测试 Redis URL"""
    return os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/12")