"""
@FileName: conftest.py
@Description: 
@Author: HiPeng
@Time: 2026/4/2 18:55
"""
"""Pytest configuration and fixtures."""

import asyncio
import os
from typing import Generator, AsyncGenerator

import pytest

from neotask import TaskScheduler, TaskExecutor, SchedulerConfig


class EchoExecutor(TaskExecutor):
    """Simple executor for testing."""

    async def execute(self, task_data: dict) -> dict:
        """Echo back the input data."""
        await asyncio.sleep(0.1)  # Simulate work
        return {"echo": task_data, "status": "success"}


class DelayedExecutor(TaskExecutor):
    """Executor with configurable delay."""

    def __init__(self, delay: float = 0.5):
        self.delay = delay

    async def execute(self, task_data: dict) -> dict:
        await asyncio.sleep(self.delay)
        return {"result": task_data.get("value", 0) * 2}


class FailingExecutor(TaskExecutor):
    """Executor that fails for testing."""

    async def execute(self, task_data: dict) -> dict:
        raise ValueError("Intentional failure for testing")


@pytest.fixture
def echo_executor() -> TaskExecutor:
    """Fixture for echo executor."""
    return EchoExecutor()


@pytest.fixture
def delayed_executor() -> TaskExecutor:
    """Fixture for delayed executor."""
    return DelayedExecutor(delay=0.2)


@pytest.fixture
def failing_executor() -> TaskExecutor:
    """Fixture for failing executor."""
    return FailingExecutor()


@pytest.fixture
def scheduler_config() -> SchedulerConfig:
    """Fixture for scheduler config."""
    config = SchedulerConfig.memory()
    config.worker.max_concurrent = 5
    config.worker.prefetch_size = 10
    return config


@pytest.fixture
def scheduler(echo_executor, scheduler_config) -> Generator[TaskScheduler, None, None]:
    """Fixture for task scheduler."""
    scheduler = TaskScheduler(
        executor=echo_executor,
        config=scheduler_config
    )
    yield scheduler
    scheduler.shutdown()


@pytest.fixture
def async_scheduler(echo_executor, scheduler_config) -> AsyncGenerator[TaskScheduler, None]:
    """Async fixture for task scheduler."""
    scheduler = TaskScheduler(
        executor=echo_executor,
        config=scheduler_config
    )
    yield scheduler
    scheduler.shutdown()


@pytest.fixture
async def temp_redis_url() -> Generator[str, None, None]:
    """Fixture for temporary Redis (requires Redis running)."""
    redis_url = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/1")
    yield redis_url
    # Clean up after tests
    import redis.asyncio as redis
    client = redis.from_url(redis_url)
    await client.flushdb()
    await client.close()
