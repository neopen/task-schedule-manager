"""
@FileName: thread_executor.py
@Description: 线程执行器
@Author: HiPeng
@Time: 2026/3/27 23:53
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Callable

from neotask.executors.base import TaskExecutor


class ThreadExecutor(TaskExecutor):
    """Executor for synchronous functions using thread pool.

    This executor runs synchronous functions in a thread pool to avoid
    blocking the asyncio event loop.

    Examples:
        >>> def my_task(data):
        ...     return {"result": data["value"] * 2}
        >>>
        >>> executor = ThreadExecutor(my_task, max_workers=4)
    """

    def __init__(self, func: Callable[[Dict[str, Any]], Dict[str, Any]],
                 max_workers: int = 10):
        """Initialize with sync function and thread pool.

        Args:
            func: Synchronous function that takes task_data and returns result
            max_workers: Maximum number of worker threads
        """
        self._func = func
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    async def execute(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute sync function in thread pool."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self._func, task_data)

    async def shutdown(self) -> None:
        """Shutdown thread pool."""
        self._executor.shutdown(wait=True)
