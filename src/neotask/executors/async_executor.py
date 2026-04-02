"""
@FileName: async_executor.py
@Description: 异步执行器
@Author: HiPeng
@Time: 2026/3/27 23:52
"""

from typing import Any, Dict, Callable, Awaitable
import inspect
from neotask.executors.base import TaskExecutor


class AsyncExecutor(TaskExecutor):
    """Executor for async functions.

    This executor wraps an async callable and executes it directly.

    Examples:
        >>> async def my_task(data):
        ...     return {"result": data["value"] * 2}
        >>>
        >>> executor = AsyncExecutor(my_task)
    """

    def __init__(self, func: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]):
        """Initialize with async function.

        Args:
            func: Async function that takes task_data and returns result

        Raises:
            TypeError: If func is not an async function
        """
        if not inspect.iscoroutinefunction(func):
            raise TypeError(f"Expected async function, got {type(func).__name__}")
        self._func = func

    async def execute(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the async function."""
        return await self._func(task_data)
