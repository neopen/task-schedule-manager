"""
@FileName: class_executor.py
@Description: Class executor for instances with execute method.
@Author: HiPeng
@Time: 2026/4/2 22:10
"""

import inspect
from typing import Any, Dict
from neotask.executors.base import TaskExecutor


class ClassExecutor(TaskExecutor):
    """Executor for class instances with execute method.

    This executor wraps any object that has an execute method (sync or async).

    Examples:
        >>> class MyWorker:
        ...     async def execute(self, data):
        ...         return {"result": data["value"] * 2}
        >>>
        >>> executor = ClassExecutor(MyWorker())

        >>> class SyncWorker:
        ...     def execute(self, data):
        ...         return {"result": data["value"] * 2}
        >>>
        >>> executor = ClassExecutor(SyncWorker())  # Auto-wraps in thread pool
    """

    def __init__(self, instance: Any):
        """Initialize with class instance.

        Args:
            instance: Object with execute method

        Raises:
            TypeError: If instance doesn't have an execute method
        """
        self._instance = instance

        # Validate that instance has an execute method
        if not hasattr(instance, 'execute'):
            raise TypeError(f"{type(instance).__name__} must have an 'execute' method")

        if not callable(instance.execute):
            raise TypeError(f"{type(instance).__name__}.execute must be callable")

        # Check if execute is async or sync
        self._is_async = inspect.iscoroutinefunction(instance.execute)

    async def execute(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute instance's execute method."""
        if self._is_async:
            # Async execute
            return await self._instance.execute(task_data)
        else:
            # Sync execute - run in thread pool
            import asyncio
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self._instance.execute, task_data)