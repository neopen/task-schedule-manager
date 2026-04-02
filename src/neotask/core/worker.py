"""
@FileName: worker.py
@Description: 任务执行器。任务执行、并发控制。Worker pool for task execution.
@Author: HiPeng
@Time: 2026/4/1 18:15
"""

import asyncio
from typing import Optional

from neotask.core.future import FutureManager
from neotask.core.queue import PriorityQueue
from neotask.executors.base import TaskExecutor
from neotask.models.task import TaskStatus
from neotask.monitor.event_bus import EventBus, TaskEvent
from neotask.storage.base import TaskRepository


class WorkerPool:
    """Worker pool managing concurrent task execution."""

    def __init__(
            self,
            executor: TaskExecutor,
            task_repo: TaskRepository,
            queue: PriorityQueue,
            future_manager: FutureManager,
            event_bus: EventBus,
            node_id: str,
            max_concurrent: int = 10,
            prefetch_size: int = 20,
    ):
        self._executor = executor
        self._task_repo = task_repo
        self._queue = queue
        self._future_manager = future_manager
        self._event_bus = event_bus
        self._node_id = node_id
        self._max_concurrent = max_concurrent
        self._prefetch_size = prefetch_size
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._running = False
        self._worker_task: Optional[asyncio.Task] = None
        self._active_tasks: dict[str, asyncio.Task] = {}

    async def start(self) -> None:
        """Start worker pool."""
        if self._running:
            return

        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop())

    async def stop(self) -> None:
        """Stop worker pool gracefully."""
        self._running = False

        # Cancel all running tasks
        for task_id, task in self._active_tasks.items():
            if not task.done():
                task.cancel()

        # Wait for all tasks to complete or be cancelled
        if self._active_tasks:
            await asyncio.gather(*self._active_tasks.values(), return_exceptions=True)

        # Cancel worker loop
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

        # Shutdown executor
        await self._executor.shutdown()

    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a running or queued task."""
        # Try to remove from queue
        if await self._queue.remove(task_id):
            task = await self._task_repo.get(task_id)
            if task and task.status == TaskStatus.PENDING:
                task.cancel()
                await self._task_repo.save(task)
                await self._future_manager.complete(task_id, error="Task cancelled")
                await self._event_bus.emit(TaskEvent("task.cancelled", task_id))
            return True

        # Try to cancel running task
        if task_id in self._active_tasks:
            self._active_tasks[task_id].cancel()
            return True

        return False

    async def _worker_loop(self) -> None:
        """Main worker loop."""
        while self._running:
            try:
                # Fetch tasks from queue
                task_ids = await self._queue.pop(self._prefetch_size)

                for task_id in task_ids:
                    # Create task for each execution
                    exec_task = asyncio.create_task(self._execute_task(task_id))
                    self._active_tasks[task_id] = exec_task

                # Clean up completed tasks from active_tasks
                completed = [tid for tid, t in self._active_tasks.items() if t.done()]
                for tid in completed:
                    self._active_tasks.pop(tid, None)

                await asyncio.sleep(0.1)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Worker loop error: {e}")
                await asyncio.sleep(0.5)

    async def _execute_task(self, task_id: str) -> None:
        """Execute a single task."""
        async with self._semaphore:
            task = await self._task_repo.get(task_id)
            if not task:
                return

            if task.status != TaskStatus.PENDING:
                return

            # Mark as processing
            task.start(self._node_id)
            await self._task_repo.save(task)
            await self._event_bus.emit(TaskEvent("task.started", task_id))

            try:
                # Execute
                result = await self._executor.execute(task.data)

                # Mark as completed
                task.complete(result)
                await self._task_repo.save(task)
                await self._future_manager.complete(task_id, result=result)
                await self._event_bus.emit(TaskEvent("task.completed", task_id, result))

            except asyncio.CancelledError:
                task.cancel()
                await self._task_repo.save(task)
                await self._future_manager.complete(task_id, error="Task cancelled")
                await self._event_bus.emit(TaskEvent("task.cancelled", task_id))

            except Exception as e:
                task.fail(str(e))
                await self._task_repo.save(task)
                await self._future_manager.complete(task_id, error=str(e))
                await self._event_bus.emit(TaskEvent("task.failed", task_id, str(e)))
