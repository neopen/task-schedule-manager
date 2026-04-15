"""
@FileName: pool.py
@Description: Worker池 - 管理任务执行器
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict

from neotask.common.logger import error, info, debug, warning
from neotask.core.lifecycle import TaskLifecycleManager
from neotask.event.bus import EventBus, TaskEvent
from neotask.executor.base import TaskExecutor
from neotask.models.task import TaskStatus
from neotask.queue.scheduler import QueueScheduler
from neotask.storage.base import TaskRepository
from neotask.worker.prefetcher import TaskPrefetcher, PrefetchConfig
from neotask.worker.reclaimer import TaskReclaimer, ReclaimerConfig


@dataclass
class WorkerStats:
    """Worker统计信息"""
    worker_id: int
    active_tasks: int
    completed_tasks: int = 0
    failed_tasks: int = 0
    last_active: Optional[datetime] = None
    is_busy: bool = False


class WorkerPool:
    """Worker池

    管理多个worker协程，控制并发执行。
    """

    def __init__(
            self,
            executor: TaskExecutor,
            task_repo: TaskRepository,
            queue_scheduler: QueueScheduler,
            event_bus: EventBus,
            lifecycle_manager: TaskLifecycleManager,
            concurrency: int = 10,
            prefetch_size: int = 20,
            task_timeout: Optional[float] = None,
            enable_prefetch: bool = True,
            enable_reclaimer: bool = True
    ):
        self._executor = executor
        self._task_repo = task_repo
        self._queue = queue_scheduler
        self._event_bus = event_bus
        self._lifecycle = lifecycle_manager
        self._concurrency = concurrency
        self._prefetch_size = prefetch_size
        self._task_timeout = task_timeout

        self._workers: Dict[int, asyncio.Task] = {}
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._worker_stats: Dict[int, WorkerStats] = {}
        self._semaphore = asyncio.Semaphore(concurrency)
        self._running = False
        self._lock = asyncio.Lock()

        # 重试配置
        self._max_retries = 3
        self._retry_delay = 1.0

        # 初始化预取器
        self._enable_prefetch = enable_prefetch
        if enable_prefetch:
            prefetch_config = PrefetchConfig(
                prefetch_size=prefetch_size,
                local_queue_size=prefetch_size * 5,
                min_threshold=prefetch_size // 4
            )
            self._prefetcher = TaskPrefetcher(queue_scheduler, prefetch_config)
        else:
            self._prefetcher = None

        # 初始化回收器
        self._enable_reclaimer = enable_reclaimer
        if enable_reclaimer:
            reclaimer_config = ReclaimerConfig(
                interval=30,
                task_timeout=task_timeout or 300,
                max_retries=self._max_retries
            )
            self._reclaimer = TaskReclaimer(
                task_repo, queue_scheduler, None, event_bus, reclaimer_config
            )
        else:
            self._reclaimer = None

    async def start(self) -> None:
        """启动worker池"""
        if self._prefetcher:
            await self._prefetcher.start()
        if self._reclaimer:
            await self._reclaimer.start()

        """启动worker池"""
        if self._running:
            return

        self._running = True

        for i in range(self._concurrency):
            worker_task = asyncio.create_task(self._worker_loop(i))
            self._workers[i] = worker_task
            self._worker_stats[i] = WorkerStats(worker_id=i, active_tasks=0)

    async def stop(self, graceful: bool = True, timeout: float = 30) -> None:
        """停止worker池

        Args:
            graceful: 是否优雅停止（等待当前任务完成）
            timeout: 优雅停止超时时间
        """
        self._running = False
        """停止worker池"""
        if self._prefetcher:
            await self._prefetcher.stop(graceful, timeout)
        if self._reclaimer:
            await self._reclaimer.stop()

        if graceful and self._running_tasks:
            # 等待当前任务完成
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._running_tasks.values(), return_exceptions=True),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                # 超时后强制取消
                for task in self._running_tasks.values():
                    if not task.done():
                        task.cancel()

        # 取消所有worker
        for worker_id, worker_task in self._workers.items():
            if not worker_task.done():
                worker_task.cancel()

        # 等待worker完成
        if self._workers:
            await asyncio.gather(*self._workers.values(), return_exceptions=True)

        self._workers.clear()
        self._running_tasks.clear()

    async def cancel_task(self, task_id: str) -> bool:
        """取消运行中的任务"""
        if task_id in self._running_tasks:
            self._running_tasks[task_id].cancel()
            return True
        return False

    def active_count(self) -> int:
        """获取活跃worker数量"""
        return len(self._running_tasks)

    def get_stats(self) -> Dict[int, WorkerStats]:
        """获取worker统计信息"""
        return self._worker_stats.copy()

    def set_retry_config(self, max_retries: int, retry_delay: float) -> None:
        """设置重试配置"""
        self._max_retries = max_retries
        self._retry_delay = retry_delay

    async def _worker_loop(self, worker_id: int) -> None:
        """Worker主循环"""
        while self._running:
            try:
                # 获取任务 - pop 返回 List[str]
                task_ids = await self._queue.pop(self._prefetch_size)

                for task_id in task_ids:
                    # 启动任务执行
                    exec_task = asyncio.create_task(self._execute_task(worker_id, task_id))
                    async with self._lock:
                        self._running_tasks[task_id] = exec_task
                        self._worker_stats[worker_id].active_tasks += 1
                        self._worker_stats[worker_id].is_busy = True

                # 清理已完成的任务
                await self._cleanup_completed_tasks()

                # 如果没有获取到任务，短暂休眠避免空转
                if not task_ids:
                    await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                break
            except Exception as e:
                error(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(0.5)

    async def _execute_task(self, worker_id: int, task_id: str) -> None:
        """执行单个任务"""
        async with self._semaphore:
            # 获取任务
            task = await self._lifecycle.get_task(task_id)
            if not task:
                warning(f"Task {task_id} not found")
                return

            # 检查任务状态 - 只有 PENDING 的任务才能执行
            if task.status != TaskStatus.PENDING:
                warning(f"Task {task_id} status is {task.status}, not PENDING, skipping")
                return

            # 开始执行 - 更新状态为 RUNNING
            success = await self._lifecycle.start_task(task_id, f"worker-{worker_id}")
            if not success:
                error(f"Failed to start task {task_id}")
                return

            debug(f"Worker {worker_id} started task {task_id}")

            # 获取当前重试次数
            current_retry = task.retry_count

            try:
                # 执行任务
                if self._task_timeout:
                    result = await asyncio.wait_for(
                        self._executor.execute(task.data),
                        timeout=self._task_timeout
                    )
                else:
                    result = await self._executor.execute(task.data)

                # 标记完成
                await self._lifecycle.complete_task(task_id, result)

                # 更新统计
                self._worker_stats[worker_id].completed_tasks += 1
                debug(f"Worker {worker_id} completed task {task_id}")

            except asyncio.CancelledError:
                info(f"Task {task_id} was cancelled")
                await self._lifecycle.cancel_task(task_id)
                return

            except asyncio.TimeoutError:
                msg = f"Task execution timeout after {self._task_timeout}s"
                error(f"Task {task_id} timeout: {msg}")
                await self._handle_task_failure(task_id, msg, current_retry, worker_id)

            except Exception as e:
                msg = str(e)
                error(f"Task {task_id} failed: {msg}")
                await self._handle_task_failure(task_id, msg, current_retry, worker_id)

            finally:
                # 更新worker统计
                async with self._lock:
                    self._worker_stats[worker_id].active_tasks = max(0, self._worker_stats[worker_id].active_tasks - 1)
                    self._worker_stats[worker_id].is_busy = self._worker_stats[worker_id].active_tasks > 0
                    self._worker_stats[worker_id].last_active = datetime.now()

    async def _handle_task_failure(self, task_id: str, error: str, retry_count: int, worker_id: int) -> None:
        """处理任务失败和重试"""
        task = await self._lifecycle.get_task(task_id)
        if not task:
            return

        if retry_count < self._max_retries:
            # 使用 lifecycle 更新重试计数
            new_retry_count = retry_count + 1
            await self._lifecycle.update_status(
                task_id,
                TaskStatus.PENDING,
                retry_count=new_retry_count,
                error=error
            )

            # 重新入队（带延迟）
            await self._queue.push(task_id, task.priority.value, delay=self._retry_delay)

            await self._event_bus.emit(TaskEvent(
                "task.retry",
                task_id,
                {"error": error, "retry_count": new_retry_count, "max_retries": self._max_retries}
            ))
        else:
            await self._lifecycle.fail_task(task_id, error)
            self._worker_stats[worker_id].failed_tasks += 1

    async def _cleanup_completed_tasks(self) -> None:
        """清理已完成的任务"""
        async with self._lock:
            completed = [
                (tid, t) for tid, t in self._running_tasks.items()
                if t.done()
            ]

            for task_id, task in completed:
                self._running_tasks.pop(task_id, None)
