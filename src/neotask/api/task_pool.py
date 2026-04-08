"""
@FileName: task_pool.py
@Description: Function Call接口 - 即时任务入口，专注于立即执行的任务。
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Callable, Union

from neotask.lock.factory import LockFactory

from neotask.core.dispatcher import TaskDispatcher
from neotask.core.future import FutureManager
from neotask.core.lifecycle import TaskLifecycleManager
from neotask.event.bus import EventBus, TaskEvent
from neotask.executor.base import TaskExecutor
from neotask.executor.factory import ExecutorFactory
from neotask.models.config import StorageConfig, LockConfig
from neotask.models.task import TaskPriority
from neotask.monitor.health import SystemHealthChecker
from neotask.monitor.metrics import MetricsCollector
from neotask.monitor.reporter import ReporterManager, ConsoleReporter
from neotask.queue.scheduler import QueueScheduler
from neotask.storage.factory import StorageFactory
from neotask.worker.pool import WorkerPool
from neotask.worker.supervisor import WorkerSupervisor


@dataclass
class TaskPoolConfig:
    """TaskPool配置"""
    # 存储配置
    storage_type: str = "memory"
    sqlite_path: str = "neotask.db"
    redis_url: Optional[str] = None

    # 执行器配置
    executor_func: Optional[Callable] = None
    executor_type: str = "auto"
    max_workers: int = 10

    # Worker配置
    worker_concurrency: int = 10
    prefetch_size: int = 20
    task_timeout: Optional[float] = None

    # 队列配置
    queue_max_size: int = 10000

    # 锁配置
    lock_type: str = "memory"

    # 监控配置
    enable_metrics: bool = True
    enable_health_check: bool = True
    enable_reporter: bool = False

    # 重试配置
    max_retries: int = 3
    retry_delay: float = 1.0


class TaskPool:
    """即时任务池

    专注于立即执行的任务，提供简洁的同步/异步API。

    使用示例：
        >>> # 同步方式
        >>> pool = TaskPool()
        >>> task_id = pool.submit({"data": "value"})
        >>> result = pool.wait_for_result(task_id)
        >>>
        >>> # 异步方式
        >>> async def main():
        ...     pool = TaskPool()
        ...     task_id = await pool.submit_async({"data": "value"})
        ...     result = await pool.wait_for_result_async(task_id)
    """

    def __init__(self, executor: Union[TaskExecutor, Callable, None] = None, config: Optional[TaskPoolConfig] = None):
        """初始化任务池

        Args:
            executor: 任务执行器（可以是TaskExecutor实例、函数或None）
            config: 配置对象
        """
        self._config = config or TaskPoolConfig()

        # 初始化执行器
        self._executor = self._init_executor(executor)

        # 初始化存储
        self._task_repo, self._queue_repo = self._init_storage()

        # 初始化锁管理器
        self._lock_manager = self._init_lock()

        # 初始化事件总线
        self._event_bus = EventBus()

        # 初始化未来管理器
        self._future_manager = FutureManager()

        # 初始化队列调度器
        self._queue_scheduler = QueueScheduler(
            repository=self._queue_repo,
            max_size=self._config.queue_max_size
        )

        # 初始化生命周期管理器
        self._lifecycle = TaskLifecycleManager(
            task_repo=self._task_repo,
            event_bus=self._event_bus
        )

        # 初始化分发器
        self._dispatcher = TaskDispatcher(
            lifecycle_manager=self._lifecycle,
            queue_scheduler=self._queue_scheduler
        )

        # 初始化Worker池
        self._worker_pool = WorkerPool(
            executor=self._executor,
            task_repo=self._task_repo,
            queue_scheduler=self._queue_scheduler,
            event_bus=self._event_bus,
            lifecycle_manager=self._lifecycle,
            concurrency=self._config.worker_concurrency,
            prefetch_size=self._config.prefetch_size,
            task_timeout=self._config.task_timeout
        )

        # 设置重试配置
        self._worker_pool.set_retry_config(
            max_retries=self._config.max_retries,
            retry_delay=self._config.retry_delay
        )

        # 初始化监督者
        self._supervisor = WorkerSupervisor(self._worker_pool)

        # 初始化监控
        self._metrics = MetricsCollector() if self._config.enable_metrics else None
        self._health_checker = SystemHealthChecker(
            task_repo=self._task_repo,
            queue=self._queue_scheduler
        ) if self._config.enable_health_check else None
        self._reporter_manager = None

        # 设置事件处理器
        self._setup_event_handlers()

        # 运行状态
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _init_executor(self, executor: Union[TaskExecutor, Callable, None]) -> TaskExecutor:
        """初始化执行器"""
        if executor is None:
            # 默认执行器
            async def default_executor(data):
                return {"result": "executed", "data": data}

            return ExecutorFactory.create(default_executor)

        if isinstance(executor, TaskExecutor):
            return executor

        return ExecutorFactory.create(
            executor,
            executor_type=self._config.executor_type,
            max_workers=self._config.max_workers
        )

    def _init_storage(self):
        """初始化存储"""
        if self._config.storage_type == "memory":
            storage_config = StorageConfig.memory()
        elif self._config.storage_type == "sqlite":
            storage_config = StorageConfig.sqlite(self._config.sqlite_path)
        elif self._config.storage_type == "redis":
            storage_config = StorageConfig.redis(self._config.redis_url)
        else:
            raise ValueError(f"Unknown storage type: {self._config.storage_type}")

        return StorageFactory.create(storage_config)

    def _init_lock(self):
        """初始化锁管理器"""
        if self._config.lock_type == "memory":
            lock_config = LockConfig.memory()
        elif self._config.lock_type == "redis":
            lock_config = LockConfig.redis(self._config.redis_url)
        else:
            raise ValueError(f"Unknown lock type: {self._config.lock_type}")

        return LockFactory.create(lock_config)

    def _setup_event_handlers(self):
        """设置事件处理器"""
        # 指标收集
        if self._metrics:
            @self._event_bus.subscribe_global
            async def metrics_handler(event: TaskEvent):
                if event.event_type == "task.submitted":
                    self._metrics.record_task_submit(event.task_id)
                elif event.event_type == "task.started":
                    self._metrics.record_task_start(event.task_id)
                elif event.event_type == "task.completed":
                    self._metrics.record_task_complete(event.task_id)
                elif event.event_type == "task.failed":
                    self._metrics.record_task_failed(event.task_id)
                elif event.event_type == "task.cancelled":
                    self._metrics.record_task_cancelled(event.task_id)

        # 未来管理器完成
        @self._event_bus.subscribe("task.completed")
        async def complete_future_handler(event: TaskEvent):
            await self._future_manager.complete(event.task_id, result=event.data)

        @self._event_bus.subscribe("task.failed")
        async def fail_future_handler(event: TaskEvent):
            error = event.data.get("error") if isinstance(event.data, dict) else str(event.data)
            await self._future_manager.complete(event.task_id, error=error)

        @self._event_bus.subscribe("task.cancelled")
        async def cancel_future_handler(event: TaskEvent):
            await self._future_manager.complete(event.task_id, error="Task cancelled")

    def _ensure_running(self):
        """确保服务正在运行"""
        if not self._running:
            self.start()

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        """获取事件循环"""
        if self._loop is None or self._loop.is_closed():
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
        return self._loop

    def _run_coroutine(self, coro):
        """运行协程"""
        loop = self._get_loop()
        if loop.is_running():
            return asyncio.create_task(coro)
        else:
            return loop.run_until_complete(coro)

    # ========== 生命周期管理 ==========

    def start(self) -> None:
        """启动任务池"""
        if self._running:
            return

        loop = self._get_loop()

        # 启动组件
        loop.run_until_complete(self._event_bus.start())
        loop.run_until_complete(self._queue_scheduler.start())
        loop.run_until_complete(self._worker_pool.start())
        loop.run_until_complete(self._supervisor.start())

        # 启动上报器
        if self._config.enable_reporter:
            self._reporter_manager = ReporterManager(interval=60)
            self._reporter_manager.add_reporter(ConsoleReporter())
            self._reporter_manager.set_metrics_callback(self.get_stats)
            loop.run_until_complete(self._reporter_manager.start())

        self._running = True

    def shutdown(self, graceful: bool = True, timeout: float = 30) -> None:
        """关闭任务池

        Args:
            graceful: 是否优雅关闭（等待当前任务完成）
            timeout: 优雅关闭超时时间
        """
        if not self._running:
            return

        self._running = False

        loop = self._get_loop()

        # 停止接收新任务
        loop.run_until_complete(self._queue_scheduler.disable())

        # 等待队列清空
        if graceful:
            loop.run_until_complete(self._queue_scheduler.wait_until_empty(timeout))

        # 停止组件
        loop.run_until_complete(self._supervisor.stop())
        loop.run_until_complete(self._worker_pool.stop(graceful, timeout))
        loop.run_until_complete(self._queue_scheduler.stop())

        if self._reporter_manager:
            loop.run_until_complete(self._reporter_manager.stop())

        loop.run_until_complete(self._event_bus.stop())

        # 关闭存储连接
        if hasattr(self._task_repo, 'close'):
            loop.run_until_complete(self._task_repo.close())
        if hasattr(self._queue_repo, 'close'):
            loop.run_until_complete(self._queue_repo.close())

    def __enter__(self):
        """上下文管理器入口"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.shutdown()

    # ========== 任务提交 API ==========

    def submit(
            self,
            data: Dict[str, Any],
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            delay: float = 0
    ) -> str:
        """提交任务（同步）

        Args:
            data: 任务数据
            task_id: 任务ID（可选）
            priority: 优先级（1-10，数字越小优先级越高）
            delay: 延迟执行时间（秒）

        Returns:
            task_id
        """
        self._ensure_running()
        return self._run_coroutine(
            self._dispatcher.dispatch(data, task_id, priority, delay)
        )

    async def submit_async(
            self,
            data: Dict[str, Any],
            task_id: Optional[str] = None,
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL,
            delay: float = 0
    ) -> str:
        """提交任务（异步）"""
        self._ensure_running()
        return await self._dispatcher.dispatch(data, task_id, priority, delay)

    def submit_batch(
            self,
            tasks: List[Dict[str, Any]],
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL
    ) -> List[str]:
        """批量提交任务（同步）"""
        self._ensure_running()
        task_ids = []
        for task_data in tasks:
            task_id = self.submit(task_data, priority=priority)
            task_ids.append(task_id)
        return task_ids

    async def submit_batch_async(
            self,
            tasks: List[Dict[str, Any]],
            priority: Union[int, TaskPriority] = TaskPriority.NORMAL
    ) -> List[str]:
        """批量提交任务（异步）"""
        self._ensure_running()
        task_ids = []
        for task_data in tasks:
            task_id = await self.submit_async(task_data, priority=priority)
            task_ids.append(task_id)
        return task_ids

    # ========== 任务等待 API ==========

    def wait_for_result(self, task_id: str, timeout: float = 300) -> Any:
        """等待任务完成并返回结果（同步）

        Args:
            task_id: 任务ID
            timeout: 超时时间（秒）

        Returns:
            任务结果

        Raises:
            TaskNotFoundError: 任务不存在
            TimeoutError: 等待超时
            Exception: 任务执行失败
        """
        return self._run_coroutine(self._lifecycle.wait_for_task(task_id, timeout))

    async def wait_for_result_async(self, task_id: str, timeout: float = 300) -> Any:
        """等待任务完成并返回结果（异步）"""
        return await self._lifecycle.wait_for_task(task_id, timeout)

    def wait_all(
            self,
            task_ids: List[str],
            timeout: float = 300
    ) -> Dict[str, Any]:
        """等待所有任务完成（同步）"""
        results = {}
        for task_id in task_ids:
            try:
                result = self.wait_for_result(task_id, timeout)
                results[task_id] = result
            except Exception as e:
                results[task_id] = {"error": str(e)}
        return results

    async def wait_all_async(
            self,
            task_ids: List[str],
            timeout: float = 300
    ) -> Dict[str, Any]:
        """等待所有任务完成（异步）"""
        results = {}
        for task_id in task_ids:
            try:
                result = await self.wait_for_result_async(task_id, timeout)
                results[task_id] = result
            except Exception as e:
                results[task_id] = {"error": str(e)}
        return results

    # ========== 任务查询 API ==========

    def get_status(self, task_id: str) -> Optional[str]:
        """获取任务状态（同步）"""
        task = self._run_coroutine(self._lifecycle.get_task(task_id))
        return task.status.value if task else None

    async def get_status_async(self, task_id: str) -> Optional[str]:
        """获取任务状态（异步）"""
        task = await self._lifecycle.get_task(task_id)
        return task.status.value if task else None

    def get_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务结果（同步）"""
        task = self._run_coroutine(self._lifecycle.get_task(task_id))
        if not task:
            return {"error": "task not found"}
        return {
            "task_id": task.task_id,
            "status": task.status.value,
            "result": task.result,
            "error": task.error,
            "created_at": task.created_at.isoformat() if task.created_at else None,
            "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        }

    async def get_result_async(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务结果（异步）"""
        task = await self._lifecycle.get_task(task_id)
        if not task:
            return {"error": "task not found"}
        return {
            "task_id": task.task_id,
            "status": task.status.value,
            "result": task.result,
            "error": task.error,
            "created_at": task.created_at.isoformat() if task.created_at else None,
            "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        }

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取完整任务信息（同步）"""
        task = self._run_coroutine(self._lifecycle.get_task(task_id))
        return task.to_dict() if task else None

    async def get_task_async(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取完整任务信息（异步）"""
        task = await self._lifecycle.get_task(task_id)
        return task.to_dict() if task else None

    def task_exists(self, task_id: str) -> bool:
        """检查任务是否存在（同步）"""
        return self._run_coroutine(self._task_repo.exists(task_id))

    async def task_exists_async(self, task_id: str) -> bool:
        """检查任务是否存在（异步）"""
        return await self._task_repo.exists(task_id)

    # ========== 任务管理 API ==========

    def cancel(self, task_id: str) -> bool:
        """取消任务（同步）"""
        self._ensure_running()
        # 从队列移除
        self._run_coroutine(self._queue_scheduler.remove(task_id))
        # 取消任务
        return self._run_coroutine(self._lifecycle.cancel_task(task_id))

    async def cancel_async(self, task_id: str) -> bool:
        """取消任务（异步）"""
        await self._queue_scheduler.remove(task_id)
        return await self._lifecycle.cancel_task(task_id)

    def delete(self, task_id: str) -> bool:
        """删除任务（同步）"""
        return self._run_coroutine(self._lifecycle.delete_task(task_id))

    async def delete_async(self, task_id: str) -> bool:
        """删除任务（异步）"""
        return await self._lifecycle.delete_task(task_id)

    def retry(self, task_id: str, delay: float = 0) -> bool:
        """重试失败的任务（同步）"""
        self._ensure_running()
        return self._run_coroutine(self._dispatcher.redispatch(task_id, delay))

    async def retry_async(self, task_id: str, delay: float = 0) -> bool:
        """重试失败的任务（异步）"""
        return await self._dispatcher.redispatch(task_id, delay)

    # ========== 统计信息 API ==========

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息（同步）"""
        stats = self._run_coroutine(self._lifecycle.get_task_stats())
        queue_size = self._run_coroutine(self._queue_scheduler.size())

        result = {
            "queue_size": queue_size,
            "total": stats.total,
            "pending": stats.pending,
            "running": stats.running,
            "completed": stats.completed,
            "failed": stats.failed,
            "cancelled": stats.cancelled,
            "success_rate": stats.success_rate if stats.total > 0 else 1.0,
        }

        if self._metrics:
            result["metrics"] = self._metrics.get_summary()

        return result

    async def get_stats_async(self) -> Dict[str, Any]:
        """获取统计信息（异步）"""
        stats = await self._lifecycle.get_task_stats()
        queue_size = await self._queue_scheduler.size()

        result = {
            "queue_size": queue_size,
            "total": stats.total,
            "pending": stats.pending,
            "running": stats.running,
            "completed": stats.completed,
            "failed": stats.failed,
            "cancelled": stats.cancelled,
            "success_rate": stats.success_rate if stats.total > 0 else 1.0,
        }

        if self._metrics:
            result["metrics"] = self._metrics.get_summary()

        return result

    def get_worker_stats(self) -> Dict[int, Any]:
        """获取Worker统计信息"""
        return self._worker_pool.get_stats()

    def get_health_status(self) -> Dict[str, Any]:
        """获取健康状态"""
        if self._health_checker:
            return self._health_checker.get_summary()
        return {"status": "unknown", "message": "Health check disabled"}

    # ========== 队列管理 API ==========

    def get_queue_size(self) -> int:
        """获取队列大小（同步）"""
        return self._run_coroutine(self._queue_scheduler.size())

    async def get_queue_size_async(self) -> int:
        """获取队列大小（异步）"""
        return await self._queue_scheduler.size()

    def pause(self) -> None:
        """暂停处理新任务（同步）"""
        self._run_coroutine(self._queue_scheduler.pause())

    async def pause_async(self) -> None:
        """暂停处理新任务（异步）"""
        await self._queue_scheduler.pause()

    def resume(self) -> None:
        """恢复处理新任务（同步）"""
        self._run_coroutine(self._queue_scheduler.resume())

    async def resume_async(self) -> None:
        """恢复处理新任务（异步）"""
        await self._queue_scheduler.resume()

    def clear_queue(self) -> None:
        """清空队列（同步）"""
        self._run_coroutine(self._queue_scheduler.clear())

    async def clear_queue_async(self) -> None:
        """清空队列（异步）"""
        await self._queue_scheduler.clear()

    # ========== 锁管理 API ==========

    def acquire_lock(self, task_id: str, ttl: int = 30) -> bool:
        """获取任务锁（同步）"""
        lock_key = f"task:{task_id}"
        return self._run_coroutine(self._lock_manager.acquire(lock_key, ttl))

    async def acquire_lock_async(self, task_id: str, ttl: int = 30) -> bool:
        """获取任务锁（异步）"""
        lock_key = f"task:{task_id}"
        return await self._lock_manager.acquire(lock_key, ttl)

    def release_lock(self, task_id: str) -> bool:
        """释放任务锁（同步）"""
        lock_key = f"task:{task_id}"
        return self._run_coroutine(self._lock_manager.release(lock_key))

    async def release_lock_async(self, task_id: str) -> bool:
        """释放任务锁（异步）"""
        lock_key = f"task:{task_id}"
        return await self._lock_manager.release(lock_key)

    # ========== 事件回调 API ==========

    def on_submitted(self, handler: Callable) -> None:
        """注册任务提交回调"""
        self._event_bus.subscribe("task.submitted", handler)

    def on_started(self, handler: Callable) -> None:
        """注册任务开始回调"""
        self._event_bus.subscribe("task.started", handler)

    def on_completed(self, handler: Callable) -> None:
        """注册任务完成回调"""
        self._event_bus.subscribe("task.completed", handler)

    def on_failed(self, handler: Callable) -> None:
        """注册任务失败回调"""
        self._event_bus.subscribe("task.failed", handler)

    def on_cancelled(self, handler: Callable) -> None:
        """注册任务取消回调"""
        self._event_bus.subscribe("task.cancelled", handler)
