"""
@FileName: metrics.py
@Description: 指标收集 - 任务执行指标
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
import time
from dataclasses import dataclass, field
from statistics import mean
from typing import Dict, List, Any


@dataclass
class TaskMetrics:
    """任务指标"""
    total_submitted: int = 0
    total_completed: int = 0
    total_failed: int = 0
    total_cancelled: int = 0

    # 执行时间统计
    execution_times: List[float] = field(default_factory=list)
    queue_times: List[float] = field(default_factory=list)

    # 当前状态
    pending: int = 0
    running: int = 0

    @property
    def success_rate(self) -> float:
        """成功率"""
        total = self.total_completed + self.total_failed
        if total == 0:
            return 1.0
        return self.total_completed / total

    @property
    def avg_execution_time(self) -> float:
        """平均执行时间"""
        if not self.execution_times:
            return 0.0
        return mean(self.execution_times)

    @property
    def p95_execution_time(self) -> float:
        """P95执行时间"""
        if not self.execution_times:
            return 0.0
        sorted_times = sorted(self.execution_times)
        idx = int(len(sorted_times) * 0.95)
        return sorted_times[idx]

    @property
    def avg_queue_time(self) -> float:
        """平均排队时间"""
        if not self.queue_times:
            return 0.0
        return mean(self.queue_times)


class MetricsCollector:
    """指标收集器

    收集任务执行的各项指标。
    """

    def __init__(self, window_size: int = 1000):
        self._metrics = TaskMetrics()
        self._window_size = window_size
        self._task_start_times: Dict[str, float] = {}
        self._task_queue_times: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    def record_task_submit(self, task_id: str) -> None:
        """记录任务提交"""
        self._task_queue_times[task_id] = time.time()

        async def _record():
            async with self._lock:
                self._metrics.total_submitted += 1
                self._metrics.pending += 1

        asyncio.create_task(_record())

    def record_task_start(self, task_id: str) -> None:
        """记录任务开始"""
        self._task_start_times[task_id] = time.time()

        # 计算排队时间
        if task_id in self._task_queue_times:
            queue_time = time.time() - self._task_queue_times[task_id]

            async def _record_queue():
                async with self._lock:
                    self._metrics.queue_times.append(queue_time)
                    # 限制队列大小
                    if len(self._metrics.queue_times) > self._window_size:
                        self._metrics.queue_times = self._metrics.queue_times[-self._window_size:]

            asyncio.create_task(_record_queue())

        async def _record():
            async with self._lock:
                self._metrics.pending -= 1
                self._metrics.running += 1

        asyncio.create_task(_record())

    def record_task_complete(self, task_id: str) -> None:
        """记录任务完成"""
        # 计算执行时间
        if task_id in self._task_start_times:
            execution_time = time.time() - self._task_start_times[task_id]

            async def _record_exec():
                async with self._lock:
                    self._metrics.execution_times.append(execution_time)
                    # 限制队列大小
                    if len(self._metrics.execution_times) > self._window_size:
                        self._metrics.execution_times = self._metrics.execution_times[-self._window_size:]

            asyncio.create_task(_record_exec())

        async def _record():
            async with self._lock:
                self._metrics.total_completed += 1
                self._metrics.running -= 1

        asyncio.create_task(_record())

        # 清理
        self._task_start_times.pop(task_id, None)
        self._task_queue_times.pop(task_id, None)

    def record_task_failed(self, task_id: str) -> None:
        """记录任务失败"""
        # 计算执行时间
        if task_id in self._task_start_times:
            execution_time = time.time() - self._task_start_times[task_id]

            async def _record_exec():
                async with self._lock:
                    self._metrics.execution_times.append(execution_time)
                    if len(self._metrics.execution_times) > self._window_size:
                        self._metrics.execution_times = self._metrics.execution_times[-self._window_size:]

            asyncio.create_task(_record_exec())

        async def _record():
            async with self._lock:
                self._metrics.total_failed += 1
                self._metrics.running -= 1

        asyncio.create_task(_record())

        # 清理
        self._task_start_times.pop(task_id, None)
        self._task_queue_times.pop(task_id, None)

    def record_task_cancelled(self, task_id: str) -> None:
        """记录任务取消"""

        async def _record():
            async with self._lock:
                self._metrics.total_cancelled += 1
                self._metrics.running -= 1

        asyncio.create_task(_record())

        # 清理
        self._task_start_times.pop(task_id, None)
        self._task_queue_times.pop(task_id, None)

    def get_metrics(self) -> TaskMetrics:
        """获取指标"""
        return self._metrics

    def get_summary(self) -> Dict[str, Any]:
        """获取指标摘要"""
        return {
            "total_submitted": self._metrics.total_submitted,
            "total_completed": self._metrics.total_completed,
            "total_failed": self._metrics.total_failed,
            "total_cancelled": self._metrics.total_cancelled,
            "success_rate": self._metrics.success_rate,
            "pending": self._metrics.pending,
            "running": self._metrics.running,
            "avg_execution_time": self._metrics.avg_execution_time,
            "p95_execution_time": self._metrics.p95_execution_time,
            "avg_queue_time": self._metrics.avg_queue_time,
            "execution_samples": len(self._metrics.execution_times),
        }

    def reset(self) -> None:
        """重置指标"""
        self._metrics = TaskMetrics()
        self._task_start_times.clear()
        self._task_queue_times.clear()
