"""
@FileName: delayed_queue.py
@Description: 延迟队列实现
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import heapq
import asyncio
from typing import List, Tuple, Optional
from datetime import datetime, timedelta


class DelayedQueue:
    """延迟队列

    支持任务延迟执行，使用时间堆实现。
    """

    def __init__(self):
        self._heap: List[Tuple[float, int, str]] = []  # (execute_time, priority, task_id)
        self._lock = asyncio.Lock()
        self._scheduler_task: Optional[asyncio.Task] = None
        self._running = False
        self._callback = None

    async def start(self, callback) -> None:
        """启动延迟队列调度器

        Args:
            callback: 延迟到期时的回调函数，签名为 async def callback(task_id: str, priority: int)
        """
        self._callback = callback
        self._running = True
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())

    async def stop(self) -> None:
        """停止延迟队列"""
        self._running = False
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

    async def schedule(self, task_id: str, priority: int, delay: float) -> None:
        """调度延迟任务

        Args:
            task_id: 任务ID
            priority: 优先级
            delay: 延迟时间（秒）
        """
        execute_time = datetime.now().timestamp() + delay

        async with self._lock:
            heapq.heappush(self._heap, (execute_time, priority, task_id))

    async def cancel(self, task_id: str) -> bool:
        """取消延迟任务"""
        async with self._lock:
            original_len = len(self._heap)
            self._heap = [item for item in self._heap if item[2] != task_id]
            heapq.heapify(self._heap)
            return len(self._heap) < original_len

    async def size(self) -> int:
        """获取延迟队列大小"""
        async with self._lock:
            return len(self._heap)

    async def _scheduler_loop(self) -> None:
        """调度循环"""
        while self._running:
            try:
                now = datetime.now().timestamp()
                tasks_to_move = []

                async with self._lock:
                    while self._heap and self._heap[0][0] <= now:
                        execute_time, priority, task_id = heapq.heappop(self._heap)
                        tasks_to_move.append((task_id, priority))

                # 触发回调
                for task_id, priority in tasks_to_move:
                    if self._callback:
                        await self._callback(task_id, priority)

                await asyncio.sleep(0.1)

            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.5)