"""
@FileName: priority_queue.py
@Description: 优先级队列实现
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import heapq
import asyncio
from typing import List, Tuple
from datetime import datetime

from neotask.queue.base import BaseQueue
from neotask.storage.base import QueueRepository


class PriorityQueue(BaseQueue):
    """优先级队列

    使用堆实现的优先级队列，支持内存和持久化两种模式。
    """

    def __init__(self, repository: Optional[QueueRepository] = None):
        self._repository = repository
        self._heap: List[Tuple[int, str, datetime]] = []  # (priority, task_id, enqueued_at)
        self._lock = asyncio.Lock()
        self._use_repository = repository is not None

    async def push(self, task_id: str, priority: int, delay: float = 0) -> bool:
        """入队"""
        if self._use_repository:
            await self._repository.push(task_id, priority)
        else:
            async with self._lock:
                heapq.heappush(self._heap, (priority, task_id, datetime.now()))
        return True

    async def pop(self, count: int = 1) -> List[str]:
        """出队"""
        if self._use_repository:
            return await self._repository.pop(count)

        async with self._lock:
            result = []
            for _ in range(min(count, len(self._heap))):
                _, task_id, _ = heapq.heappop(self._heap)
                result.append(task_id)
            return result

    async def remove(self, task_id: str) -> bool:
        """移除任务"""
        if self._use_repository:
            return await self._repository.remove(task_id)

        async with self._lock:
            original_len = len(self._heap)
            self._heap = [item for item in self._heap if item[1] != task_id]
            heapq.heapify(self._heap)
            return len(self._heap) < original_len

    async def size(self) -> int:
        """获取队列大小"""
        if self._use_repository:
            return await self._repository.size()

        async with self._lock:
            return len(self._heap)

    async def peek(self, count: int = 1) -> List[str]:
        """查看队首任务"""
        if self._use_repository:
            return await self._repository.peek(count)

        async with self._lock:
            sorted_heap = sorted(self._heap)
            return [item[1] for item in sorted_heap[:count]]

    async def clear(self) -> None:
        """清空队列"""
        if self._use_repository:
            await self._repository.clear()

        async with self._lock:
            self._heap.clear()