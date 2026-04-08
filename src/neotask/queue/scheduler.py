"""
@FileName: scheduler.py
@Description: 队列调度器 - 整合优先级队列和延迟队列
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
from typing import List, Optional
from neotask.queue.priority_queue import PriorityQueue
from neotask.queue.delayed_queue import DelayedQueue
from neotask.storage.base import QueueRepository


class QueueScheduler:
    """队列调度器

    整合优先级队列和延迟队列，提供统一的调度接口。
    """

    def __init__(
        self,
        repository: Optional[QueueRepository] = None,
        max_size: int = 10000
    ):
        self._priority_queue = PriorityQueue(repository)
        self._delayed_queue = DelayedQueue()
        self._max_size = max_size
        self._paused = False
        self._disabled = False

    async def start(self) -> None:
        """启动调度器"""
        await self._delayed_queue.start(self._on_delayed_task_ready)

    async def stop(self) -> None:
        """停止调度器"""
        await self._delayed_queue.stop()

    async def push(self, task_id: str, priority: int, delay: float = 0) -> bool:
        """推送任务到队列

        Args:
            task_id: 任务ID
            priority: 优先级（数字越小优先级越高）
            delay: 延迟执行时间（秒）

        Returns:
            是否成功入队
        """
        if self._disabled:
            return False

        if await self._priority_queue.size() >= self._max_size:
            return False

        if delay > 0:
            await self._delayed_queue.schedule(task_id, priority, delay)
        else:
            await self._priority_queue.push(task_id, priority)

        return True

    async def pop(self, count: int = 1) -> List[str]:
        """弹出任务"""
        if self._paused or self._disabled:
            return []

        return await self._priority_queue.pop(count)

    async def remove(self, task_id: str) -> bool:
        """从队列移除任务"""
        removed = await self._priority_queue.remove(task_id)
        if not removed:
            removed = await self._delayed_queue.cancel(task_id)
        return removed

    async def size(self) -> int:
        """获取队列大小"""
        priority_size = await self._priority_queue.size()
        delayed_size = await self._delayed_queue.size()
        return priority_size + delayed_size

    async def is_empty(self) -> bool:
        """检查队列是否为空"""
        return await self.size() == 0

    async def clear(self) -> None:
        """清空队列"""
        await self._priority_queue.clear()
        # 延迟队列会在调度循环中自然清空

    async def pause(self) -> None:
        """暂停调度"""
        self._paused = True

    async def resume(self) -> None:
        """恢复调度"""
        self._paused = False

    async def disable(self) -> None:
        """禁用入队"""
        self._disabled = True

    async def enable(self) -> None:
        """启用入队"""
        self._disabled = False

    async def wait_until_empty(self, timeout: float = 300) -> bool:
        """等待队列清空

        Args:
            timeout: 超时时间（秒）

        Returns:
            是否在超时前清空
        """
        import time
        start = time.time()
        while time.time() - start < timeout:
            if await self.is_empty():
                return True
            await asyncio.sleep(0.5)
        return False

    async def _on_delayed_task_ready(self, task_id: str, priority: int) -> None:
        """延迟任务到期回调"""
        await self._priority_queue.push(task_id, priority)