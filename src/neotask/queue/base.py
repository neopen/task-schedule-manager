"""
@FileName: base.py
@Description: 队列抽象基类
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class QueueItem:
    """队列项"""
    task_id: str
    priority: int
    enqueued_at: datetime
    scheduled_at: Optional[datetime] = None


class BaseQueue(ABC):
    """队列抽象基类"""

    @abstractmethod
    async def push(self, task_id: str, priority: int, delay: float = 0) -> bool:
        """入队"""
        pass

    @abstractmethod
    async def pop(self, count: int = 1) -> List[str]:
        """出队"""
        pass

    @abstractmethod
    async def remove(self, task_id: str) -> bool:
        """移除任务"""
        pass

    @abstractmethod
    async def size(self) -> int:
        """获取队列大小"""
        pass

    @abstractmethod
    async def peek(self, count: int = 1) -> List[str]:
        """查看队首任务"""
        pass

    @abstractmethod
    async def clear(self) -> None:
        """清空队列"""
        pass
