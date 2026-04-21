"""
@FileName: time_wheel.py
@Description: 时间轮实现 - 高性能定时器
@Author: HiPeng
@Time: 2026/4/21
"""

import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Callable


@dataclass
class TimeWheelTask:
    """时间轮任务"""
    task_id: str
    priority: int
    data: Optional[Dict] = None
    callback: Optional[Callable] = None
    round: int = 0  # 剩余轮数


class TimeWheel:
    """时间轮

    高性能定时器，适用于大量延时任务的场景。

    设计模式：Circular Buffer Pattern - 循环数组实现

    使用示例：
        >>> tw = TimeWheel(slot_count=60, tick_interval=1.0)
        >>> await tw.start()
        >>> task_id = await tw.add_task("task-1", delay=30)  # 30秒后执行
        >>> await tw.add_task("task-2", delay=65)  # 65秒后执行（需要多轮）
    """

    def __init__(
            self,
            slot_count: int = 60,  # 槽位数量
            tick_interval: float = 1.0,  # 每个槽位的时间间隔（秒）
            max_rounds: int = 100  # 最大轮数限制
    ):
        self._slot_count = slot_count
        self._tick_interval = tick_interval
        self._max_rounds = max_rounds
        self._slots: List[Dict[str, TimeWheelTask]] = [{} for _ in range(slot_count)]
        self._current_slot = 0
        self._running = False
        self._ticker_task: Optional[asyncio.Task] = None
        self._callback: Optional[Callable] = None
        self._lock = asyncio.Lock()

    async def start(self, callback: Callable) -> None:
        """启动时间轮

        Args:
            callback: 任务到期回调，签名为 async def callback(task_id: str, priority: int, data: Dict)
        """
        self._callback = callback
        self._running = True
        self._ticker_task = asyncio.create_task(self._ticker_loop())

    async def stop(self) -> None:
        """停止时间轮"""
        self._running = False
        if self._ticker_task:
            self._ticker_task.cancel()
            try:
                await self._ticker_task
            except asyncio.CancelledError:
                pass

    async def add_task(
            self,
            task_id: str,
            priority: int,
            delay: float,
            data: Optional[Dict] = None
    ) -> int:
        """添加延时任务

        Args:
            task_id: 任务ID
            priority: 优先级
            delay: 延迟时间（秒）
            data: 任务数据

        Returns:
            任务所在的槽位索引
        """
        # 计算需要经过的槽位数
        slots_needed = int(delay / self._tick_interval)

        if slots_needed >= self._slot_count * self._max_rounds:
            # 超过最大轮数限制，使用延迟队列作为后备
            return -1

        # 计算槽位和轮数
        target_slot = (self._current_slot + slots_needed) % self._slot_count
        rounds = slots_needed // self._slot_count

        task = TimeWheelTask(
            task_id=task_id,
            priority=priority,
            data=data,
            callback=self._callback,
            round=rounds
        )

        async with self._lock:
            self._slots[target_slot][task_id] = task

        return target_slot

    async def cancel_task(self, task_id: str) -> bool:
        """取消任务

        Args:
            task_id: 任务ID

        Returns:
            是否成功取消
        """
        async with self._lock:
            for slot in self._slots:
                if task_id in slot:
                    del slot[task_id]
                    return True
            return False

    async def size(self) -> int:
        """获取任务总数"""
        async with self._lock:
            return sum(len(slot) for slot in self._slots)

    async def _ticker_loop(self) -> None:
        """时间轮转动循环"""
        while self._running:
            try:
                await asyncio.sleep(self._tick_interval)
                await self._tick()

            except asyncio.CancelledError:
                break
            except Exception:
                continue

    async def _tick(self) -> None:
        """执行一次滴答"""
        tasks_to_execute = []

        async with self._lock:
            # 获取当前槽位的任务
            current_slot_tasks = self._slots[self._current_slot]

            for task_id, task in list(current_slot_tasks.items()):
                if task.round == 0:
                    # 轮数为0，立即执行
                    tasks_to_execute.append((task_id, task.priority, task.data))
                    del current_slot_tasks[task_id]
                else:
                    # 轮数减1，继续等待
                    task.round -= 1

            # 移动到下一个槽位
            self._current_slot = (self._current_slot + 1) % self._slot_count

        # 执行回调
        for task_id, priority, data in tasks_to_execute:
            if self._callback:
                try:
                    await self._callback(task_id, priority, data)
                except Exception:
                    pass

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "slot_count": self._slot_count,
            "tick_interval": self._tick_interval,
            "current_slot": self._current_slot,
            "total_tasks": sum(len(slot) for slot in self._slots),
            "is_running": self._running
        }
