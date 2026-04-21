"""
@FileName: bus.py
@Description: 事件总线 - 发布订阅模式
@Author: HiPeng
@Time: 2026/4/8 00:00
"""

import asyncio
from dataclasses import dataclass
from typing import Dict, List, Callable, Any, Optional


@dataclass
class TaskEvent:
    """任务事件"""
    event_type: str
    task_id: str
    data: Optional[Any] = None


class EventBus:
    """事件总线

    设计模式：Observer Pattern / Pub-Sub Pattern
    """

    def __init__(self):
        self._handlers: Dict[str, List[Callable]] = {}
        self._global_handlers: List[Callable] = []
        self._lock = asyncio.Lock()
        self._running = False
        self._queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task] = None
        # 存储原始处理器到包装器的映射，用于正确取消订阅
        self._original_to_wrapper: Dict[Callable, Callable] = {}

    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        await self.stop()

    def subscribe(self, event_type: str, handler: Callable) -> Callable:
        """订阅特定类型事件"""

        async def wrapper(event: TaskEvent):
            if asyncio.iscoroutinefunction(handler):
                await handler(event)
            else:
                handler(event)

        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(wrapper)
        # 存储映射关系
        self._original_to_wrapper[handler] = wrapper
        return handler

    def subscribe_global(self, handler: Callable) -> Callable:
        """订阅所有事件"""

        async def wrapper(event: TaskEvent):
            if asyncio.iscoroutinefunction(handler):
                await handler(event)
            else:
                handler(event)

        self._global_handlers.append(wrapper)
        self._original_to_wrapper[handler] = wrapper
        return handler

    def unsubscribe(self, event_type: str, handler: Callable) -> bool:
        """取消订阅特定类型事件"""
        # 获取包装后的处理器
        wrapper = self._original_to_wrapper.get(handler)
        if wrapper is None:
            return False

        if event_type in self._handlers:
            original_count = len(self._handlers[event_type])
            # 移除包装器
            self._handlers[event_type] = [
                h for h in self._handlers[event_type]
                if h != wrapper
            ]
            removed = len(self._handlers[event_type]) < original_count
            if removed:
                # 清理映射
                del self._original_to_wrapper[handler]
                # 如果该事件类型没有处理器了，删除键
                if not self._handlers[event_type]:
                    del self._handlers[event_type]
            return removed
        return False

    def unsubscribe_global(self, handler: Callable) -> bool:
        """取消全局订阅"""
        wrapper = self._original_to_wrapper.get(handler)
        if wrapper is None:
            return False

        original_count = len(self._global_handlers)
        self._global_handlers = [
            h for h in self._global_handlers
            if h != wrapper
        ]
        removed = len(self._global_handlers) < original_count
        if removed:
            del self._original_to_wrapper[handler]
        return removed

    async def emit(self, event: TaskEvent) -> None:
        """发送事件"""
        if not self._running:
            # 同步处理
            await self._process_event(event)
        else:
            # 异步队列处理
            await self._queue.put(event)

    async def _process_event(self, event: TaskEvent) -> None:
        """处理事件"""
        # 调用全局处理器
        for handler in self._global_handlers[:]:  # 使用副本避免迭代时修改
            try:
                await handler(event)
            except Exception:
                pass

        # 调用特定事件处理器
        handlers = self._handlers.get(event.event_type, [])[:]  # 使用副本
        for handler in handlers:
            try:
                await handler(event)
            except Exception:
                pass

    async def start(self) -> None:
        """启动事件总线"""
        if self._running:
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop())

    async def stop(self) -> None:
        """停止事件总线"""
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

    async def _worker_loop(self) -> None:
        """工作循环"""
        while self._running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self._process_event(event)
                self._queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception:
                continue

    def clear(self) -> None:
        """清空所有处理器"""
        self._handlers.clear()
        self._global_handlers.clear()
        self._original_to_wrapper.clear()

    def get_handler_count(self, event_type: Optional[str] = None) -> int:
        """获取处理器数量（用于测试）"""
        if event_type:
            return len(self._handlers.get(event_type, []))
        total = len(self._global_handlers)
        for handlers in self._handlers.values():
            total += len(handlers)
        return total
