"""
@FileName: factory.py
@Description: Executor factory for creating executor instances.
@Author: HiPeng
@Time: 2026/4/2 21:43
"""

import inspect
from typing import Any, Callable, Dict

from neotask.executors.async_executor import AsyncExecutor
from neotask.executors.base import TaskExecutor, CallbackExecutor
from neotask.executors.class_executor import ClassExecutor
from neotask.executors.process_executor import ProcessExecutor
from neotask.executors.thread_executor import ThreadExecutor


class ExecutorType:
    """Executor type constants."""

    ASYNC = "async"
    THREAD = "thread"
    PROCESS = "process"
    CLASS = "class"
    CALLBACK = "callback"
    AUTO = "auto"


class ExecutorFactory:
    """Factory for creating task executors."""

    @staticmethod
    def create(executor: Any,
               executor_type: str = ExecutorType.AUTO,
               **kwargs) -> TaskExecutor:
        """Create executor from various input types.

        Args:
            executor: Function, class instance, or TaskExecutor
            executor_type: Type of executor to create
            **kwargs: Additional arguments for specific executors

        Returns:
            TaskExecutor instance
        """
        # 如果已经是 TaskExecutor，直接返回
        if isinstance(executor, TaskExecutor):
            return executor

        # 自动检测类型
        if executor_type == ExecutorType.AUTO:
            executor_type = ExecutorFactory._detect_type(executor)

        # 根据类型创建
        if executor_type == ExecutorType.ASYNC:
            return ExecutorFactory._create_async(executor, **kwargs)

        elif executor_type == ExecutorType.THREAD:
            return ExecutorFactory._create_thread(executor, **kwargs)

        elif executor_type == ExecutorType.PROCESS:
            return ExecutorFactory._create_process(executor, **kwargs)

        elif executor_type == ExecutorType.CLASS:
            return ExecutorFactory._create_class(executor, **kwargs)

        elif executor_type == ExecutorType.CALLBACK:
            return ExecutorFactory._create_callback(executor, **kwargs)

        else:
            raise ValueError(f"Unknown executor type: {executor_type}")

    @staticmethod
    def _detect_type(executor: Any) -> str:
        """Auto-detect executor type.

        检测优先级（从高到低）:
        1. 类实例且有 async execute 方法 -> CLASS
        2. 异步函数 -> ASYNC
        3. 同步可调用对象 -> THREAD
        """
        # 检查是否是类（而不是实例）
        if inspect.isclass(executor):
            raise TypeError(
                f"Expected an instance, got class {executor.__name__}. "
                f"Please instantiate the class first: {executor.__name__}()"
            )

        # 优先级1: 检查是否是类实例且有 execute 方法
        # 注意：hasattr 对实例有效
        if hasattr(executor, 'execute'):
            execute_method = getattr(executor, 'execute')
            if callable(execute_method):
                # 检查 execute 是否是 async 方法
                if inspect.iscoroutinefunction(execute_method):
                    return ExecutorType.CLASS
                else:
                    # execute 存在但不是 async 函数
                    raise TypeError(
                        f"{type(executor).__name__}.execute must be an async function. "
                        f"Define it with 'async def execute(self, data)'"
                    )

        # 优先级2: 检查是否是异步函数
        if inspect.iscoroutinefunction(executor):
            return ExecutorType.ASYNC

        # 优先级3: 检查是否是同步可调用对象
        if callable(executor):
            return ExecutorType.THREAD

        raise ValueError(f"Cannot auto-detect executor type for {type(executor)}")

    @staticmethod
    def _create_async(func: Callable, **kwargs) -> AsyncExecutor:
        """Create async executor."""
        if not inspect.iscoroutinefunction(func):
            raise TypeError(
                f"Expected async function, got {type(func).__name__}. "
                f"Make sure your function is defined with 'async def'"
            )
        return AsyncExecutor(func)

    @staticmethod
    def _create_thread(func: Callable, max_workers: int = 10, **kwargs) -> ThreadExecutor:
        """Create thread executor for sync functions."""
        if not callable(func):
            raise TypeError(f"Expected callable, got {type(func).__name__}")
        return ThreadExecutor(func, max_workers=max_workers)

    @staticmethod
    def _create_process(func: Callable, max_workers: int = None, **kwargs) -> ProcessExecutor:
        """Create process executor for CPU-bound tasks."""
        if not callable(func):
            raise TypeError(f"Expected callable, got {type(func).__name__}")
        return ProcessExecutor(func, max_workers=max_workers)

    @staticmethod
    def _create_class(instance: Any, **kwargs) -> ClassExecutor:
        """Create executor from class instance."""
        return ClassExecutor(instance)

    @staticmethod
    def _create_callback(callback: Callable, **kwargs) -> CallbackExecutor:
        """Create executor from callback function."""
        if not callable(callback):
            raise TypeError(f"Expected callable, got {type(callback).__name__}")
        return CallbackExecutor(callback)

    # ========== 公开的便捷方法 ==========

    @staticmethod
    def create_async(func: Callable, **kwargs) -> AsyncExecutor:
        """Create async executor (public convenience method)."""
        return ExecutorFactory._create_async(func, **kwargs)

    @staticmethod
    def create_thread(func: Callable, max_workers: int = 10, **kwargs) -> ThreadExecutor:
        """Create thread executor (public convenience method)."""
        return ExecutorFactory._create_thread(func, max_workers=max_workers, **kwargs)

    @staticmethod
    def create_process(func: Callable, max_workers: int = None, **kwargs) -> ProcessExecutor:
        """Create process executor (public convenience method)."""
        return ExecutorFactory._create_process(func, max_workers=max_workers, **kwargs)

    @staticmethod
    def create_class(instance: Any, **kwargs) -> ClassExecutor:
        """Create class executor (public convenience method)."""
        return ExecutorFactory._create_class(instance, **kwargs)

    @staticmethod
    def create_callback(callback: Callable, **kwargs) -> CallbackExecutor:
        """Create callback executor (public convenience method)."""
        return ExecutorFactory._create_callback(callback, **kwargs)

    @staticmethod
    def create_from_config(config: Dict[str, Any]) -> TaskExecutor:
        """Create executor from configuration dictionary."""
        executor_type = config.get("type", ExecutorType.AUTO)
        func = config.get("func")

        if func is None:
            raise ValueError("Config must contain 'func' key")

        kwargs = {k: v for k, v in config.items() if k not in ("type", "func")}

        return ExecutorFactory.create(func, executor_type, **kwargs)
