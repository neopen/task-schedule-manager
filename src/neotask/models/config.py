"""
@FileName: config.py
@Description: 配置
@Author: HiPeng
@Time: 2026/4/1 18:23
"""

from dataclasses import dataclass, field
from typing import Optional, Literal


@dataclass
class StorageConfig:
    """Storage configuration."""

    type: Literal["memory", "redis", "sqlite"] = "memory"
    redis_url: Optional[str] = None
    sqlite_path: Optional[str] = None

    @classmethod
    def memory(cls) -> "StorageConfig":
        """Create memory storage config."""
        return cls(type="memory")

    @classmethod
    def redis(cls, url: str) -> "StorageConfig":
        """Create Redis storage config."""
        return cls(type="redis", redis_url=url)

    @classmethod
    def sqlite(cls, path: str = "tasks.db") -> "StorageConfig":
        """Create SQLite storage config."""
        return cls(type="sqlite", sqlite_path=path)


@dataclass
class LockConfig:
    """Distributed lock configuration."""

    type: Literal["memory", "redis"] = "memory"
    redis_url: Optional[str] = None
    lock_timeout: int = 30
    heartbeat_interval: int = 10
    watch_dog_enabled: bool = True

    @classmethod
    def memory(cls) -> "LockConfig":
        """Create memory lock config."""
        return cls(type="memory")

    @classmethod
    def redis(cls, url: str, timeout: int = 30) -> "LockConfig":
        """Create Redis lock config."""
        return cls(type="redis", redis_url=url, lock_timeout=timeout)


@dataclass
class WorkerConfig:
    """Worker pool configuration."""

    max_concurrent: int = 10
    prefetch_size: int = 20
    min_threshold: int = 5
    task_timeout: int = 300
    retry_max: int = 3

    @classmethod
    def default(cls) -> "WorkerConfig":
        """Create default worker config."""
        return cls()

    @classmethod
    def high_performance(cls) -> "WorkerConfig":
        """Create high performance worker config."""
        return cls(max_concurrent=50, prefetch_size=100)


@dataclass
class QueueConfig:
    """Queue configuration."""

    max_size: int = 1000
    priority_levels: int = 4

    @classmethod
    def default(cls) -> "QueueConfig":
        """Create default queue config."""
        return cls()


@dataclass
class WebUIConfig:
    """Web UI configuration."""

    enable: bool = False
    host: str = "127.0.0.1"
    port: int = 8080
    auto_open: bool = False
    enable_websocket: bool = True

    @classmethod
    def disabled(cls) -> "WebUIConfig":
        """Create disabled web UI config."""
        return cls(enable=False)

    @classmethod
    def enabled(cls, port: int = 8080, auto_open: bool = False) -> "WebUIConfig":
        """Create enabled web UI config."""
        return cls(enable=True, port=port, auto_open=auto_open)


@dataclass
class ExecutorConfig:
    """Executor configuration."""

    type: str = "async"  # async, thread, process, class, auto
    max_workers: Optional[int] = None

    @classmethod
    def async_executor(cls) -> "ExecutorConfig":
        """Create async executor config."""
        return cls(type="async")

    @classmethod
    def thread_executor(cls, max_workers: int = 10) -> "ExecutorConfig":
        """Create thread executor config."""
        return cls(type="thread", max_workers=max_workers)

    @classmethod
    def process_executor(cls, max_workers: int = None) -> "ExecutorConfig":
        """Create process executor config."""
        return cls(type="process", max_workers=max_workers)


@dataclass
class TaskPoolConfig:
    """TaskPool配置 - 专注于即时任务"""
    # 存储配置
    storage_type: str = "memory"
    sqlite_path: str = "neotask.db"
    redis_url: Optional[str] = None

    # 执行器配置
    executor_type: str = "async"
    max_workers: int = 10

    # Worker配置
    worker_concurrency: int = 10
    prefetch_size: int = 20
    task_timeout: Optional[float] = None

    # 队列配置
    queue_max_size: int = 10000
    priority_levels: int = 4

    # 锁配置
    lock_type: str = "memory"
    lock_timeout: int = 30

    # 监控配置
    enable_metrics: bool = True
    enable_health_check: bool = True
    enable_reporter: bool = False

    # 重试配置
    max_retries: int = 3
    retry_delay: float = 1.0

    # 节点标识
    node_id: str = ""

    def __post_init__(self):
        if not self.node_id:
            import socket
            self.node_id = socket.gethostname()

    @classmethod
    def memory(cls, node_id: Optional[str] = None) -> "TaskPoolConfig":
        """Create memory-only config."""
        config = cls(storage_type="memory")
        if node_id:
            config.node_id = node_id
        return config

    @classmethod
    def sqlite(cls, path: str = "neotask.db", node_id: Optional[str] = None) -> "TaskPoolConfig":
        """Create SQLite config."""
        config = cls(storage_type="sqlite", sqlite_path=path)
        if node_id:
            config.node_id = node_id
        return config

    @classmethod
    def redis(cls, url: str, node_id: Optional[str] = None) -> "TaskPoolConfig":
        """Create Redis config."""
        config = cls(storage_type="redis", redis_url=url)
        if node_id:
            config.node_id = node_id
        return config


@dataclass
class SchedulerConfig:
    """调度器配置 - 专注于定时任务"""
    storage_type: str = "memory"
    sqlite_path: str = "neotask.db"
    redis_url: Optional[str] = None

    worker_concurrency: int = 10
    max_retries: int = 3
    retry_delay: float = 1.0
    enable_persistence: bool = False
    scan_interval: float = 0.05

    @classmethod
    def memory(cls, **kwargs) -> "SchedulerConfig":
        """Create memory-only scheduler config."""
        return cls(storage_type="memory", **kwargs)

    @classmethod
    def sqlite(cls, path: str = "neotask.db", **kwargs) -> "SchedulerConfig":
        """Create SQLite scheduler config."""
        return cls(storage_type="sqlite", sqlite_path=path, **kwargs)

    @classmethod
    def redis(cls, url: str, **kwargs) -> "SchedulerConfig":
        """Create Redis scheduler config."""
        return cls(storage_type="redis", redis_url=url, **kwargs)


@dataclass
class TaskConfig:
    """统一任务配置（兼容旧版 API）"""
    node_id: str = ""
    storage: StorageConfig = field(default_factory=StorageConfig.memory)
    lock: LockConfig = field(default_factory=LockConfig.memory)
    worker: WorkerConfig = field(default_factory=WorkerConfig.default)
    queue: QueueConfig = field(default_factory=QueueConfig.default)
    executor: ExecutorConfig = field(default_factory=ExecutorConfig.async_executor)
    webui: WebUIConfig = field(default_factory=WebUIConfig.disabled)

    def __post_init__(self):
        """Generate node ID if not provided."""
        if not self.node_id:
            import socket
            self.node_id = socket.gethostname()

    @classmethod
    def memory(cls, node_id: Optional[str] = None) -> "TaskConfig":
        """Create memory-only config (single node)."""
        config = cls()
        if node_id:
            config.node_id = node_id
        return config

    @classmethod
    def redis(cls, redis_url: str, node_id: Optional[str] = None) -> "TaskConfig":
        """Create Redis-based config (distributed)."""
        config = cls(
            storage=StorageConfig.redis(redis_url),
            lock=LockConfig.redis(redis_url),
        )
        if node_id:
            config.node_id = node_id
        return config

    @classmethod
    def sqlite(cls, path: str = "tasks.db", node_id: Optional[str] = None) -> "TaskConfig":
        """Create SQLite-based config."""
        config = cls(
            storage=StorageConfig.sqlite(path),
            lock=LockConfig.memory(),
        )
        if node_id:
            config.node_id = node_id
        return config

    @classmethod
    def with_webui(cls, port: int = 8080, auto_open: bool = False, **kwargs) -> "TaskConfig":
        """Create config with web UI enabled."""
        config = cls(**kwargs)
        config.webui = WebUIConfig.enabled(port, auto_open)
        return config