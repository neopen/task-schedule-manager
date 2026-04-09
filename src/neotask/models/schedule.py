"""
@FileName: schedule.py
@Description: 
@Author: HiPeng
@Time: 2026/4/9 15:25
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any, Dict


@dataclass
class PeriodicTask:
    """周期任务定义"""
    task_id: str
    data: Dict[str, Any]
    interval_seconds: float
    priority: int
    cron_expr: Optional[str] = None
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    run_count: int = 0
    is_paused: bool = False
    created_at: datetime = field(default_factory=datetime.now)
