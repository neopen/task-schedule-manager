"""
调试优先级队列问题 - 不依赖日志系统
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import time
import asyncio
from neotask.api.task_pool import TaskPool, TaskPoolConfig
from neotask.models.task import TaskPriority


def test_priority():
    """测试优先级"""
    executed = []

    async def executor(data):
        executed.append(data["name"])
        return {"result": "done"}

    print("=" * 50)
    print("创建 TaskPool...")

    config = TaskPoolConfig(storage_type="memory", worker_concurrency=1)
    pool = TaskPool(executor, config)

    print("启动 TaskPool...")
    pool.start()
    time.sleep(0.5)

    print("提交任务...")
    print("  提交 LOW (priority=3)")
    pool.submit({"name": "LOW"}, priority=TaskPriority.LOW)
    time.sleep(0.05)

    print("  提交 NORMAL (priority=2)")
    pool.submit({"name": "NORMAL"}, priority=TaskPriority.NORMAL)
    time.sleep(0.05)

    print("  提交 HIGH (priority=1)")
    pool.submit({"name": "HIGH"}, priority=TaskPriority.HIGH)
    time.sleep(0.05)

    print("  提交 CRITICAL (priority=0)")
    pool.submit({"name": "CRITICAL"}, priority=TaskPriority.CRITICAL)

    print("等待任务执行...")
    time.sleep(2.0)

    print("关闭 TaskPool...")
    pool.shutdown()

    print(f"\n执行顺序: {executed}")
    print("=" * 50)

    if executed == ['CRITICAL', 'HIGH', 'NORMAL', 'LOW']:
        print("✅ 优先级队列工作正常!")
    else:
        print("❌ 优先级队列未生效 (FIFO 顺序)")

    return executed


if __name__ == "__main__":
    test_priority()