"""
@FileName: 09_executor_factory.py
@Description: 
@Author: HiPeng
@Time: 2026/4/2 21:47
"""
# examples/09_executor_factory.py

import asyncio
from neotask import TaskScheduler, TaskExecutor
from neotask.executor import ExecutorFactory, ExecutorType


# 1. Async function executor
async def async_task(data):
    """Async task example."""
    print(f"Async processing: {data}")
    await asyncio.sleep(0.3)
    return {"result": data["value"] * 2}


# 2. Sync function executor
def sync_task(data):
    """Sync task example."""
    print(f"Sync processing: {data}")
    import time
    time.sleep(0.3)
    return {"result": data["value"] * 2}


# 3. Class-based executor (正确实现 async execute 方法)
class MyWorker(TaskExecutor):
    """Class with async execute method."""

    async def execute(self, data):
        print(f"Class executor: {data}")
        await asyncio.sleep(0.3)
        return {"result": data["value"] * 3}


async def main():
    print("=" * 60)
    print("Executor Factory Demo")
    print("=" * 60)

    # 方法1: 异步函数（自动检测）
    print("\n1. Async function (auto-detect):")
    scheduler = TaskScheduler(executor=async_task)
    try:
        task_id = scheduler.submit({"value": 10})
        result = scheduler.wait_for_result(task_id)
        print(f"   Result: {result}")
    finally:
        scheduler.shutdown()

    # 方法2: 同步函数（需要指定类型或包装）
    print("\n2. Sync function (with ThreadExecutor):")
    scheduler = TaskScheduler(
        executor=ExecutorFactory.create_thread(sync_task, max_workers=4)
    )
    try:
        task_id = scheduler.submit({"value": 20})
        result = scheduler.wait_for_result(task_id)
        print(f"   Result: {result}")
    finally:
        scheduler.shutdown()

    # 方法3: 类实例（自动检测为 CLASS）
    print("\n3. Class instance (auto-detect):")
    worker = MyWorker()
    scheduler = TaskScheduler(executor=worker)  # 直接传入实例，自动检测
    try:
        task_id = scheduler.submit({"value": 5})
        result = scheduler.wait_for_result(task_id)
        print(f"   Result: {result}")
    finally:
        scheduler.shutdown()

    # 方法4: 使用 ExecutorType 显式指定
    print("\n4. Explicit executor_type='class':")
    worker = MyWorker()
    scheduler = TaskScheduler(
        executor=ExecutorFactory.create(worker, executor_type=ExecutorType.CLASS)
    )
    try:
        task_id = scheduler.submit({"value": 7})
        result = scheduler.wait_for_result(task_id)
        print(f"   Result: {result}")
    finally:
        scheduler.shutdown()

    print("\n" + "=" * 60)
    print("All executor tests passed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
