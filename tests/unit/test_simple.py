"""
@FileName: test_simple.py
@Description: 
@Author: HiPeng
@Time: 2026/4/9 22:25
"""
import time
from neotask import TaskScheduler, SchedulerConfig

async def test_task(data):
    print(f"执行: {data}")
    return {"ok": True}

def test_tt():
    scheduler = TaskScheduler(executor=test_task, config=SchedulerConfig.memory())
    print("1. 创建完成")
    scheduler.start()
    print("2. start() 完成")
    time.sleep(1)
    task_id = scheduler.submit_interval({"name": "test"}, interval_seconds=1, run_immediately=True)
    print(f"3. 提交任务: {task_id}")
    time.sleep(5)
    print("4. 完成")
    scheduler.shutdown()