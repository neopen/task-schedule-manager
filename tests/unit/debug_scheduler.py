"""
@FileName: debug_scheduler.py
@Description:
@Author: HiPeng
@Time: 2026/4/9 22:11
"""
import time
from neotask import TaskScheduler, SchedulerConfig


async def heartbeat_task(data: dict) -> dict:
    print(f"[{data['name']}] 心跳信号 - {time.strftime('%H:%M:%S')}")
    return {"status": "ok"}


def main():
    print("1. 创建调度器...")
    scheduler = TaskScheduler(
        executor=heartbeat_task,
        config=SchedulerConfig.memory()
    )

    print("2. 启动调度器...")
    scheduler.start()

    # 等待一下让调度器完全启动
    time.sleep(0.5)

    print("3. 提交周期任务...")
    task_id = scheduler.submit_interval(
        {"name": "心跳测试"},
        interval_seconds=1,
        run_immediately=True
    )
    print(f"   任务ID: {task_id}")

    print("4. 等待 5 秒观察...")
    for i in range(5):
        time.sleep(1)
        print(f"   第 {i + 1} 秒: 周期任务列表 = {scheduler.get_periodic_tasks()}")

    print("5. 取消任务...")
    scheduler.cancel_periodic(task_id)

    print("6. 关闭调度器...")
    scheduler.shutdown()
    print("完成!")


if __name__ == "__main__":
    main()