"""
@FileName: debug_scheduler.py
@Description:
@Author: HiPeng
@Time: 2026/4/9 22:11
"""

import time

from neotask import TaskScheduler, SchedulerConfig


async def heartbeat_task(data: dict) -> dict:
    print(f"[{data['name']}] 心跳 - {time.strftime('%H:%M:%S')}")
    return {"status": "ok"}


def main():
    scheduler = TaskScheduler(
        executor=heartbeat_task,
        config=SchedulerConfig.memory()
    )

    scheduler.start()
    print("调度器已启动")

    task_id = scheduler.submit_interval(
        {"name": "测试"},
        interval_seconds=1,
        run_immediately=True
    )
    print(f"任务ID: {task_id}")

    # 观察 10 秒
    for i in range(10):
        time.sleep(1)
        tasks = scheduler.get_periodic_tasks()
        if tasks:
            t = tasks[0]
            print(f"第{i + 1}秒: run_count={t['run_count']}, next_run={t['next_run']}")

    scheduler.cancel_periodic(task_id)
    scheduler.shutdown()


if __name__ == "__main__":
    main()
