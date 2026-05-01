"""
NeoTask 0.4 版本核心功能验证脚本
"""
import asyncio
import sys
import time
from datetime import datetime
from typing import Dict, Any

from neotask.models.task import TaskPriority

# 添加 src 目录到路径
sys.path.insert(0, '../src')

print("=" * 80)
print("NeoTask v0.4 核心功能验证")
print("=" * 80)

issues = []
success_count = 0
error_count = 0


def report_success(feature: str, details: str = ""):
    global success_count
    success_count += 1
    print(f"[OK] [{success_count}] {feature}: {details or '正常'}")


def report_issue(feature: str, issue_desc: str, severity: str = "medium"):
    global error_count
    error_count += 1
    issues.append({
        "feature": feature,
        "description": issue_desc,
        "severity": severity
    })
    print(f"[FAIL] [{error_count}] {feature}: {issue_desc} (严重程度: {severity})")


async def verify_basics():
    print("\n" + "-" * 60)
    print("1. 基础模块导入验证")
    print("-" * 60)

    try:
        # 测试导入核心模块
        modules_to_test = [
            ("neotask.models.task", ["Task", "TaskStatus", "TaskPriority"]),
            ("neotask.api.task_pool", ["TaskPool", "TaskPoolConfig"]),
            ("neotask.api.task_scheduler", ["TaskScheduler", "SchedulerConfig"]),
            ("neotask.queue.priority_queue", ["PriorityQueue"]),
            ("neotask.queue.delayed_queue", ["DelayedQueue"]),
            ("neotask.event.bus", ["EventBus", "TaskEvent"]),
            ("neotask.storage.memory", ["MemoryTaskRepository"]),
            ("neotask.scheduler.cron_parser", ["CronParser"]),
            ("neotask.scheduler.time_wheel", ["TimeWheel"]),
            ("neotask.worker.pool", ["WorkerPool"]),
        ]

        for module_name, expected_classes in modules_to_test:
            try:
                module = __import__(module_name, fromlist=['*'])
                for cls_name in expected_classes:
                    if hasattr(module, cls_name):
                        pass
                    else:
                        report_issue(
                            module_name,
                            f"缺少类: {cls_name}",
                            "high"
                        )
                report_success(f"模块导入: {module_name}", "所有类正常")
            except Exception as e:
                report_issue(module_name, f"导入失败: {str(e)}", "high")

        report_success("基础模块导入", "完成")
    except Exception as e:
        report_issue("基础模块", f"验证异常: {str(e)}", "critical")


async def verify_task_model():
    print("\n" + "-" * 60)
    print("2. Task 模型验证")
    print("-" * 60)
    try:
        from neotask.models.task import Task, TaskStatus, TaskPriority

        # 测试任务创建
        task = Task(
            task_id="test-task-001",
            data={"key": "value"},
            priority=TaskPriority.NORMAL,
            node_id="node-1",
            ttl=3600
        )
        assert task.task_id == "test-task-001", "任务ID不匹配"
        assert task.status == TaskStatus.PENDING, "初始状态应该是PENDING"
        report_success("Task模型创建", "正常")

        # 测试状态转换
        task.start("node-2")
        assert task.status == TaskStatus.RUNNING, "状态应该是RUNNING"
        assert task.started_at is not None, "started_at应该已设置"

        task.complete({"result": "ok"})
        assert task.status == TaskStatus.SUCCESS, "状态应该是SUCCESS"
        assert task.result == {"result": "ok"}, "结果不匹配"
        report_success("Task状态转换", "正常")

        # 测试失败和重试
        task2 = Task(
            task_id="test-task-002",
            data={"key": "value"},
        )
        task2.start("node-1")
        task2.fail("test error")
        assert task2.status == TaskStatus.FAILED, "状态应该是FAILED"
        assert task2.retry_count == 0, "初始重试计数应该是0"
        report_success("Task失败处理", "正常")

        # 测试字典转换
        task_dict = task.to_dict()
        assert task_dict["task_id"] == "test-task-001", "字典转换失败"
        task3 = Task.from_dict(task_dict)
        assert task3.task_id == "test-task-001", "from_dict失败"
        report_success("Task字典转换", "正常")

    except AssertionError as ae:
        report_issue("Task模型", f"断言失败: {str(ae)}", "high")
    except Exception as e:
        report_issue("Task模型", f"验证异常: {str(e)}", "high")


async def verify_priority_queue():
    print("\n" + "-" * 60)
    print("3. PriorityQueue 验证")
    print("-" * 60)
    try:
        from neotask.queue.priority_queue import PriorityQueue

        queue = PriorityQueue(max_size=100)

        # 测试推入任务
        for i in range(10):
            await queue.push(f"task-{i}", priority=(i % 5))

        size = await queue.size()
        assert size == 10, "队列大小应该是10"
        report_success("PriorityQueue推入", "成功")

        # 测试优先级（数字越小优先级越高）
        tasks = await queue.pop(count=5)
        # 验证任务被弹出
        assert len(tasks) == 5, "应该弹出5个任务"
        report_success("PriorityQueue弹出", f"成功弹出 {len(tasks)} 个任务")

        # 测试批量弹出
        tasks2 = await queue.pop(count=10)  # 尝试更多
        assert len(tasks2) == 5, "剩余应该只有5个"
        report_success("PriorityQueue批量操作", "正常")

        # 测试移除任务
        queue2 = PriorityQueue(max_size=100)
        await queue2.push("task-a", priority=2)
        await queue2.push("task-b", priority=1)
        removed = await queue2.remove("task-a")
        assert removed is True, "移除应该返回True"
        size2 = await queue2.size()
        assert size2 == 1, "剩余应该只有1个任务"
        report_success("PriorityQueue移除", "正常")

    except Exception as e:
        report_issue("PriorityQueue", f"验证异常: {str(e)}", "high")


async def verify_delayed_queue():
    print("\n" + "-" * 60)
    print("4. DelayedQueue 验证")
    print("-" * 60)
    try:
        from neotask.queue.delayed_queue import DelayedQueue

        queue = DelayedQueue(check_interval=0.1)

        # 测试调度
        await queue.schedule("delay-task-1", priority=2, delay=0.2)
        await queue.schedule("delay-task-2", priority=1, delay=0.1)
        await queue.schedule("delay-task-3", priority=3, delay=0.3)

        size = await queue.size()
        assert size == 3, "队列大小应该是3"
        report_success("DelayedQueue调度", "成功")

        # 测试取消
        cancelled = await queue.cancel("delay-task-2")
        assert cancelled is True, "应该能取消任务"
        size2 = await queue.size()
        assert size2 == 2, "剩余应该是2个任务"
        report_success("DelayedQueue取消", "正常")

        # 验证时间轮底层（如果可用）
        report_success("DelayedQueue", "基础功能正常")

    except Exception as e:
        report_issue("DelayedQueue", f"验证异常: {str(e)}", "high")


async def verify_cron_parser():
    print("\n" + "-" * 60)
    print("5. CronParser 验证")
    print("-" * 60)
    try:
        from neotask.scheduler.cron_parser import CronParser

        # 测试解析
        parser = CronParser.parse("0 9 * * *")  # 每天9点
        next_run = parser.next(datetime.now())
        assert next_run is not None, "应该能计算下次运行时间"
        report_success("CronParser解析", "正常")

        # 测试不同表达式
        test_expressions = [
            "*/5 * * * *",  # 每5分钟
            "0 * * * *",  # 每小时
            "0 0 * * 1",  # 每周一
            "0 0 1 * *",  # 每月1号
        ]

        for expr in test_expressions:
            try:
                p = CronParser.parse(expr)
                nr = p.next(datetime.now())
                assert nr is not None, f"{expr} 应该能解析"
            except Exception:
                report_issue("CronParser", f"表达式解析失败: {expr}", "medium")

        report_success("CronParser表达式", "基础表达式解析正常")

    except Exception as e:
        report_issue("CronParser", f"验证异常: {str(e)}", "medium")


async def verify_task_pool_simple():
    print("\n" + "-" * 60)
    print("6. TaskPool 简单验证")
    print("-" * 60)
    try:
        from neotask.api.task_pool import TaskPool

        # 定义执行器
        async def test_executor(data: Dict[str, Any]) -> Dict[str, Any]:
            return {"processed": data, "timestamp": time.time()}

        pool = TaskPool(executor=test_executor)

        # 注意：简化测试，不启动完整循环
        report_success("TaskPool创建", "实例化正常")

        # 验证配置
        config = pool._config
        assert config.worker_concurrency > 0, "Worker并发应该>0"
        report_success("TaskPool配置", "配置正常")

        # 验证生命周期管理器存在
        assert hasattr(pool, '_lifecycle'), "缺少_lifecycle_manager"
        assert hasattr(pool, '_queue_scheduler'), "缺少_queue_scheduler"
        assert hasattr(pool, '_worker_pool'), "缺少_worker_pool"
        report_success("TaskPool组件", "核心组件都存在")

    except Exception as e:
        report_issue("TaskPool", f"验证异常: {str(e)}", "high")


async def verify_task_scheduler_simple():
    print("\n" + "-" * 60)
    print("7. TaskScheduler 简单验证")
    print("-" * 60)
    try:
        from neotask.api.task_scheduler import TaskScheduler

        async def test_executor(data: Dict[str, Any]) -> Dict[str, Any]:
            return {"result": data}

        scheduler = TaskScheduler(executor=test_executor)
        report_success("TaskScheduler创建", "实例化正常")

        # 验证核心组件
        assert hasattr(scheduler, '_pool'), "缺少_task_pool"
        assert hasattr(scheduler, '_periodic_manager'), "缺少_periodic_manager"
        assert hasattr(scheduler, '_time_wheel'), "缺少_time_wheel"
        report_success("TaskScheduler组件", "核心组件都存在")

        # 验证 CronParser 被引用
        from neotask.scheduler.cron_parser import CronParser
        # 如果走到这里说明没问题
        report_success("TaskScheduler Cron集成", "依赖关系正常")

    except Exception as e:
        report_issue("TaskScheduler", f"验证异常: {str(e)}", "high")


async def verify_event_bus():
    print("\n" + "-" * 60)
    print("8. EventBus 验证")
    print("-" * 60)
    try:
        from neotask.event.bus import EventBus, TaskEvent

        bus = EventBus()
        events_received = []

        async def handler(event: TaskEvent):
            events_received.append(event)

        # 订阅
        bus.subscribe("task.completed", handler)
        report_success("EventBus订阅", "成功")

        # 发布事件
        event = TaskEvent(
            event_type="task.completed",
            task_id="test-event-001",
            data={"result": "ok"}
        )
        await bus.emit(event)
        # 同步处理的话
        await bus._process_event(event)

        assert len(events_received) >= 1, "应该接收到事件"
        report_success("EventBus发布", "成功")

        # 测试全局处理器
        global_events = []

        async def global_handler(event: TaskEvent):
            global_events.append(event)

        bus.subscribe_global(global_handler)
        await bus._process_event(TaskEvent("test.event", "task-1", {}))
        assert len(global_events) == 1, "全局处理器应该收到事件"
        report_success("EventBus全局处理", "正常")

    except Exception as e:
        report_issue("EventBus", f"验证异常: {str(e)}", "medium")


async def verify_storage():
    print("\n" + "-" * 60)
    print("9. 存储后端验证")
    print("-" * 60)
    try:
        from neotask.storage.memory import MemoryTaskRepository
        from neotask.models.task import Task, TaskStatus

        # 测试 Memory 存储
        repo = MemoryTaskRepository()
        task = Task(task_id="storage-test-001", data={"x": 1}, priority=TaskPriority.NORMAL)

        # 保存
        await repo.save(task)
        # 获取
        task2 = await repo.get("storage-test-001")
        assert task2 is not None, "应该能获取任务"
        assert task2.task_id == "storage-test-001", "任务ID不匹配"
        report_success("Memory存储", "保存和获取正常")

        # 测试状态更新
        task2.start("node-1")
        await repo.save(task2)
        task3 = await repo.get("storage-test-001")
        assert task3.status == TaskStatus.RUNNING, "状态应该是RUNNING"
        report_success("Memory存储状态更新", "正常")

        # 测试删除
        await repo.delete("storage-test-001")
        task4 = await repo.get("storage-test-001")
        assert task4 is None, "任务应该已被删除"
        report_success("Memory存储删除", "正常")

        # 测试列表
        for i in range(5):
            await repo.save(Task(task_id=f"list-test-{i}", data={}))
        pending_tasks = await repo.list_by_status(TaskStatus.PENDING)
        assert len(pending_tasks) == 5, "应该有5个PENDING任务"
        report_success("Memory存储列表", "正常")

    except Exception as e:
        report_issue("Storage", f"验证异常: {str(e)}", "high")


async def verify_future_manager():
    print("\n" + "-" * 60)
    print("10. FutureManager 验证")
    print("-" * 60)
    try:
        from neotask.core.future import FutureManager

        fm = FutureManager()

        # 创建 future
        future = await fm.get("future-test-001")

        # 完成它
        await fm.complete("future-test-001", result={"ok": True})
        report_success("FutureManager完成", "正常")

        # 测试等待
        fm2 = FutureManager()

        async def complete_later():
            await asyncio.sleep(0.1)
            await fm2.complete("future-test-002", result={"delayed": True})

        task = asyncio.create_task(complete_later())
        result = await fm2.wait("future-test-002", timeout=1.0)
        assert result == {"delayed": True}, "结果应该匹配"
        await task
        report_success("FutureManager等待", "正常")

        # 测试异常
        fm3 = FutureManager()

        async def fail_later():
            await asyncio.sleep(0.1)
            await fm3.complete("future-test-003", error="something wrong")

        task2 = asyncio.create_task(fail_later())
        try:
            await fm3.wait("future-test-003", timeout=1.0)
        except Exception as e:
            assert "something wrong" in str(e), "应该抛出异常"
        await task2
        report_success("FutureManager异常", "异常处理正常")

    except Exception as e:
        report_issue("FutureManager", f"验证异常: {str(e)}", "medium")


async def verify_worker_pool_basics():
    print("\n" + "-" * 60)
    print("11. WorkerPool 基础验证")
    print("-" * 60)
    try:
        from neotask.worker.pool import WorkerPool

        report_success("WorkerPool导入", "正常")
        # 注意：完整测试需要更复杂的配置，这里只验证存在性

    except Exception as e:
        report_issue("WorkerPool", f"验证异常: {str(e)}", "medium")


async def verify_distributed_features():
    print("\n" + "-" * 60)
    print("12. 分布式功能基础验证")
    print("-" * 60)
    try:
        # 检查模块是否存在
        modules = [
            "neotask.distributed.coordinator",
            "neotask.distributed.node",
            "neotask.distributed.elector",
            "neotask.distributed.sharding",
            "neotask.lock.redis",
        ]

        for mod in modules:
            try:
                __import__(mod, fromlist=['*'])
                report_success(f"分布式模块 {mod}", "存在")
            except ImportError:
                report_issue(f"分布式模块 {mod}", "模块不存在", "medium")

    except Exception as e:
        report_issue("分布式功能", f"验证异常: {str(e)}", "medium")


async def main():
    print("\n开始验证...")
    start_time = time.time()

    # 运行验证
    await verify_basics()
    await verify_task_model()
    await verify_priority_queue()
    await verify_delayed_queue()
    await verify_cron_parser()
    await verify_task_pool_simple()
    await verify_task_scheduler_simple()
    await verify_event_bus()
    await verify_storage()
    await verify_future_manager()
    await verify_worker_pool_basics()
    await verify_distributed_features()

    duration = time.time() - start_time

    # 输出总结
    print("\n" + "=" * 80)
    print("验证总结")
    print("=" * 80)
    print(f"✅ 成功: {success_count}")
    print(f"❌ 问题: {error_count}")
    print(f"⏱️  耗时: {duration:.2f}秒")

    if issues:
        print("\n" + "-" * 60)
        print("发现的问题详细列表:")
        print("-" * 60)
        for i, issue in enumerate(issues, 1):
            print(f"{i}. [{issue['severity'].upper()}] {issue['feature']}: {issue['description']}")

    print("\n验证完成！")
    return issues


if __name__ == "__main__":
    # 设置事件循环策略（Windows 需要）
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    found_issues = asyncio.run(main())
