"""
快速基准测试脚本 - 跳过可能卡住的测试
"""

import asyncio
import sys
import time
import statistics
import uuid
from typing import List, Dict

sys.path.insert(0, 'src')

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


results = []


def log_result(name, data):
    results.append({"name": name, "data": data})
    print("-" * 50)
    print("[%s]" % name)
    for key, value in data.items():
        print("  %s: %s" % (key, value))


async def noop_executor(data: Dict) -> Dict:
    return {"result": "done"}


async def light_executor(data: Dict) -> Dict:
    _ = sum(range(100))
    return {"result": "done"}


async def test_task_pool_throughput():
    """基础吞吐量测试"""
    from neotask.api.task_pool import TaskPool
    
    pool = TaskPool(executor=noop_executor)
    pool.start()
    
    total = 1000
    start = time.time()
    
    for i in range(total):
        await pool.submit_async({"index": i})
    
    await asyncio.sleep(0.5)
    duration = time.time() - start
    
    stats = pool.get_stats()
    pool.shutdown()
    
    throughput = total / duration
    log_result("TaskPool 基础吞吐量", {
        "吞吐量": f"{throughput:.2f} ops/sec",
        "总任务数": stats['total'],
        "成功率": f"{stats['success_rate']:.2%}"
    })


async def test_priority_queue():
    """优先级队列性能测试"""
    from neotask.queue.priority_queue import PriorityQueue
    
    queue = PriorityQueue()
    total = 5000
    
    start = time.time()
    for i in range(total):
        await queue.push(f"task_{i}", i % 5)
    push_duration = time.time() - start
    
    start = time.time()
    for i in range(total):
        await queue.pop(1)
    pop_duration = time.time() - start
    
    log_result("优先级队列性能", {
        "入队吞吐量": f"{total/push_duration:.2f} ops/sec",
        "出队吞吐量": f"{total/pop_duration:.2f} ops/sec"
    })


async def test_prefetcher():
    """预取器性能测试"""
    from neotask.queue.queue_scheduler import QueueScheduler
    from neotask.storage.memory import MemoryQueueRepository
    from neotask.worker.prefetcher import TaskPrefetcher, PrefetchConfig
    
    repo = MemoryQueueRepository()
    queue = QueueScheduler(repo)
    await queue.start()
    
    for i in range(2000):
        await queue.push(f"task_{i}", i % 5)
    
    start = time.time()
    for _ in range(500):
        await queue.pop(1)
    without_prefetch = time.time() - start
    
    prefetcher = TaskPrefetcher(queue, PrefetchConfig(prefetch_size=20))
    await prefetcher.start()
    
    start = time.time()
    for _ in range(500):
        await prefetcher.get(timeout=0.01)
    with_prefetch = time.time() - start
    
    await prefetcher.stop()
    await queue.stop()
    
    throughput_without = 500 / without_prefetch
    throughput_with = 500 / with_prefetch
    improvement = throughput_with / throughput_without if throughput_without > 0 else 1
    
    log_result("预取器性能对比", {
        "无预取": f"{throughput_without:.2f} ops/sec",
        "有预取": f"{throughput_with:.2f} ops/sec",
        "提升倍数": f"{improvement:.2f}x"
    })


async def test_batch_pop():
    """批量弹出性能测试"""
    from neotask.queue.priority_queue import PriorityQueue
    
    queue = PriorityQueue()
    
    for i in range(10000):
        await queue.push(f"task_{i}", i % 5)
    
    start = time.time()
    for i in range(1000):
        await queue.pop(1)
    single_time = time.time() - start
    
    for i in range(10000):
        await queue.push(f"task_{i}", i % 5)
    
    start = time.time()
    for i in range(100):
        await queue.pop(10)
    batch_time = time.time() - start
    
    log_result("批量弹出性能对比", {
        "单条弹出": f"{1000/single_time:.2f} ops/sec",
        "批量弹出": f"{1000/batch_time:.2f} ops/sec",
        "提升倍数": f"{(1000/batch_time)/(1000/single_time):.2f}x" if single_time > 0 else "N/A"
    })


async def test_worker_scalability():
    """Worker扩展性测试"""
    from neotask.api.task_pool import TaskPool, TaskPoolConfig
    
    configs = [1, 2, 4, 8]
    worker_results = []
    
    for concurrency in configs:
        config = TaskPoolConfig(storage_type="memory", worker_concurrency=concurrency)
        pool = TaskPool(executor=light_executor, config=config)
        pool.start()
        
        total = 200
        start = time.time()
        
        tasks = []
        for i in range(total):
            unique_id = "task_worker_%d_%s" % (concurrency, uuid.uuid4().hex[:8])
            tasks.append(pool.submit_async({"index": i}, task_id=unique_id))
        
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.2)
        
        duration = time.time() - start
        throughput = total / duration
        
        worker_results.append({
            "workers": concurrency,
            "throughput": "%.2f ops/sec" % throughput
        })
        
        pool.shutdown()
        await asyncio.sleep(0.2)
    
    if len(worker_results) >= 4:
        try:
            improvement = float(worker_results[2]["throughput"].split()[0]) / float(worker_results[0]["throughput"].split()[0])
            worker_results.append({"扩展效率": "%.2fx (4 workers vs 1 worker)" % improvement})
        except:
            pass
    
    log_result("Worker扩展性测试（轻量任务）", {"结果": worker_results})


async def test_high_concurrency_submit():
    """高并发提交测试"""
    from neotask.api.task_pool import TaskPool
    
    pool = TaskPool(executor=light_executor)
    pool.start()
    
    concurrency = 50
    total = 1000
    
    async def submit_batch(batch_id, start_idx, count):
        tasks = []
        for i in range(count):
            unique_id = "task_hc_%d_%d_%s" % (batch_id, i, uuid.uuid4().hex[:4])
            tasks.append(pool.submit_async({"batch": batch_id, "i": start_idx + i}, task_id=unique_id))
        return await asyncio.gather(*tasks)
    
    start = time.time()
    batch_size = total // concurrency
    batches = [submit_batch(i, i * batch_size, batch_size) for i in range(concurrency)]
    all_task_ids = await asyncio.gather(*batches)
    duration = time.time() - start
    
    await asyncio.sleep(1)
    stats = pool.get_stats()
    pool.shutdown()
    
    flat_ids = [tid for batch in all_task_ids for tid in batch]
    
    log_result("高并发提交测试", {
        "并发数": concurrency,
        "总任务": len(flat_ids),
        "耗时": "%.2fs" % duration,
        "吞吐量": "%.2f ops/sec" % (len(flat_ids)/duration),
        "成功率": "%.2f%%" % (stats['success_rate'] * 100)
    })


async def test_light_task_performance():
    """轻量任务性能"""
    from neotask.api.task_pool import TaskPool
    
    pool = TaskPool(executor=light_executor)
    pool.start()
    
    total = 500
    start = time.time()
    
    tasks = []
    for i in range(total):
        unique_id = "task_light_%s" % uuid.uuid4().hex[:8]
        tasks.append(pool.submit_async({"index": i}, task_id=unique_id))
    
    await asyncio.gather(*tasks)
    await asyncio.sleep(0.3)
    
    duration = time.time() - start
    throughput = total / duration
    
    pool.shutdown()
    
    log_result("轻量任务性能", {
        "吞吐量": "%.2f ops/sec" % throughput,
        "平均延迟": "%.2f ms" % ((duration/total)*1000)
    })


async def test_io_task_performance():
    """IO密集型任务性能"""
    from neotask.api.task_pool import TaskPool, TaskPoolConfig
    
    async def io_executor(data):
        await asyncio.sleep(data.get("sleep", 0.001))
        return {"result": "done"}
    
    pool = TaskPool(executor=io_executor, config=TaskPoolConfig(worker_concurrency=20))
    pool.start()
    
    total = 200
    start = time.time()
    
    tasks = []
    for i in range(total):
        unique_id = "task_io_%s" % uuid.uuid4().hex[:8]
        tasks.append(pool.submit_async({"sleep": 0.005}, task_id=unique_id))
    
    await asyncio.gather(*tasks)
    
    duration = time.time() - start
    throughput = total / duration
    
    pool.shutdown()
    
    log_result("IO密集型任务性能", {
        "吞吐量": "%.2f ops/sec" % throughput,
        "平均延迟": "%.2f ms" % ((duration/total)*1000)
    })


async def test_sustained_load():
    """持续负载测试"""
    from neotask.api.task_pool import TaskPool, TaskPoolConfig
    
    pool = TaskPool(
        executor=light_executor,
        config=TaskPoolConfig(worker_concurrency=20)
    )
    pool.start()
    
    duration = 5
    total_submitted = 0
    errors = 0
    
    start = time.time()
    while time.time() - start < duration:
        try:
            unique_id = "task_sust_%d_%s" % (total_submitted, uuid.uuid4().hex[:4])
            await pool.submit_async({"index": total_submitted}, task_id=unique_id)
            total_submitted += 1
            await asyncio.sleep(0.001)
        except Exception:
            errors += 1
    
    await asyncio.sleep(1)
    stats = pool.get_stats()
    pool.shutdown()
    
    throughput = total_submitted / duration
    
    log_result("持续负载测试", {
        "持续时间": "%ds" % duration,
        "总提交": total_submitted,
        "错误": errors,
        "吞吐量": "%.2f ops/sec" % throughput,
        "成功率": "%.2f%%" % (stats['success_rate'] * 100)
    })


async def test_burst_load():
    """突发负载测试"""
    from neotask.api.task_pool import TaskPool, TaskPoolConfig
    
    pool = TaskPool(
        executor=light_executor,
        config=TaskPoolConfig(worker_concurrency=50)
    )
    pool.start()
    
    burst_size = 500
    start = time.time()
    
    tasks = []
    for i in range(burst_size):
        unique_id = "task_burst_%d_%s" % (i, uuid.uuid4().hex[:4])
        tasks.append(pool.submit_async({"index": i}, task_id=unique_id))
    
    await asyncio.gather(*tasks)
    duration = time.time() - start
    
    await asyncio.sleep(0.5)
    stats = pool.get_stats()
    pool.shutdown()
    
    log_result("突发负载测试", {
        "突发任务数": burst_size,
        "总耗时": "%.2fs" % duration,
        "吞吐量": "%.2f ops/sec" % (burst_size/duration),
        "成功率": "%.2f%%" % (stats['success_rate'] * 100)
    })


async def main():
    print("=" * 60)
    print("NeoTask Benchmark Test")
    print("=" * 60)
    
    tests = [
        test_task_pool_throughput,
        test_priority_queue,
        test_prefetcher,
        test_batch_pop,
        test_worker_scalability,
        test_high_concurrency_submit,
        test_light_task_performance,
        test_io_task_performance,
        test_sustained_load,
        test_burst_load
    ]
    
    for i, test_func in enumerate(tests, 1):
        print("\n[%d/%d] Running: %s" % (i, len(tests), test_func.__name__))
        try:
            await test_func()
            print("  Test completed")
        except Exception as e:
            print("  Test failed: %s" % str(e))
    
    print("\n" + "=" * 60)
    print("All tests completed")
    print("=" * 60)
    
    # 保存结果
    with open("benchmark_results_fast.txt", "w", encoding="utf-8") as f:
        import json
        f.write(json.dumps(results, ensure_ascii=False, indent=2))
    
    print("\nResults saved to benchmark_results_fast.txt")
    return results


if __name__ == "__main__":
    asyncio.run(main())