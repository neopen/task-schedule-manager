"""
简化版基准测试脚本 - 快速获取关键性能指标
"""

import asyncio
import sys
import time
import statistics

sys.path.insert(0, 'src')

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from neotask.api.task_pool import TaskPool, TaskPoolConfig


async def noop_executor(data):
    return {"result": "done"}


async def light_executor(data):
    _ = sum(range(100))
    return {"result": "done"}


async def run_throughput_test():
    print("1. TaskPool 基础吞吐量测试")
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
    result = {
        "name": "TaskPool 吞吐量",
        "throughput": round(throughput, 2),
        "total": stats['total'],
        "success_rate": round(stats['success_rate'], 4)
    }
    print("   结果: {} ops/sec, 成功率: {:.2%}".format(result['throughput'], result['success_rate']))
    return result


async def run_latency_test():
    print("2. TaskPool 延迟分布测试")
    pool = TaskPool(executor=noop_executor)
    pool.start()
    
    latencies = []
    total = 200
    
    for i in range(total):
        start = time.time()
        task_id = await pool.submit_async({"index": i})
        await pool.wait_for_result_async(task_id)
        latencies.append(time.time() - start)
    
    pool.shutdown()
    
    sorted_latencies = sorted(latencies)
    result = {
        "name": "TaskPool 延迟分布",
        "avg_ms": round(statistics.mean(latencies)*1000, 2),
        "p50_ms": round(statistics.median(latencies)*1000, 2),
        "p95_ms": round(sorted_latencies[int(len(sorted_latencies)*0.95)]*1000, 2),
        "p99_ms": round(sorted_latencies[int(len(sorted_latencies)*0.99)]*1000, 2)
    }
    print("   平均: {}ms, P50: {}ms, P95: {}ms, P99: {}ms".format(
        result['avg_ms'], result['p50_ms'], result['p95_ms'], result['p99_ms']))
    return result


async def run_concurrency_test():
    print("3. Worker 扩展性测试")
    configs = [1, 2, 4, 8]
    results = []
    
    for concurrency in configs:
        config = TaskPoolConfig(storage_type="memory", worker_concurrency=concurrency)
        pool = TaskPool(executor=light_executor, config=config)
        pool.start()
        
        total = 200
        start = time.time()
        
        tasks = [pool.submit_async({"index": i}) for i in range(total)]
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.2)
        
        duration = time.time() - start
        throughput = total / duration
        
        results.append({
            "workers": concurrency,
            "throughput": round(throughput, 2)
        })
        
        pool.shutdown()
        await asyncio.sleep(0.1)
    
    print("   结果:")
    for r in results:
        print("     {} workers: {} ops/sec".format(r['workers'], r['throughput']))
    
    if len(results) >= 2:
        improvement = results[2]["throughput"] / results[0]["throughput"]
        print("   扩展效率: {:.2f}x (4 workers vs 1 worker)".format(improvement))
    
    return results


async def run_queue_test():
    print("4. 优先级队列性能测试")
    from neotask.queue.priority_queue import PriorityQueue
    
    queue = PriorityQueue()
    total = 5000
    
    start = time.time()
    for i in range(total):
        await queue.push("task_{}".format(i), i % 5)
    push_time = time.time() - start
    
    start = time.time()
    for i in range(total):
        await queue.pop(1)
    pop_time = time.time() - start
    
    result = {
        "name": "优先级队列",
        "push_throughput": round(total / push_time, 2),
        "pop_throughput": round(total / pop_time, 2)
    }
    print("   入队: {} ops/sec, 出队: {} ops/sec".format(result['push_throughput'], result['pop_throughput']))
    return result


async def main():
    print("=" * 60)
    print("NeoTask 性能基准测试")
    print("=" * 60)
    
    results = []
    results.append(await run_throughput_test())
    results.append(await run_latency_test())
    results.append(await run_concurrency_test())
    results.append(await run_queue_test())
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)
    
    return results


if __name__ == "__main__":
    results = asyncio.run(main())
    
    with open("benchmark_results.txt", "w", encoding="utf-8") as f:
        f.write(str(results))
    print("\n结果已保存到 benchmark_results.txt")