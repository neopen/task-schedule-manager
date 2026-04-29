"""
独立基准测试运行脚本 - 避免pytest清理问题和编码问题
"""

import asyncio
import sys

sys.path.insert(0, 'src')

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from test_performance import (
    TestBasicThroughput,
    TestQueuePerformance,
    TestConcurrencyPerformance,
    TestTaskTypePerformance,
    TestStressTest
)


# 存储测试结果用于文档生成
benchmark_results = []


async def run_all_benchmarks():
    print("=" * 70)
    print("NeoTask 性能基准测试")
    print("=" * 70)
    
    # 基础吞吐量测试
    print("\n--- 1. 基础吞吐量测试 ---")
    basic_test = TestBasicThroughput()
    await basic_test.test_task_pool_throughput()
    await basic_test.test_task_pool_latency_distribution()
    
    # 队列性能测试
    print("\n--- 2. 队列性能测试 ---")
    queue_test = TestQueuePerformance()
    await queue_test.test_priority_queue_throughput()
    await queue_test.test_prefetcher_performance()
    await queue_test.test_batch_pop_performance()
    
    # 并发性能测试
    print("\n--- 3. 并发性能测试 ---")
    concurrency_test = TestConcurrencyPerformance()
    await concurrency_test.test_worker_scalability_light_task()
    await concurrency_test.test_worker_scalability_heavy_task()
    await concurrency_test.test_high_concurrency_submit()
    
    # 任务类型性能测试
    print("\n--- 4. 任务类型性能测试 ---")
    task_type_test = TestTaskTypePerformance()
    await task_type_test.test_light_task_performance()
    await task_type_test.test_io_task_performance()
    
    # 压力测试
    print("\n--- 5. 压力测试 ---")
    stress_test = TestStressTest()
    await stress_test.test_sustained_load()
    await stress_test.test_burst_load()
    
    print("\n" + "=" * 70)
    print("所有基准测试完成")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(run_all_benchmarks())