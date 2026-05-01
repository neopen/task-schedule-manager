#!/bin/bash
# 性能基准测试运行脚本

echo "=========================================="
echo "NeoTask 性能基准测试"
echo "=========================================="

# 检查 Redis 是否运行
if ! redis-cli ping > /dev/null 2>&1; then
    echo " Redis 未运行，启动 Redis..."
    redis-server --daemonize yes
    sleep 2
fi

# 运行基础性能测试
echo ""
echo "运行基础性能测试..."
pytest tests/benchmark/test_performance.py -m benchmark -v --benchmark-only

# 运行压力测试
echo ""
echo "运行压力测试..."
pytest tests/benchmark/test_performance.py::TestStressTest -v

# 生成报告
echo ""
echo "生成性能报告..."
pytest tests/benchmark/ --benchmark-json=benchmark_results.json

echo ""
echo "性能测试完成"