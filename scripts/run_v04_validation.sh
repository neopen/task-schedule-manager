#!/bin/bash
# v0.4 分布式基础功能验证脚本

echo "=========================================="
echo "NeoTask v0.4 分布式基础功能验证"
echo "=========================================="

# 检查 Redis
echo ""
echo "🔍 检查 Redis 服务..."
if redis-cli ping > /dev/null 2>&1; then
    echo "   Redis 运行正常"
else
    echo "   Redis 未运行，请启动 Redis"
    exit 1
fi

# 1. 分布式锁测试
echo ""
echo "1. 分布式锁测试"
echo "----------------------------------------"
pytest tests/distributed/test_distributed_lock.py -v -m distributed --tb=short

# 2. 节点心跳测试
echo ""
echo "2. 节点心跳和故障检测测试"
echo "----------------------------------------"
pytest tests/distributed/test_heartbeat.py -v -m distributed --tb=short

# 3. 任务分片测试
echo ""
echo "3. 任务分片测试"
echo "----------------------------------------"
pytest tests/distributed/test_sharding.py -v -m distributed --tb=short

# 4. 汇总结果
echo ""
echo "=========================================="
echo "验证结果汇总"
echo "=========================================="

echo ""
echo "分布式锁: 待确认"
echo "节点心跳: 待确认"
echo "任务分片: 待确认"

echo ""
echo "v0.4 验证完成!"