---
name: neotask-scheduler
description: 轻量级 Python 异步任务队列管理器，支持即时/延时/周期任务调度
---

# NeoTask 任务调度

## 使用场景
当需要管理和调度异步任务时使用，例如：

- 后台数据处理任务
- 定时报表生成
- 延时通知推送
- 批量文件处理
- AI 任务异步执行

## 即时任务
- 提交后立即执行
- 支持任务优先级设置（1-5级）
- 可配置超时时间

**输入示例**：
```json
{
  "action": "submit",
  "name": "data_process",
  "data": {"file_path": "/data/input.csv"},
  "priority": 2
}
```

## 延时任务
- 指定延迟时间后执行
- 最大延迟支持 30 天
- 支持取消未执行的任务

**输入示例**：
```json
{
  "action": "schedule_delayed",
  "name": "reminder",
  "data": {"user_id": "123"},
  "delay": 300
}
```

## 周期任务
- 基于 cron 表达式定时执行
- 支持标准 5 字段 cron 格式
- 可动态启停

**输入示例**：
```json
{
  "action": "schedule_periodic",
  "name": "daily_report",
  "data": {"type": "daily"},
  "cron": "0 9 * * *"
}
```

## 存储后端
- **Memory**：内存存储，适合开发和测试
- **SQLite**：轻量级持久化，适合单机部署
- **Redis**：分布式支持，适合高可用场景

## 使用限制
- 单任务最大数据量：10MB
- 最大并发任务数：1000（内存模式）
- 超时时间范围：1-3600 秒
- 优先级范围：1-5（1 最高）

## 错误处理
| 错误码 | 说明 |
|--------|------|
| TASK_ALREADY_EXISTS | 任务 ID 已存在 |
| INVALID_CRON | cron 表达式无效 |
| TASK_TIMEOUT | 任务执行超时 |
| STORAGE_ERROR | 存储操作失败 |

## 安全规范
- 任务数据不应包含敏感信息
- 避免直接执行用户输入代码
- 建议对敏感字段进行加密

## 最佳实践
- 任务命名：`{模块}_{功能}_{时间戳}`
- 关键任务使用高优先级（1-2）
- 设置合理的超时时间
- 使用持久化存储确保任务不丢失