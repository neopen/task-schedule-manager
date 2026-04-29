"""
@FileName: conftest.py
@Description: pytest 配置 - 导入所有 fixtures
@Author: HiPeng
@Time: 2026/4/29
"""

# 导入所有 fixtures 模块
from tests.fixtures.event_loop import *
from tests.fixtures.redis import *
from tests.fixtures.storage import *
from tests.fixtures.queue import *
from tests.fixtures.task import *
from tests.fixtures.executor import *
from tests.fixtures.event import *
from tests.fixtures.monitor import *
from tests.fixtures.distributed import *
from tests.fixtures.helpers import *

# 可选：添加 pytest 配置钩子
def pytest_configure(config):
    """pytest 配置钩子"""
    # 注册自定义标记
    config.addinivalue_line("markers", "distributed: 分布式功能测试")
    config.addinivalue_line("markers", "lock: 分布式锁测试")
    config.addinivalue_line("markers", "heartbeat: 节点心跳测试")
    config.addinivalue_line("markers", "sharding: 任务分片测试")