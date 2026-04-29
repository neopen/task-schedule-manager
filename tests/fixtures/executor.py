"""
@FileName: executor.py
@Description: 执行器相关 fixtures
@Author: HiPeng
@Time: 2026/4/29
"""

from typing import Dict, Any
import pytest


@pytest.fixture
def mock_executor():
    """模拟任务执行器"""

    async def executor(data: Dict[str, Any]) -> Dict[str, Any]:
        return {"result": "success", "data": data}

    return executor


def create_test_executor():
    """创建测试执行器"""

    async def executor(data: Dict[str, Any]) -> Dict[str, Any]:
        return {"result": "processed", "data": data}

    return executor