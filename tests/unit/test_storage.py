"""
@FileName: test_storage.py
@Description: 
@Author: HiPeng
@Time: 2026/4/2 21:55
"""
from neotask.storage import StorageFactory
from neotask.models.config import StorageConfig
import pytest
from neotask.models.task import Task, TaskStatus, TaskPriority
from neotask.storage.memory import MemoryTaskRepository, MemoryQueueRepository

async def test_memory_storage():
    config = StorageConfig.memory()
    task_repo, queue_repo = StorageFactory.create(config)

    # Test exists method
    exists = await task_repo.exists("non-existent")
    print(f"Exists check: {exists}")  # Should be False

    print("Memory storage OK!")


@pytest.mark.asyncio
async def test_memory_task_repository():
    repo = MemoryTaskRepository()

    task = Task(
        task_id="test-1",
        data={"test": "data"},
        priority=TaskPriority.NORMAL
    )

    # Test save and get
    await repo.save(task)
    retrieved = await repo.get("test-1")
    assert retrieved is not None
    assert retrieved.task_id == "test-1"

    # Test update status
    await repo.update_status("test-1", TaskStatus.RUNNING)
    updated = await repo.get("test-1")
    assert updated.status == TaskStatus.RUNNING

    # Test list by status
    tasks = await repo.list_by_status(TaskStatus.RUNNING)
    assert len(tasks) == 1

    # Test exists
    assert await repo.exists("test-1") is True
    assert await repo.exists("non-existent") is False

    # Test delete
    await repo.delete("test-1")
    assert await repo.get("test-1") is None


@pytest.mark.asyncio
async def test_memory_queue_repository():
    repo = MemoryQueueRepository()

    # Test push
    await repo.push("task-1", 5)
    await repo.push("task-2", 1)  # Higher priority (lower number)
    await repo.push("task-3", 3)

    # Test size
    assert await repo.size() == 3

    # Test peek
    top = await repo.peek(2)
    assert top == ["task-2", "task-3"]

    # Test pop
    popped = await repo.pop(2)
    assert popped == ["task-2", "task-3"]
    assert await repo.size() == 1

    # Test remove
    await repo.remove("task-1")
    assert await repo.size() == 0

    # Test clear
    await repo.push("task-4", 1)
    await repo.clear()
    assert await repo.size() == 0

if __name__ == "__main__":
    import asyncio

    asyncio.run(test_memory_storage())