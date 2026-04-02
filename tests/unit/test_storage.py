"""
@FileName: test_storage.py
@Description: 
@Author: HiPeng
@Time: 2026/4/2 21:55
"""
from neotask.storage import StorageFactory
from neotask.models.config import StorageConfig


async def test_memory_storage():
    config = StorageConfig.memory()
    task_repo, queue_repo = StorageFactory.create(config)

    # Test exists method
    exists = await task_repo.exists("non-existent")
    print(f"Exists check: {exists}")  # Should be False

    print("Memory storage OK!")


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_memory_storage())