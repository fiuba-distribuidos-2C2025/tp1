import docker
import time
import logging
import aiohttp
import asyncio
from typing import Dict, List
import os
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DockerWatcher:
    def __init__(self, check_interval, health_port=9090, config_file="/app/watcher_config.json"):
        self.client = docker.from_env()
        self.check_interval = check_interval
        self.health_port = health_port
        self.config_file = config_file

        # Read from environment variable
        containers_env = os.getenv("WORKER_ADDRESSES", "")
        # Split into list, ignoring empty values
        self.containers: List[str] = [c.strip() for c in containers_env.split(",") if c.strip()]

        self.container_status: Dict[str, str] = {}
        self.restart_count: Dict[str, int] = {}
        self.last_config_mtime = None
        self.load_config()

    def load_config(self):
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                    self.check_interval = config.get('check_interval', self.check_interval)
                    self.health_port = config.get('health_port', self.health_port)
                    logger.info(f"Config loaded: check_interval={self.check_interval}s, health_port={self.health_port}")
            else:
                logger.warning(f"Config file not found at {self.config_file}, using defaults")
        except Exception as e:
            logger.error(f"Error loading config: {e}")

    async def is_container_healthy(self, container_name: str) -> bool:
        health_url = f"http://{container_name}:{self.health_port}/health"
        logger.info(f"Checking health of {container_name} at {health_url}")

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(health_url, timeout=5) as response:
                    logger.info(f"Received response from {container_name}: {response.status}")

                    if response.status == 200:
                        data = await response.json()
                        return data.get("status") == "healthy"
                    return False

        except Exception as e:
            logger.debug(f"Failed to reach health endpoint for {container_name}: {e}")
            return False

    async def restart_container(self, container_name: str) -> bool:
        try:
            def restart():
                container = self.client.containers.get(container_name)
                logger.info(f"Restarting container: {container.name}")
                container.restart(timeout=10)
                time.sleep(2)
                container.reload()
                return container.status

            status = await asyncio.to_thread(restart)

            if status == "running":
                self.restart_count[container_name] = self.restart_count.get(container_name, 0) + 1
                logger.info(f"Successfully restarted {container_name} (total restarts: {self.restart_count[container_name]})")
                return True

            logger.warning(f"Container {container_name} failed to start, status: {status}")
            return False

        except Exception as e:
            logger.error(f"Failed to restart container {container_name}: {e}")
            return False

    async def check_single_container(self, container_name: str):
        healthy = await self.is_container_healthy(container_name)
        if not healthy:
            logger.warning(f"Container '{container_name}' is not healthy")
            await self.restart_container(container_name)

    async def check_health(self):
        tasks = [
            asyncio.create_task(self.check_single_container(name))
            for name in self.containers
        ]
        await asyncio.gather(*tasks)

    async def start_async(self):
        logger.info("Docker Watcher Started (async mode)")
        try:
            while True:
                await self.check_health()

                if os.path.exists(self.config_file):
                    current_mtime = os.path.getmtime(self.config_file)
                    if self.last_config_mtime is None or current_mtime != self.last_config_mtime:
                        self.last_config_mtime = current_mtime
                        self.load_config()

                await asyncio.sleep(self.check_interval)

        except asyncio.CancelledError:
            logger.info("Docker Watcher cancelled")
        except Exception as e:
            logger.error(f"Unexpected error in watcher loop: {e}")

if __name__ == "__main__":
    watcher = DockerWatcher(check_interval=5, health_port=9090)
    asyncio.run(watcher.start_async())
