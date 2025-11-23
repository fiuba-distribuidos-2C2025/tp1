import docker
import time
import logging
from typing import Dict, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('docker_watcher.log'),
    ]
)
logger = logging.getLogger(__name__)


class DockerWatcher:
    def __init__(self, check_interval):
        self.client = docker.from_env()
        self.check_interval = check_interval
        self.container_status: Dict[str, str] = {}
        self.restart_count: Dict[str, int] = {}

    def get_containers(self) -> List[docker.models.containers.Container]:
        try:
            containers_result = []
            total_containers = self.client.containers.list(all=True)
            for container in total_containers:
                if not container.name.startswith("client"):
                    containers_result.append(container)
            return containers_result
        except Exception as e:
            logger.error(f"Error getting containers: {e}")
            return []

    def is_container_running(self, container: docker.models.containers.Container) -> bool:
        try:
            container.reload()
            return container.status == "running"
        except Exception as e:
            logger.error(f"Error checking container {container.name}: {e}")
            return False

    def restart_container(self, container: docker.models.containers.Container) -> bool:
        try:
            logger.info(f"Restarting container: {container.name}")
            container.restart(timeout=10)
            time.sleep(2)  # Give container time to start
            container.reload()

            if container.status == "running":
                self.restart_count[container.name] = self.restart_count.get(container.name, 0) + 1
                logger.info(f"Successfully restarted {container.name} (total restarts: {self.restart_count[container.name]})")
                return True
            else:
                logger.warning(f"Container {container.name} did not start properly. Status: {container.status}")
                return False
        except Exception as e:
            logger.error(f"Failed to restart container {container.name}: {e}")
            return False

    def check_health(self):
        containers = self.get_containers()

        if not containers:
            logger.warning("No containers found to monitor")
            return

        for container in containers:
            current_status = container.status

            # If container is down, attempt to restart
            if not self.is_container_running(container):
                logger.warning(f"Container '{container.name}' is not running (status: {current_status})")
                self.restart_container(container)

    def start(self):
        logger.info("Docker Watcher Started")

        try:
            while True:
                self.check_health()
                time.sleep(self.check_interval)
        except KeyboardInterrupt:
            logger.info("Docker Watcher stopped by user")
        except Exception as e:
            logger.error(f"Unexpected error in watcher loop: {e}")

if __name__ == "__main__":
    watcher = DockerWatcher(check_interval=10)
    watcher.start()
