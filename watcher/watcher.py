import docker
import time
import logging
import requests
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
        containers_env = os.getenv("TRACKED_CONTAINERS", "")
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

    def is_container_healthy(self, container_name: str) -> bool:
        try:
            # Hit the health endpoint using container name (works on Docker network)
            health_url = f"http://{container_name}:{self.health_port}/health"
            logger.info(f"Checking health of {container_name} at {health_url}")
            response = requests.get(health_url, timeout=5)
            logger.info(f"Received response from {container_name}: {response.status_code}")

            if response.status_code == 200:
                health_data = response.json()
                is_healthy = health_data.get('status') == 'healthy'
                return is_healthy
            else:
                return False

        except requests.exceptions.RequestException as e:
            logger.debug(f"Failed to reach health endpoint for {container_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error checking container health {container_name}: {e}")
            return False

    def restart_container(self, container_name: str) -> bool:
        try:
            container = self.client.containers.get(container_name)
            logger.info(f"Restarting container: {container.name}")
            container.restart(timeout=10)
            time.sleep(2)  # Give container time to start
            container.reload()
            if container.status == "running":
                self.restart_count[container_name] = self.restart_count.get(container_name, 0) + 1
                logger.info(f"Successfully restarted {container_name} (total restarts: {self.restart_count[container_name]})")
                return True
            else:
                logger.warning(f"Container {container.name} did not start properly. Status: {container.status}")
                return False
        except Exception as e:
            logger.error(f"Failed to restart container {container_name}: {e}")
            return False

    def check_health(self):
        for container_name in self.containers:
            # If container is unhealthy, attempt to restart
            if not self.is_container_healthy(container_name):
                logger.warning(f"Container '{container_name}' is not healthy")
                self.restart_container(container_name)

    def start(self):
        logger.info("Docker Watcher Started")
        try:
            while True:
                self.check_health()
                # Check if config file has been modified
                if os.path.exists(self.config_file):
                    current_mtime = os.path.getmtime(self.config_file)
                    if self.last_config_mtime is None or current_mtime != self.last_config_mtime:
                        self.last_config_mtime = current_mtime
                        self.load_config()

                time.sleep(self.check_interval)
        except KeyboardInterrupt:
            logger.info("Docker Watcher stopped by user")
        except Exception as e:
            logger.error(f"Unexpected error in watcher loop: {e}")

if __name__ == "__main__":
    watcher = DockerWatcher(check_interval=5, health_port=9090)
    watcher.start()
