import docker
import time
import logging
import aiohttp
import asyncio
from typing import List, Optional
import os
from aiohttp import web

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Watcher:
    def __init__(self):
        self.client = docker.from_env()
        self.watcher_id = int(os.getenv("WATCHER_ID", "1"))
        self.total_watchers = int(os.getenv("TOTAL_WATCHERS", "1"))
        
        self.cluster = [{"id": i, "address": f"watcher{i}:8000"} for i in range(1, self.total_watchers + 1)]
        
        self.is_leader = False
        self.current_leader = None
        self.last_heartbeat = time.time()
        
        self.containers = [c.strip() for c in os.getenv("WORKER_ADDRESSES", "").split(",") if c.strip()]
        self.session = None
        
        logger.info(f"Watcher {self.watcher_id} initialized. Cluster: {[w['id'] for w in self.cluster]}")
    
    async def run_election(self):
        logger.info(f"Watcher {self.watcher_id} starting election")
        
        # Find higher IDs
        higher = [w for w in self.cluster if w["id"] > self.watcher_id]
        
        if not higher:
            await self.become_leader()
            return
        
        responses = await asyncio.gather(
            *[self.ping_watcher(w) for w in higher],
            return_exceptions=True
        )
        
        if not any(r for r in responses if r is True):
            await self.become_leader()
        else:
            self.is_leader = False
            await asyncio.sleep(5)
    
    async def ping_watcher(self, watcher):
        try:
            url = f"http://{watcher['address']}/election"
            async with self.session.post(url, json={"from": self.watcher_id}, timeout=2) as resp:
                return resp.status == 200
        except:
            return False
    
    async def become_leader(self):
        logger.info(f"🎯 Watcher {self.watcher_id} is now LEADER")
        self.is_leader = True
        self.current_leader = self.watcher_id
        
        await asyncio.gather(
            *[self.announce_leader(w) for w in self.cluster if w["id"] != self.watcher_id],
            return_exceptions=True
        )
    
    async def announce_leader(self, watcher):
        try:
            url = f"http://{watcher['address']}/coordinator"
            async with self.session.post(url, json={"leader_id": self.watcher_id}, timeout=2) as resp:
                logger.info(f"Announced to watcher {watcher['id']}")
        except:
            pass
    
    async def send_heartbeats(self):
        while True:
            await asyncio.sleep(3)
            if self.is_leader:
                await asyncio.gather(
                    *[self.send_heartbeat(w) for w in self.cluster if w["id"] != self.watcher_id],
                    return_exceptions=True
                )
    
    async def send_heartbeat(self, watcher):
        try:
            url = f"http://{watcher['address']}/heartbeat"
            async with self.session.post(url, json={"leader_id": self.watcher_id}, timeout=2):
                pass
        except:
            pass
    
    async def check_leader(self):
        while True:
            await asyncio.sleep(1)
            if not self.is_leader and self.current_leader:
                if time.time() - self.last_heartbeat > 10:
                    logger.warning(f"Leader {self.current_leader} is dead, starting election")
                    await self.run_election()
    
    async def start_server(self):
        app = web.Application()
        app.router.add_post('/election', self.handle_election)
        app.router.add_post('/coordinator', self.handle_coordinator)
        app.router.add_post('/heartbeat', self.handle_heartbeat)
        app.router.add_get('/status', self.handle_status)
        
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, '0.0.0.0', 8000).start()
        logger.info(f"Server started on port 8000")
    
    async def handle_election(self, request):
        data = await request.json()
        from_id = data["from"]
        logger.info(f"Election from watcher {from_id}")
        
        if from_id < self.watcher_id:
            asyncio.create_task(self.run_election())
            return web.json_response({"ok": True})
        return web.json_response({"ok": False})
    
    async def handle_coordinator(self, request):
        data = await request.json()
        leader_id = data["leader_id"]
        logger.info(f"Watcher {leader_id} is now leader")
        
        self.is_leader = False
        self.current_leader = leader_id
        self.last_heartbeat = time.time()
        return web.json_response({"ok": True})
    
    async def handle_heartbeat(self, request):
        data = await request.json()
        self.current_leader = data["leader_id"]
        self.last_heartbeat = time.time()
        return web.json_response({"ok": True})
    
    async def handle_status(self, request):
        return web.json_response({
            "id": self.watcher_id,
            "is_leader": self.is_leader,
            "leader": self.current_leader
        })
    
    async def monitor_containers(self):
        while True:
            await asyncio.sleep(5)
            
            if not self.is_leader:
                continue
            
            logger.info(f"Leader checking containers")
            for container_name in self.containers:
                try:
                    async with self.session.get(f"http://{container_name}:9090/health", timeout=5) as resp:
                        if resp.status != 200:
                            raise Exception("Unhealthy")
                except:
                    logger.warning(f"Restarting {container_name}")
                    try:
                        container = await asyncio.to_thread(self.client.containers.get, container_name)
                        await asyncio.to_thread(container.restart)
                        logger.info(f"Restarted {container_name}")
                    except Exception as e:
                        logger.error(f"Failed to restart {container_name}: {e}")
    
    async def start(self):
        """Start the watcher"""
        logger.info(f"🚀 Watcher {self.watcher_id} starting")
        
        self.session = aiohttp.ClientSession()
        
        try:
            await self.start_server()
            await asyncio.sleep(2 + self.watcher_id * 0.5)
            await self.run_election()
            
            # Run all loops
            await asyncio.gather(
                self.send_heartbeats(),
                self.check_leader(),
                self.monitor_containers()
            )
        finally:
            await self.session.close()

if __name__ == "__main__":
    watcher = Watcher()
    asyncio.run(watcher.start())
