import os
import asyncio
import socket
import subprocess
import logging
import random

import aiohttp

from datetime import datetime

from aiohttp_socks import ProxyConnector
from redis import asyncio as aioredis
from dotenv import load_dotenv


log_dir = 'logs'

if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(log_dir, 'tor_proxy_manager.log'))
    ]
)

logger = logging.getLogger(__name__)

load_dotenv()

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
SITES_FILE = os.getenv('SITES_FILE', 'sites.txt')
USER_AGENTS_FILE = os.getenv('USER_AGENTS_FILE', 'user_agents.txt')
START_PORT = int(os.getenv('START_PORT', 9050))
END_PORT = int(os.getenv('END_PORT', 9350))
TIMEOUT = int(os.getenv('TIMEOUT', 10))
MAX_CONCURRENT_TASKS = int(os.getenv('MAX_CONCURRENT_TASKS', 100))
LOOP_INTERVAL = int(os.getenv('LOOP_INTERVAL', 1200))


async def check_proxy(session, proxy_url, site, user_agent):
    try:
        headers = {'User-Agent': user_agent}
        connector = ProxyConnector.from_url(proxy_url)

        async with session.get(
                f"https://{site}",
                connector=connector,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=TIMEOUT)
        ) as response:
            if response.status == 200:
                return True
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.debug(f"Proxy check failed for {site} via {proxy_url}: {str(e)}")
    return False


async def check_port(port, sites, user_agents, redis_conn, semaphore):
    proxy_url = f"socks5://127.0.0.1:{port}"
    user_agent = random.choice(user_agents)

    async with semaphore:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                result = s.connect_ex(('127.0.0.1', port))

                if result != 0:
                    logger.debug(f"Port {port} is not open, skipping")
                    return

            async with aiohttp.ClientSession() as session:
                for site in sites:
                    if await check_proxy(session, proxy_url, site, user_agent):
                        await redis_conn.sadd(f"site:{site}", port)
                        logger.info(f"Proxy {proxy_url} works for {site} with UA: {user_agent[:50]}...")

        except Exception as e:
            logger.error(f"Error checking port {port}: {e}", exc_info=True)


async def main():
    try:
        logger.info("Starting main check cycle")

        try:
            with open(SITES_FILE, 'r') as f:
                sites = [line.strip() for line in f if line.strip()]
            logger.info(f"Loaded {len(sites)} sites from {SITES_FILE}")
        except Exception as e:
            logger.error(f"Failed to load sites file: {e}")
            return

        try:
            with open(USER_AGENTS_FILE, 'r') as f:
                user_agents = [line.strip() for line in f if line.strip()]
            logger.info(f"Loaded {len(user_agents)} user agents from {USER_AGENTS_FILE}")
        except Exception as e:
            logger.error(f"Failed to load user agents file: {e}")
            return

        try:
            redis_conn = await aioredis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                decode_responses=True
            )
            await redis_conn.ping()
            logger.info("Connected to Redis successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return

        try:
            async for key in redis_conn.scan_iter("site:*"):
                await redis_conn.delete(key)
            logger.info("Cleaned up old Redis data")
        except Exception as e:
            logger.error(f"Failed to clean Redis data: {e}")

        try:
            logger.info("Restarting Tor service...")
            subprocess.run(["sudo", "systemctl", "stop", "tor"], check=True)
            subprocess.run(["sudo", "rm", "-rf", "/var/lib/tor/*"], check=True)
            subprocess.run(["sudo", "systemctl", "start", "tor"], check=True)
            logger.info("Tor service restarted successfully")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to restart Tor: {e}")
            return

        await asyncio.sleep(10)
        logger.info("Waiting period after Tor restart completed")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        tasks = []
        total_ports = END_PORT - START_PORT + 1

        logger.info(f"Starting to check {total_ports} ports (from {START_PORT} to {END_PORT})")

        for port in range(START_PORT, END_PORT + 1):
            task = asyncio.create_task(check_port(port, sites, user_agents, redis_conn, semaphore))
            tasks.append(task)

        await asyncio.gather(*tasks)
        logger.info("Completed checking all ports")

        await redis_conn.close()

    except Exception as e:
        logger.error(f"Unexpected error in main cycle: {e}", exc_info=True)
        raise


async def run_loop():
    logger.info("Starting proxy checker service")

    while True:
        try:
            start_time = datetime.now()
            logger.info(f"Starting new check cycle at {start_time}")

            await main()

            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Check cycle completed in {duration:.2f} seconds")

            logger.info(f"Waiting {LOOP_INTERVAL} seconds before next cycle...")
            await asyncio.sleep(LOOP_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, stopping service")
            break
        except Exception as e:
            logger.error(f"Unexpected error in run_loop: {e}", exc_info=True)
            logger.info(f"Waiting {LOOP_INTERVAL} seconds before retry...")
            await asyncio.sleep(LOOP_INTERVAL)


if __name__ == "__main__":
    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        logger.info("Script stopped by user")
    except Exception as ex:
        logger.error(f"Fatal error: {ex}", exc_info=True)
