import asyncio
from typing import List, Optional
import logging

from db_manager import init_db
from client import OPCUAServerClient
from monitoring_config import MONITORING_CONFIG

logger = logging.getLogger(__name__)


def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler("client_manager.log", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s", "%H:%M:%S"
    ))

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s", "%H:%M:%S"
    ))

    root.handlers = [file_handler, console_handler]


class OPCUAMultiServerManager:
    def __init__(self, db_path: str = "opc_readings.db", sampling_ms: int = 1000):
        self.db_conn = init_db(db_path)
        self.sampling_ms = sampling_ms
        self._clients: List[OPCUAServerClient] = []
        self._tasks: List[asyncio.Task] = []

    def _upsert_server(self, name: str, url: str, description: Optional[str]) -> int:
        cur = self.db_conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO servers (name, url, description) VALUES (?, ?, ?)",
            (name, url, description),
        )
        self.db_conn.commit()
        cur.execute("SELECT id FROM servers WHERE name = ?", (name,))
        server_id = cur.fetchone()[0]
        logger.info(f"[DB] Server: {name!r} -> id={server_id}")
        return server_id

    async def _run_with_restart(self, client: OPCUAServerClient, name: str):
        backoff = 5
        max_backoff = 60
        while True:
            try:
                await client.run()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"[{name}] Client crashed: {e}, restart in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                try:
                    await client.start()
                    logger.info(f"[{name}] Client restarted successfully")
                    backoff = 5
                except Exception as e2:
                    logger.error(f"[{name}] Restart failed: {e2}")
                    continue

    async def load_and_start(self):
        for srv_cfg in MONITORING_CONFIG:
            name = srv_cfg["name"]
            url = srv_cfg["url"]
            desc = srv_cfg.get("description")
            devices = srv_cfg.get("devices", [])

            if not devices:
                logger.warning(f"Server {name!r}: empty device list - skipping")
                continue

            server_id = self._upsert_server(name, url, desc)

            client = OPCUAServerClient(
                db_conn=self.db_conn,
                server_id=server_id,
                server_url=url,
                devices_config=devices,
                sampling_ms=self.sampling_ms,
                db_path=self.db_conn.db_path,
            )

            try:
                await client.start()
            except Exception as e:
                logger.error(f"Failed to start client {name!r}: {e}")
                continue

            self._clients.append(client)
            task = asyncio.create_task(self._run_with_restart(client, name), name=f"client-{name}")
            self._tasks.append(task)

    async def stop_all(self):
        for client in self._clients:
            await client.stop()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self.db_conn.close()
        logger.info("All connections closed, DB disconnected.")


async def main():
    setup_logging()
    manager = OPCUAMultiServerManager(
        db_path="opc_readings.db",
        sampling_ms=1000,
    )
    try:
        await manager.load_and_start()
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        logger.info("\nStop signal received...")
    finally:
        await manager.stop_all()


if __name__ == "__main__":
    asyncio.run(main())