import asyncio
from typing import List, Optional

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

from db_manager import init_db
from client import OPCUAServerClient
from monitoring_config import MONITORING_CONFIG


class OPCUAMultiServerManager:
    """
    Читает MONITORING_CONFIG, регистрирует серверы в БД,
    запускает OPCUAServerClient для каждого из них.
    """

    def __init__(self, db_path: str = "opc_readings.db", sampling_ms: int = 1000):
        self.db_conn = init_db(db_path)
        self.sampling_ms = sampling_ms
        self._clients: List[OPCUAServerClient] = []
        self._tasks: List[asyncio.Task] = []

    # -----------------------------------------------------------------------
    # БД: регистрация сервера
    # -----------------------------------------------------------------------

    def _upsert_server(self, name: str, url: str, description: Optional[str]) -> int:
        cur = self.db_conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO servers (name, url, description) VALUES (?, ?, ?)",
            (name, url, description),
        )
        self.db_conn.commit()
        cur.execute("SELECT id FROM servers WHERE name = ?", (name,))
        server_id = cur.fetchone()[0]
        logger.info(f"[БД] Сервер: {name!r} → id={server_id}")
        return server_id

    # -----------------------------------------------------------------------
    # Запуск / остановка
    # -----------------------------------------------------------------------

    async def load_and_start(self):
        """
        Загружает MONITORING_CONFIG и последовательно запускает
        клиент для каждого сервера.
        """
        for srv_cfg in MONITORING_CONFIG:
            name = srv_cfg["name"]
            url = srv_cfg["url"]
            desc = srv_cfg.get("description")
            devices = srv_cfg.get("devices", [])

            if not devices:
                logger.warning(f"Сервер {name!r}: список приборов пуст — пропускаем")
                continue

            server_id = self._upsert_server(name, url, desc)

            client = OPCUAServerClient(
                db_conn=self.db_conn,
                server_id=server_id,
                server_url=url,
                devices_config=devices,
                sampling_ms=self.sampling_ms,
            )

            try:
                await client.start()
            except Exception as e:
                logger.error(f"Не удалось запустить клиент {name!r}: {e}")
                continue

            self._clients.append(client)
            task = asyncio.create_task(client.run(), name=f"client-{name}")
            self._tasks.append(task)

    async def stop_all(self):
        """Останавливает все клиенты и закрывает БД."""
        for client in self._clients:
            await client.stop()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self.db_conn.close()
        logger.info("Все соединения закрыты, БД отключена.")


async def main():
    manager = OPCUAMultiServerManager(
        db_path="opc_readings.db",
        sampling_ms=1000,
    )
    try:
        await manager.load_and_start()
        # Бесконечный цикл — данные пишутся через callback подписки
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        logger.info("\nПолучен сигнал остановки...")
    finally:
        await manager.stop_all()


if __name__ == "__main__":
    asyncio.run(main())