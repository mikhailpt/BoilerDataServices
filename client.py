import asyncio
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional
import threading

from asyncua import Client, ua

import logging

# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ===========================================================================
# ОБРАБОТЧИК ПОДПИСОК
# ===========================================================================

class OPCDataHandler:
    """
    Callback-обработчик OPC UA подписок.

    Получает уведомления об изменении значений и записывает их
    в таблицу readings (device_param_id, value, server_timestamp).

    param_cache: dict  str(node.nodeid) → device_param_id (int)
    """

    def __init__(self, db_conn: sqlite3.Connection, param_cache: Dict[str, int]):
        self.db_conn = db_conn
        self.cursor = db_conn.cursor()
        self._cache = param_cache
        self._lock = threading.Lock()

    def datachange_notification(self, node, val, data):
        node_id_str = str(node.nodeid)

        if node_id_str not in self._cache:
            logger.debug(f"Неизвестный узел, пропускаем: {node_id_str}")
            return

        param_id = self._cache[node_id_str]

        # Серверная метка времени из OPC UA DataValue
        server_ts = _extract_server_timestamp(data)

        with self._lock:
            try:
                self.cursor.execute(
                    "INSERT INTO readings (device_param_id, value, server_timestamp)"
                    " VALUES (?, ?, ?)",
                    (param_id, str(val), server_ts),
                )
                self.db_conn.commit()
                logger.info(f"  {node_id_str} = {val!r}  [{server_ts}]")
            except sqlite3.Error as e:
                logger.error(f"Ошибка записи в БД: {e}")
                self.db_conn.rollback()


def _extract_server_timestamp(data) -> str:
    """
    Извлекает ServerTimestamp из OPC UA MonitoredItemNotification.
    Поле data.Value — это ua.DataValue; data.Value.ServerTimestamp — datetime.
    При недоступности возвращает текущее UTC-время клиента.
    """
    try:
        dv = data.Value                              # ua.DataValue
        ts = dv.ServerTimestamp or dv.SourceTimestamp
        if ts:
            return ts.isoformat()
    except (AttributeError, TypeError):
        pass
    return datetime.now(ZoneInfo('Asia/Novosibirsk')).isoformat()


# ===========================================================================
# КЛИЕНТ ОДНОГО OPC UA СЕРВЕРА
# ===========================================================================

class OPCUAServerClient:
    """
    Подключается к одному OPC UA серверу.

    Для каждого прибора из devices_config:
      1. Ищет объект прибора в пространстве Objects по object_name.
      2. Обходит все дочерние Variable-узлы (browsing).
      3. Регистрирует прибор и его параметры в БД.
      4. Подписывается на изменения найденных узлов.
    """

    def __init__(
        self,
        db_conn: sqlite3.Connection,
        server_id: int,
        server_url: str,
        devices_config: List[dict],
        sampling_ms: int = 1000,
    ):
        self.db_conn = db_conn
        self.server_id = server_id
        self.server_url = server_url
        self.devices_config = devices_config   # [{"key":…, "object_name":…, "description":…}, …]
        self.sampling_ms = sampling_ms
        self.client: Optional[Client] = None
        self._subscription = None
        self._running = False

    # -----------------------------------------------------------------------
    # Регистрация в БД
    # -----------------------------------------------------------------------

    def _upsert_device(self, device_key: str, object_name: str, description: Optional[str]) -> int:
        """INSERT OR IGNORE прибора, возвращает его id."""
        cur = self.db_conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO devices"
            " (server_id, device_key, object_name, description)"
            " VALUES (?, ?, ?, ?)",
            (self.server_id, device_key, object_name, description),
        )
        self.db_conn.commit()
        cur.execute(
            "SELECT id FROM devices WHERE server_id = ? AND device_key = ?",
            (self.server_id, device_key),
        )
        device_id = cur.fetchone()[0]
        logger.info(f"  [БД] Прибор: {device_key} → id={device_id}")
        return device_id

    def _upsert_device_param(self, device_id: int, info: dict) -> int:
        """
        Возвращает id параметра: создаёт новый, если не существует,
        или возвращает существующий. НЕ меняет id при повторных запусках.
        """
        cur = self.db_conn.cursor()

        cur.execute(
            """
            INSERT OR IGNORE INTO device_params
                (device_id, node_id, param_name, description, unit, data_type, is_active)
            VALUES (?, ?, ?, ?, ?, ?, 1)
            """,
            (
                device_id,
                info["node_id"],
                info["param_name"],
                info.get("description"),
                info.get("unit"),
                info.get("data_type", "string"),
            ),
        )
        self.db_conn.commit()

        cur.execute(
            "SELECT id FROM device_params WHERE device_id = ? AND node_id = ?",
            (device_id, info["node_id"]),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(
                f"Не удалось получить id для device_id={device_id}, node_id={info['node_id']}"
            )

        return row[0]  # Возвращаем стабильный id

    # -----------------------------------------------------------------------
    # OPC UA: поиск и обход узлов прибора
    # -----------------------------------------------------------------------
    async def _find_device_object(self, object_name: str):
        objects_node = self.client.nodes.objects
        return await self._recursive_search(objects_node, object_name, max_depth=10)

    async def _recursive_search(self, node, object_name: str, max_depth: int = 10, current_depth: int = 0):
        if current_depth >= max_depth:
            return None

        try:
            children = await node.get_children()
        except Exception:
            return None

        for child in children:
            try:
                # Проверяем текущий узел
                bn = await child.read_browse_name()
                if bn.Name == object_name:
                    logger.debug(f"  Найдено на глубине {current_depth + 1}: {bn.Name}")
                    return child

                # Рекурсивно ищем в детях (если это Object или Variable)
                node_class = await child.read_node_class()
                if node_class in (ua.NodeClass.Object, ua.NodeClass.Variable):
                    found = await self._recursive_search(
                        child, object_name, max_depth, current_depth + 1
                    )
                    if found is not None:
                        return found

            except Exception:
                continue

        return None

    async def _browse_device_nodes(self, device_obj) -> List[dict]:
        """
        Обходит все Variable-узлы прибора (только HasComponent, не свойства).
        Для каждого считывает: node_id, param_name, description, unit, data_type.
        Возвращает список dict с ключом 'node' (объект asyncua.Node).
        """
        # Пробуем получить только Variable-дочерние узлы через HasComponent
        try:
            children = await device_obj.get_children(
                refs=ua.ObjectIds.HasComponent,
                nodeclassmask=ua.NodeClass.Variable,
            )
        except Exception:
            # Fallback: берём всех детей и фильтруем по NodeClass
            all_children = await device_obj.get_children()
            children = []
            for ch in all_children:
                try:
                    if await ch.read_node_class() == ua.NodeClass.Variable:
                        children.append(ch)
                except Exception:
                    pass

        result = []
        for node in children:
            try:
                bn = await node.read_browse_name()
                node_id_str = str(node.nodeid)

                description: Optional[str] = None
                unit: Optional[str] = None
                data_type_str: str = "string"

                # Свойства узла: Description и EngineeringUnits
                # (добавляются в device_instance.py через node.add_property)
                try:
                    props = await node.get_properties()
                    for prop in props:
                        pbn = await prop.read_browse_name()
                        pval = await prop.read_value()
                        if pbn.Name == "Description":
                            description = str(pval)
                        elif pbn.Name == "EngineeringUnits":
                            unit = str(pval)
                except Exception:
                    pass

                # Тип данных из текущего значения узла
                try:
                    val = await node.read_value()
                    data_type_str = type(val).__name__
                except Exception:
                    pass

                result.append(
                    {
                        "node": node,               # объект Node для подписки
                        "node_id": node_id_str,
                        "param_name": bn.Name,
                        "description": description,
                        "unit": unit,
                        "data_type": data_type_str,
                    }
                )
                logger.debug(f"    Найден узел: {bn.Name}  node_id={node_id_str}")

            except Exception as e:
                logger.warning(f"    Не удалось обработать узел: {e}")

        return result

    # -----------------------------------------------------------------------
    # Запуск / остановка
    # -----------------------------------------------------------------------

    async def start(self):
        """
        1. Подключается к серверу.
        2. Для каждого прибора: browsing → регистрация в БД.
        3. Создаёт подписку на все найденные узлы.
        """
        self.client = Client(self.server_url)
        await self.client.connect()
        logger.info(f"Подключено к серверу: {self.server_url}")

        param_cache: Dict[str, int] = {}    # node_id_str → device_param_id
        subscribe_nodes: List = []          # Node-объекты для подписки

        for dev_cfg in self.devices_config:
            key = dev_cfg["key"]
            obj_name = dev_cfg["object_name"]
            desc = dev_cfg.get("description")

            logger.info(f"  Обход прибора: {key!r}  (OPC UA объект: {obj_name!r})")

            # 1. Найти объект прибора на сервере
            device_obj = await self._find_device_object(obj_name)
            if device_obj is None:
                logger.warning(
                    f"  Объект {obj_name!r} не найден на {self.server_url} — пропускаем"
                )
                continue

            # 2. Обойти все Variable-узлы прибора
            nodes_info = await self._browse_device_nodes(device_obj)
            if not nodes_info:
                logger.warning(f"  Узлы для {key!r} не найдены — пропускаем")
                continue

            # 3. Зарегистрировать прибор и параметры в БД
            device_id = self._upsert_device(key, obj_name, desc)

            for info in nodes_info:
                param_id = self._upsert_device_param(device_id, info)
                param_cache[info["node_id"]] = param_id
                subscribe_nodes.append(info["node"])

            logger.info(f"  {key}: зарегистрировано {len(nodes_info)} узлов")

        if not subscribe_nodes:
            logger.warning(f"Нет узлов для подписки на {self.server_url}")
            return

        # 4. Создать подписку и подписаться на все найденные узлы
        handler = OPCDataHandler(self.db_conn, param_cache)
        self._subscription = await self.client.create_subscription(
            self.sampling_ms, handler
        )

        ok_count = 0
        for node in subscribe_nodes:
            try:
                await self._subscription.subscribe_data_change(node)
                ok_count += 1
            except Exception as e:
                logger.error(f"  Ошибка подписки на {node.nodeid}: {e}")

        logger.info(
            f"Сервер {self.server_url}: подписки активны"
            f" ({ok_count}/{len(subscribe_nodes)} узлов)"
        )
        self._running = True

    async def run(self):
        """Цикл ожидания данных (работает пока _running == True)."""
        logger.info(f"Слушаем данные: {self.server_url}")
        try:
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info(f"Остановка: {self.server_url}")
        finally:
            await self.stop()

    async def stop(self):
        self._running = False
        if self.client:
            try:
                await self.client.disconnect()
                logger.info(f"Отключено: {self.server_url}")
            except Exception as e:
                logger.error(f"Ошибка отключения {self.server_url}: {e}")
