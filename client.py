import asyncio
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional
import threading

from asyncua import Client, ua

from node_finder import (
    OpcNodeFinder,
    browse_many,
    datavalue_to_python,
    read_values_many,
)

import logging

# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

_UNREAD_VALUE = object()


# ===========================================================================
# ОБРАБОТЧИК ПОДПИСОК
# ===========================================================================

class OPCDataWriter:
    def __init__(self, db_conn: sqlite3.Connection, queue: asyncio.Queue):
        self.db_conn = db_conn
        self.queue = queue
        self.cursor = db_conn.cursor()
        self._running = True
        self._lock = threading.Lock()

    async def run(self):
        logger.info("DB writer запущен")
        while self._running:
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if item is None:
                break
            param_id, value_str, server_ts = item
            with self._lock:
                try:
                    self.cursor.execute(
                        "INSERT INTO readings (device_param_id, value, server_timestamp) VALUES (?, ?, ?)",
                        (param_id, value_str, server_ts),
                    )
                    self.db_conn.commit()
                    logger.debug(f"  db write: param_id={param_id} = {value_str!r} [{server_ts}]")
                except sqlite3.Error as e:
                    logger.error(f"Ошибка записи в БД: {e}")
                    try:
                        self.db_conn.rollback()
                    except Exception:
                        pass
            self.queue.task_done()

    async def stop(self):
        self._running = False
        try:
            self.queue.put_nowait(None)
        except Exception:
            pass


class OPCDataHandler:
    """
    Callback-обработчик OPC UA подписок.

    Получает уведомления об изменении значений и ставит их в очередь на запись.
    """

    def __init__(self, queue: asyncio.Queue, param_cache: Dict[str, int]):
        self._queue = queue
        self._cache = param_cache

    def datachange_notification(self, node, val, data):
        node_id_str = str(node.nodeid)
        if node_id_str not in self._cache:
            logger.debug(f"Неизвестный узел, пропускаем: {node_id_str}")
            return
        param_id = self._cache[node_id_str]
        server_ts = _extract_server_timestamp(data)
        try:
            self._queue.put_nowait((param_id, str(val), server_ts))
        except Exception as e:
            logger.error(f"Ошибка постановки в очередь чтения: {e}")


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
      1. Ищет объекты приборов в пространстве Objects по object_name
         (все имена — за один BFS-проход с пакетными Browse-запросами).
      2. Обходит все дочерние Variable-узлы (browsing, тоже пакетно).
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
        self._finder: Optional[OpcNodeFinder] = None
        self._queue: Optional[asyncio.Queue] = None
        self._writer: Optional[OPCDataWriter] = None
        self._writer_task: Optional[asyncio.Task] = None
        self._last_subscribed: List = []

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
        """
        Находит объект прибора по BrowseName.

        Тонкая обёртка над OpcNodeFinder (BFS с пакетными Browse-запросами).
        Основной путь в start() — find_many(), который ищет все приборы
        за один проход по дереву.
        """
        if self.client is None:
            return None
        if self._finder is None:
            self._finder = OpcNodeFinder(self.client, max_depth=10)
        return await self._finder.find(object_name)

    async def _browse_device_nodes(self, device_obj) -> List[dict]:
        """
        Обходит все Variable-узлы прибора (только HasComponent, не свойства).
        Для каждого считывает: node_id, param_name, description, unit, data_type.
        Возвращает список dict с ключом 'node' (объект asyncua.Node).

        Пакетный вариант: Browse имён/свойств + один Read значений на все
        параметры. При любой ошибке — откат на последовательный обход.
        """
        try:
            return await self._browse_device_nodes_batched(device_obj)
        except Exception as e:
            logger.warning(f"    Пакетный обход не удался ({e}) — fallback на последовательный")
            return await self._browse_device_nodes_sequential(device_obj)

    async def _browse_device_nodes_batched(self, device_obj) -> List[dict]:
        # 1. Variable-дети через HasComponent; если пусто — все hierarchical
        variable_refs = await self._browse_variable_refs(device_obj)
        if not variable_refs:
            variable_refs = await self._browse_variable_refs(
                device_obj, reference_type=ua.ObjectIds.HierarchicalReferences
            )
        if not variable_refs:
            return []

        variables = []
        for ref in variable_refs:
            name = ref.BrowseName.Name if ref.BrowseName is not None else None
            if not name:
                continue
            variables.append((self.client.get_node(ref.NodeId), ref.NodeId, name))
        if not variables:
            return []

        # 2. Свойства Description/EngineeringUnits всех переменных — одним Browse
        variable_ids = [var[1] for var in variables]
        props_per_var = await browse_many(
            self.client,
            variable_ids,
            reference_type=ua.ObjectIds.HasProperty,
            include_subtypes=False,
            node_class_mask=ua.NodeClass.Variable,
        )

        properties: Dict[str, Dict[str, ua.NodeId]] = {}
        ids_to_read: List[ua.NodeId] = list(variable_ids)
        for var_id, prop_refs in zip(variable_ids, props_per_var):
            mapping: Dict[str, ua.NodeId] = {}
            for ref in prop_refs:
                if ref.BrowseName is None:
                    continue
                if ref.BrowseName.Name in ("Description", "EngineeringUnits"):
                    mapping[ref.BrowseName.Name] = ref.NodeId
                    ids_to_read.append(ref.NodeId)
            properties[str(var_id)] = mapping

        # 3. Один пакетный Read: значения переменных + значения свойств
        values = await read_values_many(self.client, ids_to_read)

        result = []
        for node, var_id, param_name in variables:
            var_key = str(var_id)
            data_type_str = "string"
            data_value = values.get(var_key)
            if data_value is not None:
                value = datavalue_to_python(data_value, _UNREAD_VALUE)
                if value is not _UNREAD_VALUE:
                    data_type_str = type(value).__name__

            description: Optional[str] = None
            unit: Optional[str] = None
            prop_map = properties.get(var_key, {})
            if "Description" in prop_map:
                prop_value = datavalue_to_python(values.get(str(prop_map["Description"])))
                if prop_value is not None:
                    description = str(prop_value)
            if "EngineeringUnits" in prop_map:
                prop_value = datavalue_to_python(values.get(str(prop_map["EngineeringUnits"])))
                if prop_value is not None:
                    unit = str(prop_value)

            result.append(
                {
                    "node": node,
                    "node_id": var_key,
                    "param_name": param_name,
                    "description": description,
                    "unit": unit,
                    "data_type": data_type_str,
                }
            )
            logger.debug(f"    Найден узел: {param_name}  node_id={var_key}")

        return result

    async def _browse_variable_refs(
        self, device_obj, reference_type: int = ua.ObjectIds.HasComponent
    ) -> List[ua.ReferenceDescription]:
        """Пакетно читает ссылки на Variable-узлы прибора."""
        batches = await browse_many(
            self.client,
            [device_obj.nodeid],
            reference_type=reference_type,
            include_subtypes=True,
            node_class_mask=ua.NodeClass.Variable,
        )
        return batches[0] if batches else []

    async def _browse_device_nodes_sequential(self, device_obj) -> List[dict]:
        """Последовательный обход (прежняя реализация) — резервный путь."""
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
                        "node": node,
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

    async def _on_connection_lost(self, ex: Exception):
        logger.warning(f"Потеря соединения с {self.server_url}: {ex}")
        self._running = False

    async def _cleanup_partial(self):
        self._running = False
        self._finder = None
        if self._subscription:
            try:
                await self._subscription.delete()
            except Exception:
                pass
            self._subscription = None
        if self.client:
            try:
                await self.client.disconnect()
            except Exception as e:
                logger.debug(f"Ошибка отключения при очистке {self.server_url}: {e}")
            self.client = None
        if self._writer:
            await self._writer.stop()
        if self._writer_task:
            self._writer_task.cancel()
            try:
                await self._writer_task
            except asyncio.CancelledError:
                pass
        self._writer = None
        self._writer_task = None
        self._queue = None

    async def start(self):
        """
        1. Подключается к серверу.
        2. Одним проходом находит объекты всех приборов.
        3. Для каждого прибора: browsing параметров → регистрация в БД.
        4. Создаёт подписку на все найденные узлы.
        """
        self.client = Client(self.server_url)
        self.client.connection_lost_callback = self._on_connection_lost
        await self.client.connect()
        logger.info(f"Подключено к серверу: {self.server_url}")

        param_cache: Dict[str, int] = {}    # node_id_str → device_param_id
        subscribe_nodes: List = []          # Node-объекты для подписки

        # 1. Найти объекты всех приборов за один обход дерева (BFS + батчи)
        self._finder = OpcNodeFinder(self.client, max_depth=10)
        object_names = [cfg["object_name"] for cfg in self.devices_config]
        found_objects = await self._finder.find_many(object_names)
        stats = self._finder.last_stats
        logger.info(
            f"  Поиск объектов: найдено {len(found_objects)}/{len(set(object_names))}"
            f" за {stats.get('elapsed_ms', 0.0):.1f} мс"
            f" (просмотрено {stats.get('nodes_browsed', 0)} узлов)"
        )

        for dev_cfg in self.devices_config:
            key = dev_cfg["key"]
            obj_name = dev_cfg["object_name"]
            desc = dev_cfg.get("description")

            logger.info(f"  Обход прибора: {key!r}  (OPC UA объект: {obj_name!r})")

            # Объект прибора из общего результата поиска
            device_obj = found_objects.get(obj_name)
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

        self._queue = asyncio.Queue(maxsize=10000)
        self._writer = OPCDataWriter(self.db_conn, self._queue)
        self._writer_task = asyncio.create_task(self._writer.run(), name=f"db-writer-{self.server_id}")

        if not subscribe_nodes:
            logger.warning(f"Нет узлов для подписки на {self.server_url}")
            self._running = True
            self._last_subscribed = []
            return

        # 4. Создать подписку и подписаться на все найденные узлы
        handler = OPCDataHandler(self._queue, param_cache)
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
        self._last_subscribed = subscribe_nodes
        self._running = True

    async def run(self):
        """Цикл ожидания данных с авто-переподключением при потере соединения."""
        logger.info(f"Слушаем данные: {self.server_url}")
        backoff = 1
        try:
            while True:
                if not self._running:
                    try:
                        await self.start()
                        backoff = 1
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.warning(f"Не удалось переподключиться к {self.server_url}: {e}")
                        await self._cleanup_partial()
                        await asyncio.sleep(min(backoff, 60))
                        backoff = min(backoff * 2, 60)
                        continue
                try:
                    if self.client:
                        await self.client.check_connection()
                    await asyncio.sleep(1)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning(f"Проверка соединения не удалась {self.server_url}: {e}")
                    self._running = False
                    await self._cleanup_partial()
                    await asyncio.sleep(min(backoff, 60))
                    backoff = min(backoff * 2, 60)
        except asyncio.CancelledError:
            logger.info(f"Остановка: {self.server_url}")
        finally:
            await self.stop()

    async def stop(self):
        self._running = False
        if self._subscription:
            try:
                await self._subscription.delete()
            except Exception:
                pass
            self._subscription = None
        if self._writer:
            await self._writer.stop()
        if self._writer_task:
            self._writer_task.cancel()
            try:
                await self._writer_task
            except asyncio.CancelledError:
                pass
        self._writer = None
        self._writer_task = None
        self._queue = None
        self._finder = None
        if self.client:
            try:
                await self.client.disconnect()
                logger.info(f"Отключено: {self.server_url}")
            except Exception as e:
                logger.error(f"Ошибка отключения {self.server_url}: {e}")
            self.client = None
