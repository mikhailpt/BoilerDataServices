import asyncio
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional
import threading
import logging

from asyncua import Client, ua
from db_manager import init_db, recreate_connection, DBConnection

logger = logging.getLogger(__name__)


class OPCDataHandler:
    def __init__(self, db_conn: DBConnection, param_cache: Dict[str, int], db_path: str):
        self.db_conn = db_conn
        self.cursor = db_conn.cursor()
        self._cache = param_cache
        self._lock = threading.Lock()
        self._db_path = db_path

    def datachange_notification(self, node, val, data):
        node_id_str = str(node.nodeid)

        if node_id_str not in self._cache:
            logger.debug(f"Unknown node, skipping: {node_id_str}")
            return

        param_id = self._cache[node_id_str]
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
            except sqlite3.OperationalError as e:
                logger.error(f"DB operational error: {e}")
                self.db_conn.rollback()
                self._recreate_db_connection()
            except sqlite3.DatabaseError as e:
                logger.error(f"DB database error: {e}")
                self.db_conn.rollback()
                self._recreate_db_connection()
            except sqlite3.Error as e:
                logger.error(f"DB write error: {e}")
                self.db_conn.rollback()

    def _recreate_db_connection(self):
        try:
            self.db_conn = recreate_connection(self.db_conn)
            self.cursor = self.db_conn.cursor()
            logger.info("DB connection recreated")
        except Exception as e:
            logger.error(f"Failed to recreate DB connection: {e}")


def _extract_server_timestamp(data) -> str:
    try:
        dv = data.Value
        ts = dv.ServerTimestamp or dv.SourceTimestamp
        if ts:
            return ts.isoformat()
    except (AttributeError, TypeError):
        pass
    return datetime.now(ZoneInfo('Asia/Novosibirsk')).isoformat()


class OPCUAServerClient:
    def __init__(
        self,
        db_conn: DBConnection,
        server_id: int,
        server_url: str,
        devices_config: List[dict],
        sampling_ms: int = 1000,
        db_path: str = "opc_readings.db",
    ):
        self.db_conn = db_conn
        self.server_id = server_id
        self.server_url = server_url
        self.devices_config = devices_config
        self.sampling_ms = sampling_ms
        self.db_path = db_path
        self.client: Optional[Client] = None
        self._subscription = None
        self._running = False
        self._watchdog_task: Optional[asyncio.Task] = None
        self._reconnect_backoff = 5
        self._max_backoff = 60

    def _upsert_device(self, device_key: str, object_name: str, description: Optional[str]) -> int:
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
        logger.info(f"  [DB] Device: {device_key} -> id={device_id}")
        return device_id

    def _upsert_device_param(self, device_id: int, info: dict) -> int:
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
                f"Failed to get id for device_id={device_id}, node_id={info['node_id']}"
            )

        return row[0]

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
                bn = await child.read_browse_name()
                if bn.Name == object_name:
                    logger.debug(f"  Found at depth {current_depth + 1}: {bn.Name}")
                    return child

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
        try:
            children = await device_obj.get_children(
                refs=ua.ObjectIds.HasComponent,
                nodeclassmask=ua.NodeClass.Variable,
            )
        except Exception:
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
                logger.debug(f"    Found node: {bn.Name}  node_id={node_id_str}")

            except Exception as e:
                logger.warning(f"    Node processing error: {e}")

        return result

    async def start(self):
        self.client = Client(self.server_url)
        await self.client.connect()
        logger.info(f"Connected to server: {self.server_url}")

        param_cache: Dict[str, int] = {}
        subscribe_nodes: List = []

        for dev_cfg in self.devices_config:
            key = dev_cfg["key"]
            obj_name = dev_cfg["object_name"]
            desc = dev_cfg.get("description")

            logger.info(f"  Browsing device: {key!r}  (OPC UA object: {obj_name!r})")

            device_obj = await self._find_device_object(obj_name)
            if device_obj is None:
                logger.warning(
                    f"  Object {obj_name!r} not found on {self.server_url} - skipping"
                )
                continue

            nodes_info = await self._browse_device_nodes(device_obj)
            if not nodes_info:
                logger.warning(f"  No nodes for {key!r} - skipping")
                continue

            device_id = self._upsert_device(key, obj_name, desc)

            for info in nodes_info:
                param_id = self._upsert_device_param(device_id, info)
                param_cache[info["node_id"]] = param_id
                subscribe_nodes.append(info["node"])

            logger.info(f"  {key}: registered {len(nodes_info)} nodes")

        if not subscribe_nodes:
            logger.warning(f"No nodes to subscribe on {self.server_url}")
            return

        handler = OPCDataHandler(self.db_conn, param_cache, self.db_path)
        self._subscription = await self.client.create_subscription(
            self.sampling_ms, handler
        )

        ok_count = 0
        for node in subscribe_nodes:
            try:
                await self._subscription.subscribe_data_change(node)
                ok_count += 1
            except Exception as e:
                logger.error(f"  Subscribe error on {node.nodeid}: {e}")

        logger.info(
            f"Server {self.server_url}: subscriptions active"
            f" ({ok_count}/{len(subscribe_nodes)} nodes)"
        )
        self._running = True
        self._reconnect_backoff = 5

    async def _is_subscription_alive(self) -> bool:
        if not self._subscription:
            return False
        try:
            return getattr(self._subscription, 'is_active', True)
        except Exception:
            return False

    async def _reconnect(self):
        logger.warning(f"Reconnecting to {self.server_url}...")
        await self.stop()

        while self._running:
            try:
                await self.start()
                logger.info(f"Reconnect successful: {self.server_url}")
                self._reconnect_backoff = 5
                return
            except Exception as e:
                logger.error(f"Reconnect failed: {e}, retry in {self._reconnect_backoff}s")
                await asyncio.sleep(self._reconnect_backoff)
                self._reconnect_backoff = min(self._reconnect_backoff * 2, self._max_backoff)

    async def _watchdog(self):
        logger.info(f"Watchdog started for {self.server_url}")
        while self._running:
            await asyncio.sleep(10)
            if not self._running:
                break
            alive = await self._is_subscription_alive()
            if not alive:
                logger.warning(f"Subscription dead on {self.server_url}, triggering reconnect")
                await self._reconnect()
                break
        logger.info(f"Watchdog stopped for {self.server_url}")

    async def run(self):
        logger.info(f"Listening for data: {self.server_url}")
        self._watchdog_task = asyncio.create_task(self._watchdog())
        try:
            await self._watchdog_task
        except asyncio.CancelledError:
            logger.info(f"Stopping: {self.server_url}")
        finally:
            await self.stop()

    async def stop(self):
        self._running = False
        if self._watchdog_task:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
        if self.client:
            try:
                await self.client.disconnect()
                logger.info(f"Disconnected: {self.server_url}")
            except Exception as e:
                logger.error(f"Disconnect error {self.server_url}: {e}")