from __future__ import annotations

import logging
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

from asyncua import Client, ua
from asyncua.common.node import Node

logger = logging.getLogger(__name__)

# Ссылки, по которым не имеет смысла спускаться: свойства и подтипы.
_SKIPPED_REFERENCE_IDS = frozenset(
    {
        ua.ObjectIds.HasProperty,
        ua.ObjectIds.HasSubtype,
    }
)

_MAX_CONTINUATION_PAGES = 1000


# ---------------------------------------------------------------------------
# Пакетные операции с сервисами OPC UA
# ---------------------------------------------------------------------------


def _status_is_good(status: Any) -> bool:
    """StatusCode может отсутствовать или быть None — считаем это успехом."""
    if status is None:
        return True
    try:
        return bool(status.is_good())
    except Exception:
        return True


async def browse_many(
    client: Client,
    node_ids: Sequence[ua.NodeId],
    reference_type: int = ua.ObjectIds.HierarchicalReferences,
    include_subtypes: bool = True,
    node_class_mask: ua.NodeClass = ua.NodeClass.Object | ua.NodeClass.Variable,
    chunk_size: int = 64,
    max_references_per_node: int = 0,
) -> List[List[ua.ReferenceDescription]]:
    """
    Пакетный Browse: несколько узлов за один сервисный вызов.

    Возвращает список списков ReferenceDescription, выровненный по node_ids.
    Узлы, которые не удалось прочитать, дают пустой список.
    Обрабатывает ContinuationPoint (серверы, ограничивающие выдачу).
    Если сервер отвергает большой пакет (MaxNodesPerBrowse), запрос
    автоматически дробится пополам вплоть до одного узла.
    """
    if not node_ids:
        return []

    result: List[List[ua.ReferenceDescription]] = []
    for start in range(0, len(node_ids), chunk_size):
        chunk = list(node_ids[start : start + chunk_size])
        result.extend(
            await _browse_chunk(
                client,
                chunk,
                reference_type,
                include_subtypes,
                node_class_mask,
                max_references_per_node,
            )
        )

    return result


async def _browse_chunk(
    client: Client,
    chunk: Sequence[ua.NodeId],
    reference_type: int,
    include_subtypes: bool,
    node_class_mask: ua.NodeClass,
    max_references_per_node: int,
) -> List[List[ua.ReferenceDescription]]:
    descriptions = []
    for node_id in chunk:
        desc = ua.BrowseDescription()
        desc.NodeId = node_id
        desc.BrowseDirection = ua.BrowseDirection.Forward
        desc.ReferenceTypeId = ua.NodeId(reference_type)
        desc.IncludeSubtypes = include_subtypes
        desc.NodeClassMask = node_class_mask
        desc.ResultMask = ua.BrowseResultMask.All
        descriptions.append(desc)

    params = ua.BrowseParameters()
    params.View = ua.ViewDescription()
    params.RequestedMaxReferencesPerNode = max_references_per_node
    params.NodesToBrowse = descriptions

    try:
        results = list(await client.uaclient.browse(params))
    except Exception as exc:
        if len(chunk) <= 1:
            raise
        logger.debug("Browse %d узлов не удался (%s) — дробим запрос", len(chunk), exc)
        middle = len(chunk) // 2
        left = await _browse_chunk(
            client,
            chunk[:middle],
            reference_type,
            include_subtypes,
            node_class_mask,
            max_references_per_node,
        )
        right = await _browse_chunk(
            client,
            chunk[middle:],
            reference_type,
            include_subtypes,
            node_class_mask,
            max_references_per_node,
        )
        return left + right

    return await _collect_continuation_points(client, results)


async def _collect_continuation_points(
    client: Client, results: Sequence[ua.BrowseResult]
) -> List[List[ua.ReferenceDescription]]:
    """Дочитывает страницы BrowseNext и возвращает ссылки по каждому результату."""
    references: List[List[ua.ReferenceDescription]] = []
    pending: List[tuple[int, bytes]] = []

    for index, res in enumerate(results):
        references.append(list(res.References))
        if _status_is_good(getattr(res, "StatusCode_", None)) and res.ContinuationPoint_:
            pending.append((index, res.ContinuationPoint_))

    pages = 0
    while pending and pages < _MAX_CONTINUATION_PAGES:
        params = ua.BrowseNextParameters()
        params.ReleaseContinuationPoints = False
        params.ContinuationPoints = [cp for _, cp in pending]
        try:
            next_results = list(await client.uaclient.browse_next(params))
        except Exception as exc:
            logger.warning("BrowseNext не удался: %s", exc)
            await _release_continuation_points(client, pending)
            break

        new_pending: List[tuple[int, bytes]] = []
        for (index, _), next_res in zip(pending, next_results):
            if _status_is_good(getattr(next_res, "StatusCode_", None)):
                references[index].extend(next_res.References)
                if next_res.ContinuationPoint_:
                    new_pending.append((index, next_res.ContinuationPoint_))
        pending = new_pending
        pages += 1

    if pending:
        await _release_continuation_points(client, pending)

    return references


async def _release_continuation_points(
    client: Client, pending: Sequence[tuple[int, bytes]]
) -> None:
    try:
        params = ua.BrowseNextParameters()
        params.ReleaseContinuationPoints = True
        params.ContinuationPoints = [cp for _, cp in pending]
        await client.uaclient.browse_next(params)
    except Exception:
        pass


async def read_values_many(
    client: Client,
    node_ids: Sequence[ua.NodeId],
    chunk_size: int = 500,
) -> Dict[str, ua.DataValue]:
    """
    Пакетное чтение атрибута Value: все узлы — за один (или несколько) Read.

    Возвращает словарь {str(NodeId): DataValue}. Неудачные чтения попадают
    в результат со статусом Bad — их отфильтрует datavalue_to_python.
    Если сервер отвергает большой пакет (MaxNodesPerRead), запрос дробится.
    """
    values: Dict[str, ua.DataValue] = {}
    if not node_ids:
        return values

    for start in range(0, len(node_ids), chunk_size):
        chunk = list(node_ids[start : start + chunk_size])
        try:
            values.update(await _read_chunk(client, chunk))
        except Exception as exc:
            logger.warning("Пакетное чтение %d узлов не удалось: %s", len(chunk), exc)

    return values


async def _read_chunk(
    client: Client, chunk: Sequence[ua.NodeId]
) -> Dict[str, ua.DataValue]:
    params = ua.ReadParameters()
    for node_id in chunk:
        read_id = ua.ReadValueId()
        read_id.NodeId = node_id
        read_id.AttributeId = ua.AttributeIds.Value
        params.NodesToRead.append(read_id)

    try:
        results = list(await client.uaclient.read(params))
    except Exception as exc:
        if len(chunk) <= 1:
            raise
        logger.debug("Read %d узлов не удался (%s) — дробим запрос", len(chunk), exc)
        middle = len(chunk) // 2
        values = await _read_chunk(client, chunk[:middle])
        values.update(await _read_chunk(client, chunk[middle:]))
        return values

    return {str(node_id): data_value for node_id, data_value in zip(chunk, results)}


def datavalue_to_python(data_value: Any, default: Any = None) -> Any:
    """Достаёт python-значение из DataValue, не бросая исключений."""
    if data_value is None:
        return default
    if not _status_is_good(getattr(data_value, "StatusCode_", None)):
        return default
    variant = getattr(data_value, "Value", None)
    if variant is None:
        return default
    return getattr(variant, "Value", variant)


# ---------------------------------------------------------------------------
# Поиск узлов
# ---------------------------------------------------------------------------


def _is_skipped_reference(reference: ua.ReferenceDescription) -> bool:
    ref_id = reference.ReferenceTypeId
    if ref_id is None:
        return False
    return (
        getattr(ref_id, "NamespaceIndex", 0) == 0
        and getattr(ref_id, "Identifier", None) in _SKIPPED_REFERENCE_IDS
    )


class OpcNodeFinder:
    """
    Ищет узлы по BrowseName в поддереве OPC UA.

    Типовое использование:

        finder = OpcNodeFinder(client)
        found = await finder.find_many({"Boiler", "GasAnalyser"})
        boiler = found.get("Boiler")

    Результат кэшируется: повторный поиск того же имени не делает запросов.
    Отрицательный результат кэшируется только если обход завершён
    (а не прерван лимитом max_nodes).
    """

    def __init__(
        self,
        client: Client,
        max_depth: int = 10,
        chunk_size: int = 64,
        max_nodes: int = 50000,
        node_class_mask: ua.NodeClass = ua.NodeClass.Object | ua.NodeClass.Variable,
        root: Optional[Node] = None,
        skip_properties: bool = True,
        max_references_per_node: int = 0,
    ):
        self.client = client
        self.max_depth = max_depth
        self.chunk_size = max(1, chunk_size)
        self.max_nodes = max_nodes
        self.node_class_mask = node_class_mask
        self._root = root
        self.skip_properties = skip_properties
        self.max_references_per_node = max_references_per_node

        self._traversable_classes = (ua.NodeClass.Object, ua.NodeClass.Variable)
        self._cache: Dict[str, Optional[Node]] = {}
        self.last_stats: Dict[str, Any] = {}

    @property
    def root(self) -> Node:
        return self._root if self._root is not None else self.client.nodes.objects

    def reset_cache(self) -> None:
        self._cache.clear()

    async def find(self, name: str) -> Optional[Node]:
        """Находит один узел по BrowseName или возвращает None."""
        found = await self.find_many([name])
        return found.get(name)

    async def find_many(self, names: Iterable[str]) -> Dict[str, Node]:
        """
        Находит несколько узлов за один проход по дереву.

        Возвращает только найденные узлы: {BrowseName: Node}.
        """
        requested: List[str] = []
        seen: Set[str] = set()
        for name in names:
            if name and name not in seen:
                seen.add(name)
                requested.append(name)

        result: Dict[str, Node] = {}
        remaining: Set[str] = set()

        for name in requested:
            if name in self._cache:
                cached = self._cache[name]
                if cached is not None:
                    result[name] = cached
            else:
                remaining.add(name)

        if remaining:
            found = await self._search(remaining)
            for name, node in found.items():
                self._cache[name] = node
                if node is not None:
                    result[name] = node

        return result

    async def _search(self, targets: Set[str]) -> Dict[str, Optional[Node]]:
        """BFS по уровням дерева; один Browse-запрос на чанк узлов уровня."""
        found: Dict[str, Optional[Node]] = {}
        remaining = set(targets)
        visited: Set[str] = set()
        level: List[Node] = [self.root]

        depth = 0
        browsed = 0
        truncated = False
        started_at = time.perf_counter()

        while level and depth < self.max_depth and remaining:
            current: List[Node] = []
            for node in level:
                key = str(node.nodeid)
                if key in visited:
                    continue
                visited.add(key)
                current.append(node)
            level = []

            if not current:
                break

            if browsed + len(current) > self.max_nodes:
                logger.warning(
                    "Поиск узлов прерван: превышен лимит max_nodes=%d", self.max_nodes
                )
                truncated = True
                break

            try:
                references_per_node = await browse_many(
                    self.client,
                    [node.nodeid for node in current],
                    reference_type=ua.ObjectIds.HierarchicalReferences,
                    include_subtypes=True,
                    node_class_mask=self.node_class_mask,
                    chunk_size=self.chunk_size,
                    max_references_per_node=self.max_references_per_node,
                )
            except Exception as exc:
                logger.warning("Ошибка browsing на глубине %d: %s", depth, exc)
                truncated = True
                break

            browsed += len(current)

            for references in references_per_node:
                for reference in references:
                    if not reference.IsForward:
                        continue
                    if self.skip_properties and _is_skipped_reference(reference):
                        continue

                    browse_name = reference.BrowseName
                    name = browse_name.Name if browse_name is not None else None
                    if name in remaining:
                        found[name] = self.client.get_node(reference.NodeId)
                        remaining.discard(name)

                    if reference.NodeClass_ in self._traversable_classes:
                        level.append(self.client.get_node(reference.NodeId))

                if not remaining:
                    break

            depth += 1

        elapsed_ms = (time.perf_counter() - started_at) * 1000
        self.last_stats = {
            "elapsed_ms": elapsed_ms,
            "targets": len(targets),
            "found": sum(1 for node in found.values() if node is not None),
            "nodes_browsed": browsed,
            "truncated": truncated,
        }

        logger.info(
            "Поиск узлов: найдено %d/%d за %.1f мс, просмотрено %d узлов%s",
            self.last_stats["found"],
            len(targets),
            elapsed_ms,
            browsed,
            " (прервано по лимиту)" if truncated else "",
        )

        if not truncated:
            for name in remaining:
                found[name] = None

        return found
