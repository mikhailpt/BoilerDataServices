from asyncua import ua
import logging
import importlib

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

class DeviceInstance:
    """Класс-обёртка для экземпляра устройства: клиент + узлы + конфиг"""

    def __init__(self, device_id: str, device_config: dict, namespace_idx: int, parent_object):
        self.device_id = device_id
        self.config = device_config
        self.object_name = device_config["object_name"]
        self.nodes = {}  # { param_name: node_object }
        self.client = None
        self.params_config = {}
        self.namespace_idx = namespace_idx
        self.parent_object = parent_object

    async def initialize(self):
        """Инициализация клиента и создание узлов устройства"""
        # 1. Динамический импорт клиента
        client_path = self.config["client"]
        module_path, class_name = client_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        client_class = getattr(module, class_name)
        self.client = client_class()
        _logger.info(f"[{self.device_id}] Клиент инициализирован: {client_path}")

        # 2. Динамический импорт параметров
        params_path = self.config["params"]
        module_path, params_name = params_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        self.params_config = getattr(module, params_name)

        # 3. Создание объекта устройства в OPC UA
        device_object = await self.parent_object.add_object(
            self.namespace_idx,
            self.object_name
        )

        # 4. Создание узлов параметров
        for param_name, param_config in self.params_config.items():
            try:
                node = await self._create_parameter_node(
                    device_object,
                    param_name,
                    param_config
                )
                self.nodes[param_name] = node
                _logger.debug(f"[{self.device_id}] Создан узел: {param_name}")
            except Exception as e:
                _logger.error(f"[{self.device_id}] Ошибка создания узла {param_name}: {e}")

    @staticmethod
    async def _create_parameter_node(parent, name: str, config: dict):
        """Создание отдельного параметра-узла"""
        node = await parent.add_variable(
            config.get("namespace_idx", 2),  # можно переопределять в конфиге
            name,
            config["default"],
            datatype=config["data_type"]
        )

        if config.get("writable", False):
            await node.set_writable()

        if "description" in config:
            await node.add_property(
                config.get("namespace_idx", 2),
                "Description",
                config["description"]
            )

        if "unit" in config:
            await node.add_property(
                config.get("namespace_idx", 2),
                "EngineeringUnits",
                config["unit"]
            )

        return node

    async def update_parameters(self):
        """Обновление значений параметров для этого устройства"""
        if not self.client:
            return

        for param_name, param_config in self.params_config.items():
            try:
                # ⚠ПОКА ПРОПУСКАЕМ реальный вызов клиента (по запросу)
                # Если нужно протестировать — раскомментируйте:
                command = param_config.get("command")
                if command:
                    response = await self.client.send_command(command)
                    if response["success"] and response["value"] is not None:
                        value = self._convert_value(response["value"], param_config["data_type"])
                        await self._write_node_value(param_name, value)

                # MOCK-режим: эмуляция обновления (можно убрать при интеграции)
                # await self._mock_update(param_name, param_config)

            except Exception as e:
                _logger.error(f"[{self.device_id}] Ошибка обновления {param_name}: {e}")
                continue

    @staticmethod
    def _convert_value(value, target_type: ua.VariantType):
        """Конвертация значения в целевой тип данных OPC UA"""
        if target_type == ua.VariantType.Int32:
            return int(round(float(value)))
        elif target_type == ua.VariantType.Boolean:
            return bool(int(value))
        elif target_type == ua.VariantType.Float:
            return float(value)
        return value

    async def _write_node_value(self, param_name: str, value):
        """Запись значения в узел с проверкой существования"""
        node = self.nodes.get(param_name)
        if node:
            await node.write_value(value)
            _logger.debug(f"[{self.device_id}] {param_name} = {value}")
        else:
            _logger.warning(f"[{self.device_id}] Узел {param_name} не найден")

    async def _mock_update(self, param_name: str, param_config: dict):
        """MOCK: эмуляция получения данных (для тестов без реального девайса)"""
        # Генерируем "плавающее" значение для демонстрации
        import random
        default = param_config["default"]
        if isinstance(default, (int, float)):
            mock_value = default + random.uniform(-0.5, 0.5)
            mock_value = self._convert_value(mock_value, param_config["data_type"])
            await self._write_node_value(param_name, mock_value)