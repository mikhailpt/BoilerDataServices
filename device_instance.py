from asyncua import ua
import logging
import importlib

_logger = logging.getLogger(__name__)


class DeviceInstance:
    def __init__(self, device_id: str, device_config: dict, namespace_idx: int, parent_object):
        self.device_id = device_id
        self.config = device_config
        self.object_name = device_config["object_name"]
        self.nodes = {}
        self.client = None
        self.params_config = {}
        self.namespace_idx = namespace_idx
        self.parent_object = parent_object

    async def initialize(self):
        client_path = self.config["client"]
        module_path, class_name = client_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        client_class = getattr(module, class_name)
        self.client = client_class()
        _logger.info(f"[{self.device_id}] Client initialized: {client_path}")

        params_path = self.config["params"]
        module_path, params_name = params_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        self.params_config = getattr(module, params_name)

        device_object = await self.parent_object.add_object(
            self.namespace_idx,
            self.object_name
        )

        for param_name, param_config in self.params_config.items():
            try:
                node = await self._create_parameter_node(
                    device_object,
                    param_name,
                    param_config
                )
                self.nodes[param_name] = node
                _logger.debug(f"[{self.device_id}] Created node: {param_name}")
            except Exception as e:
                _logger.error(f"[{self.device_id}] Node creation error {param_name}: {e}")

    @staticmethod
    async def _create_parameter_node(parent, name: str, config: dict):
        node = await parent.add_variable(
            config.get("namespace_idx", 2),
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

    async def health_check(self) -> bool:
        if not self.client:
            return False
        if hasattr(self.client, 'is_connected'):
            return self.client.is_connected()
        return True

    async def _recreate_client(self):
        _logger.warning(f"[{self.device_id}] Recreating client...")
        client_path = self.config["client"]
        module_path, class_name = client_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        client_class = getattr(module, class_name)
        self.client = client_class()
        _logger.info(f"[{self.device_id}] Client recreated: {client_path}")

    async def update_parameters(self):
        if not self.client:
            return

        if not await self.health_check():
            _logger.warning(f"[{self.device_id}] Health check failed, recreating client")
            await self._recreate_client()
            return

        for param_name, param_config in self.params_config.items():
            try:
                command = param_config.get("command")
                if command:
                    response = await self.client.send_command(command)
                    if response["success"] and response["value"] is not None:
                        value = self._convert_value(response["value"], param_config["data_type"])
                        await self._write_node_value(param_name, value)

            except Exception as e:
                _logger.error(f"[{self.device_id}] Parameter update error {param_name}: {e}")
                continue

    @staticmethod
    def _convert_value(value, target_type: ua.VariantType):
        if target_type == ua.VariantType.Int32:
            return int(round(float(value)))
        elif target_type == ua.VariantType.Boolean:
            return bool(int(value))
        elif target_type == ua.VariantType.Float:
            return float(value)
        return value

    async def _write_node_value(self, param_name: str, value):
        node = self.nodes.get(param_name)
        if node:
            await node.write_value(value)
            _logger.debug(f"[{self.device_id}] {param_name} = {value}")
        else:
            _logger.warning(f"[{self.device_id}] Node {param_name} not found")