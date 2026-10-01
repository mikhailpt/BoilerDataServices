import asyncio
import logging

from devices_clients.trm138_ import Device

_logger = logging.getLogger(__name__)


class TRM138Client:
    """
    реализует send_command(command: str) -> dict
    """

    # Маппинг команд в параметры метода read()/write()
    CMD_MAP = {
        "read_temp": {"d_type": "temp", "write": False, "ch_param": True},
        "read_temp_fp": {"d_type": "temp_fp", "write": False, "ch_param": True},
        "read_error": {"d_type": "error", "write": False, "ch_param": True},
        "read_set_point": {"d_type": "set_point", "write": False, "ch_param": True},
        "read_hyst": {"d_type": "hyst", "write": False, "ch_param": True},
        "read_out_state": {"d_type": "out_state", "write": False, "ch_param": True},
        "write_set_point": {"d_type": "set_point", "write": True, "ch_param": True},
        "write_op_set_point": {"d_type": "op_set_point", "write": True, "ch_param": True},
        "write_hyst": {"d_type": "hyst", "write": True, "ch_param": True},
        "write_out_state": {"d_type": "out_state", "write": True, "ch_param": True},
    }

    def __init__(self, port: str = "/dev/ttyUSB0", addr: int = 16, baud: int = 9600):
        self.port = port
        self.addr = addr
        self.baud = baud
        self.device: Device = None
        self._connected = False
        self._init_device()

    def _init_device(self):
        try:
            # Ваш Device ожидает аргумент addr=...
            self.device = Device(addr=self.addr)
            # Если нужно переопределить порт/скорость — можно добавить логику
            # например, через патчинг device.dev_port или device.br
            self._connected = True
            _logger.info(f"TRM138 клиент инициализирован: addr={self.addr}")
        except Exception as e:
            _logger.error(f"Ошибка инициализации TRM138: {e}")
            self._connected = False

    def _sync_call(self, command: str) -> any:
        """
        Синхронный вызов вашего кода (выполняется в отдельном потоке).
        Формат команды: "cmd_name:channel[:value]"
        """
        parts = command.strip().split(":")
        cmd_key = parts[0]
        channel = int(parts[1]) if len(parts) > 1 else 1
        value = float(parts[2]) if len(parts) > 2 else None

        if cmd_key not in self.CMD_MAP:
            raise ValueError(f"Неизвестная команда: {cmd_key}")

        cfg = self.CMD_MAP[cmd_key]

        if not self.device or not self._connected:
            self._init_device()
            if not self._connected:
                raise ConnectionError("Не удалось подключиться к устройству")

        if cfg["write"]:
            # Запись: вызываем device.write(data, d_type=..., ch=...)
            result = self.device.write(int(value), d_type=cfg["d_type"], ch=channel)
            return result
        else:
            # Чтение: вызываем device.read(d_type=..., ch=...)
            # После чтения значение берётся из соответствующего массива устройства
            self.device.read(d_type=cfg["d_type"], ch=channel)

            # Извлекаем результат из нужного атрибута устройства
            attr_map = {
                "temp": "temp", "temp_fp": "temp", "error": "error",
                "set_point": "set_point", "op_set_point": "set_point",
                "hyst": "hyst", "out_state": "out_state"
            }
            attr_name = attr_map.get(cfg["d_type"], "temp")
            result = getattr(self.device, attr_name)[channel - 1]
            return result

    async def send_command(self, command: str) -> dict:
        """
        Асинхронный интерфейс для DeviceInstance.
        Формат команды: "command_name:channel[:value]"
        Примеры:
            "read_temp_fp:1"        → чтение температуры канала 1
            "read_set_point:3"      → чтение уставки канала 3
            "write_set_point:2:30"  → запись уставки 30 в канал 2
        """
        try:
            result = await asyncio.to_thread(self._sync_call, command)
            return {
                "success": self.device.state == 1 if self.device else False,
                "value": result,
                "error": None,
                "raw": result
            }
        except asyncio.CancelledError:
            raise
        except ValueError as e:
            _logger.error(f"TRM138 ошибка парсинга команды '{command}': {e}")
            return {"success": False, "error": f"Parse: {e}", "value": None}
        except Exception as e:
            _logger.error(f"TRM138 ошибка выполнения '{command}': {e}")
            if self.device:
                self._connected = False  # Сброс для переподключения при следующей команде
            return {"success": False, "error": str(e), "value": None}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass