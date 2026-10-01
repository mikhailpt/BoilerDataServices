import asyncio
import logging
from asyncua import Server
from device_instance import DeviceInstance

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)
logging.getLogger('asyncua.server.address_space').setLevel(logging.WARNING)

OPCUA_PORT = 4840
UPDATE_INTERVAL = 5

# Импорт конфигурации устройств
from devices import DEVICES
#from moq.devices import DEVICES

class OpcUaServer:
    """Основной сервер с поддержкой динамического добавления устройств"""

    def __init__(self):
        self.server = None
        self.devices: dict[str, DeviceInstance] = {}  # { device_id: DeviceInstance }

    async def setup(self):
        """Инициализация сервера и всех включённых устройств"""
        self.server = Server()
        await self.server.init()
        self.server.set_endpoint(f"opc.tcp://0.0.0.0:{OPCUA_PORT}/")

        # Регистрация пространства имён
        namespace_idx = await self.server.register_namespace(
            "https://examples.freeopcua.github.io/"
        )

        # Создаём устройства только если они enabled
        for device_id, device_config in DEVICES.items():
            if not device_config.get("enabled", True):
                _logger.info(f"Устройство {device_id} отключено в конфиге — пропускаем")
                continue

            try:
                device = DeviceInstance(
                    device_id=device_id,
                    device_config=device_config,
                    namespace_idx=namespace_idx,
                    parent_object=self.server.nodes.objects
                )
                await device.initialize()
                self.devices[device_id] = device
                _logger.info(f"Устройство инициализировано: {device_id}")
            except Exception as e:
                _logger.error(f"Ошибка инициализации устройства {device_id}: {e}")

        _logger.info(f"Сервер запущен на opc.tcp://0.0.0.0:{OPCUA_PORT}/")
        _logger.info(f"Активных устройств: {len(self.devices)}")

    async def update_loop(self):
        """Цикл опроса всех устройств"""
        iteration = 0
        while True:
            iteration += 1
            _logger.debug(f"Цикл обновления #{iteration}")

            for device_id, device in self.devices.items():
                try:
                    await device.update_parameters()
                except Exception as e:
                    _logger.error(f"[{device_id}] Критическая ошибка в update_parameters: {e}")
                    continue  # Не останавливаем другие устройства

            _logger.info(f"Цикл завершён | Устройств: {len(self.devices)} | "
                         f"Всего узлов: {sum(len(d.nodes) for d in self.devices.values())}")
            await asyncio.sleep(UPDATE_INTERVAL)

    async def run(self):
        """Точка входа"""
        await self.setup()

        async with self.server:
            update_task = asyncio.create_task(self.update_loop())
            try:
                while True:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                _logger.info("Остановка сервера...")
                update_task.cancel()
                await update_task


if __name__ == "__main__":
    try:
        asyncio.run(OpcUaServer().run())
    except KeyboardInterrupt:
        print("\nСервер остановлен пользователем.")