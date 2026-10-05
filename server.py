import asyncio
import logging
from asyncua import Server
from device_instance import DeviceInstance

_logger = logging.getLogger(__name__)

OPCUA_PORT = 4840
UPDATE_INTERVAL = 5

from devices import DEVICES


def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler("server.log", encoding="utf-8")
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
    logging.getLogger('asyncua.server.address_space').setLevel(logging.WARNING)


class OpcUaServer:
    def __init__(self):
        self.server = None
        self.devices: dict[str, DeviceInstance] = {}

    async def setup(self):
        self.server = Server()
        await self.server.init()
        self.server.set_endpoint(f"opc.tcp://0.0.0.0:{OPCUA_PORT}/")

        namespace_idx = await self.server.register_namespace(
            "https://examples.freeopcua.github.io/"
        )

        for device_id, device_config in DEVICES.items():
            if not device_config.get("enabled", True):
                _logger.info(f"Device {device_id} disabled in config - skipping")
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
                _logger.info(f"Device initialized: {device_id}")
            except Exception as e:
                _logger.error(f"Device init error {device_id}: {e}")

        _logger.info(f"Server started on opc.tcp://0.0.0.0:{OPCUA_PORT}/")
        _logger.info(f"Active devices: {len(self.devices)}")

    async def update_loop(self):
        iteration = 0
        while True:
            iteration += 1
            _logger.debug(f"Update cycle #{iteration}")

            for device_id, device in self.devices.items():
                try:
                    if not await device.health_check():
                        _logger.warning(f"[{device_id}] Health check failed, recreating client")
                        await device._recreate_client()
                        continue
                    await device.update_parameters()
                except Exception as e:
                    _logger.error(f"[{device_id}] Critical error in update_parameters: {e}")
                    continue

            _logger.info(f"Cycle completed | Devices: {len(self.devices)} | "
                         f"Total nodes: {sum(len(d.nodes) for d in self.devices.values())}")
            await asyncio.sleep(UPDATE_INTERVAL)

    async def run(self):
        await self.setup()

        async with self.server:
            update_task = asyncio.create_task(self.update_loop())
            try:
                while True:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                _logger.info("Stopping server...")
                update_task.cancel()
                await update_task


if __name__ == "__main__":
    setup_logging()
    try:
        asyncio.run(OpcUaServer().run())
    except KeyboardInterrupt:
        print("\nServer stopped by user.")