import asyncio
import struct
import time
import os
import logging
import serial
import serial.tools.list_ports
import minimalmodbus

_logger = logging.getLogger(__name__)


class AirAnalyserBonerP20_001_Client:
    def __init__(self):
        self.port = os.getenv("AIR_ANALYSER_PORT", "auto")
        self.baudrate = int(os.getenv("AIR_ANALYSER_BAUD", "9600"))
        self.address = int(os.getenv("AIR_ANALYSER_ADDR", "1"))
        self.timeout = float(os.getenv("AIR_ANALYSER_TIMEOUT", "1.0"))

        self.instrument = None
        self._cache = {}
        self._cache_time = 0
        self._cache_ttl = 4.0
        self._reconnect_task: asyncio.Task | None = None
        self._reconnect_backoff = 5
        self._max_backoff = 60

        self.reg_map = {
            "O2": 3, "H2": 7, "CO": 11, "CO2": 15, "CH4": 19,
            "SO2": 23, "NO": 27, "NO2": 31, "N2O": 35,
            "Tv": 39, "P": 43
        }

    def _find_port(self):
        for port in serial.tools.list_ports.comports():
            if port.vid == 1027 and port.pid == 24577:
                _logger.info(f"Auto-detected gas analyser port: {port.device}")
                return port.device
        _logger.warning("Gas analyser port not found. Check connection.")
        return None

    def _init_instrument(self):
        if self.instrument is not None:
            return True
        if self.port == "auto":
            self.port = self._find_port()
        if not self.port:
            return False

        try:
            self.instrument = minimalmodbus.Instrument(self.port, self.address, mode="rtu")
            self.instrument.serial.baudrate = self.baudrate
            self.instrument.serial.bytesize = 8
            self.instrument.serial.parity = serial.PARITY_NONE
            self.instrument.serial.stopbits = 1
            self.instrument.serial.timeout = self.timeout
            _logger.info(f"Modbus initialized on {self.port}")
            if self._reconnect_task:
                self._reconnect_task.cancel()
                self._reconnect_task = None
            self._reconnect_backoff = 5
            return True
        except Exception as e:
            _logger.error(f"Modbus init error: {e}")
            self.instrument = None
            return False

    def is_connected(self) -> bool:
        return self.instrument is not None

    def _schedule_reconnect(self):
        if self._reconnect_task and not self._reconnect_task.done():
            return
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self):
        _logger.info("Starting Modbus reconnect loop (P20_001)")
        while self.instrument is None:
            _logger.info(f"Attempting reconnect in {self._reconnect_backoff}s...")
            await asyncio.sleep(self._reconnect_backoff)
            _logger.info("Reconnecting Modbus...")
            if self._init_instrument():
                _logger.info("Modbus reconnect successful")
                break
            self._reconnect_backoff = min(self._reconnect_backoff * 2, self._max_backoff)
        self._reconnect_task = None

    def _read_registers_sync(self, start, count):
        if not self._init_instrument():
            return None
        try:
            return self.instrument.read_registers(start, count)
        except Exception as e:
            _logger.error(f"Modbus read error: {e}")
            self.instrument = None
            self._schedule_reconnect()
            return None

    @staticmethod
    def _regs_to_float(regs):
        if len(regs) != 2:
            return 0.0
        combined = (regs[0] << 16) | (regs[1] & 0xFFFF)
        return struct.unpack('>f', combined.to_bytes(4, byteorder='big'))[0]

    async def send_command(self, command: str) -> dict:
        now = time.time()
        if not self._cache or (now - self._cache_time) > self._cache_ttl:
            self._cache.clear()
            try:
                all_regs = await asyncio.to_thread(self._read_registers_sync, 0, 45)

                if all_regs and len(all_regs) >= 45:
                    for cmd, start_reg in self.reg_map.items():
                        self._cache[cmd] = self._regs_to_float(all_regs[start_reg:start_reg+2])
                    self._cache_time = now
                else:
                    return {"success": False, "value": None}
            except Exception as e:
                _logger.error(f"Cache update failed: {e}")
                return {"success": False, "value": None}

        value = self._cache.get(command)
        if value is not None:
            return {"success": True, "value": value}
        return {"success": False, "value": None}


class AirAnalyserBonerP23_08_Client:
    def __init__(self):
        self.port = os.getenv("AIR_ANALYSER_PORT", "auto")
        self.baudrate = int(os.getenv("AIR_ANALYSER_BAUD", "9600"))
        self.address = int(os.getenv("AIR_ANALYSER_ADDR", "1"))
        self.timeout = float(os.getenv("AIR_ANALYSER_TIMEOUT", "1.0"))

        self.instrument = None
        self._cache = {}
        self._cache_time = 0
        self._cache_ttl = 4.0
        self._reconnect_task: asyncio.Task | None = None
        self._reconnect_backoff = 5
        self._max_backoff = 60

        self.reg_map = {
            "O2": 3, "H2": 7, "CO": 11, "CO2": 15, "CH4": 19,
            "COн": 23, "COнн": 27, "NO": 31, "NO2": 35,
            "SO2": 39, "H2S": 43, "H2н": 47, "Tv": 51, "P": 55
        }

    def _find_port(self):
        for port in serial.tools.list_ports.comports():
            if (port.vid == 1027 and port.pid == 24577) or (port.vid == 1659 and port.pid == 8963):
                _logger.info(f"Auto-detected gas analyser port: {port.device}")
                return port.device
        _logger.warning("Gas analyser port not found. Check connection.")
        return None

    def _init_instrument(self):
        if self.instrument is not None:
            return True
        if self.port == "auto":
            self.port = self._find_port()
        if not self.port:
            return False

        try:
            self.instrument = minimalmodbus.Instrument(self.port, self.address, mode="rtu")
            self.instrument.serial.baudrate = self.baudrate
            self.instrument.serial.bytesize = 8
            self.instrument.serial.parity = serial.PARITY_NONE
            self.instrument.serial.stopbits = 1
            self.instrument.serial.timeout = self.timeout
            _logger.info(f"Modbus initialized on {self.port}")
            if self._reconnect_task:
                self._reconnect_task.cancel()
                self._reconnect_task = None
            self._reconnect_backoff = 5
            return True
        except Exception as e:
            _logger.error(f"Modbus init error: {e}")
            self.instrument = None
            return False

    def is_connected(self) -> bool:
        return self.instrument is not None

    def _schedule_reconnect(self):
        if self._reconnect_task and not self._reconnect_task.done():
            return
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self):
        _logger.info("Starting Modbus reconnect loop (P23_08)")
        while self.instrument is None:
            _logger.info(f"Attempting reconnect in {self._reconnect_backoff}s...")
            await asyncio.sleep(self._reconnect_backoff)
            _logger.info("Reconnecting Modbus...")
            if self._init_instrument():
                _logger.info("Modbus reconnect successful")
                break
            self._reconnect_backoff = min(self._reconnect_backoff * 2, self._max_backoff)
        self._reconnect_task = None

    def _read_registers_sync(self, start, count):
        if not self._init_instrument():
            return None
        try:
            return self.instrument.read_registers(start, count)
        except Exception as e:
            _logger.error(f"Modbus read error: {e}")
            self.instrument = None
            self._schedule_reconnect()
            return None

    @staticmethod
    def _regs_to_float(regs):
        if len(regs) != 2:
            return 0.0
        combined = (regs[0] << 16) | (regs[1] & 0xFFFF)
        return struct.unpack('>f', combined.to_bytes(4, byteorder='big'))[0]

    async def send_command(self, command: str) -> dict:
        now = time.time()
        if not self._cache or (now - self._cache_time) > self._cache_ttl:
            self._cache.clear()
            try:
                all_regs = await asyncio.to_thread(self._read_registers_sync, 0, 57)

                if all_regs and len(all_regs) >= 57:
                    for cmd, start_reg in self.reg_map.items():
                        self._cache[cmd] = self._regs_to_float(all_regs[start_reg:start_reg+2])
                    self._cache_time = now
                else:
                    return {"success": False, "value": None}
            except Exception as e:
                _logger.error(f"Cache update failed: {e}")
                return {"success": False, "value": None}

        value = self._cache.get(command)
        if value is not None:
            return {"success": True, "value": value}
        return {"success": False, "value": None}


air = AirAnalyserBonerP23_08_Client()
print(air._find_port())

all_regs = air._read_registers_sync(0, 57)
if all_regs and len(all_regs) >= 57:
    for cmd, start_reg in air.reg_map.items():
        val = air._regs_to_float(all_regs[start_reg: start_reg+2])
        print(f'{cmd} => {val}')