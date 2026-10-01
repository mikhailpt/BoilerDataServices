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

        # Маппинг команд на начальные регистры
        self.reg_map = {
            "O2": 3, "H2": 7, "CO": 11, "CO2": 15, "CH4": 19,
            "SO2": 23, "NO": 27, "NO2": 31, "N2O": 35,
            "Tv": 39, "P": 43
        }

    def _find_port(self):
        """Поиск COM-порта по VID/PID"""
        for port in serial.tools.list_ports.comports():
            if port.vid == 1027 and port.pid == 24577:
                _logger.info(f"Авто-обнаружен порт газоанализатора: {port.device}")
                return port.device
        _logger.warning("Порт газоанализатора не найден. Проверьте подключение.")
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
            _logger.info(f"Modbus инициализирован на {self.port}")
            return True
        except Exception as e:
            _logger.error(f"Ошибка инициализации Modbus: {e}")
            self.instrument = None
            return False

    def _read_registers_sync(self, start, count):
        """Синхронное чтение"""
        if not self._init_instrument():
            return None
        try:
            return self.instrument.read_registers(start, count)
        except Exception as e:
            _logger.error(f"Ошибка чтения Modbus: {e}")
            self.instrument = None  # Сброс для переподключения при следующей попытке
            return None

    @staticmethod
    def _regs_to_float(regs):
        if len(regs) != 2:
            return 0.0
        combined = (regs[0] << 16) | (regs[1] & 0xFFFF)
        return struct.unpack('>f', combined.to_bytes(4, byteorder='big'))[0]

    async def send_command(self, command: str) -> dict:
        now = time.time()
        # Если кеш просрочен или пуст -> обновляем его одним запросом ко всем регистрам
        if not self._cache or (now - self._cache_time) > self._cache_ttl:
            self._cache.clear()
            try:
                # Читаем 45 регистров за один раз в фоновом потоке
                all_regs = await asyncio.to_thread(self._read_registers_sync, 0, 45)
                
                if all_regs and len(all_regs) >= 45:
                    for cmd, start_reg in self.reg_map.items():
                        self._cache[cmd] = self._regs_to_float(all_regs[start_reg:start_reg+2])
                    self._cache_time = now
                else:
                    return {"success": False, "value": None}
            except Exception as e:
                _logger.error(f"Обновление кеша failed: {e}")
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

        # Маппинг команд на начальные регистры
        self.reg_map = {
            "O2": 3, "H2": 7, "CO": 11, "CO2": 15, "CH4": 19,
            "COн": 23, "COнн": 27, "NO": 31, "NO2": 35,
            "SO2": 39, "H2S": 43, "H2н": 47, "Tv": 51, "P": 55
        }

    def _find_port(self):
        """Поиск COM-порта по VID/PID"""
        for port in serial.tools.list_ports.comports():
            if (port.vid == 1027 and port.pid == 24577) or (port.vid == 1659 and port.pid == 8963):
                _logger.info(f"Авто-обнаружен порт газоанализатора: {port.device}")
                return port.device
        _logger.warning("Порт газоанализатора не найден. Проверьте подключение.")
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
            _logger.info(f"Modbus инициализирован на {self.port}")
            return True
        except Exception as e:
            _logger.error(f"Ошибка инициализации Modbus: {e}")
            self.instrument = None
            return False

    def _read_registers_sync(self, start, count):
        """Синхронное чтение"""
        if not self._init_instrument():
            return None
        try:
            return self.instrument.read_registers(start, count)
        except Exception as e:
            _logger.error(f"Ошибка чтения Modbus: {e}")
            self.instrument = None  # Сброс для переподключения при следующей попытке
            return None

    @staticmethod
    def _regs_to_float(regs):
        if len(regs) != 2:
            return 0.0
        combined = (regs[0] << 16) | (regs[1] & 0xFFFF)
        return struct.unpack('>f', combined.to_bytes(4, byteorder='big'))[0]

    async def send_command(self, command: str) -> dict:
        now = time.time()
        # Если кеш просрочен или пуст -> обновляем его одним запросом ко всем регистрам
        if not self._cache or (now - self._cache_time) > self._cache_ttl:
            self._cache.clear()
            try:
                # Читаем 57 регистров за один раз в фоновом потоке
                all_regs = await asyncio.to_thread(self._read_registers_sync, 0, 57)
                
                if all_regs and len(all_regs) >= 57:
                    for cmd, start_reg in self.reg_map.items():
                        self._cache[cmd] = self._regs_to_float(all_regs[start_reg:start_reg+2])
                    self._cache_time = now
                else:
                    return {"success": False, "value": None}
            except Exception as e:
                _logger.error(f"Обновление кеша failed: {e}")
                return {"success": False, "value": None}

        value = self._cache.get(command)
        if value is not None:
            return {"success": True, "value": value}
        return {"success": False, "value": None}


air = AirAnalyserBonerP23_08_Client()
print(air._find_port())
#for port in serial.tools.list_ports.comports():
#	print(f'dev={port.device}, vid={port.vid}, pid={port.pid}')

#while True:
all_regs = air._read_registers_sync(0, 57)
if all_regs and len(all_regs) >= 57:
    for cmd, start_reg in air.reg_map.items():
        val = air._regs_to_float(all_regs[start_reg: start_reg+2])
        print(f'{cmd} => {val}')

