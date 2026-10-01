import re
import asyncio

# ----- ЛОГИРОВАНИЕ -------
import logging
_logger = logging.getLogger(__name__)

# Константы протокола (из исходного кода vcontrold)
PROMPT = "> "  # Приглашение сервера
ERR_PREFIX = "ERR:"  # Префикс ошибки
BYE = "BYE"  # Ответ на quit

class VcontroldTelnetClient:
    def __init__(self, host='localhost', port=3002, prompt=None):
        self.host = host
        self.port = port
        self.prompt = (prompt or "> ").encode('utf-8')
        self.reader = None
        self.writer = None
        self._connected = False
        self._prompt_variants = [b"> ", b">", b"vctrld> ", b"vctrld>", b"# "]  # fallback-варианты

    async def connect(self):
        """Подключение с улучшенной синхронизацией"""
        try:
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
            await self._read_until_any_prompt()
            self._connected = True
            _logger.info(f"Подключено к vcontrold на {self.host}:{self.port}")
            return True
        except asyncio.TimeoutError:
            _logger.error(f"Таймаут при подключении к {self.host}:{self.port}")
        except Exception as e:
            _logger.error(f"Ошибка подключения к vcontrold: {e}")
        finally:
            if not self._connected:
                await self._force_close()  # Безопасное закрытие без quit
        return False

    async def _read_until_any_prompt(self) -> str:
        """Чтение до любого из известных вариантов приглашения"""
        buffer = bytearray()
        while True:
            chunk = await asyncio.wait_for(self.reader.read(1024), timeout=10.0)
            if not chunk:
                raise ConnectionError("Соединение закрыто сервером")

            buffer.extend(chunk)
            _logger.debug(f"RAW RECV: {buffer[-100:]}")  # Отладка: последние 100 байт

            for prompt in self._prompt_variants:
                if buffer.endswith(prompt):
                    response = buffer[:-len(prompt)].decode('utf-8', errors='replace').strip()
                    response = response.replace('\r', '').strip()
                    _logger.debug(f"✓ PROMPT DETECTED: {repr(prompt)}, response: {repr(response)}")
                    return response
            if len(buffer) > 4096:
                buffer = buffer[-2048:]

    async def disconnect(self):
        """Корректное закрытие: quit только если соединение живо"""
        if not self.writer or self.writer.is_closing() or self.writer.is_closed():
            await self._force_close()
            return

        try:
            if not self._connected:
                await self._force_close()
                return

            _logger.debug("Отправляем quit для корректного завершения...")
            self.writer.write(b"quit\n")
            await asyncio.wait_for(self.writer.drain(), timeout=2.0)

            try:
                await asyncio.wait_for(self._read_until_string(b"BYE"), timeout=2.0)
            except asyncio.TimeoutError:
                _logger.debug("Таймаут ожидания BYE, закрываем соединение")
        except Exception as e:
            _logger.debug(f"Информация: не удалось отправить quit корректно: {e}")
        finally:
            await self._force_close()

    async def _force_close(self):
        """Принудительное закрытие без отправки команд"""
        if self.writer and not self.writer.is_closed():
            self.writer.close()
            try:
                await asyncio.wait_for(self.writer.wait_closed(), timeout=2.0)
            except Exception as e:
                _logger.debug(f"Ошибка принудительного закрытия: {e}")
        self.reader = None
        self.writer = None
        self._connected = False

    async def send_command(self, command: str) -> dict:
        if not self._connected:
            if not await self.connect():
                return {'success': False, 'error': 'Connection failed'}

        try:
            self.writer.write((command + "\n").encode('utf-8'))
            await self.writer.drain()
            _logger.debug(f"SEND: {command}")

            raw_response = await self._read_until_any_prompt()
            raw_response = raw_response.replace('\r', '').strip()

            result = {'success': True, 'raw': raw_response, 'value': None, 'error': None}

            if raw_response.startswith("ERR:"):
                result['success'] = False
                result['error'] = raw_response
                _logger.warning(f"Ошибка от сервера: {raw_response}")
            elif raw_response:
                match = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", raw_response)
                if match:
                    try:
                        result['value'] = float(match.group())
                    except ValueError:
                        pass
            return result

        except asyncio.TimeoutError:
            _logger.error(f"Таймаут на команду '{command}'")
            self._connected = False
            return {'success': False, 'error': 'Timeout'}
        except (ConnectionError, OSError) as e:
            _logger.warning(f"Разрыв соединения: {e}")
            self._connected = False
            return {'success': False, 'error': f'Connection: {e}'}
        except Exception as e:
            _logger.error(f"Неожиданная ошибка: {e}", exc_info=True)
            self._connected = False
            return {'success': False, 'error': f'Unexpected: {e}'}

    async def __aenter__(self):
        """Поддержка async context manager"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()