import re
import asyncio
import logging

_logger = logging.getLogger(__name__)

PROMPT = "> "
ERR_PREFIX = "ERR:"
BYE = "BYE"


class VcontroldTelnetClient:
    def __init__(self, host='localhost', port=3002, prompt=None):
        self.host = host
        self.port = port
        self.prompt = (prompt or "> ").encode('utf-8')
        self.reader = None
        self.writer = None
        self._connected = False
        self._prompt_variants = [b"> ", b">", b"vctrld> ", b"vctrld>", b"# "]
        self._reconnect_task: asyncio.Task | None = None
        self._reconnect_backoff = 5
        self._max_backoff = 60

    async def connect(self):
        try:
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
            await self._read_until_any_prompt()
            self._connected = True
            _logger.info(f"Connected to vcontrold at {self.host}:{self.port}")
            if self._reconnect_task:
                self._reconnect_task.cancel()
                self._reconnect_task = None
            self._reconnect_backoff = 5
            return True
        except asyncio.TimeoutError:
            _logger.error(f"Timeout connecting to {self.host}:{self.port}")
        except Exception as e:
            _logger.error(f"Connection error to vcontrold: {e}")
        finally:
            if not self._connected:
                await self._force_close()
        return False

    async def _read_until_any_prompt(self) -> str:
        buffer = bytearray()
        while True:
            chunk = await asyncio.wait_for(self.reader.read(1024), timeout=10.0)
            if not chunk:
                raise ConnectionError("Connection closed by server")

            buffer.extend(chunk)
            _logger.debug(f"RAW RECV: {buffer[-100:]}")

            for prompt in self._prompt_variants:
                if buffer.endswith(prompt):
                    response = buffer[:-len(prompt)].decode('utf-8', errors='replace').strip()
                    response = response.replace('\r', '').strip()
                    _logger.debug(f"PROMPT DETECTED: {repr(prompt)}, response: {repr(response)}")
                    return response
            if len(buffer) > 4096:
                buffer = buffer[-2048:]

    async def disconnect(self):
        if not self.writer or self.writer.is_closing() or self.writer.is_closed():
            await self._force_close()
            return

        try:
            if not self._connected:
                await self._force_close()
                return

            _logger.debug("Sending quit for graceful shutdown...")
            self.writer.write(b"quit\n")
            await asyncio.wait_for(self.writer.drain(), timeout=2.0)

            try:
                await asyncio.wait_for(self._read_until_string(b"BYE"), timeout=2.0)
            except asyncio.TimeoutError:
                _logger.debug("Timeout waiting for BYE, closing connection")
        except Exception as e:
            _logger.debug(f"Could not send quit gracefully: {e}")
        finally:
            await self._force_close()

    async def _read_until_string(self, target: bytes) -> str:
        buffer = bytearray()
        while True:
            chunk = await self.reader.read(1024)
            if not chunk:
                raise ConnectionError("Connection closed")
            buffer.extend(chunk)
            if target in buffer:
                idx = buffer.index(target)
                return buffer[:idx].decode('utf-8', errors='replace')

    async def _force_close(self):
        if self.writer and not self.writer.is_closed():
            self.writer.close()
            try:
                await asyncio.wait_for(self.writer.wait_closed(), timeout=2.0)
            except Exception as e:
                _logger.debug(f"Force close error: {e}")
        self.reader = None
        self.writer = None
        self._connected = False

    def is_connected(self) -> bool:
        if not self._connected:
            return False
        if not self.writer:
            return False
        if self.writer.is_closing() or self.writer.is_closed():
            return False
        return True

    def _schedule_reconnect(self):
        if self._reconnect_task and not self._reconnect_task.done():
            return
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self):
        _logger.info("Starting reconnect loop for vcontrold")
        while not self._connected:
            _logger.info(f"Attempting reconnect in {self._reconnect_backoff}s...")
            await asyncio.sleep(self._reconnect_backoff)
            _logger.info("Reconnecting to vcontrold...")
            if await self.connect():
                _logger.info("Reconnect successful")
                break
            self._reconnect_backoff = min(self._reconnect_backoff * 2, self._max_backoff)
        self._reconnect_task = None

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
                _logger.warning(f"Server error: {raw_response}")
            elif raw_response:
                match = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", raw_response)
                if match:
                    try:
                        result['value'] = float(match.group())
                    except ValueError:
                        pass
            return result

        except asyncio.TimeoutError:
            _logger.error(f"Timeout on command '{command}'")
            self._connected = False
            self._schedule_reconnect()
            return {'success': False, 'error': 'Timeout'}
        except (ConnectionError, OSError) as e:
            _logger.warning(f"Connection lost: {e}")
            self._connected = False
            self._schedule_reconnect()
            return {'success': False, 'error': f'Connection: {e}'}
        except Exception as e:
            _logger.error(f"Unexpected error: {e}", exc_info=True)
            self._connected = False
            self._schedule_reconnect()
            return {'success': False, 'error': f'Unexpected: {e}'}

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()