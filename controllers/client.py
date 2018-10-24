import asyncio

from controllers.server import ControlServer


class ControlClient:
    def __init__(self):
        self._reader = None
        self._writer = None

    async def connect(self):
        for port in ControlServer.PORT_RANGE:
            try:
                self._reader, self._writer = await asyncio.open_connection(host=ControlServer.HOST, port=port)

                message = await self._reader.readexactly(len(ControlServer.HANDSHAKE_MESSAGE))
                if message != ControlServer.HANDSHAKE_MESSAGE:
                    raise ValueError('Unknown protocol')
            except Exception as e:
                self.close()
                self._reader = None
                self._writer = None
                print('failed to connect to port {}:{}'.format(port, repr(e)))
            else:
                break
        else:
            raise RuntimeError('Failed to connect to server')

    async def execute(self, action):
        ControlServer.send_object(action, self._writer)
        result = await ControlServer.receive_object(self._reader)

        if isinstance(result, Exception):
            raise result
        return result

    def close(self):
        if self._writer is not None:
            self._writer.close()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
