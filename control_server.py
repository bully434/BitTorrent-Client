import asyncio

from control_manager import ControlManager

class ControlServer:
    def __init__(self, control):
        self.control = control
        self.server = None

        self.flag = False

    HANDSHAKE_MESSAGE = b'bit-torrent:ControlServer\n'
    PORT_RANGE = range(6881,6890)

    async def accept(self, reader, writer):
        addr_repr = ':'.join((map(str, writer.get_extra_info('peername'))))
        print('accepted connection from {}'.format(addr_repr))
        writer.write(ControlServer.HANDSHAKE_MESSAGE)

        try:
            for info_hash in self.control.torrents:
                if not self.flag:
                    await self.control.pause(info_hash)
                else:
                    self.control.resume(info_hash)
            self.flag = not self.flag
            writer.write(b'success\n')
        except Exception as e:
            writer.write(repr(e).encode() + b'\n')

    async def start(self):
        for port in ControlServer.PORT_RANGE:
            try:
                self.server = await asyncio.start_server(self.accept, host='127.0.0.1', port=port)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print('exception on starting server {}:{}'.format(port, repr(e)))
            else:
                print('server started on port {}'.format(port))
                return
        else:
            raise RuntimeError('Failed to start a control server')

    async def stop(self):
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            print('server stopped')