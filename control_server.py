import asyncio
import pickle
import struct

from control_manager import ControlManager

class ControlServer:
    def __init__(self, control):
        self.control = control
        self.server = None

        self.flag = False

    HANDSHAKE_MESSAGE = b'bit-torrent:ControlServer\n'
    PORT_RANGE = range(6881,6890)
    LENGTH_FORMAT = '!I'
    HOST = '127.0.0.1'

    @staticmethod
    async def receive_object(reader):
        length_data = await reader.readexactly(struct.calcsize(ControlServer.LENGTH_FORMAT))
        (length, ) = struct.unpack(ControlServer.LENGTH_FORMAT, length_data)
        data = await reader.readexactly(length)
        return pickle.loads(data)

    @staticmethod
    def send_object(obj, writer):
        data = pickle.dumps(obj)
        length_data = struct.pack(ControlServer.LENGTH_FORMAT, len(data))
        writer.write(length_data)
        writer.write(data)

    async def accept(self, reader, writer):
        addr_repr = ':'.join((map(str, writer.get_extra_info('peername'))))
        print('accepted connection from {}'.format(addr_repr))

        try:
            writer.write(ControlServer.HANDSHAKE_MESSAGE)
            while True:
                action = await ControlServer.receive_object(reader)
                try:
                    result = action(self.control)
                    if asyncio.iscoroutine(result):
                        result = await result
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    result = e

                ControlServer.send_object(result, writer)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print('{} disconnected because of {}'.format(addr_repr, repr(e)))
        finally:
            writer.close()

    async def start(self):
        for port in ControlServer.PORT_RANGE:
            try:
                self.server = await asyncio.start_server(self.accept, host=ControlServer.HOST, port=port)
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