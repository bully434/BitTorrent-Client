import asyncio

from models import Peer
from peer_tcp import PeerTCP


class PeerTCPServer:
    def __init__(self, peer_id, torrent_managers):
        self.peer_id = peer_id
        self.torrent_managers = torrent_managers

        self.server = None
        self.port = None

    async def accept(self, reader, writer):
        addr = writer.get_extra_info('peername')
        peer = Peer(addr[0], addr[1])

        client = PeerTCP(self.peer_id, peer)

        try:
            info_hash = await client.accept(reader, writer)
            if info_hash not in self.torrent_managers:
                raise ValueError('Unknown info hash')
        except Exception as e:
            client.close()

            if isinstance(e, asyncio.CancelledError):
                raise
            else:
                print('{} was not accepted because of {}'.format(peer, repr(e)))
        else:
            self.torrent_managers[info_hash].accept_client(peer, client)

    PORT_RANGE = range(6881, 6890)

    async def start(self):
        for port in PeerTCPServer.PORT_RANGE:
            try:
                self.server = await asyncio.start_server(self.accept, port=port)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print('exception of server {}:{}'.format(port, repr(e)))
            else:
                self.port = port
                print('server started on port {}'.format(port))
                return
        else:
            print('failed to start server')

    async def stop(self):
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            print('server stopped')
