import struct
import random
import asyncio

from enum import Enum
from models import Peer, grouper

__all__ = ['UDPTracker']


class TrackerError(Exception):
    pass


class EventType(Enum):
    none = 0
    completed = 1
    started = 2
    stopped = 3


class ActionType(Enum):
    connect = 0
    announce = 1
    scrape = 2
    error = 3


def pack(*data):
    assert len(data) % 2 == 0
    format = '!' + ''.join(fmt for fmt in data[::2])
    values = [e for e in data[1::2]]
    return struct.pack(format, *values)


def humanize_size(size):
    return '{:.1f} Mb'.format(size / UDPTracker.BPMb)


class DatagramProtocol:
    def __init__(self):
        self._buffer = bytearray()
        self._waiter = None
        self._connection_lost = False
        self._exception = None

    def connection_made(self, transport):
        pass

    async def receive(self):
        if self._waiter is not None:
            raise RuntimeError('Another coroutine is already waiting for incoming data')
        if not self._connection_lost and not self._buffer:
            self._waiter = asyncio.Future()
            try:
                await self._waiter
            finally:
                self._waiter = None
        if self._connection_lost:
            if self._exception is None:
                raise ConnectionResetError('Connection lost')
            else:
                raise self._exception
        buffer = self._buffer
        self._buffer = bytearray()
        return buffer

    def _wakeup_waiter(self):
        if self._waiter is not None:
            self._waiter.set_result(None)

    def datagram_received(self, data, addr):
        self._buffer.extend(data)
        self._wakeup_waiter()

    def error_received(self, exc):
        self._exception = exc
        self._wakeup_waiter()

    def connection_lost(self, exc):
        self._connection_lost = True
        self._exception = exc
        self._wakeup_waiter()


class UDPTracker:
    BPMb = 2 ** 20

    def __init__(self, url, download_info, client_peer_id, *, loop=None):
        self.download_info = download_info
        self.statistics = self.download_info.session_stats
        self.peer_id = client_peer_id
        self.interval = None
        self.min_interval = None
        self.seeders = None
        self.leechers = None
        self.peers = None

        if url.scheme != 'udp':
            raise ValueError('UDPTracker expected UDP protocol')
        self.host = url.hostname
        self.port = url.port

        self.loop = asyncio.get_event_loop() if loop is None else loop

        self.key = random.randint(0, 2**32-1)

    REQUEST_TIMEOUT = 12
    CONNECTION_ID = 0x41727101980
    RESPONSE_HEADER_FORMAT = '!II'
    RESPONSE_HEADER_LEN = struct.calcsize(RESPONSE_HEADER_FORMAT)

    @staticmethod
    def check_response(response, expected_id, expected_action):
        action, transaction_id = struct.unpack_from(UDPTracker.RESPONSE_HEADER_FORMAT, response)

        if transaction_id != expected_id:
            raise ValueError('Unexpected transaction ID')

        action = ActionType(action)
        if action == ActionType.error:
            message = response[UDPTracker.RESPONSE_HEADER_LEN:]
            raise TrackerError(message.decode())
        if action != expected_action:
            raise ValueError('Unexpected action ID (exp {} real {})'.format(expected_action.name, action.name))

    @staticmethod
    def _parse_compact_peers_list(data):
        if len(data) % 6 != 0:
            raise ValueError('Invalid length of a compact representation of peers')
        return list(map(Peer.from_compact_form, grouper(data, 6)))

    async def announce(self, server_port, event):
        
        transport, protocol = await self.loop.create_datagram_endpoint(
            DatagramProtocol, remote_addr=(self.host, self.port))
        try:
            transaction_id = random.randint(0, 2**32-1)
            request = pack(
                'Q', UDPTracker.CONNECTION_ID,
                'I', ActionType.connect.value,
                'I', transaction_id,
            )
            transport.sendto(request)
            response = await protocol.receive()

            UDPTracker.check_response(response, transaction_id, ActionType.connect)
            (conn_id,) = struct.unpack_from('!Q', response, UDPTracker.RESPONSE_HEADER_LEN)
            request = pack(
                'Q', conn_id,
                'I', ActionType.announce.value,
                'I', transaction_id,
                '20s', self.download_info.info_hash,
                '20s', self.peer_id,
                'Q', self.statistics.total_downloaded,
                'Q', self.download_info.bytes_left,
                'Q', self.statistics.total_uploaded,
                'I', event.value,
                'I', 0,
                'I', self.key,
                'i', -1,
                'H', server_port,
            )
            transport.sendto(request)

            response = await asyncio.wait_for(protocol.receive(), UDPTracker.REQUEST_TIMEOUT)
            UDPTracker.check_response(response, transaction_id, ActionType.announce)
            format = '!3I'
            self.interval, self.leechers, self.seeders = struct.unpack_from(
                format, response, UDPTracker.RESPONSE_HEADER_LEN
            )
            self.min_interval = self.interval
            compact_peer_list = response[UDPTracker.RESPONSE_HEADER_LEN + struct.calcsize(format):]
            self.peers = UDPTracker._parse_compact_peers_list(compact_peer_list)
        finally:
            transport.close()


