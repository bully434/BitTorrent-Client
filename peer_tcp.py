import enum
import struct
import asyncio
import bitarray

from math import ceil
from typing import cast
from models import SHA1_DIGEST_LEN, BlockRequest


class PeerMessages(enum.Enum):
    choke = 0
    unchoke = 1
    interested = 2
    not_interested = 3
    have = 4
    bitfield = 5
    request = 6
    piece = 7
    cancel = 8
    port = 9


class SeedError(Exception):
    pass


class PeerTCP:
    def __init__(self, client_peer_id, peer):
        self.peer_id = client_peer_id
        self.peer = peer

        self._am_choking = True
        self._am_interested = False
        self._peer_choking = True
        self._peer_interested = False
        self.download_info = None
        self.file_structure = None
        self.piece_owned = None

        self._downloaded = 0
        self._uploaded = 0

        self._reader = None
        self._writer = None
        self._connected = False

    _handshake_message = b'BitTorrent protocol'
    CONNECT_TIMEOUT = 5
    READ_TIMEOUT = 5
    WRITE_TIMEOUT = 5
    MAX_SILENCE_DURATION = 3 * 60
    MAX_REQUEST_LENGTH = 2 ** 17
    MAX_MESSAGE_LENGTH = 2 ** 18
    HANDSHAKE_DATA = bytes([len(_handshake_message)]) +_handshake_message
    RESERVED_BYTES = b'\0' * 8

    def send_protocol_data(self):
        self._writer.write(PeerTCP.HANDSHAKE_DATA + PeerTCP.RESERVED_BYTES)

    async def receive_protocol_data(self):
        data_len = len(PeerTCP.HANDSHAKE_DATA) + len(PeerTCP.RESERVED_BYTES)

        response = await asyncio.wait_for(self._reader.readexactly(data_len), PeerTCP.READ_TIMEOUT)

        if response[:len(PeerTCP.HANDSHAKE_DATA)] != PeerTCP.HANDSHAKE_DATA:
            raise ValueError('Unknown protocol')

    def populate_info(self, download_info, file_structure):
        self.download_info = download_info
        self.file_structure = file_structure
        self.piece_owned = bitarray.bitarray(download_info.piece_count)
        self.piece_owned.setall(False)

        self._writer.write(self.download_info.info_hash + self.peer_id)

    async def receive_info(self):
        data_len = SHA1_DIGEST_LEN + len(self.peer_id)
        response = await asyncio.wait_for(self._reader.readexactly(data_len), PeerTCP.READ_TIMEOUT)

        actual_info_hash = response[:SHA1_DIGEST_LEN]
        actual_peer_id = response[SHA1_DIGEST_LEN:]

        if self.peer_id == actual_peer_id:
            raise ValueError('Connected to yourself')
        if self.peer.peer_id is not None and self.peer.peer_id != actual_peer_id:
            raise ValueError('Unexpected peer id')
        self.peer.peer_id = actual_peer_id

        return actual_info_hash

    def send_bitfield(self):
        if self.download_info.downloaded_piece_count:
            arr = bitarray.bitarray([info.downloaded for info in self.download_info.pieces], endian='big')
            self.send_message(PeerMessages.bitfield, arr.tobytes())

    async def connect(self, download_info, file_structure):
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.peer.host, self.peer.port), PeerTCP.CONNECT_TIMEOUT)

        self.send_protocol_data()
        self.populate_info(download_info, file_structure)
        await self.receive_protocol_data()

        if await self.receive_info() != download_info.info_hash:
            raise ValueError('hashes do not match')

        self.send_bitfield()
        self._connected = True

    async def accept(self, reader, writer):
        self._reader = reader
        self._writer = writer
        self.send_protocol_data()

        await self.receive_protocol_data()
        return await self.receive_info()

    def confirm_info_hash(self,download_info, file_structure):
        self.populate_info(download_info,file_structure)
        self.send_bitfield()
        self._connected = True

    async def receive_message(self):
        data = await asyncio.wait_for(self._reader.readexactly(4), PeerTCP.MAX_SILENCE_DURATION)
        (length,) = struct.unpack('!I', data)
        if length == 0:  # keep-alive
            return None
        if length > PeerTCP.MAX_MESSAGE_LENGTH:
            raise ValueError('Too large message')

        data = await asyncio.wait_for(self._reader.readexactly(length), PeerTCP.READ_TIMEOUT)
        try:
            message_id = PeerMessages(data[0])
        except ValueError:
            print('Unknown message type {}'.format(data[0]))
            return None
        payload = memoryview(data)[1:]
        return message_id, payload

    KEEP_ALIVE_MESSAGE = b'\0' * 4

    def send_message(self, message_id=None, *payload):
        # keep-alive message
        if message_id is None:
            self._writer.write(PeerTCP.KEEP_ALIVE_MESSAGE)
            return

        length = sum(len(portion) for portion in payload) + 1

        self._writer.write(struct.pack('!IB', length, message_id.value))
        for portion in payload:
            self._writer.write(portion)

    @staticmethod
    def _check_payload_len(message_id, payload, expected_len):
        if len(payload) != expected_len:
            raise ValueError('Invalid payload length on message_id = {} '
                             '(expected {}, got {})'.format(message_id.name, expected_len, len(payload)))

    def handle_setting_states(self, message_id, payload):
        PeerTCP._check_payload_len(message_id, payload, 0)

        if message_id == PeerMessages.choke:
            self._peer_choking = True
        elif message_id == PeerMessages.unchoke:
            self._peer_choking = False
        elif message_id == PeerMessages.interested:
            self._peer_interested = True
        elif message_id == PeerMessages.not_interested:
            self._peer_interested = False

    def mark_owner(self, index):
        self.piece_owned[index] = True
        self.download_info.pieces[index].owners.add(self.peer)
        if index in self.download_info.interesting_pieces:
            self.am_interested = True

    def handle_have(self, message_id, payload):
        if message_id == PeerMessages.have:
            (index,) = struct.unpack('!I', payload)
            self.mark_owner(index)

        elif message_id == PeerMessages.bitfield:
            piece_count = self.download_info.piece_count
            PeerTCP._check_payload_len(message_id, payload, int(ceil(piece_count / 8)))

            arr = bitarray.bitarray(endian='big')
            arr.frombytes(payload.tobytes())
            for i in range(piece_count):
                if arr[i]:
                    self.mark_owner(i)
            for i in range(piece_count, len(arr)):
                if arr[i]:
                    raise ValueError('Spare bits in "bitfield" message must be zero')

    async def handle_requests(self, message_id, payload):
        piece_index, begin, length = struct.unpack('!3I', cast(bytes, payload))
        # TODO : Check position range
        request = BlockRequest(piece_index, begin, length)

        if message_id == PeerMessages.request:
            if length > PeerTCP.MAX_REQUEST_LENGTH:
                raise ValueError('Requested {} bytes, but the current policy allows to accept requests '
                                 'of not more than {} bytes'.format(length, PeerTCP.MAX_REQUEST_LENGTH))
            if (self._am_choking or not self._peer_interested or
                    not self.download_info.pieces[piece_index].downloaded):
                return
            await self._send_block(request)
            await self.drain()

        elif message_id == PeerMessages.cancel:
            pass

    async def handle_block(self, payload):
        if not self._am_interested:
            return
        fmt = '!2I'
        piece_index, block_begin = struct.unpack_from(fmt, payload)
        block_data = memoryview(payload)[struct.calcsize(fmt):]
        block_length = len(block_data)
        request = BlockRequest(piece_index, block_begin, block_length)
        if not block_length:
            return
        async with self.file_structure.lock:
            piece_info = self.download_info.pieces[piece_index]
            if piece_info.validating or piece_info.downloaded:
                return
            self._downloaded += block_length
            self.download_info.session_stats.add_downloaded(self.peer, block_length)

            await self.file_structure.write(piece_index * self.download_info.piece_length + block_begin, block_data,
                                            acquire_lock = False)
            piece_info.mark_downloaded_blocks(self.peer, request)

    @property
    def am_choking(self):
        return self._am_choking

    @property
    def am_interested(self):
        return self._am_interested

    def _check_connect(self):
        if not self._connected:
            raise RuntimeError("Can't change state when the client isn't connected")

    @am_choking.setter
    def am_choking(self, value: bool):
        self._check_connect()
        if self._am_choking != value:
            self._am_choking = value
            self.send_message(PeerMessages.choke if value else PeerMessages.unchoke)

    @am_interested.setter
    def am_interested(self, value: bool):
        self._check_connect()
        if self._am_interested != value:
            self._am_interested = value
            self.send_message(PeerMessages.interested if value else PeerMessages.not_interested)

    @property
    def peer_choking(self):
        return self._peer_choking

    @property
    def peer_interested(self):
        return self._peer_interested

    @property
    def downloaded(self):
        return self._downloaded

    @property
    def uploaded(self):
        return self._uploaded

    async def run(self):
        while True:
            message = await self.receive_message()
            if message is None:
                continue
            message_id, payload = message
            if message_id in (PeerMessages.choke, PeerMessages.unchoke,
                              PeerMessages.interested, PeerMessages.not_interested):
                self.handle_setting_states(message_id, payload)
            elif message_id in (PeerMessages.have, PeerMessages.bitfield):
                self.handle_have(message_id, payload)
            elif message_id in (PeerMessages.request, PeerMessages.cancel):
                await self.handle_requests(message_id, payload)
            elif message_id == PeerMessages.piece:
                await self.handle_block(payload)
            elif message_id == PeerMessages.port:
                PeerTCP._check_payload_len(message_id, payload, 2)
            else:
                print('Unknown message')
                pass

    async def _send_block(self, request):
        block = await self.file_structure.read(request.piece_index * self.download_info.piece_length + request.block_begin, request.block_length)

        self.send_message(PeerMessages.piece, struct.pack('!2I', request.piece_index, request.block_begin), block)

        self._uploaded += request.block_length
        self.download_info.session_stats.add_uploaded(self.peer, request.block_length)

    def send_keep_alive(self):
        self.send_message(None)

    def send_have(self, index):
        self.send_message(PeerMessages.have, struct.pack('!I', index))

    def send_request(self, request, cancel = False):
        if not cancel:
            assert self.peer in self.download_info.pieces[request.piece_index].owners

        self.send_message(PeerMessages.request if not cancel else PeerMessages.cancel, struct.pack('!3I', request.piece_index, request.block_begin, request.block_length))

    async def drain(self):
        await asyncio.wait_for(self._writer.drain(), PeerTCP.WRITE_TIMEOUT)

    def close(self):
        if self._writer is not None:
            self._writer.close()

        self._connected = False
