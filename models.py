import random
import socket
import struct
import hashlib
import asyncio
import bencoder
import bitarray
from collections import OrderedDict
from typing import cast
from math import ceil

SHA1_DIGEST_LEN = 20


def grouper(arr, group_size):
    return [arr[i:i + group_size] for i in range(0, len(arr), group_size)]


def generate_peer_id():
    return bytes(random.randint(0, 255) for _ in range(20))


class BlockRequest:
    def __init__(self, piece_index, block_begin, block_length):
        self.piece_index = piece_index
        self.block_begin = block_begin
        self.block_length = block_length

    def __eq__(self, other):
        if not isinstance(other, BlockRequest):
            return False
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash((self.piece_index, self.block_begin, self.block_length))


class BlockRequestFuture(asyncio.Future, BlockRequest):
    def __init__(self, piece_index, block_begin, block_length):
        asyncio.Future.__init__(self)
        BlockRequest.__init__(self, piece_index, block_begin,block_length)

        self.prev_performer = set()
        self.performer = None

    __eq__ = asyncio.Future.__eq__
    __hash__ = asyncio.Future.__hash__

class Peer:
    def __init__(self, host, port, peer_id=None):
        self._host = host
        self._port = port
        self.peer_id = peer_id
        self._hash = hash((host, port))

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    def __eq__(self, other):
        if not isinstance(other, Peer):
            return False
        return self._host == other._host and self._port == other._port

    def __hash__(self):
        return self._hash

    def __repr__(self):
        return '{}:{}'.format(self._host, self._port)

    @classmethod
    def from_dict(cls, dictionary):
        return cls(dictionary[b'ip'].decode(), dictionary[b'port'], dictionary.get(b'peer id'))

    @classmethod
    def from_compact_form(cls, data):
        ip, port = struct.unpack('!4sH', data)
        host = socket.inet_ntoa(ip)
        return cls(host, port)


class FileInfo:
    def __init__(self, length, path, md5sum=None):
        self.length = length
        self.path = path
        self.md5sum = md5sum

    @classmethod
    def get_info(self, dictionary):
        if b'path' in dictionary:
            path = list(map(bytes.decode, dictionary[b'path']))
        else:
            path = []

        return self(dictionary[b'length'], path, md5sum=dictionary.get(b'md5sum'))


class DownloadInfo:
    PIECE_SIZE = 2 ** 10
    DISTRUST_RATE_TO_BAN = 5

    def __init__(self, info_hash, piece_length, piece_hashes, suggested_name, files, private=False):
        self.info_hash = info_hash
        self.piece_length = piece_length
        self.piece_hashes = piece_hashes
        self.suggested_name = suggested_name
        self.files = files
        self.private = private
        self.host_distrust_rates = {}

        piece_count = len(piece_hashes)
        if ceil(self.total_size / piece_length) != piece_count:
            raise ValueError('Invalid count of hashes')
        self.piece_sources = [set() for _ in range(piece_count)]
        self.piece_downloaded = bitarray.bitarray(piece_count, endian='big')
        self.piece_downloaded.setall(False)
        self.downloaded_piece_count = 0
        self.interesting_pieces = set()

        blocks_count = ceil(piece_length / DownloadInfo.PIECE_SIZE)
        self.piece_blocks_downloaded = [None] * piece_count * blocks_count

        self.piece_owners = None
        self.piece_validating = None
        self.interesting_pieces = None
        self.piece_blocks_expected = None
        self.total_downloaded = None
        self.total_uploaded = None
        self.reset_run_state()

    def reset_run_state(self):
        self.piece_owners = [set() for _ in range(self.piece_count)]
        self.piece_validating = bitarray.bitarray(self.piece_count)
        self.piece_validating.setall(False)
        self.interesting_pieces = set()
        self.piece_blocks_expected = [set() for _ in range(self.piece_count)]
        self.total_uploaded = 0
        self.total_downloaded = 0

    @classmethod
    def get_download_info(cls, dictionary):
        info_hash = hashlib.sha1(bencoder.encode(dictionary)).digest()

        if len(dictionary[b'pieces']) % SHA1_DIGEST_LEN != 0:
            raise ValueError('Invalid length of "pieces" string')
        piece_hashes = grouper(dictionary[b'pieces'], SHA1_DIGEST_LEN)

        if b'files' in dictionary:
            files = list(map(FileInfo.get_info, dictionary[b'files']))
        else:
            files = [FileInfo.get_info(dictionary)]

        return cls(info_hash, dictionary[b'piece length'], piece_hashes,
                   dictionary[b'name'].decode(), files, private=dictionary.get('private', False))

    @property
    def is_complete(self):
        return self.piece_downloaded == self.piece_count

    @property
    def piece_count(self):
        return len(self.piece_hashes)

    @property
    def bytes_left(self):
        result = (self.piece_count - self.downloaded_piece_count) * self.piece_length
        last_index = self.piece_count - 1
        if not self.piece_downloaded[last_index]:
            result += self.get_piece_length(last_index) - self.piece_length
        return result

    @property
    def total_size(self):
        return sum(file.length for file in self.files)

    def get_piece_length(self, index):
        if index == self.piece_count - 1:
            return self.total_size - self.piece_length * (self.piece_count - 1)
        else:
            return self.piece_length

    def reset_piece(self, index):
        self.piece_downloaded[index] = False
        self.piece_sources[index] = set()
        self.piece_blocks_downloaded[index] = None
        self.piece_blocks_expected[index] = set()

    def mark_piece_downloaded(self, index):
        if self.piece_downloaded[index]:
            raise ValueError('The piece is already downloaded')

        self.piece_downloaded[index] = True
        self.downloaded_piece_count += 1

        # Delete data structures for this piece to save memory
        self.piece_sources[index] = None
        self.piece_blocks_downloaded[index] = None
        self.piece_blocks_expected[index] = None

    def is_all_piece_blocks_downloaded(self, index):
        if self.piece_downloaded[index]:
            raise ValueError('Piece was marked as downloaded')
        return self.piece_blocks_downloaded is not None and self.piece_blocks_expected[index].all()

    def mark_downloaded_blocks(self, source, request):
        if self.piece_downloaded[request.piece_index]:
            raise ValueError('Piece Already Downloaded')
        piece_length = self.get_piece_length(request.piece_index)

        arr = self.piece_blocks_downloaded[request.piece_index]
        if arr is None:
            arr = bitarray.bitarray(ceil(piece_length / DownloadInfo.PIECE_SIZE))
            arr.setall(False)
            self.piece_blocks_downloaded[request.piece_index] = arr

        mark_begin = ceil(request.block_begin / DownloadInfo.PIECE_SIZE)
        if request.block_begin + request.block_length == piece_length:
            mark_end = len(arr)
        else:
            mark_end = (request.block_begin + request.block_length) // DownloadInfo.PIECE_SIZE
        arr[mark_begin:mark_end] = True

        blocks_expected = self.piece_blocks_expected[request.piece_index]
        downloaded = []

        for future in blocks_expected:
            query_begin = future.block_begin // DownloadInfo.PIECE_SIZE
            query_end = ceil((future.block_begin + future.block_length) / DownloadInfo.PIECE_SIZE)
            if arr[query_begin:query_end].all():
                downloaded.append(future)
                future.set_result(source)
        for future in downloaded:
            blocks_expected.remove(future)

    def increase_distrust(self, peer):
        self.host_distrust_rates[peer.host] = self.host_distrust_rates.get(peer.host, 0) + 1

    def is_banned(self, peer):
        return (peer.host in self.host_distrust_rates and
                self.host_distrust_rates[peer.host] >= DownloadInfo.DISTRUST_RATE_TO_BAN)


class TorrentInfo:
    def __init__(self, download_info, announce_url, *, download_dir):
        self.download_info = download_info
        self.announce_url = announce_url
        self.download_dir = download_dir
        self.paused = False

    @classmethod
    def get_info(self, file, **kwargs):
        dictionary = cast(OrderedDict, bencoder.decode(file))
        download_info = DownloadInfo.get_download_info(dictionary[b'info'])
        announce = dictionary[b'announce'].decode()
        return self(download_info, announce, **kwargs)

