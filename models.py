import time
import copy
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

class PieceInfo:
    def __init__(self, piece_hash, length):
        self.piece_hash = piece_hash
        self.length = length

        self.selected = True
        self.owners = set()
        self.validating = False

        self.downloaded = None
        self.sources = None
        self.block_downloaded = None
        self.blocks_expected = None
        self.reset_content()

    def reset_content(self):
        self.downloaded = False
        self.sources = set()

        self.block_downloaded = None
        self.blocks_expected = set()

    def reset_run_state(self):
        self.owners = set()
        self.validating = False
        self.blocks_expected = set()

    def mark_downloaded_blocks(self, source, request):
        if self.downloaded:
            raise ValueError('Piece Already Downloaded')
        self.sources.add(source)

        arr = self.block_downloaded
        if arr is None:
            arr = bitarray.bitarray(ceil(self.length / DownloadInfo.PIECE_SIZE))
            arr.setall(False)
            self.block_downloaded = arr

        mark_begin = ceil(request.block_begin / DownloadInfo.PIECE_SIZE)
        if request.block_begin + request.block_length == self.length:
            mark_end = len(arr)
        else:
            mark_end = (request.block_begin + request.block_length) // DownloadInfo.PIECE_SIZE
        arr[mark_begin:mark_end] = True

        blocks_expected = self.blocks_expected
        downloaded = []

        for future in blocks_expected:
            query_begin = future.block_begin // DownloadInfo.PIECE_SIZE
            query_end = ceil((future.block_begin + future.block_length) / DownloadInfo.PIECE_SIZE)
            if arr[query_begin:query_end].all():
                downloaded.append(future)
                future.set_result(source)
        for future in downloaded:
            blocks_expected.remove(future)

    def are_all_blocks_downloaded(self):
        return self.downloaded or(self.block_downloaded is not None and self.block_downloaded.all())

    def mark_as_downloaded(self):
        if self.downloaded:
            raise ValueError('Piece is already downloaded')
        self.downloaded = True

        self.sources = None
        self.block_downloaded = None
        self.blocks_expected = None


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
    def __init__(self, length, path, *, md5sum=None):
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


class SessionStat:
    def __init__(self, previous_stats):
        self.peer_count = 0
        self.peer_last_download = {}
        self.peer_last_upload = {}
        self.downloaded_per_session = 0
        self.uploaded_per_session = 0
        self.download_speed = None
        self.upload_speed = None

        if previous_stats is not None:
            self.total_downloaded = previous_stats.total_downloaded
            self.total_uploaded = previous_stats.total_uploaded
        else:
            self.total_downloaded = 0
            self.total_uploaded = 0

    PEER_CONSIDERATION_TIME = 10

    @staticmethod
    def get_peer_count(time_dict):
        cur_time = time.time()
        return sum(1 for t in time_dict.values() if cur_time - t <= DownloadInfo.PEER_CONSIDERATION_TIME)

    @property
    def downloading_peer_count(self):
        return SessionStat.get_peer_count(self.peer_last_download)

    @property
    def uploading_peer_count(self):
        return SessionStat.get_peer_count(self.peer_last_upload)

    def add_downloaded(self, peer, size):
        self.peer_last_download[peer] = time.time()
        self.downloaded_per_session += size
        self.total_downloaded += size

    def add_uploaded(self, peer, size):
        self.peer_last_upload[peer] = time.time()
        self.uploaded_per_session += size
        self.total_uploaded += size


class DownloadInfo:
    PIECE_SIZE = 2 ** 10
    DISTRUST_RATE_TO_BAN = 5

    def __init__(self, info_hash, piece_length, piece_hashes, suggested_name, files, private=False):
        self.info_hash = info_hash
        self.piece_length = piece_length
        self.suggested_name = suggested_name
        self.session_stats = SessionStat(None)
        self.files = files
        self.private = private
        self.host_distrust_rates = {}

        self.pieces = [PieceInfo(item, piece_length) for item in piece_hashes[:-1]]
        last_piece_length = self.total_size - (len(piece_hashes ) -1) * self.piece_length
        self.pieces.append(PieceInfo(piece_hashes[-1], last_piece_length))

        piece_count = len(piece_hashes)
        if ceil(self.total_size / piece_length) != piece_count:
            raise ValueError('Invalid count of hashes')
        self.downloaded_piece_count = 0
        self.interesting_pieces = None
        self._complete = False

    def reset_run_state(self):
        self.pieces = [copy.copy(info) for info in self.pieces]
        for info in self.pieces:
            info.reset_run_state()
        self.interesting_pieces = set()

    def reset_stats(self):
        self.session_stats = SessionStat(None)

    PEER_CONSIDERATION_TIME = 10

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
    def piece_count(self):
        return len(self.pieces)

    @property
    def bytes_left(self):
        result = (self.piece_count - self.downloaded_piece_count) * self.piece_length
        last_index = self.piece_count - 1
        if not self.pieces[last_index].downloaded:
            result += self.pieces[last_index].length - self.piece_length
        return result

    @property
    def total_size(self):
        return sum(file.length for file in self.files)

    def get_piece_length(self, index):
        if index == self.piece_count - 1:
            return self.total_size - self.piece_length * (self.piece_count - 1)
        else:
            return self.piece_length

    @property
    def complete(self):
        return self._complete

    @complete.setter
    def complete(self, value):
        if value:
            assert all(info.downloaded or not info.selected for info in self.pieces)
        self.complete = value

    def increase_distrust(self, peer):
        self.host_distrust_rates[peer.host] = self.host_distrust_rates.get(peer.host, 0) + 1

    def is_banned(self, peer):
        return (peer.host in self.host_distrust_rates and
                self.host_distrust_rates[peer.host] >= DownloadInfo.DISTRUST_RATE_TO_BAN)


class TorrentInfo:
    def __init__(self, download_info, announce_list, *, download_dir):
        self.download_info = download_info
        self.announce_list = announce_list
        self.download_dir = download_dir
        self.paused = False

    @classmethod
    def get_info(self, file, **kwargs):
        dictionary = cast(OrderedDict, bencoder.decode(file))
        download_info = DownloadInfo.get_download_info(dictionary[b'info'])
        if b'announce-list' in dictionary:
            announce_list = [[url.decode() for url in tier]
                             for tier in dictionary[b'announce-list']]
        else:
            announce_list = [[dictionary[b'announce'].decode()]]

        return self(download_info, announce_list, **kwargs)

