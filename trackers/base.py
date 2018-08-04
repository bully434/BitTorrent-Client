from enum import Enum
from urllib.parse import urlparse

from models import DownloadInfo, Peer, TorrentInfo


def grouper(arr, group_size):
    return [arr[i:i + group_size] for i in range(0, len(arr), group_size)]


__all__ = ['EventType', 'TrackerError', 'BaseTrackerClient']


class EventType(Enum):
    none = 0
    completed = 1
    started = 2
    stopped = 3


class TrackerError(Exception):
    pass


class BaseTrackerClient:
    def __init__(self, download_info, peer_id):
        self.download_info = download_info
        self.statistics = self.download_info.session_stats

        self.peer_id = peer_id

        self.interval = None
        self.min_interval = None
        self.seeders = None
        self.leechers = None
        self.peers = None

    async def announce(self, server_port, event):
        raise NotImplementedError


def parse_compact_list(data):
    if len(data) % 6 != 0:
        raise ValueError('Invalid length of compact repres of peers')
    return list(map(Peer.from_compact_form, grouper(data, 6)))