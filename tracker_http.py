import re
import aiohttp
import bencoder
import contextlib

from typing import cast
from urllib.parse import urlencode
from urllib.request import urlopen
from collections import OrderedDict
from models import Peer, TorrentInfo, grouper


def humanize_size(size):
    return '{:.1f} Mb'.format(size / TrackerGetRequest.BPMb)

class TrackerGetRequest:
    def __init__(self, torrent_info, client_peer_id):
        if re.match(r'https?://', torrent_info.announce_url) is None:
            raise ValueError('Only HTTP/HTTPS protocols available')

        self.torrent_info = torrent_info
        self.download_info = torrent_info.download_info
        self.peer_id = client_peer_id
        self.statistics = self.download_info.session_stats

        self.interval = None
        self.min_interval = None
        self.seeders = None
        self.leechers = None
        self._tracker_id = None
        self._peers = set()

    REQUEST_TIMEOUT = 5

    @staticmethod
    def _parse_compact_peers_list(data):
        if len(data) % 6 != 0:
            raise ValueError('Invalid length of a compact representation of peers')
        return list(map(Peer.from_compact_form, grouper(data, 6)))

    def handle_tracker_response(self, response):
        if b'failure reason' in response:
            ex = response[b'failure reason'].decode()
            raise Exception(ex)

        if b'warning message' in response:
            print('Tracker returned warning message: {}'.format(response[b'warning message'].decode()))

        self.interval = response[b'interval']
        if b'min interval' in response:
            self.min_interval = response[b'min interval']
            if self.min_interval > self.interval:
                raise ValueError('Tracker returned min_interval > default interval')

        if b'tracker id' in response:
            self._tracker_id = response[b'tracker id']

        if b'complete' in response:
            self.seeders = response[b'complete']

        if b'incomplete' in response:
            self.leechers = response[b'incomplete']

        peers = response[b'peers']
        if isinstance(peers, bytes):
            self._peers = TrackerGetRequest._parse_compact_peers_list(peers)
        else:
            self._peers = list(map(Peer.from_dict, peers))

        print('{} peers, interval {}, min_interval {}'.format(len(self._peers),
                                                              self.interval,
                                                              self.min_interval))

    BPMb = 2 ** 20

    async def announce(self, server_port, event):
        print('announce {} (uploaded {} Mb, downloaded {} Mb, left {} Mb)'.format(
            event,
            humanize_size(self.statistics.uploaded_per_session),
            humanize_size(self.statistics.downloaded_per_session),
            humanize_size(self.download_info.bytes_left)))

        request_parameters = {
            'info_hash': self.download_info.info_hash,
            'peer_id': self.peer_id,
            'port': server_port,
            'uploaded': self.statistics.total_uploaded,
            'downloaded': self.statistics.total_downloaded,
            'left': self.download_info.bytes_left,
            'event': event,
            'compact': 1,
        }
        if event is not None:
            request_parameters['event'] = event

        if self._tracker_id is not None:
            request_parameters['trackerid'] = self._tracker_id

        url = self.torrent_info.announce_url + '?' + urlencode(request_parameters)
        with aiohttp.Timeout(TrackerGetRequest.REQUEST_TIMEOUT):
            with contextlib.closing(urlopen(url)) as connection:
                response = connection.read()
        response = bencoder.decode(response)
        if not response:
            if event == 'started':
                raise ValueError('Empty answer on start announcement')
            return
        response = cast(OrderedDict, response)
        self.handle_tracker_response(response)

    @property
    def peers(self):
        return self._peers

