import aiohttp
import bencoder
import contextlib

from typing import cast
from urllib.parse import urlencode
from urllib.request import urlopen
from collections import OrderedDict
from models import Peer, grouper
from trackers.base import BaseTrackerClient, parse_compact_list, EventType

__all__ = ['HTTPTracker']


def humanize_size(size):
    return '{:.1f} Mb'.format(size / HTTPTracker.BPMb)


class HTTPTracker(BaseTrackerClient):
    def __init__(self, url, download_info, client_peer_id):
        super().__init__(download_info, client_peer_id)
        self.announce_url = url.geturl()
        if url.scheme not in ('http', 'https'):
            raise ValueError('HTTPTracker expected HTTP/HTTPS protocols')

        self._tracker_id = None

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
            self.peers = parse_compact_list(peers)
        else:
            self.peers = list(map(Peer.from_dict, peers))

        print('{} peers, interval {}, min_interval {}'.format(len(self.peers),
                                                              self.interval,
                                                              self.min_interval))

    BPMb = 2 ** 20

    async def announce(self, server_port, event):
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
        if event != EventType.none:
            request_parameters['event'] = event.name

        if self._tracker_id is not None:
            request_parameters['trackerid'] = self._tracker_id

        url = self.announce_url + '?' + urlencode(request_parameters)
        with aiohttp.Timeout(HTTPTracker.REQUEST_TIMEOUT):
            with contextlib.closing(urlopen(url)) as connection:
                response = connection.read()
        response = bencoder.decode(response)
        if not response:
            if event == EventType.started:
                raise ValueError('Empty answer on start announcement')
            return
        response = cast(OrderedDict, response)
        self.handle_tracker_response(response)


